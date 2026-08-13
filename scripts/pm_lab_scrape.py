#!/usr/bin/env python3
"""Owner PM lab taramaları (fotoğraf yok) — Mac Firefox → Railway ingest.

  .venv/bin/python scripts/pm_lab_scrape.py --sync --ingest
  .venv/bin/python scripts/pm_lab_scrape.py --jobs serp,competitors --ingest
"""
from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


_load_dotenv()

from backend.services.scrape_browser import google_profile_dir, launch_ephemeral, launch_persistent  # noqa: E402

INGEST_URL = (
    os.environ.get("PM_LAB_INGEST_URL")
    or os.environ.get("PLAY_CONSOLE_INGEST_URL", "").replace("play-console", "pm-lab")
    or "https://projectcontrol.up.railway.app/api/pm-lab/ingest"
).strip()

from backend.services.pm_lab_store import (  # noqa: E402
    SERP_KEYWORDS,
    SERP_BATCH_COUNT,
    serp_keyword_batches,
    serp_keywords_for_batch,
)
SERP_PAGES = 4
SERP_PAGE_SLEEP_SEC = float(os.environ.get("PM_LAB_SERP_PAGE_SLEEP_SEC") or "3.5")
SERP_KEYWORD_SLEEP_SEC = float(os.environ.get("PM_LAB_SERP_KEYWORD_SLEEP_SEC") or "5.0")
SERP_BATCH_GAP_SEC = int(os.environ.get("PM_LAB_SERP_BATCH_GAP_SEC") or str(15 * 60))

NEWS_KEYWORDS = (
    "dolar",
    "altın",
    "gram altın",
    "çeyrek altın",
    "kripto para",
    "ons altın",
    "bitcoin",
    "benzin",
    "motorin",
    "akaryakıt",
)

ASSETS = (
    {"id": "usd", "label": "Dolar"},
    {"id": "eur", "label": "Euro"},
    {"id": "gram_altin", "label": "Gram Altın"},
    {"id": "ceyrek_altin", "label": "Çeyrek Altın"},
    {"id": "ons_altin", "label": "Ons Altın"},
    {"id": "gram_gumus", "label": "Gram Gümüş"},
    {"id": "bitcoin", "label": "Bitcoin"},
    {"id": "brent", "label": "Brent Petrol"},
    {"id": "bist100", "label": "BIST 100"},
)

SITES = (
    {"id": "doviz", "label": "Döviz", "home": "https://www.doviz.com/"},
    {"id": "tradingview", "label": "Trading", "home": "https://www.tradingview.com/"},
    {"id": "canlidoviz", "label": "Canlı Döviz", "home": "https://canlidoviz.com/"},
    {"id": "foreks", "label": "Foreks", "home": "https://www.foreks.com/"},
    {"id": "investing", "label": "Investing", "home": "https://www.investing.com/"},
    {"id": "bigpara", "label": "Bigpara", "home": "https://bigpara.hurriyet.com.tr/"},
    {"id": "uzmanpara", "label": "Uzmanpara", "home": "https://uzmanpara.milliyet.com.tr/"},
    {"id": "bloomberght", "label": "Bloomberg", "home": "https://www.bloomberght.com/"},
    {"id": "cnbce", "label": "CNBC-e", "home": "https://www.cnbce.com/"},
    {"id": "cnnturk", "label": "CNN", "home": "https://finans.cnnturk.com/"},
    {"id": "enuygun", "label": "Enuygun", "home": "https://www.enuygunfinans.com/"},
    {"id": "paratic", "label": "Paratic", "home": "https://piyasa.paratic.com/"},
)

# Ana sayfada yoksa bu liste/vitrin sayfalarından çek (kullanıcının verdiği URL’ler).
SITE_LIST_URLS: dict[str, tuple[str, ...]] = {
    "enuygun": (
        "https://www.enuygunfinans.com/doviz-fiyatlari/",
        "https://www.enuygunfinans.com/altin-fiyatlari/",
        "https://www.enuygunfinans.com/borsa/bist-100-hisseleri/",
    ),
    "foreks": (
        "https://www.foreks.com/doviz/",
        "https://www.foreks.com/altin/",
        "https://www.foreks.com/emtia/",
    ),
    "investing": (
        "https://www.investing.com/currencies/",
        "https://tr.investing.com/",
    ),
    "bigpara": (
        "https://bigpara.hurriyet.com.tr/doviz/",
        "https://bigpara.hurriyet.com.tr/altin/",
        "https://bigpara.hurriyet.com.tr/borsa/",
        "https://bigpara.hurriyet.com.tr/borsa/endeksler/",
        "https://bigpara.hurriyet.com.tr/emtia/",
        "https://bigpara.hurriyet.com.tr/kripto/kripto-para-piyasasi/",
    ),
    "tradingview": (
        "https://www.tradingview.com/markets/turkey/",
        "https://www.tradingview.com/markets/currencies/rates-middle-east/",
    ),
    "cnbce": (
        "https://www.cnbce.com/piyasalar",
        "https://www.cnbce.com/doviz",
        "https://www.cnbce.com/altin",
        "https://www.cnbce.com/emtia",
        "https://www.cnbce.com/kripto",
        "https://www.cnbce.com/kripto/bitcoin",
    ),
    "cnnturk": (
        "https://finans.cnnturk.com/canli-borsa",
        "https://finans.cnnturk.com/bitcoin",
    ),
    "bloomberght": (
        "https://www.bloomberght.com/piyasalar",
        "https://www.bloomberght.com/doviz",
        "https://www.bloomberght.com/emtia",
        "https://www.bloomberght.com/altin",
        "https://www.bloomberght.com/doviz/bitcoin",
        "https://www.bloomberght.com/kripto",
    ),
    "paratic": (
        "https://piyasa.paratic.com/doviz/dolar/",
        "https://piyasa.paratic.com/doviz/euro/",
        "https://piyasa.paratic.com/borsa/",
        "https://piyasa.paratic.com/altin/gram/",
        "https://piyasa.paratic.com/altin/ons/",
        "https://piyasa.paratic.com/altin/ceyrek/",
        "https://piyasa.paratic.com/forex/emtia/brent-petrol/",
        "https://piyasa.paratic.com/forex/emtia/gumus-gram/",
        "https://piyasa.paratic.com/kripto-coin/bitcoin/",
    ),
    "uzmanpara": (
        "https://uzmanpara.milliyet.com.tr/doviz/",
        "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/",
        "https://uzmanpara.milliyet.com.tr/kripto-paralar/",
        "https://uzmanpara.milliyet.com.tr/kripto-paralar/bitcoin/",
    ),
}

# TradingView scanner (HTTP) — JS ticker şeridine gerek yok.
TV_SCANNER_SYMBOLS: dict[str, str] = {
    "usd": "FX_IDC:USDTRY",
    "eur": "FX_IDC:EURTRY",
    "bist100": "BIST:XU100",
    "ons_altin": "OANDA:XAUUSD",
    "brent": "NYMEX:BZ1!",
    "bitcoin": "BITSTAMP:BTCUSD",
}

ASSET_URLS: dict[str, dict[str, str]] = {
    "doviz": {
        "usd": "https://kur.doviz.com/serbest-piyasa/amerikan-dolari",
        "eur": "https://kur.doviz.com/serbest-piyasa/euro",
        "gram_altin": "https://altin.doviz.com/gram-altin",
        "gram_gumus": "https://altin.doviz.com/gumus",
        "ons_altin": "https://altin.doviz.com/ons",
        "ceyrek_altin": "https://altin.doviz.com/ceyrek-altin",
        "brent": "https://www.doviz.com/emtia/brent-petrol",
        "bist100": "https://borsa.doviz.com/endeksler/xu100",
        "bitcoin": "https://www.doviz.com/kripto-paralar/bitcoin",
    },
    "tradingview": {
        "usd": "https://www.tradingview.com/symbols/USDTRY/?exchange=FX_IDC",
        "eur": "https://www.tradingview.com/symbols/EURTRY/?exchange=FX_IDC",
        "bist100": "https://www.tradingview.com/symbols/BIST-XU100/",
        "ons_altin": "https://www.tradingview.com/symbols/XAUUSD/",
        "brent": "https://www.tradingview.com/symbols/TVC-UKOIL/",
        "gram_gumus": "https://tr.tradingview.com/symbols/XAGTRYG/",
        "gram_altin": "https://tr.tradingview.com/symbols/XAUTRYG/",
        "bitcoin": "https://www.tradingview.com/symbols/BTCUSD/",
    },
    "foreks": {
        "usd": "https://www.foreks.com/doviz/",
        "eur": "https://www.foreks.com/doviz/",
        "bist100": "https://www.foreks.com/",
        "gram_altin": "https://www.foreks.com/altin/",
        "ceyrek_altin": "https://www.foreks.com/altin/",
        "ons_altin": "https://www.foreks.com/altin/",
        "gram_gumus": "https://www.foreks.com/emtia/",
        "brent": "https://www.foreks.com/emtia/",
        "bitcoin": "https://www.foreks.com/bitcoin/",
    },
    "canlidoviz": {
        "usd": "https://canlidoviz.com/doviz-kurlari/dolar",
        "eur": "https://canlidoviz.com/doviz-kurlari/euro",
        "gram_altin": "https://canlidoviz.com/altin-fiyatlari/gram-altin",
        "gram_gumus": "https://canlidoviz.com/altin-fiyatlari/gumus",
        "ons_altin": "https://canlidoviz.com/altin-fiyatlari/ons-altin",
        "ceyrek_altin": "https://canlidoviz.com/altin-fiyatlari/ceyrek-altin",
        "brent": "https://canlidoviz.com/emtia-fiyatlari/brent-petrol",
        "bist100": "https://canlidoviz.com/endeks/bist-100",
        "bitcoin": "https://canlidoviz.com/kripto-paralar/bitcoin",
    },
    "investing": {
        "usd": "https://www.investing.com/currencies/usd-try",
        "eur": "https://www.investing.com/currencies/eur-try",
        "bist100": "https://www.investing.com/indices/ise-100",
        "ons_altin": "https://www.investing.com/currencies/xau-usd",
        "brent": "https://www.investing.com/commodities/brent-oil",
        "gram_gumus": "https://tr.investing.com/currencies/xagg-try",
        "gram_altin": "https://tr.investing.com/currencies/gau-try",
        "ceyrek_altin": "https://tr.investing.com/commodities/turkey-gold-quarter",
        "bitcoin": "https://www.investing.com/crypto/bitcoin",
    },
    "bigpara": {
        "usd": "https://bigpara.hurriyet.com.tr/doviz/dolar/",
        "eur": "https://bigpara.hurriyet.com.tr/doviz/euro/",
        "gram_altin": "https://bigpara.hurriyet.com.tr/altin/gram-altin-fiyati/",
        "ceyrek_altin": "https://bigpara.hurriyet.com.tr/altin/ceyrek-altin-fiyati/",
        "ons_altin": "https://bigpara.hurriyet.com.tr/altin/altin-ons-fiyati/",
        "bist100": "https://bigpara.hurriyet.com.tr/borsa/endeksler/",
        "gram_gumus": "https://bigpara.hurriyet.com.tr/altin/",
        "brent": "https://bigpara.hurriyet.com.tr/emtia/",
        "bitcoin": "https://bigpara.hurriyet.com.tr/kripto/kripto-para-piyasasi/",
    },
    "uzmanpara": {
        "usd": "https://uzmanpara.milliyet.com.tr/doviz/",
        "eur": "https://uzmanpara.milliyet.com.tr/doviz/",
        "gram_altin": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/",
        "ceyrek_altin": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/",
        "bist100": "https://uzmanpara.milliyet.com.tr/",
        "gram_gumus": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/",
        "ons_altin": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/",
        "brent": "https://uzmanpara.milliyet.com.tr/",
        "bitcoin": "https://uzmanpara.milliyet.com.tr/kripto-paralar/bitcoin/",
    },
    "bloomberght": {
        "usd": "https://www.bloomberght.com/doviz",
        "eur": "https://www.bloomberght.com/doviz",
        "gram_altin": "https://www.bloomberght.com/piyasalar",
        "bist100": "https://www.bloomberght.com/piyasalar",
        "ons_altin": "https://www.bloomberght.com/piyasalar",
        "brent": "https://www.bloomberght.com/emtia",
        "ceyrek_altin": "https://www.bloomberght.com/altin",
        "gram_gumus": "https://www.bloomberght.com/altin",
        "bitcoin": "https://www.bloomberght.com/doviz/bitcoin",
    },
    "cnbce": {
        "usd": "https://www.cnbce.com/doviz",
        "eur": "https://www.cnbce.com/doviz",
        "bist100": "https://www.cnbce.com/piyasalar",
        "ons_altin": "https://www.cnbce.com/altin",
        "gram_altin": "https://www.cnbce.com/altin",
        "brent": "https://www.cnbce.com/emtia",
        "ceyrek_altin": "https://www.cnbce.com/altin",
        "gram_gumus": "https://www.cnbce.com/emtia",
        "bitcoin": "https://www.cnbce.com/kripto/bitcoin",
    },
    "cnnturk": {
        "usd": "https://finans.cnnturk.com/",
        "eur": "https://finans.cnnturk.com/",
        "gram_altin": "https://finans.cnnturk.com/",
        "bist100": "https://finans.cnnturk.com/",
        "ons_altin": "https://finans.cnnturk.com/",
        "ceyrek_altin": "https://finans.cnnturk.com/",
        "gram_gumus": "https://finans.cnnturk.com/altin",
        "brent": "https://finans.cnnturk.com/",
        "bitcoin": "https://finans.cnnturk.com/bitcoin",
    },
    "enuygun": {
        "usd": "https://www.enuygunfinans.com/doviz-fiyatlari/",
        "eur": "https://www.enuygunfinans.com/doviz-fiyatlari/",
        "gram_altin": "https://www.enuygunfinans.com/altin-fiyatlari/",
        "ceyrek_altin": "https://www.enuygunfinans.com/altin-fiyatlari/",
        "ons_altin": "https://www.enuygunfinans.com/altin-fiyatlari/",
        "gram_gumus": "https://www.enuygunfinans.com/altin-fiyatlari/",
        "bist100": "https://www.enuygunfinans.com/borsa/bist-100-hisseleri/",
    },
    "paratic": {
        "usd": "https://piyasa.paratic.com/doviz/dolar/",
        "eur": "https://piyasa.paratic.com/doviz/euro/",
        "bist100": "https://piyasa.paratic.com/borsa/",
        "gram_altin": "https://piyasa.paratic.com/altin/gram/",
        "ons_altin": "https://piyasa.paratic.com/altin/ons/",
        "ceyrek_altin": "https://piyasa.paratic.com/altin/ceyrek/",
        "brent": "https://piyasa.paratic.com/forex/emtia/brent-petrol/",
        "gram_gumus": "https://piyasa.paratic.com/forex/emtia/gumus-gram/",
        "bitcoin": "https://piyasa.paratic.com/kripto-coin/bitcoin/",
    },
}

ASSET_LABELS: dict[str, tuple[str, ...]] = {
    "usd": (
        "usdtry",
        "usd/try",
        "usd try",
        "usd to try",
        "u.s. dollar / turkish",
        "amerikan dolari",
        "abd dolari",
        "dolar kuru",
        "dolar",
        "usd",
    ),
    "eur": ("eurtry", "eur/try", "eur try", "eur to try", "euro", "avro"),
    "bist100": ("bist 100", "bist100", "xu100", "bist-100"),
    "gram_altin": ("gram altin", "ga altin", "xautryg", "xautry", "gau-try", "altin (tl/gr)", "spot altin", "gldgr", "altin"),
    "gram_gumus": ("gram gumus", "gumus gram", "ga gumus", "xagtryg", "xagg-try", "xagg", "gumus (tl/gr)", "sxaggr", "gumus"),
    "ons_altin": ("ons altin", "altin/ons", "altin ons", "altin (ons)", "xauusd", "gold ounce", "altin ($/ons)"),
    "brent": ("brent petrol", "brent", "ukoil", "petrol"),
    "ceyrek_altin": ("ceyrek altin", "ceyrek", "sgldc"),
    "bitcoin": ("btcusd", "btc/usd", "btc usd", "bitcoin (btc)", "bitcoin", "btc"),
}

_ASSET_MATCH_ORDER = (
    "bitcoin",
    "ceyrek_altin",
    "gram_gumus",
    "gram_altin",
    "ons_altin",
    "bist100",
    "brent",
    "eur",
    "usd",
)

ASSET_RANGES: dict[str, tuple[float, float]] = {
    "usd": (25.0, 90.0),
    "eur": (30.0, 110.0),
    "bist100": (5000.0, 30000.0),
    "gram_altin": (4000.0, 9000.0),
    "gram_gumus": (80.0, 160.0),
    "ons_altin": (1500.0, 10000.0),
    "brent": (30.0, 250.0),
    "ceyrek_altin": (3000.0, 40000.0),
    "bitcoin": (25000.0, 150000.0),
}

ASSET_LINE_EXCLUDE: dict[str, tuple[str, ...]] = {
    "gram_altin": ("harem", "kapalicarsi", "kapali carsi", "ons", "ceyrek", "senaryo"),
    "usd": ("harem", "kanada", "avustralya", "endeks", "jpy", "yen", "yuan", "bitcoin", "banka", "bank"),
    "eur": ("harem", "banka", "bank"),
    "ceyrek_altin": ("harem", "bist", "xu100"),
    "gram_gumus": ("harem", "petrol", "brent", "bitcoin", "btc"),
    "ons_altin": ("gram", "senaryo"),
    "brent": ("benzin", "bitcoin", "gumus", "silver", "ham petrol", "wti"),
    "bist100": ("hisse",),
    "bitcoin": ("ethereum", "wrapped", "btctry"),
}

PLAY_PACKAGE = "com.Doviz"
IOS_APP_ID = "465599322"
IOS_FINANCE_GENRE = 6015
OUR_HOSTS = ("doviz.com",)

JOB_IDS = ("serp", "competitors", "store_charts", "google_news")

_NUM_RE = re.compile(
    r"\$?\s*("
    r"\d{1,3}(?:\.\d{3})+,\d{1,4}"
    r"|\d{1,3}(?:,\d{3})+\.\d{1,4}"
    r"|\d+[.,]\d{2,6}"
    r"|\d{1,3}(?:\.\d{3})+"
    r")"
)
_CHANGE_RE = re.compile(r"%\s*-?\d+[.,]?\d*")
_FOLD_TABLE = str.maketrans("ıİşŞğĞüÜöÖçÇ", "iissgguuoooc")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ingest_token() -> str:
    return (
        os.environ.get("PM_LAB_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("PLAY_CONSOLE_INGEST_TOKEN")
        or os.environ.get("BRIDGE_INGEST_TOKEN")
        or ""
    ).strip()


def post_ingest(sections: dict[str, Any], *, message: str = "") -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    body = {
        "sections": sections,
        "scraped_at": _now(),
        "source": "pm_lab_scrape",
        "sync_ok": True,
        "sync_message": message[:512],
        "replace": False,
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Notification-Ingest-Token": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"ok": True}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:240]}


def _dismiss_consent(page: Any) -> None:
    for name in ("Tümünü kabul et", "Accept all", "I agree", "Accept", "Kabul et", "Agree"):
        try:
            loc = page.get_by_role("button", name=name)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=2000)
                page.wait_for_timeout(400)
                return
        except Exception:
            continue


def _goto(page: Any, url: str, *, timeout: int = 60_000) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
    page.wait_for_timeout(700)
    _dismiss_consent(page)
    page.wait_for_timeout(300)


def _text(page: Any, limit: int = 12000) -> str:
    try:
        return (page.inner_text("body") or "")[:limit]
    except Exception:
        return ""


def _domain_of(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""
    return host.removeprefix("www.")


def _is_our_host(host: str) -> bool:
    h = (host or "").lower().removeprefix("www.")
    for ours in OUR_HOSTS:
        if h == ours or h.endswith("." + ours):
            return True
    return False


def _extract_serp(page: Any) -> dict[str, Any]:
    data = page.evaluate(
        """() => {
          const organic = [];
          const seen = new Set();
          const skipHref = (href) => {
            if (!href || href.startsWith('javascript:')) return true;
            const low = href.toLowerCase();
            return low.includes('google.com/search') || low.includes('webcache') || low.includes('accounts.google');
          };
          const snippetFrom = (block, title) => {
            if (!block) return '';
            const t = (block.innerText || '').split('\\n').map((x) => x.trim()).filter(Boolean);
            const rest = t.filter((line) => line !== title);
            return rest.slice(0, 4).join(' ').slice(0, 320);
          };
          const push = (a, h3) => {
            if (!a || !a.href || skipHref(a.href) || seen.has(a.href)) return;
            seen.add(a.href);
            const title = ((h3 && h3.innerText) || a.innerText || '').trim();
            if (!title) return;
            let host = '';
            try { host = new URL(a.href).hostname.replace(/^www\\./, ''); } catch (e) {}
            const block =
              (h3 && h3.closest('div[data-sokoban-container], div.g, div.Gx5Zad, div.MjjYud, div[data-hveid], div.tF2Cxc')) ||
              a.closest('div[data-sokoban-container], div.g, div.Gx5Zad, div.MjjYud, div[data-hveid], div.tF2Cxc') ||
              a.parentElement;
            organic.push({
              rank: organic.length + 1,
              title,
              url: a.href,
              domain: host,
              snippet: snippetFrom(block, title)
            });
          };
          document.querySelectorAll('#search a h3, #rso a h3, div.g a h3, div[data-sokoban-container] a h3, div.MjjYud a h3, div.Gx5Zad a h3, div.tF2Cxc a h3, div.WZkRb a h3').forEach((h3) => {
            push(h3.closest('a'), h3);
          });
          if (!organic.length) {
            document.querySelectorAll('#search a[href^="http"], #rso a[href^="http"], div[data-sokoban-container] a[href^="http"]').forEach((a) => {
              if (a.querySelector('h3')) return;
              push(a, a.querySelector('h3, [role="heading"]'));
            });
          }
          if (!organic.length) {
            document.querySelectorAll('div#rso div[data-hveid] a[href^="http"], main a[href^="http"]').forEach((a) => {
              push(a, a.querySelector('h3, [role="heading"]'));
            });
          }
          const paa = [];
          document.querySelectorAll('div[jsname] span, div[role="button"] span').forEach((el) => {
            const t = (el.innerText || '').trim();
            if (t.endsWith('?') && t.length > 12 && t.length < 140 && !paa.includes(t)) paa.push(t);
          });
          return { organic, paa: paa.slice(0, 8) };
        }"""
    )
    return data if isinstance(data, dict) else {}


def _google_blocked(page: Any) -> bool:
    try:
        body = (page.inner_text("body") or "").lower()[:5000]
    except Exception:
        return False
    markers = (
        "unusual traffic",
        "automated queries",
        "captcha",
        "recaptcha",
        "olağandışı trafik",
        "robot olmadığınız",
        "robot değil",
        "/sorry/",
    )
    return any(m in body for m in markers)


def _load_serp_page(page: Any, url: str) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for attempt in range(3):
        _goto(page, url, timeout=75_000)
        if _google_blocked(page):
            return {"organic": [], "blocked": True}
        try:
            page.wait_for_selector(
                "#search, #rso, div.g, div[data-sokoban-container], div.MjjYud",
                timeout=12_000,
            )
        except Exception:
            pass
        page.wait_for_timeout(1200 + attempt * 700)
        parsed = _extract_serp(page)
        if parsed.get("organic"):
            return parsed
        if _google_blocked(page):
            return {"organic": [], "blocked": True}
        try:
            page.evaluate("window.scrollTo(0, Math.min(900, document.body.scrollHeight))")
        except Exception:
            pass
        page.wait_for_timeout(900 + attempt * 500)
        parsed = _extract_serp(page)
        if parsed.get("organic"):
            return parsed
        if _google_blocked(page):
            return {"organic": [], "blocked": True}
        if attempt < 2:
            page.wait_for_timeout(2200 + attempt * 1800)
    return parsed if isinstance(parsed, dict) else {}


def job_serp(page: Any, *, batch_index: int | None = None) -> dict[str, Any]:
    batches = serp_keyword_batches()
    batch_total = len(batches) or 1
    if batch_index is None:
        target_keywords = list(SERP_KEYWORDS)
        batch_idx = None
    else:
        batch_idx = int(batch_index) % batch_total
        target_keywords = list(serp_keywords_for_batch(batch_idx))
    keywords: list[dict[str, Any]] = []
    blocked = False
    for kw in target_keywords:
        rows: list[dict[str, Any]] = []
        our = None
        for pno in range(SERP_PAGES):
            start = pno * 10
            url = f"https://www.google.com/search?q={quote(kw)}&hl=tr&gl=tr&pws=0&num=10&start={start}"
            try:
                parsed = _load_serp_page(page, url)
            except Exception:
                break
            if parsed.get("blocked"):
                blocked = True
                break
            organic = parsed.get("organic") or []
            if not organic and pno == 0:
                break
            for i, row in enumerate(organic, 1):
                rec = {
                    "keyword": kw,
                    "page": pno + 1,
                    "rank": pno * 10 + i,
                    "title": row.get("title") or "",
                    "url": row.get("url") or "",
                    "domain": row.get("domain") or _domain_of(str(row.get("url") or "")),
                    "snippet": row.get("snippet") or "",
                    "ours": False,
                }
                rec["ours"] = _is_our_host(str(rec["domain"]))
                if rec["ours"] and our is None:
                    our = rec["rank"]
                rows.append(rec)
            time.sleep(SERP_PAGE_SLEEP_SEC)
        keywords.append(
            {
                "keyword": kw,
                "our_rank": our,
                "row_count": len(rows),
                "rows": rows,
            }
        )
        if blocked:
            break
        time.sleep(SERP_KEYWORD_SLEEP_SEC)
    total = sum(k.get("row_count") or 0 for k in keywords)
    empty_kw = sum(1 for k in keywords if not (k.get("rows") or []))
    message = ""
    if blocked:
        message = (
            f"Google captcha/limit — batch durdu ({len(keywords)}/{len(target_keywords)} kelime). "
            f"{SERP_BATCH_GAP_SEC // 60} dk bekleyip sonraki batch."
        )
    elif not total:
        message = "SERP boş"
    elif empty_kw:
        message = f"{empty_kw} kelime boş (Google limiti) — sonraki batch'i bekleyin"
    summary = f"{len(keywords)} kelime · {SERP_PAGES} sayfa · {total} sonuç"
    if batch_idx is not None:
        summary = f"batch {batch_idx + 1}/{batch_total} · {summary}"
    return {
        "ok": total > 0 and not blocked,
        "scraped_at": _now(),
        "summary": summary,
        "message": message,
        "keywords": keywords,
        "pages": SERP_PAGES,
        "batch_index": batch_idx,
        "batch_total": batch_total,
        "blocked": blocked,
    }


def _fold(s: str) -> str:
    return (s or "").translate(_FOLD_TABLE).lower()


def _to_float(raw: str) -> float | None:
    s = (raw or "").strip().replace(" ", "").replace("$", "")
    if not s:
        return None
    if re.match(r"^0+\d", s):
        return None
    if s.count(",") == 1 and s.count(".") >= 1:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    elif s.count(".") > 1:
        s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def _in_range(aid: str, val: float) -> bool:
    bounds = ASSET_RANGES.get(aid)
    if not bounds:
        return True
    lo, hi = bounds
    return lo <= val <= hi


def _value_candidates(raw: str, val: float) -> list[float]:
    out = [val]
    s = (raw or "").strip()
    if re.fullmatch(r"\d{1,2}\.\d{3}", s) and val < 500:
        try:
            alt = float(s.replace(".", ""))
        except ValueError:
            alt = None
        if alt is not None and alt != val:
            out.append(alt)
    return out


# Hacim/likiditeye göre sapma eşiği (yüzde). Dar bant = yüksek hacimli kotasyon.
# (uyarı, kırmızı) — |sapma| < uyarı yeşil, arası sarı, kırmızı eşiğin üstü.
SAPMA_THRESHOLDS: dict[str, tuple[float, float]] = {
    "usd": (0.08, 0.22),
    "eur": (0.08, 0.22),
    "gram_altin": (0.12, 0.35),
    "ons_altin": (0.12, 0.35),
    "bist100": (0.15, 0.40),
    "brent": (0.20, 0.55),
    "bitcoin": (0.25, 0.70),
    "gram_gumus": (0.35, 0.90),
    "ceyrek_altin": (0.50, 1.20),
}
_SAPMA_DEFAULT = (0.20, 0.50)
_SAPMA_MIN_PEERS = 2
SAPMA_MAIL_THRESHOLD_PCT = 2.0
# Mail eşiği (mutlak sapma %). Varsayılan ±2%; varlık bazlı override.
SAPMA_MAIL_THRESHOLDS: dict[str, float] = {
    "brent": 8.0,
    "gram_altin": 3.0,
    "ons_altin": 3.0,
    "ceyrek_altin": 3.0,
}


def sapma_mail_threshold_pct(asset_id: str) -> float:
    return float(SAPMA_MAIL_THRESHOLDS.get(asset_id, SAPMA_MAIL_THRESHOLD_PCT))


def _parse_quote(aid: str, raw: str) -> float | None:
    val = _to_float(raw)
    if val is None:
        return None
    for cand in _value_candidates(raw, val):
        if _in_range(aid, cand):
            return cand
    return val if _in_range(aid, val) else None


def _sapma_result(
    asset_id: str,
    doviz: float | None,
    peer: float | None,
    *,
    n: int,
) -> dict[str, Any]:
    warn, alert = SAPMA_THRESHOLDS.get(asset_id, _SAPMA_DEFAULT)
    empty = {"pct": None, "avg": None, "n": n, "warn": warn, "alert": alert, "band": ""}
    if doviz is None or peer is None or peer == 0:
        return empty
    pct = (doviz - peer) / peer * 100.0
    ap = abs(pct)
    band = "ok" if ap < warn else ("warn" if ap < alert else "hot")
    return {
        "pct": round(pct, 4),
        "avg": round(peer, 6),
        "n": n,
        "warn": warn,
        "alert": alert,
        "band": band,
    }


def compute_price_sapma(
    asset_id: str,
    cells: dict[str, Any],
    doviz_id: str = "doviz",
) -> dict[str, Any]:
    """Döviz kotasyonu vs diğer sitelerin ortalaması (yüzde sapma)."""
    doviz = _parse_quote(asset_id, str((cells.get(doviz_id) or {}).get("value") or ""))
    peers: list[float] = []
    for sid, cell in (cells or {}).items():
        if sid == doviz_id:
            continue
        parsed = _parse_quote(asset_id, str((cell or {}).get("value") or ""))
        if parsed is not None:
            peers.append(parsed)
    if doviz is None or len(peers) < _SAPMA_MIN_PEERS:
        return _sapma_result(asset_id, None, None, n=len(peers))
    avg = sum(peers) / len(peers)
    return _sapma_result(asset_id, doviz, avg, n=len(peers))


def compute_pair_sapma(
    asset_id: str,
    cells: dict[str, Any],
    peer_id: str = "foreks",
    doviz_id: str = "doviz",
) -> dict[str, Any]:
    """Döviz kotasyonu vs tek akran site (Foreks)."""
    doviz = _parse_quote(asset_id, str((cells.get(doviz_id) or {}).get("value") or ""))
    peer = _parse_quote(asset_id, str((cells.get(peer_id) or {}).get("value") or ""))
    rec = _sapma_result(asset_id, doviz, peer, n=1 if peer is not None else 0)
    rec["peer"] = peer_id
    return rec


def format_sapma_pct(pct: float) -> str:
    sign = "+" if pct > 0 else ("−" if pct < 0 else "")
    return f"{sign}{abs(pct):.2f}%".replace(".", ",")


def collect_sapma_alerts(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mutlak sapması mail eşiğini geçen ortalama / Foreks sapmaları."""
    out: list[dict[str, Any]] = []
    for row in matrix or []:
        if not isinstance(row, dict):
            continue
        aid = str(row.get("id") or "")
        label = str(row.get("label") or aid)
        cells = row.get("cells") if isinstance(row.get("cells"), dict) else {}
        if not aid:
            continue
        mail_thr = sapma_mail_threshold_pct(aid)
        avg = compute_price_sapma(aid, cells)
        foreks = compute_pair_sapma(aid, cells)
        for kind, rec, kind_label in (
            ("avg", avg, "Sapma"),
            ("foreks", foreks, "Foreks sapma"),
        ):
            if rec.get("pct") is None:
                continue
            pct = float(rec["pct"])
            if abs(pct) < mail_thr:
                continue
            out.append(
                {
                    "asset_id": aid,
                    "asset": label,
                    "kind": kind,
                    "kind_label": kind_label,
                    "pct": pct,
                    "pct_text": format_sapma_pct(pct),
                    "band": rec["band"],
                    "warn": rec["warn"],
                    "alert": rec["alert"],
                    "mail_threshold": mail_thr,
                    "n": rec.get("n") or 0,
                    "subject": f"Doviz - {kind_label} - {label} - {format_sapma_pct(pct)}",
                }
            )
    return out


def _numbers_on_line(line: str) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for m in _NUM_RE.finditer(line):
        ctx = line[max(0, m.start() - 2) : m.end() + 1]
        if "%" in ctx:
            continue
        raw = m.group(1).strip()
        val = _to_float(raw)
        if val is None:
            continue
        out.append((raw, val))
    return out


def _iter_label_spans(folded: str, aid: str):
    excludes = tuple(_fold(b) for b in (ASSET_LINE_EXCLUDE.get(aid) or ()) if b)
    labels = sorted((_fold(x) for x in (ASSET_LABELS.get(aid) or ()) if x), key=len, reverse=True)
    seen: set[int] = set()
    for lab in labels:
        start = 0
        while True:
            i = folded.find(lab, start)
            if i < 0:
                break
            around = folded[max(0, i - 24) : i + len(lab) + 24]
            if any(bad and bad in around for bad in excludes):
                start = i + 1
                continue
            rest = folded[i + len(lab) : i + len(lab) + 4]
            if aid in ("usd", "eur") and re.match(r"/[a-z]{3}", rest) and not rest.startswith("/try"):
                start = i + 1
                continue
            if aid == "bitcoin" and (rest.startswith("/try") or rest.startswith("try")):
                start = i + 1
                continue
            if i not in seen:
                seen.add(i)
                yield i, i + len(lab)
            start = i + 1


def _label_span(folded: str, aid: str) -> tuple[int, int] | None:
    for span in _iter_label_spans(folded, aid):
        return span
    return None


def _line_has_label(folded: str, aid: str) -> bool:
    return _label_span(folded, aid) is not None


_FOREIGN_SKIP = {
    "ons_altin": {"usd", "eur"},
    "gram_altin": {"usd", "eur"},
    "ceyrek_altin": {"usd", "eur"},
    "brent": {"usd", "eur"},
    "gram_gumus": {"usd", "eur"},
    "bitcoin": {"usd", "eur"},
}


def _foreign_cut(folded_window: str, aid: str) -> int:
    cut = len(folded_window)
    skip = _FOREIGN_SKIP.get(aid) or set()
    for other in _ASSET_MATCH_ORDER:
        if other == aid or other in skip:
            continue
        for start, _end in _iter_label_spans(folded_window, other):
            if 0 <= start < cut:
                cut = start
            break
    for bad in ASSET_LINE_EXCLUDE.get(aid) or ():
        fb = _fold(bad)
        if not fb:
            continue
        i = folded_window.find(fb)
        if 0 <= i < cut:
            cut = i
    return cut


def _looks_date_number(text: str, start: int, end: int) -> bool:
    after = text[end : end + 8]
    if re.match(r"[./-]\d{2,4}", after):
        return True
    before = text[max(0, start - 3) : start]
    if re.search(r"\d{2}[./-]$", before):
        return True
    return False


def _extract_from_line(line: str, aid: str) -> dict[str, str] | None:
    folded = _fold(line)
    for _ls, le in _iter_label_spans(folded, aid):
        window = line[le : le + 72]
        cut = _foreign_cut(_fold(window), aid)
        search_blob = window[: max(cut, 1)]
        for m in _NUM_RE.finditer(search_blob):
            if _looks_date_number(search_blob, m.start(), m.end()):
                continue
            ctx = search_blob[max(0, m.start() - 2) : m.end() + 1]
            if "%" in ctx:
                continue
            raw = m.group(1).strip()
            val = _to_float(raw)
            if val is None:
                continue
            if any(_in_range(aid, cand) for cand in _value_candidates(raw, val)):
                ch = _CHANGE_RE.search(search_blob) or _CHANGE_RE.search(line)
                return {"value": raw, "change": (ch.group(0).replace(" ", "") if ch else "")}
    return None


def _extract_from_blob(text: str, aid: str) -> dict[str, str] | None:
    return _extract_from_line(text, aid)


def _parse_assets_from_text(text: str) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    blob = " ".join((text or "").split())
    if blob:
        for aid in _ASSET_MATCH_ORDER:
            hit = _extract_from_line(blob, aid)
            if hit:
                found[aid] = hit
    if len(found) >= len(_ASSET_MATCH_ORDER):
        return found
    raw_lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    lines: list[str] = []
    for ln in raw_lines:
        compact = " ".join(ln.split())
        if 6 <= len(compact) <= 12000:
            lines.append(compact)
    for i, ln in enumerate(lines):
        folded = _fold(ln)
        window = ln
        if not _numbers_on_line(ln) and i + 1 < len(lines):
            nxt = lines[i + 1]
            nxt_aids = [a for a in _ASSET_MATCH_ORDER if _line_has_label(_fold(nxt), a)]
            cur_aids = [a for a in _ASSET_MATCH_ORDER if _line_has_label(folded, a)]
            if not nxt_aids or set(nxt_aids) & set(cur_aids) or not cur_aids:
                window = ln + " " + nxt
                folded = _fold(window)
        for aid in _ASSET_MATCH_ORDER:
            if aid in found:
                continue
            if not _line_has_label(folded, aid):
                continue
            hit = _extract_from_line(window, aid)
            if hit:
                found[aid] = hit
    return found


def _parse_one_asset(text: str, aid: str) -> dict[str, str] | None:
    return _parse_assets_from_text(text).get(aid)


def _ticker_text(page: Any) -> str:
    try:
        blocks = page.evaluate(
            """() => {
              const out = [];
              const seen = new Set();
              const add = (t) => {
                t = (t || '').replace(/\\s+/g, ' ').trim();
                if (t.length < 6 || t.length > 900) return;
                if (seen.has(t)) return;
                seen.add(t);
                out.push(t);
              };
              document.querySelectorAll(
                'tr, li, table, [class*="ticker"], [class*="parity"], [class*="market"], [class*="price"], [class*="symbol"], [data-symbol], [class*="kur"], [class*="quote"]'
              ).forEach((el) => add(el.innerText));
              return out.slice(0, 220);
            }"""
        )
        if isinstance(blocks, list) and blocks:
            return "\n".join(str(x) for x in blocks)
    except Exception:
        pass
    return ""


def _page_blob(page: Any, *, limit: int = 16000) -> str:
    return (_ticker_text(page) + "\n" + _text(page, limit)).strip()


def _norm_url(url: str) -> str:
    return str(url or "").split("#")[0].split("?")[0].rstrip("/")


_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
}
_SAFARI_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
)
_HTTP_JAR = urllib.request.HTTPCookieProcessor()
_HTTP_OPENER = urllib.request.build_opener(_HTTP_JAR)


def _http_get(url: str, *, timeout: int = 18, retry_403: bool = True) -> str:
    headers = dict(_HTTP_HEADERS)
    host = (urllib.parse.urlparse(url).netloc or "").lower()
    if "paratic.com" in host:
        headers["User-Agent"] = _SAFARI_UA
        headers["Referer"] = "https://piyasa.paratic.com/"
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    req = urllib.request.Request(url, headers=headers)
    try:
        with _HTTP_OPENER.open(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        if retry_403 and exc.code == 403 and "paratic.com" in host:
            _paratic_warmup()
            return _http_get(url, timeout=timeout, retry_403=False)
        raise


def _html_visible_text(html: str) -> str:
    html = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html or "")
    html = re.sub(r"(?is)<!--.*?-->", " ", html)
    html = re.sub(r"(?is)<br\s*/?>", "\n", html)
    html = re.sub(r"(?is)</(tr|li|p|div|h[1-6]|td|th|section|article)>", "\n", html)
    html = re.sub(r"(?is)<[^>]+>", " ", html)
    return html_lib.unescape(html)


def _http_parse_url(url: str) -> dict[str, dict[str, str]]:
    try:
        html = _http_get(url)
    except Exception:
        return {}
    low = html.lower()
    if "just a moment" in low or "cf-challenge-running" in low:
        return {}
    return _parse_assets_from_text(_html_visible_text(html))


def _fmt_quote(val: float) -> str:
    if abs(val) >= 100:
        return f"{val:,.2f}"
    s = f"{val:.6f}".rstrip("0").rstrip(".")
    return s


def _http_tradingview_quotes() -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    for aid, symbol in TV_SCANNER_SYMBOLS.items():
        url = (
            "https://scanner.tradingview.com/symbol?symbol="
            + quote(symbol, safe=":")
            + "&fields=close,change,change_percent"
        )
        try:
            raw = _http_get(url, timeout=12)
            data = json.loads(raw)
        except Exception:
            continue
        close = data.get("close") if isinstance(data, dict) else None
        try:
            val = float(close)
        except (TypeError, ValueError):
            continue
        if not _in_range(aid, val):
            continue
        ch = data.get("change_percent")
        change = ""
        try:
            if ch is not None:
                change = f"{float(ch):+.2f}%".replace(".", ",")
        except (TypeError, ValueError):
            change = ""
        found[aid] = {"value": _fmt_quote(val), "change": change}
    return found


def _merge_found(found: dict[str, dict[str, str]], parsed: dict[str, dict[str, str]]) -> None:
    for aid, rec in (parsed or {}).items():
        if aid not in found and rec and rec.get("value"):
            found[aid] = rec


def _site_urls(sid: str, home: str) -> list[str]:
    out: list[str] = [home]
    out.extend(SITE_LIST_URLS.get(sid) or ())
    extra = ASSET_URLS.get(sid) or {}
    for url in extra.values():
        if url:
            out.append(url)
    seen: set[str] = set()
    uniq: list[str] = []
    for url in out:
        key = _norm_url(url)
        if not key or key in seen:
            continue
        seen.add(key)
        uniq.append(url)
    return uniq


_PARATIC_ASK_RE = re.compile(
    r'data-type="ask"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)',
    re.I,
)
_PARATIC_LAST_RE = re.compile(
    r'data-type="last"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)',
    re.I,
)
_PARATIC_HERO_ASK_RE = re.compile(
    r'class="[^"]*\bprice\b[^"]*"[^>]*data-type="(?:ask|last)"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)'
    r'|data-type="(?:ask|last)"[^>]*class="[^"]*\bprice\b[^"]*"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)',
    re.I,
)
_PARATIC_SAT_RE = re.compile(r"\bSAT\s+([\d.,]+)", re.I)
_PARATIC_CHANGE_RE = re.compile(r'data-type="change"[^>]*>\s*([+\-]?\s*[\d.,]+)', re.I)
PARATIC_CODES = {
    "usd": ("USD/TRL", "USDTRY", "USD/TRY"),
    "eur": ("EUR/TRL", "EURTRY", "EUR/TRY"),
    "gram_altin": ("XGLD", "GAU/TRY", "XAU/TRY"),
    "ons_altin": ("XAU/USD", "XAUUSD", "ONS"),
    "ceyrek_altin": ("CEYREK", "XCEYREK", "CEYREKALTIN"),
    "gram_gumus": ("XSLV", "XAG/TRY", "XAGTRY"),
    "brent": ("UKOIL", "BRENT", "XTBRN"),
    "bitcoin": ("BTCUSD", "BTC/USD", "BTCUSDT", "BTC/USDT"),
    "bist100": ("XU100",),
}


def _paratic_quote_from_html(html: str, aid: str, *, strict: bool = False) -> dict[str, str] | None:
    """Paratic SAT (ask) / last; BIST sayfasında XU100 SON.

    strict=True: only data-code matches for this asset (list pages mix many tickers).
    """
    raws: list[str] = []
    codes = PARATIC_CODES.get(aid) or ()
    for code in codes:
        cm = re.search(
            rf'data-code="{re.escape(code)}"[^>]*data-type="(?:ask|last)"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)'
            rf'|data-type="(?:ask|last)"[^>]*data-code="{re.escape(code)}"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)',
            html or "",
            re.I,
        )
        if cm:
            raws.append((cm.group(1) or cm.group(2) or "").strip())
    if aid == "bist100":
        m = re.search(
            r'data-code="XU100"[^>]*data-type="(?:last|close|price|ask)"[^>]*>\s*(?:<[^>]+>\s*)*?([\d.,]+)',
            html or "",
            re.I,
        )
        if m:
            raws.append(m.group(1).strip())
        if not strict:
            vis = _html_visible_text(html)
            hit = _parse_one_asset(vis, "bist100")
            if hit:
                return hit
    if not strict:
        for m in _PARATIC_HERO_ASK_RE.finditer(html or ""):
            raws.append((m.group(1) or m.group(2) or "").strip())
        raws.extend(m.group(1).strip() for m in _PARATIC_ASK_RE.finditer(html or ""))
        raws.extend(m.group(1).strip() for m in _PARATIC_LAST_RE.finditer(html or ""))
        vis = _html_visible_text(html)
        sat = _PARATIC_SAT_RE.search(vis)
        if sat:
            raws.append(sat.group(1).strip())
    change = ""
    ch = _PARATIC_CHANGE_RE.search(html or "")
    if ch:
        change = ch.group(1).replace(" ", "")
        if change and "%" not in change:
            change = f"%{change}"
    seen: set[str] = set()
    for raw in raws:
        if not raw or raw in seen:
            continue
        seen.add(raw)
        val = _to_float(raw)
        if val is None:
            continue
        if any(_in_range(aid, cand) for cand in _value_candidates(raw, val)):
            return {"value": raw, "change": change}
    if strict:
        return None
    return _parse_one_asset(_html_visible_text(html), aid)


def _paratic_warmup() -> dict[str, str]:
    pages: dict[str, str] = {}
    for url in (
        "https://piyasa.paratic.com/",
        "https://piyasa.paratic.com/doviz/",
        "https://piyasa.paratic.com/altin/",
        "https://piyasa.paratic.com/kripto-coin/",
        "https://piyasa.paratic.com/borsa/",
    ):
        try:
            html = _http_get(url, timeout=15, retry_403=False)
        except Exception:
            continue
        if html:
            pages[url] = html
    return pages


def _paratic_merge_html(found: dict[str, dict[str, str]], html: str, wanted: list[str]) -> None:
    if not html:
        return
    for aid in wanted:
        if aid in found:
            continue
        rec = _paratic_quote_from_html(html, aid, strict=True)
        if rec and rec.get("value"):
            found[aid] = rec


def _http_fill_paratic() -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    extra = ASSET_URLS.get("paratic") or {}
    wanted = [aid for aid, url in extra.items() if url]
    pages = _paratic_warmup()
    for html in pages.values():
        _paratic_merge_html(found, html, wanted)
        if len(found) >= len(wanted):
            return found
    for aid, url in extra.items():
        if aid in found or not url:
            continue
        html = ""
        for attempt in range(3):
            try:
                html = _http_get(url)
                break
            except urllib.error.HTTPError as exc:
                if exc.code != 403 or attempt == 2:
                    html = ""
                    break
                time.sleep(0.8 * (attempt + 1))
                _paratic_warmup()
            except Exception:
                html = ""
                break
        if html:
            rec = _paratic_quote_from_html(html, aid)
            if rec and rec.get("value"):
                found[aid] = rec
        time.sleep(0.45)
    return found


def _http_fill_site(sid: str, home: str) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    if sid in ("investing", "foreks"):
        return found
    if sid == "paratic":
        return _http_fill_paratic()
    if sid == "tradingview":
        _merge_found(found, _http_tradingview_quotes())
    for url in _site_urls(sid, home):
        _merge_found(found, _http_parse_url(url))
        if set(_CORE_ASSETS) <= found.keys():
            break
    return found


_CHROME_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


def _new_context_page(page: Any):
    browser = page.context.browser
    ctx = browser.new_context(
        locale="en-US",
        user_agent=_CHROME_UA,
        viewport={"width": 1280, "height": 900},
    )
    return ctx, ctx.new_page()


def _investing_dom_quote(page: Any) -> dict[str, str] | None:
    try:
        loc = page.locator('[data-test="instrument-price-last"]')
        if not loc.count():
            return None
        raw = (loc.first.inner_text() or "").strip()
        if not raw:
            return None
        change = ""
        ch = page.locator('[data-test="instrument-price-change-percent"]')
        if ch.count():
            change = (ch.first.inner_text() or "").strip().replace(" ", "")
        return {"value": raw, "change": change}
    except Exception:
        return None


FOREKS_FIELDS: dict[str, str] = {
    "usd": "o10_l",
    "eur": "o11_l",
    "bist100": "H3558_l",
    "gram_altin": "o15_l",
    "ceyrek_altin": "o34_l",
    "ons_altin": "o13_l",
    "gram_gumus": "o16_l",
    "brent": "o2627_l",
    "bitcoin": "o1836_l",
}

FOREKS_PAGES = (
    "https://www.foreks.com/doviz/",
    "https://www.foreks.com/altin/",
    "https://www.foreks.com/emtia/",
)


def _foreks_dom_fields(page: Any) -> tuple[dict[str, str], dict[str, str]]:
    try:
        data = page.evaluate(
            """() => {
              const last = {}, ch = {};
              document.querySelectorAll('[data-field]').forEach((el) => {
                const k = el.getAttribute('data-field') || '';
                const t = (el.innerText || '').trim();
                if (!k || !t) return;
                if (k.endsWith('_l') && !last[k]) last[k] = t;
                if (k.endsWith('_C') && !ch[k]) ch[k] = t;
              });
              return {last, ch};
            }"""
        )
    except Exception:
        return {}, {}
    if not isinstance(data, dict):
        return {}, {}
    last = data.get("last") if isinstance(data.get("last"), dict) else {}
    ch = data.get("ch") if isinstance(data.get("ch"), dict) else {}
    return last, ch


def _browser_fill_foreks(page: Any, found: dict[str, dict[str, str]]) -> None:
    wanted = set(FOREKS_FIELDS)
    for url in FOREKS_PAGES:
        if wanted <= found.keys():
            break
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(800)
            try:
                page.wait_for_function(
                    """() => {
                      const el = document.querySelector('[data-field="o10_l"], [data-field="o15_l"], [data-field="o16_l"]');
                      return el && (el.innerText || '').trim().length > 1;
                    }""",
                    timeout=14000,
                )
            except Exception:
                pass
            last, ch = _foreks_dom_fields(page)
            for aid, field in FOREKS_FIELDS.items():
                if aid in found:
                    continue
                raw = str(last.get(field) or "").strip()
                if not raw:
                    continue
                val = _to_float(raw)
                if val is None or not _in_range(aid, val):
                    continue
                pct = str(ch.get(field.replace("_l", "_C")) or "").strip().replace(" ", "")
                if pct and "%" not in pct:
                    pct = f"%{pct}"
                found[aid] = {"value": raw, "change": pct}
        except Exception:
            continue


def _tv_dom_quote(page: Any) -> dict[str, str] | None:
    try:
        loc = page.locator('[data-qa-id="symbol-last-value"], .js-symbol-last')
        if not loc.count():
            return None
        raw = (loc.first.inner_text() or "").strip()
        if not raw or not re.search(r"\d", raw):
            return None
        change = ""
        ch = page.locator(".js-symbol-change-pt, [data-qa-id='symbol-change-pt']")
        if ch.count():
            change = (ch.first.inner_text() or "").strip().replace(" ", "")
        return {"value": raw, "change": change}
    except Exception:
        return None


def _browser_fill_tradingview(page: Any, found: dict[str, dict[str, str]]) -> None:
    extra = ASSET_URLS.get("tradingview") or {}
    for aid, url in extra.items():
        if aid in found or not url:
            continue
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(1200)
            try:
                page.wait_for_function(
                    """() => {
                      const el = document.querySelector('[data-qa-id="symbol-last-value"], .js-symbol-last');
                      return el && /\\d/.test((el.innerText || '').trim());
                    }""",
                    timeout=14000,
                )
            except Exception:
                pass
            hit = _tv_dom_quote(page)
            if hit:
                val = _to_float(hit["value"])
                if val is not None and any(_in_range(aid, c) for c in _value_candidates(hit["value"], val)):
                    found[aid] = hit
                    continue
            blob = _page_blob(page, limit=12000)
            parsed = _parse_one_asset(blob, aid)
            if parsed:
                found[aid] = parsed
        except Exception:
            continue
        time.sleep(0.4)


def _paratic_dom_quote(page: Any, aid: str) -> dict[str, str] | None:
    codes = list(PARATIC_CODES.get(aid) or ())
    try:
        rec = page.evaluate(
            """(codes) => {
              const num = (el) => {
                const t = ((el && el.innerText) || '').replace(/\\s+/g, ' ').trim();
                const m = t.match(/[\\d][\\d.,]*/);
                return m ? m[0] : '';
              };
              for (const code of codes) {
                const el = document.querySelector(
                  '.price[data-code="' + code + '"][data-type="ask"], [data-code="' + code + '"][data-type="ask"], .price[data-code="' + code + '"][data-type="last"], [data-code="' + code + '"][data-type="last"]'
                );
                const v = num(el);
                if (v) return {value: v, change: ''};
              }
              const hero = document.querySelector('.ins_alsat .price[data-type="ask"], .price[data-type="ask"], .ins_alsat .price[data-type="last"]');
              const hv = num(hero);
              return hv ? {value: hv, change: ''} : null;
            }""",
            codes,
        )
    except Exception:
        rec = None
    if not isinstance(rec, dict) or not rec.get("value"):
        return None
    raw = str(rec.get("value") or "").strip()
    val = _to_float(raw)
    if val is None:
        return None
    if not any(_in_range(aid, cand) for cand in _value_candidates(raw, val)):
        return None
    return {"value": raw, "change": str(rec.get("change") or "")}


def _browser_fill_paratic(page: Any, found: dict[str, dict[str, str]]) -> None:
    extra = ASSET_URLS.get("paratic") or {}
    wanted = [aid for aid, url in extra.items() if url]
    for seed in (
        "https://piyasa.paratic.com/",
        "https://piyasa.paratic.com/doviz/",
        "https://piyasa.paratic.com/altin/",
    ):
        try:
            _goto(page, seed, timeout=45_000)
            page.wait_for_timeout(900)
            html = ""
            try:
                html = page.content() or ""
            except Exception:
                html = ""
            _paratic_merge_html(found, html, wanted)
        except Exception:
            continue
        if len(found) >= len(wanted):
            return
    for aid, url in extra.items():
        if aid in found or not url:
            continue
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(2200)
            try:
                page.wait_for_function(
                    """() => {
                      const els = [...document.querySelectorAll('.price[data-type="ask"], .price[data-type="last"], [data-type="ask"]')];
                      return els.some((el) => /\\d/.test((el.innerText || '').replace(/\\s/g, '')));
                    }""",
                    timeout=12000,
                )
            except Exception:
                pass
            rec = _paratic_dom_quote(page, aid)
            if rec and rec.get("value"):
                found[aid] = rec
                continue
            html = ""
            try:
                html = page.content() or ""
            except Exception:
                html = ""
            rec = _paratic_quote_from_html(html, aid) if html else None
            if rec and rec.get("value"):
                found[aid] = rec
                continue
            blob = _page_blob(page, limit=16000)
            hit = _parse_one_asset(blob, aid)
            if hit:
                found[aid] = hit
        except Exception:
            continue
        time.sleep(0.35)


def _browser_fill_investing(page: Any, found: dict[str, dict[str, str]]) -> None:
    extra = ASSET_URLS.get("investing") or {}
    for aid, url in extra.items():
        if aid in found or not url:
            continue
        ctx2 = None
        try:
            ctx2, p2 = _new_context_page(page)
            p2.goto(url, wait_until="domcontentloaded", timeout=45_000)
            p2.wait_for_timeout(2800)
            hit = _investing_dom_quote(p2)
            if hit:
                val = _to_float(hit["value"])
                if val is not None and _in_range(aid, val):
                    found[aid] = hit
            blob = _page_blob(p2, limit=12000)
            parsed = _parse_assets_from_text(blob)
            if aid not in found and aid in parsed:
                found[aid] = parsed[aid]
            _merge_found(found, parsed)
        except Exception:
            continue
        finally:
            if ctx2 is not None:
                try:
                    ctx2.close()
                except Exception:
                    pass
        time.sleep(0.5)


_CORE_ASSETS = {
    "usd",
    "eur",
    "bist100",
    "gram_altin",
    "gram_gumus",
    "ons_altin",
    "brent",
    "ceyrek_altin",
    "bitcoin",
}


def _browser_fill_gaps(page: Any, sid: str, home: str, found: dict[str, dict[str, str]]) -> None:
    wanted = set(_CORE_ASSETS)
    if wanted <= found.keys():
        return
    seen_urls: set[str] = set()
    extra = ASSET_URLS.get(sid) or {}
    queue: list[str] = []
    if not found:
        queue = [home]
        queue.extend(SITE_LIST_URLS.get(sid) or ())
    for aid in wanted:
        if aid in found:
            continue
        url = extra.get(aid)
        if url:
            queue.append(url)
    if not queue:
        queue = [home]
        queue.extend(SITE_LIST_URLS.get(sid) or ())
    for url in queue:
        if wanted <= found.keys():
            break
        key = _norm_url(url)
        if not key or key in seen_urls:
            continue
        seen_urls.add(key)
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(2200)
            try:
                page.wait_for_function(
                    r"() => /\d+[.,]\d{2,}/.test((document.body && document.body.innerText) || '')",
                    timeout=6000,
                )
            except Exception:
                pass
            blob = _page_blob(page, limit=22000)
            parsed = _parse_assets_from_text(blob)
            _merge_found(found, parsed)
            if url in extra.values():
                for aid, asset_url in extra.items():
                    if aid in found or _norm_url(asset_url) != key:
                        continue
                    hit = _parse_one_asset(blob, aid)
                    if hit:
                        found[aid] = hit
        except Exception:
            continue


def job_competitors(page: Any) -> dict[str, Any]:
    columns = [{"id": s["id"], "label": s["label"], "url": s["home"]} for s in SITES]
    values: dict[str, dict[str, dict[str, str]]] = {a["id"]: {} for a in ASSETS}
    notes: dict[str, str] = {}
    for site in SITES:
        sid = site["id"]
        found: dict[str, dict[str, str]] = {}
        http_n = 0
        try:
            found = _http_fill_site(sid, site["home"])
            http_n = len(found)
            if sid == "investing":
                _browser_fill_investing(page, found)
            elif sid == "foreks":
                _browser_fill_foreks(page, found)
            elif sid == "tradingview":
                _browser_fill_tradingview(page, found)
            elif sid == "paratic":
                _browser_fill_paratic(page, found)
            else:
                _browser_fill_gaps(page, sid, site["home"], found)
            for aid, rec in found.items():
                values.setdefault(aid, {})[sid] = rec
            notes[sid] = f"{len(found)} varlık (http {http_n})"
        except Exception as exc:
            notes[sid] = str(exc)[:160]
        time.sleep(0.25)

    matrix = []
    for asset in ASSETS:
        row = {"id": asset["id"], "label": asset["label"], "cells": {}}
        for site in SITES:
            cell = (values.get(asset["id"]) or {}).get(site["id"])
            row["cells"][site["id"]] = cell or {"value": "", "change": ""}
        row["sapma"] = compute_price_sapma(asset["id"], row["cells"])
        row["foreks_sapma"] = compute_pair_sapma(asset["id"], row["cells"])
        matrix.append(row)

    filled = sum(1 for r in matrix for c in r["cells"].values() if c.get("value"))
    return {
        "ok": filled > 0,
        "scraped_at": _now(),
        "summary": f"{filled} hücre dolu · {len(SITES)} site",
        "message": "",
        "assets": list(ASSETS),
        "columns": columns,
        "matrix": matrix,
        "notes": notes,
    }


def _play_chart_packages(limit: int = 200) -> list[str]:
    from backend.services.app_intel import _extract_android_packages

    import httpx

    inner = json.dumps(
        [[None, [[None, [None, max(200, limit)]], None, None, [113]], [2, "topselling_free", "FINANCE"]]],
        separators=(",", ":"),
    )
    body = "f.req=" + quote(json.dumps([[["vyAe2", inner]]], separators=(",", ":")))
    url = "https://play.google.com/_/PlayStoreUi/data/batchexecute?hl=tr&gl=tr"
    with httpx.Client(timeout=35.0, follow_redirects=True) as client:
        r = client.post(
            url,
            content=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
            },
        )
        r.raise_for_status()
        return _extract_android_packages(r.text or "")[:limit]


_APP_ICON_CACHE = ROOT / "scratch" / "pm_lab_app_icons.json"


def _load_app_icon_cache() -> dict[str, Any]:
    if not _APP_ICON_CACHE.is_file():
        return {}
    try:
        data = json.loads(_APP_ICON_CACHE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_app_icon_cache(cache: dict[str, Any]) -> None:
    _APP_ICON_CACHE.parent.mkdir(parents=True, exist_ok=True)
    _APP_ICON_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _cache_app_meta(cache: dict[str, Any], key: str, name: str, icon: str) -> None:
    rec = cache.get(key) if isinstance(cache.get(key), dict) else {}
    if name:
        rec["name"] = name
    if icon:
        rec["icon"] = icon
    if rec:
        cache[key] = rec


def _play_titles(packages: list[str]) -> dict[str, str]:
    return {pkg: rec.get("name") or pkg for pkg, rec in _play_meta(packages).items()}


def _play_meta(packages: list[str], cache: dict[str, Any] | None = None) -> dict[str, dict[str, str]]:
    """Play başlık + ikon. İkon cache'te varsa tekrar çekilmez."""
    cache = _load_app_icon_cache() if cache is None else cache
    out: dict[str, dict[str, str]] = {}
    need: list[str] = []
    for pkg in packages:
        hit = cache.get(f"android:{pkg}") if isinstance(cache.get(f"android:{pkg}"), dict) else {}
        name = str(hit.get("name") or "").strip()
        icon = str(hit.get("icon") or "").strip()
        if icon:
            out[pkg] = {"name": name or pkg, "icon": icon}
        else:
            need.append(pkg)
    if not need:
        return out
    try:
        from google_play_scraper import app as gp_app
    except ImportError:
        return out

    def one(pkg: str) -> tuple[str, str, str]:
        try:
            meta = gp_app(pkg, lang="tr", country="tr")
            return pkg, str(meta.get("title") or pkg), str(meta.get("icon") or "").strip()
        except Exception:
            return pkg, pkg, ""

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(one, p) for p in need]
        for fut in as_completed(futs):
            pkg, title, icon = fut.result()
            out[pkg] = {"name": title or pkg, "icon": icon}
            _cache_app_meta(cache, f"android:{pkg}", title, icon)
    _save_app_icon_cache(cache)
    return out


def _ios_lockup_id(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("adamId") or item.get("id") or "").strip()


def _ios_chart_ids_html(limit: int = 200) -> list[str]:
    url = f"https://apps.apple.com/tr/iphone/charts/{IOS_FINANCE_GENRE}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    m = re.search(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return []
    page_data = json.loads(m.group(1))
    segments = ((page_data.get("data") or [{}])[0].get("data") or {}).get("segments") or []
    by_chart: dict[str, list[str]] = {}
    for segment in segments:
        chart = str(segment.get("chart") or "unknown")
        ids: list[str] = []
        for shelf in segment.get("shelves") or []:
            for item in shelf.get("items") or []:
                aid = _ios_lockup_id(item)
                if aid:
                    ids.append(aid)
        for item in (segment.get("nextPage") or {}).get("remainingContent") or []:
            aid = _ios_lockup_id(item)
            if aid:
                ids.append(aid)
        if ids:
            by_chart[chart] = ids
    picked = by_chart.get("top-free") or by_chart.get("topfreeapplications") or []
    if not picked and by_chart:
        picked = max(by_chart.values(), key=len)
    seen: set[str] = set()
    out: list[str] = []
    for aid in picked:
        if not aid or aid in seen:
            continue
        seen.add(aid)
        out.append(aid)
        if len(out) >= limit:
            break
    return out


def _ios_chart_apps_rss(limit: int = 200) -> list[dict[str, Any]]:
    url = (
        f"https://itunes.apple.com/tr/rss/topfreeapplications/"
        f"genre={IOS_FINANCE_GENRE}/limit={min(200, limit)}/json"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    entries = ((payload.get("feed") or {}).get("entry")) or []
    if isinstance(entries, dict):
        entries = [entries]
    apps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for e in entries:
        if not isinstance(e, dict):
            continue
        aid = str((((e.get("id") or {}).get("attributes") or {}).get("im:id") or "")).strip()
        name = str(((e.get("im:name") or {}).get("label") or "")).strip()
        if not aid or aid in seen:
            continue
        seen.add(aid)
        icon = ""
        imgs = e.get("im:image") or []
        if isinstance(imgs, list):
            for im in imgs:
                url = str((im.get("label") if isinstance(im, dict) else "") or "").strip()
                if url:
                    icon = url
        apps.append(
            {
                "rank": len(apps) + 1,
                "name": name or aid,
                "id": aid,
                "icon": icon,
                "is_ours": aid == IOS_APP_ID,
            }
        )
        if len(apps) >= limit:
            break
    return apps


def _ios_chart_apps(limit: int = 200) -> list[dict[str, Any]]:
    ids: list[str] = []
    try:
        ids = _ios_chart_ids_html(limit)
    except Exception:
        ids = []
    if len(ids) < limit:
        rss = _ios_chart_apps_rss(limit)
        if len(rss) > len(ids):
            return rss
    if not ids:
        return _ios_chart_apps_rss(limit)
    meta = _ios_meta(ids)
    return [
        {
            "rank": i,
            "name": (meta.get(aid) or {}).get("name") or aid,
            "id": aid,
            "icon": (meta.get(aid) or {}).get("icon") or "",
            "is_ours": aid == IOS_APP_ID,
        }
        for i, aid in enumerate(ids[:limit], 1)
    ]


def _ios_titles(ids: list[str]) -> dict[str, str]:
    return {aid: rec.get("name") or aid for aid, rec in _ios_meta(ids).items()}


def _ios_meta(ids: list[str], cache: dict[str, Any] | None = None) -> dict[str, dict[str, str]]:
    cache = _load_app_icon_cache() if cache is None else cache
    out: dict[str, dict[str, str]] = {}
    need: list[str] = []
    for aid in ids:
        hit = cache.get(f"ios:{aid}") if isinstance(cache.get(f"ios:{aid}"), dict) else {}
        name = str(hit.get("name") or "").strip()
        icon = str(hit.get("icon") or "").strip()
        if icon:
            out[aid] = {"name": name or aid, "icon": icon}
        else:
            need.append(aid)
    for i in range(0, len(need), 50):
        chunk = need[i : i + 50]
        url = f"https://itunes.apple.com/lookup?id={','.join(chunk)}&country=tr"
        try:
            with urllib.request.urlopen(url, timeout=25) as resp:
                info = json.loads(resp.read().decode("utf-8", errors="replace"))
            for row in info.get("results") or []:
                aid = str(row.get("trackId") or "")
                name = str(row.get("trackName") or "")
                icon = str(row.get("artworkUrl100") or row.get("artworkUrl60") or "").strip()
                if not aid:
                    continue
                out[aid] = {"name": name or aid, "icon": icon}
                _cache_app_meta(cache, f"ios:{aid}", name, icon)
        except Exception:
            continue
    if need:
        _save_app_icon_cache(cache)
    return out


def job_store_charts(page: Any) -> dict[str, Any]:
    del page
    charts: list[dict[str, Any]] = []
    pkgs = _play_chart_packages(200)
    play_meta = _play_meta(pkgs)
    play_apps = [
        {
            "rank": i,
            "name": (play_meta.get(pkg) or {}).get("name") or pkg,
            "id": pkg,
            "icon": (play_meta.get(pkg) or {}).get("icon") or "",
            "is_ours": pkg.lower() == PLAY_PACKAGE.lower(),
        }
        for i, pkg in enumerate(pkgs[:200], 1)
    ]
    ours = next((a for a in play_apps if a["is_ours"]), None)
    charts.append(
        {
            "id": "android",
            "title": "Play · Finance free (TR)",
            "our_label": f"Döviz #{ours['rank']} / {len(play_apps)}" if ours else f"Döviz not listed · {len(play_apps)} apps",
            "apps": play_apps,
        }
    )
    try:
        ios_apps = _ios_chart_apps(200)
    except Exception:
        ios_apps = []
    ours_ios = next((a for a in ios_apps if a["is_ours"]), None)
    charts.append(
        {
            "id": "ios",
            "title": "App Store · Finance free (TR)",
            "our_label": f"Döviz #{ours_ios['rank']} / {len(ios_apps)}" if ours_ios else f"Döviz not listed · {len(ios_apps)} apps",
            "apps": ios_apps,
        }
    )
    icon_map: dict[str, str] = {}
    cache = _load_app_icon_cache()
    for chart in charts:
        plat = "android" if chart.get("id") == "android" else "ios"
        for app in chart.get("apps") or []:
            icon = str(app.get("icon") or "").strip()
            aid = str(app.get("id") or "").strip()
            name = str(app.get("name") or "").strip()
            if aid:
                _cache_app_meta(cache, f"{plat}:{aid}", name, icon)
            if icon and aid:
                icon_map[f"{plat}:{aid}"] = icon
                if not app.get("icon"):
                    app["icon"] = icon
            elif aid and not icon:
                remembered = (cache.get(f"{plat}:{aid}") or {}).get("icon") if isinstance(cache.get(f"{plat}:{aid}"), dict) else ""
                if remembered:
                    app["icon"] = remembered
                    icon_map[f"{plat}:{aid}"] = remembered
    _save_app_icon_cache(cache)
    return {
        "ok": bool(play_apps or ios_apps),
        "scraped_at": _now(),
        "summary": f"Play {len(play_apps)} · iOS {len(ios_apps)}",
        "message": "",
        "charts": charts,
        "icon_map": icon_map,
    }


def _news_rss_articles(keyword: str, *, limit: int = 25) -> list[dict[str, str]]:
    rss_url = f"https://news.google.com/rss/search?q={quote(keyword)}&hl=tr&gl=TR&ceid=TR:tr"
    req = urllib.request.Request(
        rss_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.5",
            "Referer": "https://news.google.com/",
        },
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        raw = resp.read()
    sniff = raw.lstrip()[:80].lower()
    if sniff.startswith(b"<!doctype") or sniff.startswith(b"<html"):
        return []
    root = ET.fromstring(raw)
    items = root.findall("./channel/item")
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        title = html_lib.unescape((item.findtext("title") or "").strip())
        link = (item.findtext("link") or "").strip()
        source_el = item.find("source")
        source = (source_el.text or "").strip() if source_el is not None else ""
        if source and title.endswith(" - " + source):
            title = title[: -len(source) - 3].strip()
        pub = (item.findtext("pubDate") or "").strip()[:40]
        if not title or title in seen:
            continue
        seen.add(title)
        out.append({"title": title[:200], "url": link, "source": source[:80], "time": pub})
        if len(out) >= limit:
            break
    return out


def _news_html_articles(page: Any, *, limit: int = 25) -> list[dict[str, str]]:
    arts = page.evaluate(
        """(limit) => {
          const out = [];
          const seen = new Set();
          const push = (title, url, source, time) => {
            title = (title || '').trim();
            if (!title || seen.has(title)) return;
            seen.add(title);
            out.push({
              title: title.slice(0, 200),
              url: url || '',
              source: (source || '').slice(0, 80),
              time: (time || '').slice(0, 40)
            });
          };
          document.querySelectorAll('article').forEach((art) => {
            const a = art.querySelector('a[href]');
            const title = ((art.querySelector('h3,h4') || a || {}).innerText || '').trim();
            const timeEl = art.querySelector('time');
            let source = '';
            if (timeEl && timeEl.previousElementSibling) source = (timeEl.previousElementSibling.innerText || '').trim();
            if (!source) {
              const lines = (art.innerText || '').split('\\n').map(s => s.trim()).filter(Boolean);
              source = lines[1] || '';
            }
            push(title, a && a.href, source, timeEl && (timeEl.innerText || timeEl.getAttribute('datetime')));
          });
          if (!out.length) {
            document.querySelectorAll('a[href*="./articles/"], a[href*="/articles/"]').forEach((a) => {
              const title = (a.innerText || '').trim();
              if (title.length < 18) return;
              const block = a.closest('article, div') || a.parentElement;
              const lines = ((block && block.innerText) || '').split('\\n').map(s => s.trim()).filter(Boolean);
              push(title, a.href, lines[1] || '', '');
            });
          }
          return out.slice(0, limit);
        }""",
        limit,
    )
    return arts if isinstance(arts, list) else []


def job_google_news(page: Any) -> dict[str, Any]:
    keywords: list[dict[str, Any]] = []
    for kw in NEWS_KEYWORDS:
        search_url = f"https://news.google.com/search?q={quote(kw)}&hl=tr&gl=TR&ceid=TR:tr"
        rec: dict[str, Any] = {"keyword": kw, "url": search_url, "articles": []}
        alias_err = ""
        try:
            rec["articles"] = _news_rss_articles(kw, limit=25)
        except Exception as exc:
            alias_err = str(exc)[:180]
        if not rec["articles"]:
            try:
                _goto(page, search_url, timeout=75_000)
                page.wait_for_timeout(1600)
                rec["articles"] = _news_html_articles(page, limit=25)
            except Exception as exc:
                alias_err = alias_err or str(exc)[:180]
        if alias_err and not rec["articles"]:
            rec["message"] = alias_err
        keywords.append(rec)
        time.sleep(0.4)
    total = sum(len(k.get("articles") or []) for k in keywords)
    return {
        "ok": total > 0,
        "scraped_at": _now(),
        "summary": f"{total} haber · {len(keywords)} kelime",
        "message": "" if total else "Google News boş (RSS/HTML)",
        "keywords": keywords,
    }


def _write_scratch(job: str, payload: dict[str, Any]) -> None:
    out = ROOT / "scratch" / f"pm_lab_{job}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_job(name: str, fn, page: Any, *, ingest: bool) -> dict[str, Any]:
    print(f"== {name}", flush=True)
    try:
        result = fn(page)
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "scraped_at": _now(), "message": str(exc)[:240], "summary": "hata"}
    scratch_name = name
    if name == "serp" and isinstance(result, dict) and result.get("batch_index") is not None:
        scratch_name = f"serp_batch_{int(result['batch_index']) + 1}"
    _write_scratch(scratch_name, result)
    print(f"  · {result.get('summary') or result.get('message') or ''} · ok={result.get('ok')}", flush=True)
    if ingest:
        ing = post_ingest({name: result}, message=f"{name} tarama")
        print(f"  · ingest: {ing}", flush=True)
    return result


def _parse_serp_batch_arg(raw: str | None) -> int | None:
    text = str(raw or "").strip()
    if not text or text.lower() in ("all", "-1", "none"):
        return None
    try:
        return int(text)
    except ValueError:
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Owner PM lab taramaları")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--jobs", default="")
    parser.add_argument(
        "--serp-batch",
        default="",
        help="SERP dilimi 0..N-1 (5'er kelime). Boş = tüm kelimeler (manuel/test).",
    )
    args = parser.parse_args(argv)

    wanted = [j.strip() for j in (args.jobs or "").split(",") if j.strip()]
    if args.sync:
        wanted = [j for j in JOB_IDS if j != "serp"]
    elif not wanted:
        wanted = [j for j in JOB_IDS if j != "serp"]
    for j in wanted:
        if j not in JOB_IDS:
            print(f"bilinmeyen job: {j}", flush=True)
            return 2

    serp_batch = _parse_serp_batch_arg(args.serp_batch or os.environ.get("PM_LAB_SERP_BATCH"))
    # Google SERP headless'ta sonuç dönmüyor; fx-google profili headed gerekir.
    google_jobs = [j for j in wanted if j in ("serp",)]
    public = [j for j in wanted if j in ("competitors", "store_charts", "google_news")]
    if args.headed:
        headed_serp = True
        headed_public = True
    elif os.environ.get("PM_LAB_HEADLESS", "").strip() in ("1", "true", "yes"):
        headed_serp = False
        headed_public = False
    else:
        headed_serp = bool(google_jobs)
        headed_public = False

    from playwright.sync_api import sync_playwright

    if serp_batch is not None:
        serp_fn = lambda page, b=serp_batch: job_serp(page, batch_index=b)  # noqa: E731
    else:
        serp_fn = lambda page: job_serp(page, batch_index=None)  # noqa: E731
    fns = {
        "serp": serp_fn,
        "competitors": job_competitors,
        "store_charts": job_store_charts,
        "google_news": job_google_news,
    }

    failures = 0
    with sync_playwright() as pw:
        if public:
            browser, ctx = launch_ephemeral(
                pw,
                headed=headed_public,
                locale="tr-TR",
                viewport={"width": 1280, "height": 900},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
            )
            page = ctx.new_page()
            try:
                for name in public:
                    res = _run_job(name, fns[name], page, ingest=bool(args.ingest))
                    if not res.get("ok"):
                        failures += 1
            finally:
                ctx.close()
                browser.close()
        if google_jobs:
            ctx = launch_persistent(
                pw,
                google_profile_dir(),
                headed=headed_serp,
                viewport={"width": 1440, "height": 1100},
            )
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                for name in google_jobs:
                    res = _run_job(name, fns[name], page, ingest=bool(args.ingest))
                    if not res.get("ok"):
                        failures += 1
            finally:
                ctx.close()
    print(f"bitti · failures={failures}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
