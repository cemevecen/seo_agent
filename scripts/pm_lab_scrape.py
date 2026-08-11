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

SERP_KEYWORDS = (
    "gram gümüş",
    "usd",
    "çeyrek altın",
    "harem çeyrek altın",
    "harem gram altın",
    "harem dolar",
    "kapalıçarşı gram altın",
    "gram altın",
)
SERP_PAGES = 4

NEWS_KEYWORDS = (
    "dolar",
    "altın",
    "gram altın",
    "çeyrek altın",
    "kripto para",
    "ons altın",
)

ASSETS = (
    {"id": "usd", "label": "Dolar"},
    {"id": "bist100", "label": "BIST 100"},
    {"id": "eur", "label": "Euro"},
    {"id": "gram_altin", "label": "Gram Altın"},
    {"id": "gram_gumus", "label": "Gram Gümüş"},
    {"id": "ons_altin", "label": "Ons Altın"},
    {"id": "brent", "label": "Brent Petrol"},
    {"id": "ceyrek_altin", "label": "Çeyrek Altın"},
    {"id": "bitcoin", "label": "Bitcoin"},
)

SITES = (
    {"id": "doviz", "label": "Döviz", "home": "https://www.doviz.com/"},
    {"id": "tradingview", "label": "TradingView", "home": "https://www.tradingview.com/"},
    {"id": "canlidoviz", "label": "Canlı Döviz", "home": "https://canlidoviz.com/"},
    {"id": "foreks", "label": "Foreks", "home": "https://www.foreks.com/"},
    {"id": "investing", "label": "Investing", "home": "https://www.investing.com/"},
    {"id": "bigpara", "label": "Bigpara", "home": "https://bigpara.hurriyet.com.tr/"},
    {"id": "uzmanpara", "label": "Uzmanpara", "home": "https://uzmanpara.milliyet.com.tr/"},
    {"id": "bloomberght", "label": "Bloomberg HT", "home": "https://www.bloomberght.com/"},
    {"id": "cnbce", "label": "CNBC-e", "home": "https://www.cnbce.com/"},
    {"id": "cnnturk", "label": "CNN Türk Finans", "home": "https://finans.cnnturk.com/"},
    {"id": "enuygun", "label": "Enuygun Finans", "home": "https://www.enuygunfinans.com/"},
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
    "uzmanpara": (
        "https://uzmanpara.milliyet.com.tr/doviz/",
        "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/",
        "https://uzmanpara.milliyet.com.tr/kripto-paralar/",
    ),
    "bloomberght": (
        "https://www.bloomberght.com/piyasalar",
        "https://www.bloomberght.com/doviz",
        "https://www.bloomberght.com/emtia",
        "https://www.bloomberght.com/altin",
        "https://www.bloomberght.com/doviz/bitcoin",
        "https://www.bloomberght.com/kripto",
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
        "gram_gumus": "https://www.tradingview.com/symbols/XAGUSD/",
        "gram_altin": "https://www.tradingview.com/symbols/XAUTRY/",
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
        "gram_gumus": "https://www.investing.com/currencies/xag-try",
        "gram_altin": "https://www.investing.com/currencies/xau-try",
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
        "bitcoin": "https://uzmanpara.milliyet.com.tr/kripto-paralar/",
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
    "gram_altin": ("gram altin", "ga altin", "xautry", "altin (tl/gr)", "spot altin", "gldgr", "altin"),
    "gram_gumus": ("gram gumus", "gumus gram", "ga gumus", "gumus (tl/gr)", "sxaggr", "gumus"),
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

JOB_IDS = ("serp", "competitors", "sikayet", "store_charts", "google_news")

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
          document.querySelectorAll('h3').forEach((h3) => {
            const a = h3.closest('a');
            if (!a || !a.href) return;
            const href = a.href;
            if (href.includes('google.com/search') || href.includes('webcache')) return;
            if (seen.has(href)) return;
            seen.add(href);
            let host = '';
            try { host = new URL(href).hostname.replace(/^www\\./, ''); } catch (e) {}
            const block = h3.closest('div.g, div[data-hveid], div[data-sokoban-container]') || h3.parentElement;
            let snippet = '';
            if (block) {
              const t = (block.innerText || '').split('\\n').filter(Boolean);
              snippet = t.slice(1, 5).join(' ').slice(0, 320);
            }
            organic.push({
              rank: organic.length + 1,
              title: (h3.innerText || '').trim(),
              url: href,
              domain: host,
              snippet
            });
          });
          const paa = [];
          document.querySelectorAll('div[jsname] span, div[role="button"] span').forEach((el) => {
            const t = (el.innerText || '').trim();
            if (t.endsWith('?') && t.length > 12 && t.length < 140 && !paa.includes(t)) paa.push(t);
          });
          return { organic, paa: paa.slice(0, 8) };
        }"""
    )
    return data if isinstance(data, dict) else {}


def job_serp(page: Any) -> dict[str, Any]:
    keywords: list[dict[str, Any]] = []
    for kw in SERP_KEYWORDS:
        rows: list[dict[str, Any]] = []
        our = None
        for pno in range(SERP_PAGES):
            start = pno * 10
            url = f"https://www.google.com/search?q={quote(kw)}&hl=tr&gl=tr&pws=0&num=10&start={start}"
            try:
                _goto(page, url, timeout=75_000)
                page.wait_for_timeout(900)
                parsed = _extract_serp(page)
            except Exception:
                break
            organic = parsed.get("organic") or []
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
            time.sleep(1.6)
        keywords.append(
            {
                "keyword": kw,
                "our_rank": our,
                "row_count": len(rows),
                "rows": rows,
            }
        )
    total = sum(k.get("row_count") or 0 for k in keywords)
    return {
        "ok": total > 0,
        "scraped_at": _now(),
        "summary": f"{len(keywords)} kelime · {SERP_PAGES} sayfa · {total} sonuç",
        "message": "" if total else "SERP boş",
        "keywords": keywords,
        "pages": SERP_PAGES,
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


def _http_get(url: str, *, timeout: int = 18) -> str:
    req = urllib.request.Request(url, headers=_HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


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


def _http_fill_site(sid: str, home: str) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    if sid in ("investing", "foreks"):
        return found
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


def _eksi_last_page_href(page: Any) -> str:
    try:
        href = page.evaluate(
            """() => {
              let max = 0, href = '';
              document.querySelectorAll('.pager a').forEach((a) => {
                const n = parseInt((a.innerText || '').trim(), 10);
                if (n > max && a.href) { max = n; href = a.href; }
              });
              const last = document.querySelector('.pager a.last, .pager a[title*="son"]');
              if (last && last.href) return last.href;
              return href;
            }"""
        )
        return str(href or "")
    except Exception:
        return ""


def _eksi_extract_entries(page: Any) -> list[dict[str, Any]]:
    try:
        rows = page.evaluate(
            """() => {
              const out = [];
              const seen = new Set();
              document.querySelectorAll('li[id^="entry-item"], [id^="entry-item"]').forEach((el) => {
                const contentEl = el.querySelector('.content');
                if (!contentEl) return;
                const id = el.getAttribute('data-id') || el.id.replace(/^entry-item-?/, '');
                if (!id || seen.has(id)) return;
                const text = (contentEl.innerText || '').trim();
                if (text.length < 20) return;
                seen.add(id);
                const author = ((el.querySelector('a.entry-author') || {}).innerText || '').trim();
                const dateEl = el.querySelector('a.entry-date');
                out.push({
                  id: String(id),
                  text: text.slice(0, 800),
                  author,
                  date: ((dateEl && dateEl.innerText) || '').trim().slice(0, 48),
                  url: (dateEl && dateEl.href) || window.location.href
                });
              });
              return out;
            }"""
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _eksi_newest(page: Any, start_url: str, *, limit: int = 10) -> tuple[str, list[dict[str, Any]]]:
    _goto(page, start_url, timeout=70_000)
    page.wait_for_timeout(1100)
    last_href = _eksi_last_page_href(page)
    urls: list[str] = []
    if last_href:
        urls.append(last_href)
        m = re.search(r"([?&]p=)(\d+)", last_href)
        if m and int(m.group(2)) > 1:
            urls.append(re.sub(r"([?&]p=)\d+", rf"\g<1>{int(m.group(2)) - 1}", last_href, count=1))
    else:
        urls.append(page.url)
    collected: list[dict[str, Any]] = []
    seen: set[str] = set()
    final_url = page.url
    for url in urls:
        if url and url != page.url:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(900)
        final_url = page.url
        page_rows = list(reversed(_eksi_extract_entries(page)))
        for row in page_rows:
            rid = str(row.get("id") or "")
            fp = " ".join(str(row.get("text") or "").split())[:80]
            if (rid and rid in seen) or (fp and fp in seen):
                continue
            if rid:
                seen.add(rid)
            if fp:
                seen.add(fp)
            collected.append(row)
            if len(collected) >= limit:
                return final_url, collected[:limit]
    return final_url, collected[:limit]


_TR_FOLD = str.maketrans(
    {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
        "Ç": "c",
        "Ğ": "g",
        "İ": "i",
        "Ö": "o",
        "Ş": "s",
        "Ü": "u",
        "â": "a",
        "î": "i",
        "û": "u",
    }
)


def _fold_tr(s: str) -> str:
    return (s or "").translate(_TR_FOLD).lower().replace("www.", "")


def _matches_query(blob: str, query: str) -> bool:
    """True when the writing was entered as the brand string (e.g. doviz.com / Döviz.com)."""
    blob_l = _fold_tr(blob)
    q = _fold_tr(query).strip()
    if not q:
        return True
    if q in blob_l:
        return True
    return q.replace(".", " ") in blob_l


def _url_has_brand(url: str, query: str) -> bool:
    compact = (query or "").replace(".", "").lower()
    return bool(compact) and compact in (url or "").lower()


def _filter_query_rows(rows: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        url = str(row.get("url") or "")
        blob = " ".join(str(row.get(k) or "") for k in ("title", "text", "url", "author"))
        if _matches_query(blob, query) or _url_has_brand(url, query):
            out.append(row)
    return out


def _harvest_host_results(page: Any, *, hosts: tuple[str, ...], limit: int = 10) -> list[dict[str, Any]]:
    try:
        rows = page.evaluate(
            """([hosts, limit]) => {
              const unwrap = (href) => {
                try {
                  const u = new URL(href);
                  const host = u.hostname;
                  if (host.includes('google.') && (u.pathname === '/url' || u.pathname === '/url')) {
                    return u.searchParams.get('q') || u.searchParams.get('url') || href;
                  }
                  if (host.includes('duckduckgo.')) {
                    const uddg = u.searchParams.get('uddg');
                    if (uddg) return decodeURIComponent(uddg);
                  }
                  if (host.includes('bing.') && u.searchParams.get('u')) {
                    let raw = u.searchParams.get('u') || '';
                    if (raw.startsWith('a1')) {
                      try { raw = atob(raw.slice(2).replace(/_/g, '/').replace(/-/g, '+')); } catch (e) {}
                    }
                    if (raw.startsWith('http')) return raw;
                  }
                } catch (e) {}
                return href;
              };
              const out = [];
              const seen = new Set();
              const engine = ['google.', 'bing.', 'duckduckgo.', 'brave.com', 'microsoft.com'];
              document.querySelectorAll('a[href]').forEach((a) => {
                let href = unwrap((a.href || '').split('#')[0]);
                if (!href || !href.startsWith('http')) return;
                const low = href.toLowerCase();
                if (engine.some((h) => low.includes(h))) return;
                if (!hosts.some((h) => low.includes(h))) return;
                const key = href.split('?')[0];
                if (seen.has(key)) return;
                seen.add(key);
                const title = (a.innerText || '').trim().replace(/\\s+/g, ' ');
                const block = a.closest('li, article, .b_algo, div.g, .result') || a.parentElement;
                const snippet = ((block && block.innerText) || title).trim().replace(/\\s+/g, ' ');
                out.push({
                  title: title.slice(0, 180),
                  text: title.slice(0, 700),
                  excerpt: snippet.slice(0, 420),
                  author: '',
                  date: '',
                  url: key
                });
              });
              return out.slice(0, limit);
            }""",
            [list(hosts), limit],
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _web_search_mentions(
    page: Any,
    query: str,
    *,
    hosts: tuple[str, ...],
    site_query: str,
    limit: int = 10,
) -> tuple[str, list[dict[str, Any]]]:
    q = f'{site_query} "{query}"'
    engines = [
        f"https://www.google.com/search?q={quote(q)}&hl=tr&num=10",
        f"https://search.brave.com/search?q={quote(q)}",
        f"https://www.bing.com/search?q={quote(q)}&setlang=tr-TR",
        f"https://duckduckgo.com/?q={quote(q)}&ia=web",
    ]
    final = engines[0]
    collected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for url in engines:
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(1800)
            final = page.url or url
            if "sorry" in (final or "").lower():
                continue
            rows = _harvest_host_results(page, hosts=hosts, limit=limit * 2)
            if "google." in (final or "") and not rows:
                parsed = _extract_serp(page)
                for org in parsed.get("organic") or []:
                    rows.append(
                        {
                            "title": org.get("title") or "",
                            "text": org.get("snippet") or org.get("title") or "",
                            "excerpt": org.get("snippet") or "",
                            "url": org.get("url") or "",
                            "author": "",
                            "date": "",
                        }
                    )
            for row in _filter_query_rows(rows, query):
                key = str(row.get("url") or row.get("text") or "")[:120]
                if not key or key in seen:
                    continue
                seen.add(key)
                collected.append(row)
                if len(collected) >= limit:
                    return final, collected[:limit]
        except Exception:
            continue
    return final, collected[:limit]


def _sikayet_complaint_url(url: str, *, brand_slug: str = "") -> bool:
    try:
        path = urllib.parse.urlparse(url).path.strip("/")
    except Exception:
        return False
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2 or len(parts[-1]) < 20:
        return False
    if brand_slug and parts[0].lower() not in {brand_slug.lower(), brand_slug.replace("com", "").lower()}:
        return False
    return True


def _sikayet_extract(page: Any, *, query: str = "", limit: int = 10) -> list[dict[str, Any]]:
    try:
        rows = page.evaluate(
            """(limit) => {
              const out = [];
              const seen = new Set();
              document.querySelectorAll('a[href]').forEach((a) => {
                const href = (a.href || '').split('?')[0];
                if (!href || seen.has(href) || !href.includes('sikayetvar.com')) return;
                let path = '';
                try { path = new URL(href).pathname; } catch (e) { return; }
                const parts = path.split('/').filter(Boolean);
                if (parts.length < 2) return;
                if (['search','sikayetler','write','login','uye','blog'].includes(parts[0])) return;
                const slug = parts[parts.length - 1] || '';
                if (slug.length < 20) return;
                const title = (a.innerText || '').trim().replace(/\\s+/g, ' ');
                if (title.length < 12) return;
                seen.add(href);
                const card = a.closest('article, .card, li, .complaint, .search-item') || a.parentElement;
                const blob = ((card && card.innerText) || '').trim();
                const lines = blob.split('\\n').map(s => s.trim()).filter(Boolean);
                out.push({
                  title: title.slice(0, 180),
                  url: href,
                  meta: lines.slice(1, 3).join(' · ').slice(0, 160),
                  excerpt: lines.slice(1, 8).join(' ').slice(0, 420)
                });
              });
              return out.slice(0, Math.max(limit, 40));
            }""",
            limit,
        )
        rows = rows if isinstance(rows, list) else []
        slug = query.replace(".", "")
        rows = [r for r in rows if _sikayet_complaint_url(str(r.get("url") or ""), brand_slug=slug)]
        return _filter_query_rows(rows, query)[:limit]
    except Exception:
        return []


def _x_extract_tweets(page: Any) -> list[dict[str, Any]]:
    try:
        rows = page.evaluate(
            """() => {
              const out = [];
              const seen = new Set();
              document.querySelectorAll('article[data-testid="tweet"], article').forEach((art) => {
                const textEl = art.querySelector('[data-testid="tweetText"]');
                const text = ((textEl && textEl.innerText) || '').trim();
                if (text.length < 12) return;
                const status = art.querySelector('a[href*="/status/"]');
                const url = (status && status.href) || '';
                if (url && seen.has(url)) return;
                if (url) seen.add(url);
                else {
                  const fp = text.slice(0, 80);
                  if (seen.has(fp)) return;
                  seen.add(fp);
                }
                const timeEl = art.querySelector('time');
                const nameEl = art.querySelector('[data-testid="User-Name"]');
                const author = ((nameEl && nameEl.innerText) || '').split('\\n').filter(Boolean)[0] || '';
                out.push({
                  text: text.slice(0, 700),
                  author: author.slice(0, 80),
                  date: ((timeEl && (timeEl.getAttribute('datetime') || timeEl.innerText)) || '').slice(0, 48),
                  url: url || window.location.href
                });
              });
              return out.slice(0, 10);
            }"""
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _x_newest(page: Any, query: str, *, limit: int = 10) -> tuple[str, list[dict[str, Any]]]:
    search = f"https://x.com/search?q={quote(query)}&src=typed_query&f=live"
    items: list[dict[str, Any]] = []
    final = search
    try:
        _goto(page, search, timeout=70_000)
        page.wait_for_timeout(1800)
        final = page.url
        items = _filter_query_rows(_x_extract_tweets(page), query)
    except Exception:
        items = []
    if len(items) < 3:
        web_url, extra = _web_search_mentions(
            page,
            query,
            hosts=("x.com/", "twitter.com/"),
            site_query="site:x.com OR site:twitter.com",
            limit=limit,
        )
        seen = {str(x.get("url") or x.get("text") or "")[:80] for x in items}
        for row in extra:
            key = str(row.get("url") or row.get("text") or "")[:80]
            if key in seen:
                continue
            seen.add(key)
            if not row.get("text"):
                row["text"] = row.get("excerpt") or row.get("title") or ""
            items.append(row)
            if len(items) >= limit:
                break
        final = web_url or final
    status = [x for x in items if "/status/" in str(x.get("url") or "")]
    other = [x for x in items if x not in status]
    return final, (status + other)[:limit]


def _sikayet_newest(page: Any, query: str, *, limit: int = 10) -> tuple[str, list[dict[str, Any]]]:
    compact = query.replace(".", "")
    urls = [
        f"https://www.sikayetvar.com/{compact}",
        f"https://www.sikayetvar.com/search?q={quote(query)}",
        f"https://www.sikayetvar.com/sikayetler?search={quote(query)}",
    ]
    final = urls[0]
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for url in urls:
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(2200)
            try:
                page.mouse.wheel(0, 2800)
                page.wait_for_timeout(700)
            except Exception:
                pass
            final = page.url
            for row in _sikayet_extract(page, query=query, limit=limit):
                key = str(row.get("url") or "")[:120]
                if not key or key in seen:
                    continue
                seen.add(key)
                items.append(row)
            if len(items) >= limit:
                break
        except Exception:
            continue
    if len(items) < limit:
        web_url, extra = _web_search_mentions(
            page,
            query,
            hosts=("sikayetvar.com/",),
            site_query="site:sikayetvar.com",
            limit=limit,
        )
        for row in extra:
            href = str(row.get("url") or "")
            if not _sikayet_complaint_url(href, brand_slug=compact):
                continue
            if not _filter_query_rows([row], query):
                continue
            key = href[:120]
            if not key or key in seen:
                continue
            seen.add(key)
            items.append(row)
            if len(items) >= limit:
                break
        if extra:
            final = web_url or final
    items = [
        row
        for row in items
        if _sikayet_complaint_url(str(row.get("url") or ""), brand_slug=compact)
    ]
    return final, items[:limit]


def job_sikayet(page: Any) -> dict[str, Any]:
    brands = ("doviz.com", "sinemalar.com")
    out: list[dict[str, Any]] = []
    for brand in brands:
        rec: dict[str, Any] = {
            "brand": brand,
            "x": {"url": "", "items": []},
            "eksi": {"url": "", "entries": []},
            "sikayetvar": {"url": "", "items": []},
        }
        try:
            x_url, x_items = _x_newest(page, brand, limit=10)
            rec["x"]["url"] = x_url
            rec["x"]["items"] = x_items
        except Exception as exc:
            rec["x"]["message"] = str(exc)[:200]
        try:
            sv_url, sv_items = _sikayet_newest(page, brand, limit=10)
            rec["sikayetvar"]["url"] = sv_url
            rec["sikayetvar"]["title"] = (page.title() or "")[:160]
            rec["sikayetvar"]["items"] = sv_items
        except Exception as exc:
            rec["sikayetvar"]["message"] = str(exc)[:200]
        try:
            eksi_q = f"https://eksisozluk.com/?q={quote(brand)}"
            final_url, entries = _eksi_newest(page, eksi_q, limit=10)
            rec["eksi"]["url"] = final_url
            rec["eksi"]["title"] = (page.title() or "")[:160]
            rec["eksi"]["entries"] = entries
        except Exception as exc:
            rec["eksi"]["message"] = str(exc)[:200]
        out.append(rec)
        time.sleep(0.4)
    n = sum(
        len((b.get("x") or {}).get("items") or [])
        + len((b.get("sikayetvar") or {}).get("items") or [])
        + len((b.get("eksi") or {}).get("entries") or [])
        for b in out
    )
    return {
        "ok": n > 0,
        "scraped_at": _now(),
        "summary": f"{n} kayıt · {len(out)} marka · X/Ekşi/Şikayetvar son 10",
        "message": "",
        "brands": out,
        "sources": ["x", "eksi", "sikayetvar"],
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


def _play_titles(packages: list[str]) -> dict[str, str]:
    names: dict[str, str] = {}
    try:
        from google_play_scraper import app as gp_app
    except ImportError:
        return names

    def one(pkg: str) -> tuple[str, str]:
        try:
            meta = gp_app(pkg, lang="tr", country="tr")
            return pkg, str(meta.get("title") or pkg)
        except Exception:
            return pkg, pkg

    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(one, p) for p in packages]
        for fut in as_completed(futs):
            pkg, title = fut.result()
            names[pkg] = title
    return names


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
        apps.append(
            {
                "rank": len(apps) + 1,
                "name": name or aid,
                "id": aid,
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
    names = _ios_titles(ids)
    return [
        {
            "rank": i,
            "name": names.get(aid) or aid,
            "id": aid,
            "is_ours": aid == IOS_APP_ID,
        }
        for i, aid in enumerate(ids[:limit], 1)
    ]


def _ios_titles(ids: list[str]) -> dict[str, str]:
    names: dict[str, str] = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i : i + 50]
        url = f"https://itunes.apple.com/lookup?id={','.join(chunk)}&country=tr"
        try:
            with urllib.request.urlopen(url, timeout=25) as resp:
                info = json.loads(resp.read().decode("utf-8", errors="replace"))
            for row in info.get("results") or []:
                names[str(row.get("trackId"))] = str(row.get("trackName") or "")
        except Exception:
            continue
    return names


def job_store_charts(page: Any) -> dict[str, Any]:
    del page
    charts: list[dict[str, Any]] = []
    pkgs = _play_chart_packages(200)
    titles = _play_titles(pkgs)
    play_apps = [
        {
            "rank": i,
            "name": titles.get(pkg) or pkg,
            "id": pkg,
            "is_ours": pkg.lower() == PLAY_PACKAGE.lower(),
        }
        for i, pkg in enumerate(pkgs[:200], 1)
    ]
    ours = next((a for a in play_apps if a["is_ours"]), None)
    charts.append(
        {
            "id": "android",
            "title": "Play · Finans ücretsiz (TR)",
            "our_label": f"Döviz #{ours['rank']} / {len(play_apps)}" if ours else f"Döviz listede yok · {len(play_apps)} uygulama",
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
            "title": "App Store · Finance ücretsiz (TR)",
            "our_label": f"Döviz #{ours_ios['rank']} / {len(ios_apps)}" if ours_ios else f"Döviz listede yok · {len(ios_apps)} uygulama",
            "apps": ios_apps,
        }
    )
    return {
        "ok": bool(play_apps or ios_apps),
        "scraped_at": _now(),
        "summary": f"Play {len(play_apps)} · iOS {len(ios_apps)}",
        "message": "",
        "charts": charts,
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
    _write_scratch(name, result)
    print(f"  · {result.get('summary') or result.get('message') or ''} · ok={result.get('ok')}", flush=True)
    if ingest:
        ing = post_ingest({name: result}, message=f"{name} tarama")
        print(f"  · ingest: {ing}", flush=True)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Owner PM lab taramaları")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--jobs", default="")
    args = parser.parse_args(argv)

    wanted = [j.strip() for j in (args.jobs or "").split(",") if j.strip()]
    if args.sync or not wanted:
        wanted = list(JOB_IDS)
    for j in wanted:
        if j not in JOB_IDS:
            print(f"bilinmeyen job: {j}", flush=True)
            return 2

    headed = bool(args.headed)
    if os.environ.get("PM_LAB_HEADLESS", "").strip() in ("1", "true", "yes"):
        headed = False

    from playwright.sync_api import sync_playwright

    public = [j for j in wanted if j in ("competitors", "store_charts", "google_news")]
    google_jobs = [j for j in wanted if j in ("serp", "sikayet")]
    fns = {
        "serp": job_serp,
        "competitors": job_competitors,
        "sikayet": job_sikayet,
        "store_charts": job_store_charts,
        "google_news": job_google_news,
    }

    failures = 0
    with sync_playwright() as pw:
        if public:
            browser, ctx = launch_ephemeral(
                pw,
                headed=headed,
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
                headed=headed,
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
