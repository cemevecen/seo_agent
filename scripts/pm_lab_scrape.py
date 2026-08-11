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
    {"id": "harem_gram_altin", "label": "Harem Gram Altın"},
    {"id": "kapalicarsi_gram_altin", "label": "Kapalıçarşı Gram Altın"},
    {"id": "gram_gumus", "label": "Gram Gümüş"},
    {"id": "ons_altin", "label": "Ons Altın"},
    {"id": "brent", "label": "Brent Petrol"},
    {"id": "ceyrek_altin", "label": "Çeyrek Altın"},
)

SITES = (
    {"id": "doviz", "label": "Döviz", "home": "https://www.doviz.com/"},
    {"id": "tradingview", "label": "TradingView", "home": "https://www.tradingview.com/"},
    {"id": "canlidoviz", "label": "Canlı Döviz", "home": "https://canlidoviz.com/"},
    {"id": "investing", "label": "Investing", "home": "https://www.investing.com/"},
    {"id": "bigpara", "label": "Bigpara", "home": "https://bigpara.hurriyet.com.tr/"},
    {"id": "uzmanpara", "label": "Uzmanpara", "home": "https://uzmanpara.milliyet.com.tr/"},
    {"id": "bloomberght", "label": "Bloomberg HT", "home": "https://www.bloomberght.com/"},
    {"id": "cnbce", "label": "CNBC-e", "home": "https://www.cnbce.com/"},
    {"id": "cnnturk", "label": "CNN Türk Finans", "home": "https://finans.cnnturk.com/"},
    {"id": "enuygun", "label": "Enuygun Finans", "home": "https://www.enuygunfinans.com/"},
)

ASSET_URLS: dict[str, dict[str, str]] = {
    "doviz": {
        "usd": "https://kur.doviz.com/serbest-piyasa/amerikan-dolari",
        "eur": "https://kur.doviz.com/serbest-piyasa/euro",
        "gram_altin": "https://altin.doviz.com/gram-altin",
        "kapalicarsi_gram_altin": "https://altin.doviz.com/gram-altin",
        "harem_gram_altin": "https://altin.doviz.com/harem/gram-altin",
        "gram_gumus": "https://altin.doviz.com/gumus",
        "ons_altin": "https://altin.doviz.com/ons",
        "ceyrek_altin": "https://altin.doviz.com/ceyrek-altin",
        "brent": "https://www.doviz.com/emtia/brent-petrol",
        "bist100": "https://borsa.doviz.com/endeksler/xu100",
    },
    "tradingview": {
        "usd": "https://www.tradingview.com/symbols/USDTRY/",
        "eur": "https://www.tradingview.com/symbols/EURTRY/",
        "bist100": "https://www.tradingview.com/symbols/BIST-XU100/",
        "ons_altin": "https://www.tradingview.com/symbols/XAUUSD/",
        "brent": "https://www.tradingview.com/symbols/TVC-UKOIL/",
        "gram_gumus": "https://www.tradingview.com/symbols/XAGUSD/",
        "gram_altin": "https://www.tradingview.com/symbols/GOLD-TRY/",
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
    },
    "investing": {
        "usd": "https://www.investing.com/currencies/usd-try",
        "eur": "https://www.investing.com/currencies/eur-try",
        "bist100": "https://www.investing.com/indices/ise-100",
        "ons_altin": "https://www.investing.com/commodities/gold",
        "brent": "https://www.investing.com/commodities/brent-oil",
        "gram_gumus": "https://www.investing.com/commodities/silver",
        "ceyrek_altin": "https://tr.investing.com/commodities/turkey-gold-quarter",
    },
    "bigpara": {
        "usd": "https://bigpara.hurriyet.com.tr/doviz/dolar/",
        "eur": "https://bigpara.hurriyet.com.tr/doviz/euro/",
        "gram_altin": "https://bigpara.hurriyet.com.tr/altin/gram-altin/",
        "ceyrek_altin": "https://bigpara.hurriyet.com.tr/altin/ceyrek-altin/",
        "ons_altin": "https://bigpara.hurriyet.com.tr/altin/altin-ons/",
        "bist100": "https://bigpara.hurriyet.com.tr/borsa/endeks/xu100/",
    },
    "uzmanpara": {
        "usd": "https://uzmanpara.milliyet.com.tr/dolar-ne-kadar/",
        "eur": "https://uzmanpara.milliyet.com.tr/euro-ne-kadar/",
        "gram_altin": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/gram-altin/",
        "ceyrek_altin": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/ceyrek-altin/",
        "bist100": "https://uzmanpara.milliyet.com.tr/borsa/",
        "gram_gumus": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/gumus/",
        "ons_altin": "https://uzmanpara.milliyet.com.tr/altin-fiyatlari/ons-altin/",
    },
    "bloomberght": {
        "usd": "https://www.bloomberght.com/dolar",
        "eur": "https://www.bloomberght.com/euro",
        "gram_altin": "https://www.bloomberght.com/gram-altin",
        "bist100": "https://www.bloomberght.com/xu100",
        "ons_altin": "https://www.bloomberght.com/ons",
        "brent": "https://www.bloomberght.com/brent-petrol",
        "ceyrek_altin": "https://www.bloomberght.com/ceyrek-altin",
    },
    "cnbce": {
        "usd": "https://www.cnbce.com/",
        "eur": "https://www.cnbce.com/",
        "bist100": "https://www.cnbce.com/",
        "ons_altin": "https://www.cnbce.com/",
        "brent": "https://www.cnbce.com/",
    },
    "cnnturk": {
        "usd": "https://finans.cnnturk.com/",
        "eur": "https://finans.cnnturk.com/",
        "gram_altin": "https://finans.cnnturk.com/",
        "bist100": "https://finans.cnnturk.com/",
        "ons_altin": "https://finans.cnnturk.com/",
        "ceyrek_altin": "https://finans.cnnturk.com/",
    },
    "enuygun": {
        "usd": "https://www.enuygunfinans.com/dolar-kuru",
        "eur": "https://www.enuygunfinans.com/euro-kuru",
        "gram_altin": "https://www.enuygunfinans.com/gram-altin",
        "ceyrek_altin": "https://www.enuygunfinans.com/ceyrek-altin",
        "ons_altin": "https://www.enuygunfinans.com/ons-altin",
        "bist100": "https://www.enuygunfinans.com/bist-100",
    },
}

ASSET_LABELS: dict[str, tuple[str, ...]] = {
    "usd": ("usd/try", "usd try", "amerikan dolari", "abd dolari", "dolar kuru", "dolar", "usd"),
    "eur": ("eur/try", "euro", "avro"),
    "bist100": ("bist 100", "bist100", "xu100", "bist-100"),
    "gram_altin": ("gram altin", "ga altin"),
    "harem_gram_altin": ("harem gram altin", "harem gram"),
    "kapalicarsi_gram_altin": ("kapalicarsi gram", "kapali carsi gram", "kapali carsi"),
    "gram_gumus": ("gram gumus", "gumus gram", "ga gumus"),
    "ons_altin": ("ons altin", "altin/ons", "xauusd", "gold ounce", "altin ons"),
    "brent": ("brent petrol", "brent", "ham petrol"),
    "ceyrek_altin": ("ceyrek altin", "ceyrek"),
}

_ASSET_MATCH_ORDER = (
    "harem_gram_altin",
    "kapalicarsi_gram_altin",
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
    "harem_gram_altin": (2500.0, 20000.0),
    "kapalicarsi_gram_altin": (2500.0, 20000.0),
    "gram_gumus": (80.0, 160.0),
    "ons_altin": (1500.0, 10000.0),
    "brent": (30.0, 250.0),
    "ceyrek_altin": (3000.0, 40000.0),
}

ASSET_LINE_EXCLUDE: dict[str, tuple[str, ...]] = {
    "gram_altin": ("harem", "kapalicarsi", "kapali carsi"),
    "usd": ("harem",),
    "eur": ("harem",),
    "ceyrek_altin": ("harem",),
    "gram_gumus": ("harem", "dolar", "usd", "euro", "sterlin", "gbp"),
    "ons_altin": ("gram",),
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
        s = s.replace(".", "").replace(",", ".")
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


def _line_has_label(folded: str, aid: str) -> bool:
    labels = tuple(_fold(x) for x in (ASSET_LABELS.get(aid) or ()))
    if not any(lab and lab in folded for lab in labels):
        return False
    for bad in ASSET_LINE_EXCLUDE.get(aid) or ():
        if _fold(bad) in folded:
            return False
    return True


def _extract_from_line(line: str, aid: str) -> dict[str, str] | None:
    nums = _numbers_on_line(line)
    for raw, val in nums:
        if _in_range(aid, val):
            ch = _CHANGE_RE.search(line)
            return {"value": raw, "change": (ch.group(0).replace(" ", "") if ch else "")}
    return None


def _parse_assets_from_text(text: str) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    raw_lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    lines: list[str] = []
    for ln in raw_lines:
        compact = " ".join(ln.split())
        if 6 <= len(compact) <= 180:
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
                if (t.length < 6 || t.length > 160) return;
                if (seen.has(t)) return;
                seen.add(t);
                out.push(t);
              };
              document.querySelectorAll(
                'tr, li, [class*="ticker"], [class*="parity"], [class*="market"], [class*="price"], [class*="symbol"]'
              ).forEach((el) => add(el.innerText));
              return out.slice(0, 140);
            }"""
        )
        if isinstance(blocks, list) and blocks:
            return "\n".join(str(x) for x in blocks)
    except Exception:
        pass
    return ""


def _page_blob(page: Any, *, limit: int = 16000) -> str:
    return (_ticker_text(page) + "\n" + _text(page, limit)).strip()


def job_competitors(page: Any) -> dict[str, Any]:
    columns = [{"id": s["id"], "label": s["label"], "url": s["home"]} for s in SITES]
    values: dict[str, dict[str, dict[str, str]]] = {a["id"]: {} for a in ASSETS}
    notes: dict[str, str] = {}
    for site in SITES:
        sid = site["id"]
        home = str(site["home"]).rstrip("/")
        try:
            _goto(page, site["home"], timeout=70_000)
            page.wait_for_timeout(1200)
            found = _parse_assets_from_text(_page_blob(page, limit=16000))
            extra = ASSET_URLS.get(sid) or {}
            for aid, url in extra.items():
                if not url or url.rstrip("/") == home:
                    continue
                try:
                    _goto(page, url, timeout=55_000)
                    page.wait_for_timeout(900)
                    hit = _parse_one_asset(_page_blob(page, limit=9000), aid)
                    if hit:
                        found[aid] = hit
                except Exception:
                    continue
            for aid, rec in found.items():
                values.setdefault(aid, {})[sid] = rec
            notes[sid] = f"{len(found)} varlık"
        except Exception as exc:
            notes[sid] = str(exc)[:160]
        time.sleep(0.4)

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


def _matches_query(blob: str, query: str) -> bool:
    """True when the writing was entered as the brand string (e.g. doviz.com)."""
    blob_l = (blob or "").lower().replace("www.", "")
    q = (query or "").strip().lower().replace("www.", "")
    if not q:
        return True
    if q in blob_l:
        return True
    return q.replace(".", " ") in blob_l


def _filter_query_rows(rows: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        blob = " ".join(
            str(row.get(k) or "")
            for k in ("title", "text", "excerpt", "url", "author", "meta")
        )
        if _matches_query(blob, query):
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
              const engine = ['google.', 'bing.', 'duckduckgo.', 'microsoft.com'];
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
                  text: snippet.slice(0, 700),
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
    urls = [
        f"https://www.sikayetvar.com/search?q={quote(query)}",
        f"https://www.sikayetvar.com/sikayetler?search={quote(query)}",
    ]
    final = urls[0]
    items: list[dict[str, Any]] = []
    for url in urls:
        try:
            _goto(page, url, timeout=70_000)
            page.wait_for_timeout(2500)
            try:
                page.wait_for_selector("a[href]", timeout=8000)
            except Exception:
                pass
            final = page.url
            items = _sikayet_extract(page, query=query, limit=limit)
            if items:
                break
        except Exception:
            continue
    if len(items) < 3:
        web_url, extra = _web_search_mentions(
            page,
            query,
            hosts=("sikayetvar.com/",),
            site_query="site:sikayetvar.com",
            limit=limit,
        )
        seen = {str(x.get("url") or "")[:80] for x in items}
        for row in extra:
            key = str(row.get("url") or "")[:80]
            if not key or key in seen:
                continue
            seen.add(key)
            items.append(row)
            if len(items) >= limit:
                break
        if extra:
            final = web_url or final
    profiles: list[str] = []
    for row in items:
        href = str(row.get("url") or "")
        try:
            path = urllib.parse.urlparse(href).path.strip("/")
        except Exception:
            continue
        parts = [p for p in path.split("/") if p]
        if len(parts) == 1 and "sikayetvar.com" in href:
            profiles.append(href.split("?")[0])
    complaint_items = [
        row
        for row in items
        if len([p for p in urllib.parse.urlparse(str(row.get("url") or "")).path.split("/") if p]) >= 2
    ]
    if len(complaint_items) < 3:
        for purl in profiles[:3]:
            try:
                _goto(page, purl, timeout=70_000)
                page.wait_for_timeout(2200)
                extra_prof = _sikayet_extract(page, query=query, limit=limit)
                if extra_prof:
                    final = page.url or final
                seen = {str(x.get("url") or "")[:80] for x in complaint_items}
                for row in extra_prof:
                    key = str(row.get("url") or "")[:80]
                    if not key or key in seen:
                        continue
                    seen.add(key)
                    complaint_items.append(row)
                    if len(complaint_items) >= limit:
                        break
            except Exception:
                continue
            if len(complaint_items) >= limit:
                break
        items = complaint_items or items
    else:
        items = complaint_items
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
                if isinstance(item, dict) and item.get("id"):
                    ids.append(str(item["id"]))
        for item in (segment.get("nextPage") or {}).get("remainingContent") or []:
            if isinstance(item, dict) and item.get("id"):
                ids.append(str(item["id"]))
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
