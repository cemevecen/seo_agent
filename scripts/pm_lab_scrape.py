#!/usr/bin/env python3
"""Owner PM lab taramaları (fotoğraf yok) — Mac Firefox → Railway ingest.

  .venv/bin/python scripts/pm_lab_scrape.py --sync --ingest
  .venv/bin/python scripts/pm_lab_scrape.py --jobs serp,competitors --ingest
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
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
    "usd": ("usd/try", "usd try", "amerikan doları", "abd doları", "dolar kuru", " dolar ", "usd "),
    "eur": ("eur/try", "euro", "avro"),
    "bist100": ("bist 100", "bist100", "xu100", "bist-100"),
    "gram_altin": ("gram altın", "gram altin", "ga altın"),
    "harem_gram_altin": ("harem gram", "harem altın"),
    "kapalicarsi_gram_altin": ("kapalıçarşı gram", "kapalicarsi gram", "kapalı çarşı"),
    "gram_gumus": ("gram gümüş", "gram gumus", "gümüş gram", "gumus gram"),
    "ons_altin": ("ons altın", "ons altin", "xauusd", "altın/ons", "gold ounce"),
    "brent": ("brent", "brent petrol", "ham petrol"),
    "ceyrek_altin": ("çeyrek altın", "ceyrek altin", "çeyrek"),
}

PLAY_PACKAGE = "com.Doviz"
IOS_APP_ID = "465599322"
IOS_FINANCE_GENRE = 6015
OUR_HOSTS = ("doviz.com",)

JOB_IDS = ("serp", "competitors", "sikayet", "store_charts", "google_news")

_PRICE_RE = re.compile(
    r"([+-]?%?\s*\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{1,4})|\d+[.,]\d{2,4})"
)


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


def _pick_price(text: str, labels: tuple[str, ...]) -> dict[str, str] | None:
    low = (text or "").lower().replace("ı", "i").replace("ş", "s").replace("ğ", "g").replace("ü", "u").replace("ö", "o").replace("ç", "c")
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        ln_n = ln.lower().replace("ı", "i").replace("ş", "s").replace("ğ", "g").replace("ü", "u").replace("ö", "o").replace("ç", "c")
        if not any(lab.strip() in ln_n or lab.strip() in low[max(0, text.lower().find(ln.lower()) - 40):] for lab in labels):
            if not any(lab in ln_n for lab in labels):
                continue
        window = " ".join(lines[i : i + 4])
        prices = _PRICE_RE.findall(window)
        nums = [p.strip() for p in prices if re.search(r"\d", p) and "%" not in p]
        if not nums:
            continue
        change = next((p.strip() for p in prices if "%" in p), "")
        return {"value": nums[0], "change": change}
    return None


def _parse_assets_from_text(text: str) -> dict[str, dict[str, str]]:
    found: dict[str, dict[str, str]] = {}
    for asset in ASSETS:
        hit = _pick_price(text, ASSET_LABELS.get(asset["id"]) or (asset["label"].lower(),))
        if hit:
            found[asset["id"]] = hit
    return found


def job_competitors(page: Any) -> dict[str, Any]:
    columns = [{"id": s["id"], "label": s["label"], "url": s["home"]} for s in SITES]
    values: dict[str, dict[str, dict[str, str]]] = {a["id"]: {} for a in ASSETS}
    notes: dict[str, str] = {}
    for site in SITES:
        sid = site["id"]
        try:
            _goto(page, site["home"], timeout=70_000)
            page.wait_for_timeout(1200)
            found = _parse_assets_from_text(_text(page, 16000))
            extra = ASSET_URLS.get(sid) or {}
            for aid, url in extra.items():
                if found.get(aid) and aid not in ("harem_gram_altin", "kapalicarsi_gram_altin"):
                    continue
                try:
                    _goto(page, url, timeout=55_000)
                    page.wait_for_timeout(900)
                    more = _parse_assets_from_text(_text(page, 9000))
                    if aid in more:
                        found[aid] = more[aid]
                    elif more.get("gram_altin") and aid in ("kapalicarsi_gram_altin", "gram_altin"):
                        found[aid] = more["gram_altin"]
                    elif more.get("usd") and aid == "usd":
                        found[aid] = more["usd"]
                except Exception:
                    continue
            for aid, rec in found.items():
                values.setdefault(aid, {})[sid] = rec
            notes[sid] = f"{len(found)} varlık"
        except Exception as exc:  # noqa: BLE001
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


def job_sikayet(page: Any) -> dict[str, Any]:
    brands = (
        {
            "id": "doviz.com",
            "sikayet": "https://www.sikayetvar.com/doviz-com",
            "eksi": "https://eksisozluk.com/?q=doviz.com",
        },
        {
            "id": "sinemalar.com",
            "sikayet": "https://www.sikayetvar.com/sinemalar-com",
            "eksi": "https://eksisozluk.com/?q=sinemalar.com",
        },
    )
    out: list[dict[str, Any]] = []
    for brand in brands:
        rec: dict[str, Any] = {"brand": brand["id"], "sikayetvar": {"url": brand["sikayet"], "items": []}, "eksi": {"url": brand["eksi"], "entries": []}}
        try:
            _goto(page, brand["sikayet"], timeout=70_000)
            page.wait_for_timeout(1200)
            rec["sikayetvar"]["url"] = page.url
            rec["sikayetvar"]["title"] = (page.title() or "")[:160]
            items = page.evaluate(
                """() => {
                  const out = [];
                  document.querySelectorAll('article, a[href*="/sikayet/"], .card').forEach((el) => {
                    const a = el.querySelector('a') || (el.tagName === 'A' ? el : null);
                    const title = ((el.querySelector('h2,h3,.title') || a || {}).innerText || '').trim();
                    if (!title || title.length < 8) return;
                    const href = (a && a.href) || '';
                    const text = (el.innerText || '').trim();
                    const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
                    out.push({
                      title: title.slice(0, 180),
                      url: href,
                      meta: lines.slice(1, 3).join(' · ').slice(0, 160),
                      excerpt: lines.slice(3, 8).join(' ').slice(0, 420)
                    });
                  });
                  return out.slice(0, 16);
                }"""
            )
            rec["sikayetvar"]["items"] = items if isinstance(items, list) else []
        except Exception as exc:  # noqa: BLE001
            rec["sikayetvar"]["message"] = str(exc)[:200]
        try:
            _goto(page, brand["eksi"], timeout=70_000)
            page.wait_for_timeout(1200)
            rec["eksi"]["url"] = page.url
            rec["eksi"]["title"] = (page.title() or "")[:160]
            entries = page.evaluate(
                """() => {
                  const out = [];
                  document.querySelectorAll('[id^="entry-item"] .content, [id^="entry-item"]').forEach((el) => {
                    const t = (el.innerText || '').trim();
                    if (t.length < 40) return;
                    out.push({ text: t.slice(0, 600), url: window.location.href });
                  });
                  return out.slice(0, 10);
                }"""
            )
            if isinstance(entries, list) and entries:
                rec["eksi"]["entries"] = entries
            else:
                rec["eksi"]["entries"] = [
                    {"text": ln.strip()[:500], "url": page.url}
                    for ln in _text(page, 5000).splitlines()
                    if len(ln.strip()) > 50
                ][:8]
        except Exception as exc:  # noqa: BLE001
            rec["eksi"]["message"] = str(exc)[:200]
        out.append(rec)
        time.sleep(0.5)
    n = sum(len((b.get("sikayetvar") or {}).get("items") or []) + len((b.get("eksi") or {}).get("entries") or []) for b in out)
    return {
        "ok": n > 0,
        "scraped_at": _now(),
        "summary": f"{n} kayıt · 2 marka",
        "message": "",
        "brands": out,
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


def _ios_chart_apps(limit: int = 200) -> list[dict[str, Any]]:
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
    missing = [a["id"] for a in apps if a["name"] == a["id"]]
    if missing:
        names = _ios_titles(missing)
        for a in apps:
            if a["name"] == a["id"] and names.get(a["id"]):
                a["name"] = names[a["id"]]
    return apps


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


def job_google_news(page: Any) -> dict[str, Any]:
    keywords: list[dict[str, Any]] = []
    for kw in NEWS_KEYWORDS:
        url = f"https://news.google.com/search?q={quote(kw)}&hl=tr&gl=TR&ceid=TR:tr"
        rec: dict[str, Any] = {"keyword": kw, "url": url, "articles": []}
        try:
            _goto(page, url, timeout=75_000)
            page.wait_for_timeout(1400)
            arts = page.evaluate(
                """() => {
                  const out = [];
                  document.querySelectorAll('article').forEach((art) => {
                    const a = art.querySelector('a');
                    const title = ((a && a.innerText) || (art.querySelector('h3,h4') || {}).innerText || '').trim();
                    if (!title) return;
                    let source = '';
                    const timeEl = art.querySelector('time');
                    if (timeEl && timeEl.previousElementSibling) source = (timeEl.previousElementSibling.innerText || '').trim();
                    if (!source) source = (art.innerText.split('\\n')[1] || '').trim();
                    const time = ((timeEl && (timeEl.innerText || timeEl.getAttribute('datetime'))) || '').slice(0, 40);
                    out.push({ title: title.slice(0, 200), url: (a && a.href) || '', source: source.slice(0, 80), time });
                  });
                  return out.slice(0, 25);
                }"""
            )
            rec["articles"] = arts if isinstance(arts, list) else []
        except Exception as exc:  # noqa: BLE001
            rec["message"] = str(exc)[:180]
        keywords.append(rec)
        time.sleep(0.7)
    total = sum(len(k.get("articles") or []) for k in keywords)
    return {
        "ok": total > 0,
        "scraped_at": _now(),
        "summary": f"{total} haber · {len(keywords)} kelime",
        "message": "",
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

    public = [j for j in wanted if j in ("competitors", "sikayet", "store_charts", "google_news")]
    google_jobs = [j for j in wanted if j == "serp"]
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
