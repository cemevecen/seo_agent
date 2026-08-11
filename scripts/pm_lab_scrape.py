#!/usr/bin/env python3
"""Owner PM lab — seçilen tarama maddeleri (Mac Firefox → Railway ingest).

  .venv/bin/python scripts/pm_lab_scrape.py --sync --ingest
  .venv/bin/python scripts/pm_lab_scrape.py --jobs serp,competitors --ingest

Env:
  PM_LAB_INGEST_URL          default …/api/pm-lab/ingest
  NOTIFICATION_INGEST_TOKEN
  PM_LAB_HEADLESS=1
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
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

from backend.services.scrape_browser import (  # noqa: E402
    asc_profile_dir,
    google_profile_dir,
    launch_ephemeral,
    launch_persistent,
)

INGEST_URL = (
    os.environ.get("PM_LAB_INGEST_URL")
    or os.environ.get("PLAY_CONSOLE_INGEST_URL", "").replace("play-console", "pm-lab")
    or "https://projectcontrol.up.railway.app/api/pm-lab/ingest"
).strip()

SERP_KEYWORDS = (
    "gümüş",
    "gram altın",
    "bitcoin",
    "harem altın",
    "dolar",
    "altın fiyatı",
)

COMPETITORS = (
    {"id": "bigpara", "label": "Bigpara", "url": "https://bigpara.hurriyet.com.tr/"},
    {"id": "uzmanpara", "label": "Uzmanpara", "url": "https://uzmanpara.milliyet.com.tr/"},
    {"id": "tradingview", "label": "TradingView", "url": "https://tr.tradingview.com/markets/currencies/rates-turkey/"},
    {"id": "canlidoviz", "label": "Canlı Döviz", "url": "https://www.canlidoviz.com/"},
    {"id": "investing", "label": "Investing", "url": "https://tr.investing.com/"},
)

ADS_DOMAINS = ("doviz.com", "sinemalar.com")
OUR_HOSTS = ("doviz.com", "canlidoviz.com")
PLAY_PACKAGE = "com.Doviz"
IOS_APP_ID = "465599322"
IOS_FINANCE_GENRE = 6015
GSC_RESOURCE = "sc-domain:doviz.com"

JOB_IDS = (
    "serp",
    "competitors",
    "ads_transparency",
    "sikayet",
    "app_rank",
    "store_charts",
    "google_news",
    "firebase_perf",
    "gsc_index",
    "apple_search_ads",
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


def _shot(page: Any, *, full_page: bool = False, quality: int = 42) -> str:
    try:
        raw = page.screenshot(type="jpeg", quality=quality, full_page=full_page)
    except Exception:
        try:
            raw = page.screenshot(type="jpeg", quality=quality, full_page=False)
        except Exception:
            return ""
    if not raw:
        return ""
    if len(raw) > 420_000 and full_page:
        try:
            raw = page.screenshot(type="jpeg", quality=34, full_page=False)
        except Exception:
            pass
    return base64.b64encode(raw).decode("ascii")


def _dismiss_consent(page: Any) -> None:
    names = (
        "Tümünü kabul et",
        "Accept all",
        "I agree",
        "Accept",
        "Kabul et",
        "Agree",
        "Reject all",
        "Tümünü reddet",
    )
    for name in names:
        try:
            loc = page.get_by_role("button", name=name)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=2500)
                page.wait_for_timeout(600)
                return
        except Exception:
            continue
    for sel in ("#L2AGLb", "button[aria-label='Accept all']", "button[aria-label='Tümünü kabul et']"):
        try:
            loc = page.locator(sel)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=2500)
                page.wait_for_timeout(600)
                return
        except Exception:
            continue


def _goto(page: Any, url: str, *, wait: str = "domcontentloaded", timeout: int = 60_000) -> None:
    page.goto(url, wait_until=wait, timeout=timeout)
    page.wait_for_timeout(900)
    _dismiss_consent(page)
    page.wait_for_timeout(400)


def _text(page: Any, limit: int = 8000) -> str:
    try:
        return (page.inner_text("body") or "")[:limit]
    except Exception:
        return ""


def _needs_google_login(page: Any) -> bool:
    try:
        url = (page.url or "").lower()
    except Exception:
        url = ""
    if "accounts.google.com" in url:
        return True
    try:
        body = (_text(page, 1500) or "").lower()
    except Exception:
        body = ""
    markers = (
        "email or phone",
        "e-posta veya telefon",
        "şifrenizi girin",
        "enter your password",
        "iki adımlı doğrulama",
        "verify it’s you",
        "verify it's you",
    )
    return any(m in body for m in markers)


def _domain_of(url: str) -> str:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""
    return host.removeprefix("www.")


def _our_rank(organic: list[dict[str, Any]]) -> tuple[int | None, str]:
    for row in organic:
        host = (row.get("domain") or _domain_of(str(row.get("url") or ""))).lower()
        if any(h in host for h in OUR_HOSTS):
            try:
                return int(row.get("rank") or 0) or None, str(row.get("url") or "")
            except (TypeError, ValueError):
                return None, str(row.get("url") or "")
    return None, ""


def _extract_serp(page: Any) -> dict[str, Any]:
    data = page.evaluate(
        """() => {
          const organic = [];
          const seen = new Set();
          const h3s = document.querySelectorAll('#search h3, #rso h3');
          h3s.forEach((h3) => {
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
              snippet = t.slice(1, 4).join(' ').slice(0, 280);
            }
            organic.push({ rank: organic.length + 1, title: (h3.innerText || '').trim(), url: href, domain: host, snippet });
          });
          const ads = [];
          document.querySelectorAll('#tads a, #tvcap a, [data-text-ad] a').forEach((a) => {
            const title = (a.innerText || '').trim().split('\\n')[0];
            if (title && title.length > 2) ads.push({ title: title.slice(0, 160), url: a.href || '' });
          });
          const paa = [];
          document.querySelectorAll('div[jsname] span, div[role="button"] span').forEach((el) => {
            const t = (el.innerText || '').trim();
            if (t.endsWith('?') && t.length > 12 && t.length < 140 && !paa.includes(t)) paa.push(t);
          });
          const related = [];
          document.querySelectorAll('#botstuff a, div.k8XOCe, a[data-ved]').forEach((a) => {
            const t = (a.innerText || '').trim();
            if (t && t.length < 80 && related.length < 12 && !related.includes(t) && !t.includes('http')) {
              if (a.closest('#search')) return;
              related.push(t);
            }
          });
          return { organic, ads: ads.slice(0, 12), paa: paa.slice(0, 8), related: related.slice(0, 10) };
        }"""
    )
    return data if isinstance(data, dict) else {}


def job_serp(page: Any) -> dict[str, Any]:
    keywords: list[dict[str, Any]] = []
    shots: dict[str, str] = {}
    for kw in SERP_KEYWORDS:
        slug = re.sub(r"[^a-z0-9]+", "_", kw.lower().replace("ü", "u").replace("ı", "i").replace("ş", "s").replace("ğ", "g").replace("ö", "o").replace("ç", "c"))
        pages_out: list[dict[str, Any]] = []
        all_organic: list[dict[str, Any]] = []
        ads_count = 0
        for start, pno in ((0, 1), (10, 2)):
            q = quote(kw)
            url = f"https://www.google.com/search?q={q}&hl=tr&gl=tr&pws=0&num=10&start={start}"
            try:
                _goto(page, url, timeout=75_000)
                page.wait_for_timeout(1200)
                parsed = _extract_serp(page)
            except Exception as exc:  # noqa: BLE001
                pages_out.append({"page": pno, "url": url, "error": str(exc)[:180], "organic": [], "ads": [], "paa": [], "related": []})
                continue
            organic = parsed.get("organic") or []
            ads = parsed.get("ads") or []
            ads_count += len(ads)
            all_organic.extend(organic)
            pages_out.append(
                {
                    "page": pno,
                    "url": url,
                    "organic": organic,
                    "ads": ads,
                    "paa": parsed.get("paa") or [],
                    "related": parsed.get("related") or [],
                    "captcha": "unusual traffic" in _text(page, 800).lower() or "/sorry/" in (page.url or ""),
                }
            )
            shot = _shot(page, full_page=True)
            if shot:
                shots[f"{slug}_p{pno}"] = shot
            time.sleep(1.4)
        rank, our_url = _our_rank(all_organic)
        keywords.append(
            {
                "keyword": kw,
                "our_rank": rank,
                "our_url": our_url,
                "ads_count": ads_count,
                "pages": pages_out,
            }
        )
    total_org = sum(len(p.get("organic") or []) for kw in keywords for p in kw.get("pages") or [])
    return {
        "ok": total_org > 0,
        "scraped_at": _now(),
        "summary": f"{len(keywords)} kelime · {len(shots)} fotoğraf · {total_org} organik",
        "message": "" if total_org else "SERP boş veya doğrulama istedi",
        "keywords": keywords,
        "shots": shots,
    }


_QUOTE_NAME_RE = re.compile(
    r"(dolar|usd|euro|eur|sterlin|gbp|gram\s*alt[ıi]n|alt[ıi]n|g[üu]m[üu][şs]|silver|bitcoin|btc|bts|çeyrek|yarım|ata)",
    re.I,
)
_PRICE_RE = re.compile(r"(?:₺|TL|\$|€)?\s*\d{1,3}(?:[.\s]\d{3})*(?:[.,]\d{2,4})")


def _quotes_from_text(text: str) -> list[dict[str, str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for i, ln in enumerate(lines):
        if not _QUOTE_NAME_RE.search(ln):
            continue
        name = ln[:48]
        window = " ".join(lines[i : i + 4])
        prices = _PRICE_RE.findall(window)
        if not prices:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        rec = {"name": name, "value": prices[0].strip()}
        if len(prices) > 1:
            rec["change"] = prices[1].strip()
        out.append(rec)
        if len(out) >= 16:
            break
    return out


def _extract_table_quotes(page: Any) -> list[dict[str, str]]:
    try:
        rows = page.evaluate(
            """() => {
              const out = [];
              const trs = document.querySelectorAll('table tr, [class*="row"]');
              trs.forEach((tr) => {
                const t = (tr.innerText || '').replace(/\\s+/g, ' ').trim();
                if (!t || t.length > 180) return;
                if (!/dolar|usd|euro|eur|alt[ıi]n|g[üu]m[üu][şs]|bitcoin|btc|sterlin/i.test(t)) return;
                out.push(t);
              });
              return out.slice(0, 24);
            }"""
        )
    except Exception:
        rows = []
    quotes: list[dict[str, str]] = []
    for raw in rows or []:
        prices = _PRICE_RE.findall(raw)
        name = _QUOTE_NAME_RE.search(raw)
        if not prices:
            continue
        quotes.append(
            {
                "name": (name.group(0) if name else raw.split()[0])[:40],
                "value": prices[0].strip(),
                "change": prices[1].strip() if len(prices) > 1 else "",
            }
        )
        if len(quotes) >= 16:
            break
    return quotes


def job_competitors(page: Any) -> dict[str, Any]:
    sites: list[dict[str, Any]] = []
    shots: dict[str, str] = {}
    for spec in COMPETITORS:
        rec: dict[str, Any] = {"id": spec["id"], "label": spec["label"], "url": spec["url"], "quotes": [], "notes": []}
        try:
            _goto(page, spec["url"], timeout=75_000)
            page.wait_for_timeout(1800)
            quotes = _extract_table_quotes(page)
            if len(quotes) < 3:
                quotes = _quotes_from_text(_text(page, 12000))
            rec["quotes"] = quotes
            rec["title"] = (page.title() or "")[:120]
            if not quotes:
                rec["message"] = "Fiyat satırı ayrıştırılamadı; fotoğraf kaydedildi."
                rec["notes"] = [ln for ln in _text(page, 2500).splitlines() if ln.strip()][:12]
            shot = _shot(page, full_page=False)
            if shot:
                shots[spec["id"]] = shot
        except Exception as exc:  # noqa: BLE001
            rec["message"] = str(exc)[:200]
        sites.append(rec)
        time.sleep(0.6)
    filled = sum(1 for s in sites if s.get("quotes"))
    return {
        "ok": filled > 0,
        "scraped_at": _now(),
        "summary": f"{filled}/{len(sites)} sitede fiyat",
        "message": "",
        "sites": sites,
        "shots": shots,
    }


def job_ads_transparency(page: Any) -> dict[str, Any]:
    advertisers: list[dict[str, str]] = []
    ads: list[dict[str, str]] = []
    raw_lines: list[str] = []
    shots: dict[str, str] = {}
    for domain in ADS_DOMAINS:
        url = f"https://adstransparency.google.com/?region=TR&domain={quote(domain)}"
        try:
            _goto(page, url, timeout=90_000)
            page.wait_for_timeout(2500)
            body = _text(page, 9000)
            raw_lines.append(f"--- {domain} ---")
            raw_lines.extend([ln.strip() for ln in body.splitlines() if ln.strip()][:40])
            parsed = page.evaluate(
                """() => {
                  const ads = [];
                  document.querySelectorAll('a, article, li').forEach((el) => {
                    const t = (el.innerText || '').trim();
                    if (!t || t.length < 8 || t.length > 240) return;
                    if (/advertiser|reklam veren|shown|gösterildi|format/i.test(t)) {
                      ads.push({ text: t.slice(0, 220), url: el.href || '' });
                    }
                  });
                  const advertisers = [];
                  document.querySelectorAll('h1,h2,h3,[role="heading"]').forEach((h) => {
                    const t = (h.innerText || '').trim();
                    if (t) advertisers.push({ name: t.slice(0, 120), detail: '' });
                  });
                  return { ads: ads.slice(0, 20), advertisers: advertisers.slice(0, 8) };
                }"""
            )
            if isinstance(parsed, dict):
                for a in parsed.get("ads") or []:
                    a["advertiser"] = domain
                    ads.append(a)
                for adv in parsed.get("advertisers") or []:
                    adv["detail"] = domain
                    advertisers.append(adv)
            shot = _shot(page, full_page=True)
            if shot:
                shots[domain.replace(".", "_")] = shot
        except Exception as exc:  # noqa: BLE001
            raw_lines.append(f"{domain}: {exc}"[:200])
        time.sleep(0.8)
    return {
        "ok": bool(ads or advertisers or shots),
        "scraped_at": _now(),
        "summary": f"{len(ads)} reklam satırı · {len(ADS_DOMAINS)} alan",
        "message": "" if ads or shots else "Transparency sayfası boş veya doğrulama",
        "advertisers": advertisers,
        "ads": ads,
        "raw_lines": raw_lines[:80],
        "shots": shots,
    }


def job_sikayet(page: Any) -> dict[str, Any]:
    shots: dict[str, str] = {}
    sikayetvar: dict[str, Any] = {"url": "https://www.sikayetvar.com/doviz", "items": []}
    try:
        _goto(page, sikayetvar["url"], timeout=75_000)
        page.wait_for_timeout(1500)
        if "bulunamadı" in _text(page, 400).lower() or page.url.endswith("/"):
            _goto(page, "https://www.sikayetvar.com/doviz-com", timeout=75_000)
            sikayetvar["url"] = page.url
        body = _text(page, 10000)
        score_m = re.search(r"(\d+[.,]\d)\s*/\s*5", body)
        count_m = re.search(r"(\d[\d.]*)\s*(?:şikayet|Şikayet)", body)
        sikayetvar["score"] = score_m.group(1) if score_m else ""
        sikayetvar["count"] = count_m.group(1) if count_m else ""
        solved_m = re.search(r"%\s*(\d+)|(\d+)\s*%", body)
        sikayetvar["solved"] = (solved_m.group(0) if solved_m else "")[:20]
        items = page.evaluate(
            """() => {
              const out = [];
              document.querySelectorAll('article, .card, a[href*="/doviz"]').forEach((el) => {
                const titleEl = el.querySelector('h2,h3,.title,a');
                const title = ((titleEl && titleEl.innerText) || '').trim();
                if (!title || title.length < 8) return;
                const meta = (el.innerText || '').split('\\n').slice(1, 3).join(' · ').slice(0, 160);
                const excerpt = (el.innerText || '').split('\\n').slice(3, 6).join(' ').slice(0, 280);
                out.push({ title: title.slice(0, 160), meta, excerpt });
              });
              return out.slice(0, 12);
            }"""
        )
        sikayetvar["items"] = items if isinstance(items, list) else []
        shot = _shot(page, full_page=False)
        if shot:
            shots["sikayetvar"] = shot
    except Exception as exc:  # noqa: BLE001
        sikayetvar["message"] = str(exc)[:200]

    eksi: dict[str, Any] = {"url": "https://eksisozluk.com/doviz-com", "title": "doviz.com", "entries": []}
    try:
        _goto(page, "https://eksisozluk.com/?q=doviz.com", timeout=75_000)
        page.wait_for_timeout(1200)
        eksi["url"] = page.url
        eksi["title"] = (page.title() or "doviz.com")[:120]
        entries = page.evaluate(
            """() => {
              const out = [];
              document.querySelectorAll('[id^="entry-item"], .content, li').forEach((el) => {
                const t = (el.innerText || '').trim();
                if (t.length > 40 && t.length < 500) out.push(t.slice(0, 420));
              });
              return out.slice(0, 8);
            }"""
        )
        eksi["entries"] = entries if isinstance(entries, list) else []
        if not eksi["entries"]:
            eksi["entries"] = [ln.strip() for ln in _text(page, 4000).splitlines() if len(ln.strip()) > 40][:8]
        shot = _shot(page, full_page=False)
        if shot:
            shots["eksi"] = shot
    except Exception as exc:  # noqa: BLE001
        eksi["message"] = str(exc)[:200]

    return {
        "ok": bool(sikayetvar.get("items") or eksi.get("entries") or shots),
        "scraped_at": _now(),
        "summary": f"şikayet {len(sikayetvar.get('items') or [])} · ekşi {len(eksi.get('entries') or [])}",
        "message": "",
        "sikayetvar": sikayetvar,
        "eksi": eksi,
        "shots": shots,
    }


def job_app_rank(page: Any) -> dict[str, Any]:
    shots: dict[str, str] = {}
    play: dict[str, Any] = {}
    ios: dict[str, Any] = {}
    third: list[dict[str, Any]] = []

    play_url = f"https://play.google.com/store/apps/details?id={PLAY_PACKAGE}&hl=tr&gl=tr"
    try:
        _goto(page, play_url, timeout=75_000)
        body = _text(page, 8000)
        rank_m = re.search(r"#\s*([\d.]+)\s*(?:sırada|in)?\s*([^\n]{0,40})?", body, re.I)
        inst_m = re.search(r"([\d.,]+\s*[KkMm+]*\s*(?:indirme|downloads|\+))", body, re.I)
        score_m = re.search(r"(\d[.,]\d)\s*(?:star|yıldız|\n)", body)
        play = {
            "url": play_url,
            "rank": rank_m.group(1) if rank_m else "",
            "rank_label": rank_m.group(0).strip() if rank_m else "",
            "category": (rank_m.group(2) or "").strip() if rank_m and rank_m.lastindex and rank_m.lastindex >= 2 else "Finans",
            "installs": inst_m.group(1).strip() if inst_m else "",
            "score": score_m.group(1) if score_m else "",
        }
        try:
            from google_play_scraper import app as gp_app

            meta = gp_app(PLAY_PACKAGE, lang="tr", country="tr")
            play["installs"] = play["installs"] or str(meta.get("installs") or meta.get("realInstalls") or "")
            play["score"] = play["score"] or str(meta.get("score") or "")
            play["ratings"] = str(meta.get("ratings") or "")
            play["category"] = play["category"] or str((meta.get("genre") or ""))
            play["title"] = meta.get("title") or ""
        except Exception:
            pass
        shot = _shot(page, full_page=False)
        if shot:
            shots["play_details"] = shot
    except Exception as exc:  # noqa: BLE001
        play = {"message": str(exc)[:200], "url": play_url}

    ios_url = f"https://apps.apple.com/tr/app/id{IOS_APP_ID}"
    try:
        _goto(page, ios_url, timeout=75_000)
        body = _text(page, 8000)
        rank_m = re.search(r"#\s*(\d+)\s+in\s+([^\n]{2,40})", body, re.I)
        if not rank_m:
            rank_m = re.search(r"(\d+)\.?\s*(?:sırada|sıra).*?(Finans|Finance)", body, re.I)
        ios = {
            "url": ios_url,
            "rank": rank_m.group(1) if rank_m else "",
            "rank_label": rank_m.group(0).strip() if rank_m else "",
            "category": (rank_m.group(2).strip() if rank_m and rank_m.lastindex and rank_m.lastindex >= 2 else "Finance"),
            "chart": "top-free",
        }
        try:
            lookup_url = f"https://itunes.apple.com/lookup?id={IOS_APP_ID}&country=tr"
            with urllib.request.urlopen(lookup_url, timeout=20) as resp:
                info = json.loads(resp.read().decode("utf-8", errors="replace"))
            res = (info.get("results") or [{}])[0]
            ios["score"] = str(res.get("averageUserRating") or "")
            ios["category"] = ios.get("category") or str(res.get("primaryGenreName") or "")
            ios["title"] = res.get("trackName") or ""
        except Exception:
            pass
        shot = _shot(page, full_page=False)
        if shot:
            shots["ios_details"] = shot
    except Exception as exc:  # noqa: BLE001
        ios = {"message": str(exc)[:200], "url": ios_url}

    third_urls = (
        ("Sensor Tower", f"https://sensortower.com/ios/tr/finance/app/doviz/465599322"),
        ("data.ai", f"https://www.data.ai/apps/ios/app/{IOS_APP_ID}/app-overview/"),
    )
    for name, url in third_urls:
        rec = {"name": name, "url": url, "status": "ok", "notes": ""}
        try:
            _goto(page, url, timeout=60_000)
            body = _text(page, 3500)
            low = body.lower()
            if any(x in low for x in ("sign in", "log in", "oturum", "create account", "subscribe")):
                rec["status"] = "oturum / duvar"
            rec["notes"] = " ".join(ln.strip() for ln in body.splitlines() if ln.strip())[:400]
            shot = _shot(page, full_page=False)
            if shot:
                shots[name.lower().replace(" ", "_").replace(".", "_")] = shot
        except Exception as exc:  # noqa: BLE001
            rec["status"] = "hata"
            rec["notes"] = str(exc)[:200]
        third.append(rec)
        time.sleep(0.5)

    return {
        "ok": bool(play or ios),
        "scraped_at": _now(),
        "summary": f"Play {play.get('rank_label') or '—'} · iOS {ios.get('rank_label') or '—'}",
        "message": "",
        "play": play,
        "ios": ios,
        "third_party": third,
        "shots": shots,
    }


def job_store_charts(page: Any) -> dict[str, Any]:
    shots: dict[str, str] = {}
    charts: list[dict[str, Any]] = []

    play_url = "https://play.google.com/store/apps/category/FINANCE?hl=tr&gl=tr"
    try:
        _goto(page, play_url, timeout=75_000)
        page.wait_for_timeout(2000)
        try:
            page.mouse.wheel(0, 2400)
            page.wait_for_timeout(800)
        except Exception:
            pass
        apps = page.evaluate(
            """() => {
              const out = [];
              const seen = new Set();
              document.querySelectorAll('a[href*="/store/apps/details?id="]').forEach((a) => {
                const href = a.href || '';
                const m = href.match(/id=([^&]+)/);
                if (!m) return;
                const id = decodeURIComponent(m[1]);
                if (seen.has(id)) return;
                seen.add(id);
                const name = ((a.getAttribute('aria-label') || a.innerText || '').trim().split('\\n')[0] || id).slice(0, 80);
                out.push({ rank: out.length + 1, name, subtitle: id, package: id, is_ours: id === 'com.Doviz' });
              });
              return out.slice(0, 40);
            }"""
        )
        apps = apps if isinstance(apps, list) else []
        ours = next((a for a in apps if a.get("is_ours")), None)
        charts.append(
            {
                "title": "Play · Finans (TR)",
                "url": play_url,
                "our_label": f"Döviz #{ours['rank']}" if ours else "Döviz listede (ilk 40) yok",
                "apps": apps,
            }
        )
        shot = _shot(page, full_page=False)
        if shot:
            shots["play_finance"] = shot
    except Exception as exc:  # noqa: BLE001
        charts.append({"title": "Play · Finans", "our_label": str(exc)[:160], "apps": []})

    ios_url = f"https://apps.apple.com/tr/iphone/charts/{IOS_FINANCE_GENRE}"
    ios_apps: list[dict[str, Any]] = []
    try:
        with urllib.request.urlopen(
            urllib.request.Request(ios_url, headers={"User-Agent": "Mozilla/5.0"}),
            timeout=25,
        ) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        m = re.search(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL)
        if m:
            page_data = json.loads(m.group(1))
            segments = ((page_data.get("data") or [{}])[0].get("data") or {}).get("segments") or []
            for segment in segments:
                if segment.get("chart") not in ("top-free", "top-free-iphone", ""):
                    if segment.get("chart") and "free" not in str(segment.get("chart")):
                        continue
                ids: list[str] = []
                names: dict[str, str] = {}
                for shelf in segment.get("shelves") or []:
                    for item in shelf.get("items") or []:
                        if isinstance(item, dict) and item.get("id"):
                            ids.append(str(item["id"]))
                            names[str(item["id"])] = str(item.get("name") or item.get("title") or "")
                for item in (segment.get("nextPage") or {}).get("remainingContent") or []:
                    if isinstance(item, dict) and item.get("id"):
                        ids.append(str(item["id"]))
                        names[str(item["id"])] = str(item.get("name") or "")
                for i, aid in enumerate(ids[:40], 1):
                    ios_apps.append(
                        {
                            "rank": i,
                            "name": names.get(aid) or aid,
                            "subtitle": aid,
                            "is_ours": aid == IOS_APP_ID,
                        }
                    )
                if ios_apps:
                    break
        ours = next((a for a in ios_apps if a.get("is_ours")), None)
        charts.append(
            {
                "title": "App Store · Finance ücretsiz (TR)",
                "url": ios_url,
                "our_label": f"Döviz #{ours['rank']}" if ours else "Döviz listede (ilk 40) yok",
                "apps": ios_apps,
            }
        )
    except Exception as exc:  # noqa: BLE001
        charts.append({"title": "App Store · Finance", "our_label": str(exc)[:160], "apps": []})

    try:
        _goto(page, ios_url, timeout=60_000)
        shot = _shot(page, full_page=False)
        if shot:
            shots["ios_finance"] = shot
    except Exception:
        pass

    return {
        "ok": any((c.get("apps") for c in charts)),
        "scraped_at": _now(),
        "summary": " · ".join(c.get("our_label") or c.get("title") or "" for c in charts),
        "message": "",
        "charts": charts,
        "shots": shots,
    }


def job_google_news(page: Any) -> dict[str, Any]:
    keywords: list[dict[str, Any]] = []
    shots: dict[str, str] = {}
    for kw in SERP_KEYWORDS:
        slug = re.sub(r"[^a-z0-9]+", "_", kw.lower().replace("ü", "u").replace("ı", "i").replace("ş", "s").replace("ğ", "g").replace("ö", "o").replace("ç", "c"))
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
                    const source = ((art.querySelector('time') && art.querySelector('time').previousElementSibling)
                      ? art.querySelector('time').previousElementSibling.innerText
                      : (art.innerText.split('\\n')[1] || '')).trim().slice(0, 80);
                    const time = ((art.querySelector('time') && (art.querySelector('time').innerText || art.querySelector('time').getAttribute('datetime'))) || '').slice(0, 40);
                    out.push({ title: title.slice(0, 180), url: (a && a.href) || '', source, time });
                  });
                  return out.slice(0, 12);
                }"""
            )
            rec["articles"] = arts if isinstance(arts, list) else []
            if not rec["articles"]:
                rec["articles"] = [
                    {"title": ln.strip()[:180], "url": url, "source": "", "time": ""}
                    for ln in _text(page, 3000).splitlines()
                    if 24 < len(ln.strip()) < 160
                ][:8]
            if kw in ("dolar", "altın fiyatı", "bitcoin"):
                shot = _shot(page, full_page=False)
                if shot:
                    shots[slug] = shot
        except Exception as exc:  # noqa: BLE001
            rec["message"] = str(exc)[:180]
        keywords.append(rec)
        time.sleep(0.8)
    total = sum(len(k.get("articles") or []) for k in keywords)
    return {
        "ok": total > 0,
        "scraped_at": _now(),
        "summary": f"{total} haber · {len(keywords)} kelime",
        "message": "",
        "keywords": keywords,
        "shots": shots,
    }


def _metric_cards_from_text(text: str) -> list[dict[str, str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: list[dict[str, str]] = []
    keys = (
        "app start",
        "slow",
        "frozen",
        "network",
        "screen",
        "trace",
        "crash-free",
        "anr",
        "duration",
        "success",
        "http",
        "lcp",
        "cold",
        "warm",
        "başlat",
        "ağ",
        "ekran",
        "iz",
    )
    for i, ln in enumerate(lines):
        low = ln.lower()
        if any(k in low for k in keys) and i + 1 < len(lines):
            nxt = lines[i + 1]
            if re.search(r"\d", nxt) and len(nxt) < 40:
                out.append({"name": ln[:48], "value": nxt[:40]})
        if len(out) >= 12:
            break
    return out


def job_firebase_perf(page: Any) -> dict[str, Any]:
    platforms = (
        {
            "id": "android",
            "label": "Android · doviz-android",
            "url": "https://console.firebase.google.com/u/0/project/doviz-android/performance",
        },
        {
            "id": "ios",
            "label": "iOS · doviz-ios",
            "url": "https://console.firebase.google.com/u/0/project/doviz-ios/performance",
        },
    )
    out: list[dict[str, Any]] = []
    shots: dict[str, str] = {}
    login_block = False
    for spec in platforms:
        rec: dict[str, Any] = {**spec, "metrics": [], "traces": []}
        try:
            _goto(page, spec["url"], timeout=90_000)
            page.wait_for_timeout(2500)
            if _needs_google_login(page):
                login_block = True
                rec["message"] = "Google oturumu gerekli (fx-google)."
            else:
                body = _text(page, 9000)
                rec["metrics"] = _metric_cards_from_text(body)
                rec["traces"] = [ln.strip() for ln in body.splitlines() if "trace" in ln.lower() or "custom" in ln.lower()][:12]
                if not rec["metrics"]:
                    rec["notes_preview"] = [ln.strip() for ln in body.splitlines() if ln.strip()][:20]
            shot = _shot(page, full_page=False)
            if shot:
                shots[spec["id"]] = shot
        except Exception as exc:  # noqa: BLE001
            rec["message"] = str(exc)[:200]
        out.append(rec)
    return {
        "ok": (not login_block) and any(p.get("metrics") or p.get("traces") for p in out),
        "scraped_at": _now(),
        "summary": "oturum gerekli" if login_block else f"{len(out)} proje",
        "message": "Firebase Performance için fx-google oturumu yok." if login_block else "",
        "platforms": out,
        "shots": shots,
    }


def _stats_pairs(text: str) -> list[list[str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    pairs: list[list[str]] = []
    for i, ln in enumerate(lines):
        if i + 1 >= len(lines):
            break
        nxt = lines[i + 1]
        if len(ln) < 48 and re.search(r"\d", nxt) and len(nxt) < 36:
            if re.search(r"crawl|tarama|index|dizin|response|yanıt|page|sayfa|not indexed|hariç|excluded|fetched", ln, re.I):
                pairs.append([ln, nxt])
        if len(pairs) >= 16:
            break
    return pairs


def job_gsc_index(page: Any) -> dict[str, Any]:
    rid = quote(GSC_RESOURCE, safe="")
    crawl_url = f"https://search.google.com/u/0/search-console/settings/crawl-stats?resource_id={rid}&hl=tr"
    index_url = f"https://search.google.com/u/0/search-console/index?resource_id={rid}&hl=tr"
    shots: dict[str, str] = {}
    crawl: dict[str, Any] = {"url": crawl_url, "stats": []}
    index: dict[str, Any] = {"url": index_url, "stats": [], "reasons": []}
    login_block = False

    try:
        _goto(page, crawl_url, timeout=90_000)
        page.wait_for_timeout(2200)
        if _needs_google_login(page):
            login_block = True
            crawl["message"] = "Google oturumu gerekli."
        else:
            crawl["stats"] = _stats_pairs(_text(page, 9000))
            if not crawl["stats"]:
                crawl["preview"] = [ln.strip() for ln in _text(page, 3000).splitlines() if ln.strip()][:24]
        shot = _shot(page, full_page=False)
        if shot:
            shots["crawl"] = shot
    except Exception as exc:  # noqa: BLE001
        crawl["message"] = str(exc)[:200]

    try:
        _goto(page, index_url, timeout=90_000)
        page.wait_for_timeout(2200)
        if _needs_google_login(page):
            login_block = True
            index["message"] = "Google oturumu gerekli."
        else:
            body = _text(page, 12000)
            index["stats"] = _stats_pairs(body)
            reasons = page.evaluate(
                """() => {
                  const out = [];
                  document.querySelectorAll('tr, li, [role="row"]').forEach((el) => {
                    const t = (el.innerText || '').replace(/\\s+/g, ' ').trim();
                    if (!t || t.length > 160) return;
                    const m = t.match(/(\\d[\\d.\\s]*)$/);
                    if (!m) return;
                    const reason = t.slice(0, t.length - m[1].length).trim();
                    if (reason.length < 4) return;
                    out.push({ reason, count: m[1].trim() });
                  });
                  return out.slice(0, 20);
                }"""
            )
            index["reasons"] = reasons if isinstance(reasons, list) else []
        shot = _shot(page, full_page=True)
        if shot:
            shots["index"] = shot
    except Exception as extra:  # noqa: BLE001
        index["message"] = str(extra)[:200]

    return {
        "ok": (not login_block) and bool(crawl.get("stats") or index.get("reasons") or shots),
        "scraped_at": _now(),
        "summary": "oturum gerekli" if login_block else f"{len(index.get('reasons') or [])} neden",
        "message": "GSC için fx-google oturumu yok." if login_block else "",
        "crawl": crawl,
        "index": index,
        "shots": shots,
    }


def job_apple_search_ads(page: Any) -> dict[str, Any]:
    url = "https://app.searchads.apple.com/cm/app"
    shots: dict[str, str] = {}
    campaigns: list[dict[str, str]] = []
    raw_lines: list[str] = []
    message = ""
    try:
        _goto(page, url, timeout=90_000)
        page.wait_for_timeout(2500)
        body = _text(page, 10000)
        raw_lines = [ln.strip() for ln in body.splitlines() if ln.strip()][:40]
        low = body.lower()
        if any(x in low for x in ("sign in", "apple id", "oturum aç", "log in")):
            message = "Apple Search Ads oturumu gerekli (Apple ID)."
        rows = page.evaluate(
            """() => {
              const out = [];
              document.querySelectorAll('table tr, [role="row"]').forEach((tr) => {
                const cells = Array.from(tr.querySelectorAll('td,th,[role="cell"]')).map(c => (c.innerText || '').trim());
                if (cells.length >= 3 && cells[0] && cells[0].toLowerCase() !== 'campaign') {
                  out.push({
                    name: cells[0].slice(0, 80),
                    status: (cells[1] || '').slice(0, 40),
                    spend: (cells[2] || '').slice(0, 40),
                    installs: (cells[3] || '').slice(0, 40),
                    note: (cells.slice(4).join(' · ') || '').slice(0, 80),
                  });
                }
              });
              return out.slice(0, 20);
            }"""
        )
        campaigns = rows if isinstance(rows, list) else []
        shot = _shot(page, full_page=False)
        if shot:
            shots["asa"] = shot
    except Exception as exc:  # noqa: BLE001
        message = str(exc)[:200]
    return {
        "ok": bool(campaigns),
        "scraped_at": _now(),
        "summary": f"{len(campaigns)} kampanya" if campaigns else (message or "boş"),
        "message": message,
        "campaigns": campaigns,
        "raw_lines": raw_lines,
        "shots": shots,
    }


def _write_scratch(job: str, payload: dict[str, Any]) -> None:
    out = ROOT / "scratch" / f"pm_lab_{job}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    slim = dict(payload)
    shots = slim.get("shots") if isinstance(slim.get("shots"), dict) else {}
    slim["shots"] = sorted(shots.keys())
    out.write_text(json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_job(name: str, fn, page: Any, *, ingest: bool) -> dict[str, Any]:
    print(f"== {name}", flush=True)
    try:
        result = fn(page)
    except Exception as exc:  # noqa: BLE001
        result = {"ok": False, "scraped_at": _now(), "message": str(exc)[:240], "summary": "hata"}
    _write_scratch(name, result)
    nshot = len(result.get("shots") or {})
    print(f"  · {result.get('summary') or result.get('message') or ''} · shots={nshot} · ok={result.get('ok')}", flush=True)
    if ingest:
        ing = post_ingest({name: result}, message=f"{name} tarama")
        print(f"  · ingest: {ing}", flush=True)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Owner PM lab taramaları")
    parser.add_argument("--sync", action="store_true", help="Tüm maddeler")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--jobs", default="", help="virgülle job id")
    args = parser.parse_args(argv)

    wanted = [j.strip() for j in (args.jobs or "").split(",") if j.strip()]
    if args.sync or not wanted:
        wanted = list(JOB_IDS)
    for j in wanted:
        if j not in JOB_IDS:
            print(f"bilinmeyen job: {j}", flush=True)
            return 2

    headed = bool(args.headed) or os.environ.get("PM_LAB_HEADLESS", "").strip() in ("0", "false", "no")
    if os.environ.get("PM_LAB_HEADLESS", "").strip() in ("1", "true", "yes"):
        headed = False

    from playwright.sync_api import sync_playwright

    public = [j for j in wanted if j in ("serp", "competitors", "ads_transparency", "sikayet", "app_rank", "store_charts", "google_news")]
    google_jobs = [j for j in wanted if j in ("firebase_perf", "gsc_index")]
    asa_jobs = [j for j in wanted if j == "apple_search_ads"]

    fns = {
        "serp": job_serp,
        "competitors": job_competitors,
        "ads_transparency": job_ads_transparency,
        "sikayet": job_sikayet,
        "app_rank": job_app_rank,
        "store_charts": job_store_charts,
        "google_news": job_google_news,
        "firebase_perf": job_firebase_perf,
        "gsc_index": job_gsc_index,
        "apple_search_ads": job_apple_search_ads,
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

        if asa_jobs:
            ctx = launch_persistent(
                pw,
                asc_profile_dir(),
                headed=headed,
                viewport={"width": 1440, "height": 1100},
            )
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                for name in asa_jobs:
                    res = _run_job(name, fns[name], page, ingest=bool(args.ingest))
                    if not res.get("ok"):
                        failures += 1
            finally:
                ctx.close()

    print(f"bitti · failures={failures}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
