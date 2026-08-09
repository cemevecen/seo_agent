#!/usr/bin/env python3
"""Google Search Console Core Web Vitals + AMP scrape (Mac bridge).

Playwright (play-console-profile) ile GSC CWV / AMP raporlarını çeker → Railway ingest.
Satır limiti yok — tablo sonuna kadar kaydırılır.

  .venv/bin/python scripts/gsc_cwv_scrape.py --login
  .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest
  .venv/bin/python scripts/gsc_cwv_scrape.py --sync --ingest --site doviz

Env:
  GSC_CWV_PROFILE_DIR / GSC_LINKS_PROFILE_DIR / PLAY_CONSOLE_PROFILE_DIR
  GSC_CWV_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key, val = key.strip(), val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

PROFILE_DIR = Path(
    os.environ.get("GSC_CWV_PROFILE_DIR")
    or os.environ.get("GSC_LINKS_PROFILE_DIR")
    or os.environ.get("PLAY_CONSOLE_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "play-console-profile")
).expanduser()

INGEST_URL = (
    os.environ.get("GSC_CWV_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
).strip()

# device=2 → Mobil, device=1 → Masaüstü (GSC UI)
DEVICE_MOBILE = 2
DEVICE_DESKTOP = 1

PROPERTIES: list[dict[str, str]] = [
    {
        "site_key": "doviz",
        "site_domain": "www.doviz.com",
        "resource_id": "sc-domain:doviz.com",
        "label": "doviz.com",
    },
    {
        "site_key": "sinemalar",
        "site_domain": "www.sinemalar.com",
        "resource_id": "https://www.sinemalar.com/",
        "label": "www.sinemalar.com",
    },
]

_PUA_RE = re.compile(r"[\ue000-\uf8ff\u0000-\u001f]")
_NUM_RE = re.compile(r"([\d\.\,]+)\s*(?:B|K|M)?", re.I)


def _ingest_token() -> str:
    return (
        os.environ.get("GSC_CWV_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("GSC_LINKS_INGEST_TOKEN")
        or ""
    ).strip()


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", _PUA_RE.sub("", s or "")).strip()


def _parse_count(raw: str) -> int:
    s = _clean(raw).replace("\u00a0", " ")
    if not s or s in {"-", "—", "Yok", "N/A"}:
        return 0
    # 14,3 B / 14.347 / 1,97 B
    m = re.search(r"([\d\.\,]+)\s*([BbKkMm])?", s)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else 0
    num = m.group(1).replace(".", "").replace(",", ".")
    try:
        val = float(num)
    except ValueError:
        return 0
    suf = (m.group(2) or "").upper()
    if suf == "B":  # bin (TR)
        val *= 1000
    elif suf == "K":
        val *= 1000
    elif suf == "M":
        val *= 1_000_000
    # TR binlik: 14.347
    if suf == "" and "," not in m.group(1) and m.group(1).count(".") == 1:
        # already handled by replace
        pass
    if suf == "" and "." in m.group(1) and "," not in m.group(1):
        # 14.347 → already removed dots above incorrectly if US style
        # recover: original had dots as thousands
        raw_digits = m.group(1)
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", raw_digits):
            val = float(raw_digits.replace(".", ""))
    return int(round(val))


def _cwv_url(resource_id: str, path: str = "", **params: Any) -> str:
    rid = quote(resource_id, safe="")
    base = f"https://search.google.com/u/0/search-console/core-web-vitals{path}"
    q = f"resource_id={rid}&hl=en"
    for k, v in params.items():
        if v is None or v == "":
            continue
        q += f"&{k}={quote(str(v), safe='')}"
    return f"{base}?{q}"


def _amp_url(resource_id: str, path: str = "", **params: Any) -> str:
    rid = quote(resource_id, safe="")
    base = f"https://search.google.com/u/0/search-console/amp{path}"
    q = f"resource_id={rid}&hl=en"
    for k, v in params.items():
        if v is None or v == "":
            continue
        q += f"&{k}={quote(str(v), safe='')}"
    return f"{base}?{q}"


def _looks_signed_in(page) -> bool:
    try:
        url = (page.url or "").lower()
        if "accounts.google.com" in url or "signin" in url:
            return False
        body = ""
        try:
            body = (page.inner_text("body") or "")[:800].lower()
        except Exception:
            pass
        if "email or phone" in body or "e-posta veya telefon" in body:
            return False
        return "search.google.com/search-console" in url or "search.google.com/u/" in url
    except Exception:
        return False


def _launch_context(*, headed: bool):
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (PROFILE_DIR / name).unlink(missing_ok=True)
        except Exception:
            pass
    pw = sync_playwright().start()
    channel = (
        os.environ.get("GSC_CWV_BROWSER_CHANNEL")
        or os.environ.get("GSC_LINKS_BROWSER_CHANNEL")
        or os.environ.get("PLAY_CONSOLE_BROWSER_CHANNEL")
        or "chrome"
    ).strip()
    kwargs: dict[str, Any] = {
        "user_data_dir": str(PROFILE_DIR),
        "headless": not headed,
        "viewport": {"width": 1440, "height": 1100},
        "locale": "en-US",
        "accept_downloads": True,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    if channel and channel.lower() not in ("0", "none", "chromium"):
        kwargs["channel"] = channel
    try:
        context = pw.chromium.launch_persistent_context(**kwargs)
    except Exception:
        kwargs.pop("channel", None)
        context = pw.chromium.launch_persistent_context(**kwargs)
    return pw, context


def run_login_interactive(timeout_sec: int = 600) -> dict[str, Any]:
    url = _cwv_url("sc-domain:doviz.com")
    pw, context = _launch_context(headed=True)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        print(
            f"Tarayıcıda GSC giriş yap. CWV sayfası açılınca {timeout_sec}s içinde otomatik kapanır.",
            flush=True,
        )
        deadline = time.time() + max(60, timeout_sec)
        while time.time() < deadline:
            if _looks_signed_in(page) and "core-web-vitals" in (page.url or ""):
                time.sleep(2)
                return {"ok": True, "url": page.url, "profile": str(PROFILE_DIR)}
            time.sleep(2)
        return {"ok": False, "message": "Login zaman aşımı", "url": page.url, "profile": str(PROFILE_DIR)}
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _scroll_table_fully(page, *, max_rounds: int = 800) -> int:
    """Tabloyu sonuna kadar kaydır — uygulama tarafında satır limiti yok."""
    try:
        return int(
            page.evaluate(
                """async (maxRounds) => {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const rowCount = () => document.querySelectorAll('table tbody tr').length;
  const scrollTargets = () => {
    const out = [];
    const table = document.querySelector('table');
    let el = table;
    while (el && el !== document.body) {
      const st = window.getComputedStyle(el);
      const oy = st.overflowY || st.overflow || '';
      if ((oy.includes('auto') || oy.includes('scroll')) && el.scrollHeight > el.clientHeight + 40) {
        out.push(el);
      }
      el = el.parentElement;
    }
    if (document.scrollingElement) out.push(document.scrollingElement);
    out.push(document.documentElement, document.body);
    return out;
  };
  const clickMore = () => {
    const nodes = [...document.querySelectorAll('button, a, [role=button], span, div')];
    for (const n of nodes) {
      const t = ((n.innerText || n.textContent || '') + '').trim().toLowerCase();
      if (!t || t.length > 48) continue;
      if (t.includes('daha fazla') || t.includes('show more') || t.includes('load more')) {
        try { n.click(); return true; } catch (_) {}
      }
    }
    return false;
  };
  let last = rowCount();
  let stable = 0;
  for (let i = 0; i < maxRounds; i++) {
    clickMore();
    for (const el of scrollTargets()) {
      try { el.scrollTop = el.scrollHeight; } catch (_) {}
    }
    try { window.scrollTo(0, document.body.scrollHeight); } catch (_) {}
    await sleep(280);
    const now = rowCount();
    if (now <= last) {
      stable += 1;
      if (stable >= 10) break;
    } else {
      stable = 0;
      last = now;
    }
  }
  return rowCount();
}""",
                max_rounds,
            )
        )
    except Exception:
        return 0


def _extract_table(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const clean = (s) => (s || '').replace(/[\\ue000-\\uf8ff]/g, '').replace(/\\s+/g, ' ').trim();
      const table = document.querySelector('table');
      if (!table) return { headers: [], rows: [], row_count: 0 };
      const headers = [...table.querySelectorAll('thead th, thead td')].map((el) => clean(el.innerText));
      const rows = [...table.querySelectorAll('tbody tr')].map((tr) =>
        [...tr.querySelectorAll('td')].map((td) => clean(td.innerText))
      );
      return { headers, rows, row_count: rows.length };
    }"""
    )


def _extract_page_meta(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const clean = (s) => (s || '').replace(/[\\ue000-\\uf8ff]/g, '').replace(/\\s+/g, ' ').trim();
      const body = clean((document.body && document.body.innerText) || '');
      const title = clean((document.querySelector('h1, [role=heading]') || {}).innerText || '');
      return { title, body_head: body.slice(0, 2500), url: location.href };
    }"""
    )


def _status_from_text(text: str) -> str:
    t = (text or "").lower()
    if "yetersiz" in t or "poor" in t or "kötü" in t or t.startswith("error"):
        return "poor"
    if "iyileştir" in t or "needs" in t or "improvement" in t or "warning" in t:
        return "needs_improvement"
    if "iyi" in t or "good" in t or "check_circle" in t:
        return "good"
    return "unknown"


def _metric_from_issue(title: str) -> str:
    t = (title or "").upper()
    for m in ("LCP", "INP", "CLS", "FID", "FCP", "TTFB"):
        if m in t:
            return m
    if "görüntü" in (title or "").lower() or "image" in (title or "").lower():
        return "AMP_IMAGE"
    return "OTHER"


def explain_causes(metric: str, status: str, title: str = "") -> list[str]:
    """Olası sebepler — panoda ve mailde gösterilir."""
    m = (metric or "").upper()
    causes: list[str] = []
    if m == "LCP":
        causes = [
            "Büyük hero / LCP görselleri (özellikle /amp sayfalarında boyut önerisinin altında kalan img).",
            "Yavaş sunucu yanıtı (TTFB) veya render-blocking CSS/JS.",
            "Client-side hydration / geç yüklenen kritik içerik.",
            "Üçüncü taraf reklam veya widget’ların LCP öğesini geciktirmesi.",
        ]
    elif m == "INP":
        causes = [
            "Uzun ana-thread görevleri (ağır JS, senkron iş).",
            "Tıklama/scroll sırasında pahalı event handler’lar.",
            "Çok sayıda üçüncü taraf script (ads, analytics, chat).",
            "Büyük DOM ve sık layout thrashing.",
        ]
    elif m == "CLS":
        causes = [
            "Boyutsuz görseller / iframe / reklam slotları.",
            "Web font swap (FOUT) ile metin kayması.",
            "Dinamik enjekte edilen banner / sticky elemanlar.",
            "Lazy-load içeriklerin yer tutucu olmadan gelmesi.",
        ]
    elif m == "AMP_IMAGE":
        causes = [
            "AMP görsellerinin önerilen boyuttan küçük olması (geçerli ama best-practice dışı).",
            "srcset / width-height eksikliği.",
            "CDN dönüşümlerinde düşük çözünürlük üretimi.",
        ]
    else:
        causes = [
            "CrUX alan verisinde eşik aşımı — sayfa şablonunu ve üçüncü tarafları gözden geçirin.",
            f"GSC sorunu: {title or metric}",
        ]
    if status == "poor":
        causes.insert(0, "Durum: Poor — kullanıcı deneyimi eşiğinin altında; öncelikli düzeltme.")
    elif status == "needs_improvement":
        causes.insert(0, "Durum: Needs improvement — Good bandına çekmek için iyileştirme gerekir.")
    return causes


def _parse_overview_counts(body: str) -> dict[str, dict[str, int]]:
    out = {
        "mobile": {"poor": 0, "needs_improvement": 0, "good": 0},
        "desktop": {"poor": 0, "needs_improvement": 0, "good": 0},
    }
    # Mobil ... Masaüstü ...
    low = body
    mob = re.search(
        r"Mobil.{0,80}?(\d[\d\.\\,]*)\s*kötü.{0,40}?([\d\.\\,]+(?:\s*B)?)\s*URL.{0,40}?iyileştir.{0,40}?([\d\.\\,]+(?:\s*B)?)\s*iyi",
        low,
        re.I | re.S,
    )
    if not mob:
        mob = re.search(
            r"Mobil.*?(\d[\d\.\\,]*)\s*kötü URL.*?([\d\.\\,]+)\s*URL'nin iyileştirilmesi.*?([\d\.\\,]+)\s*iyi URL",
            low,
            re.I | re.S,
        )
    desk = re.search(
        r"Masaüstü.{0,80}?(\d[\d\.\\,]*)\s*kötü.{0,40}?([\d\.\\,]+(?:\s*B)?)\s*URL.{0,40}?iyileştir.{0,40}?([\d\.\\,]+(?:\s*B)?)\s*iyi",
        low,
        re.I | re.S,
    )
    if not desk:
        desk = re.search(
            r"Masaüstü.*?(\d[\d\.\\,]*)\s*kötü URL.*?([\d\.\\,]+)\s*URL'nin iyileştirilmesi.*?([\d\.\\,]+)\s*iyi URL",
            low,
            re.I | re.S,
        )
    if mob:
        out["mobile"] = {
            "poor": _parse_count(mob.group(1)),
            "needs_improvement": _parse_count(mob.group(2)),
            "good": _parse_count(mob.group(3)),
        }
    if desk:
        out["desktop"] = {
            "poor": _parse_count(desk.group(1)),
            "needs_improvement": _parse_count(desk.group(2)),
            "good": _parse_count(desk.group(3)),
        }
    return out


def _wait_table(page, timeout_ms: int = 20000) -> None:
    try:
        page.wait_for_selector("table tbody tr", timeout=timeout_ms)
    except Exception:
        pass
    time.sleep(1.2)


def _scrape_url_table(page) -> list[dict[str, Any]]:
    _scroll_table_fully(page)
    raw = _extract_table(page)
    headers = [h.lower() for h in (raw.get("headers") or [])]
    rows_out: list[dict[str, Any]] = []
    for row in raw.get("rows") or []:
        if not row or not any(row):
            continue
        item: dict[str, Any] = {"cells": row}
        # URL
        url = ""
        for cell in row:
            if cell.startswith("http://") or cell.startswith("https://"):
                url = cell
                break
        if url:
            item["url"] = url
        # group count
        for i, h in enumerate(headers):
            if i >= len(row):
                break
            if "grup" in h or "url sayısı" in h or "urls" in h:
                item["group_url_count"] = _parse_count(row[i])
            if h.startswith("lcp") or "lcp" in h:
                item["metric_value"] = row[i]
                item["metric"] = "LCP"
            if h.startswith("inp") or "inp" in h:
                item["metric_value"] = row[i]
                item["metric"] = "INP"
            if h.startswith("cls") or "cls" in h:
                item["metric_value"] = row[i]
                item["metric"] = "CLS"
            if "tarama" in h or "crawl" in h:
                item["last_crawl"] = row[i]
        if not item.get("url") and row[0].startswith("http"):
            item["url"] = row[0]
        if item.get("url") or item.get("cells"):
            rows_out.append(item)
    return rows_out


def _scrape_device(page, *, resource_id: str, device: int, label: str) -> dict[str, Any]:
    print(f"  · {label} summary…", flush=True)
    page.goto(_cwv_url(resource_id, "/summary", device=device), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    _wait_table(page)
    meta = _extract_page_meta(page)
    body = meta.get("body_head") or ""
    # KPIs from summary header
    kpis = {"poor": 0, "needs_improvement": 0, "good": 0}
    m_poor = re.search(r"Yetersiz\s+([\d\.\,]+(?:\s*B)?)", body, re.I)
    m_ni = re.search(r"İyileştirme gerektiriyor\s+([\d\.\,]+(?:\s*B)?)", body, re.I)
    m_good = re.search(r"İyi\s+([\d\.\,]+(?:\s*B)?)", body, re.I)
    if m_poor:
        kpis["poor"] = _parse_count(m_poor.group(1))
    if m_ni:
        kpis["needs_improvement"] = _parse_count(m_ni.group(1))
    if m_good:
        kpis["good"] = _parse_count(m_good.group(1))

    issues_raw = _extract_table(page)
    issues: list[dict[str, Any]] = []
    for row in issues_raw.get("rows") or []:
        if len(row) < 2:
            continue
        status = _status_from_text(row[0])
        title = row[1]
        urls_n = _parse_count(row[-1]) if row else 0
        issues.append(
            {
                "status": status,
                "title": title,
                "verification": row[2] if len(row) > 2 else "",
                "url_count": urls_n,
                "metric": _metric_from_issue(title),
                "causes": explain_causes(_metric_from_issue(title), status, title),
            }
        )

    # Click each issue row → drilldown (discover item_key)
    drilldowns: list[dict[str, Any]] = []
    issue_count = len(issues)
    for idx in range(issue_count):
        page.goto(_cwv_url(resource_id, "/summary", device=device), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        _wait_table(page)
        rows = page.locator("table tbody tr")
        n = rows.count()
        if idx >= n:
            break
        print(f"    issue {idx + 1}/{n}…", flush=True)
        rows.nth(idx).click()
        time.sleep(3.5)
        cur = page.url or ""
        qs = parse_qs(urlparse(cur).query)
        item_key = (qs.get("item_key") or [""])[0]
        dmeta = _extract_page_meta(page)
        title = issues[idx]["title"] if idx < len(issues) else (dmeta.get("title") or "")
        status = issues[idx]["status"] if idx < len(issues) else _status_from_text(dmeta.get("body_head") or "")
        metric = _metric_from_issue(title)
        urls = _scrape_url_table(page)
        for u in urls:
            u.setdefault("metric", metric)
        drilldowns.append(
            {
                "status": status,
                "title": title,
                "metric": metric,
                "item_key": item_key,
                "source_url": cur,
                "url_rows": urls,
                "url_row_count": len(urls),
                "causes": explain_causes(metric, status, title),
            }
        )
        if idx < len(issues):
            issues[idx]["item_key"] = item_key
            issues[idx]["drilldown_url"] = cur

    # Good URLs drilldown
    print(f"  · {label} good URLs…", flush=True)
    page.goto(_cwv_url(resource_id, "/drilldown", device=device), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    _wait_table(page)
    good_urls = _scrape_url_table(page)
    good_meta = _extract_page_meta(page)

    return {
        "device": device,
        "label": label,
        "kpis": kpis,
        "last_updated": "",
        "issues": issues,
        "issue_drilldowns": drilldowns,
        "good_urls": good_urls,
        "good_url_count": len(good_urls),
        "good_page_url": good_meta.get("url") or "",
        "summary_url": _cwv_url(resource_id, "/summary", device=device),
    }


def _scrape_amp(page, *, resource_id: str) -> dict[str, Any]:
    print("  · AMP overview…", flush=True)
    page.goto(_amp_url(resource_id), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    meta = _extract_page_meta(page)
    issues_table = _extract_table(page)
    amp_issues: list[dict[str, Any]] = []
    # Prefer known drilldown + click rows
    overview_issues = []
    for row in issues_table.get("rows") or []:
        if len(row) < 1:
            continue
        title = row[0] if not row[0].startswith("http") else (row[1] if len(row) > 1 else row[0])
        overview_issues.append({"title": _clean(title), "cells": row})

    # Seed with user-provided image-size issue
    seed_keys = ["GgoIIBACIgAiACIA"]
    discovered_keys: list[str] = list(seed_keys)

    # Click issue rows on AMP summary if present
    n = page.locator("table tbody tr").count()
    for idx in range(n):
        page.goto(_amp_url(resource_id), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        _wait_table(page, 10000)
        rows = page.locator("table tbody tr")
        if idx >= rows.count():
            break
        try:
            rows.nth(idx).click()
            time.sleep(3)
        except Exception:
            continue
        qs = parse_qs(urlparse(page.url).query)
        key = (qs.get("item_key") or [""])[0]
        if key and key not in discovered_keys:
            discovered_keys.append(key)

    for key in discovered_keys:
        print(f"  · AMP drilldown {key}…", flush=True)
        page.goto(_amp_url(resource_id, "/drilldown", item_key=key), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(4)
        _wait_table(page)
        dmeta = _extract_page_meta(page)
        body = dmeta.get("body_head") or ""
        title_m = re.search(
            r"AMP\s+(.+?)\s+(?:İHRACAT|EXPORT|PAYLAŞ|DIŞA)",
            body,
            re.I,
        )
        title = (title_m.group(1).strip() if title_m else "") or "AMP sorunu"
        # refine from known pattern
        if "Görüntü boyutu" in body or "Image" in body:
            title = "Görüntü boyutu önerilen boyuttan daha küçük"
        status = "needs_improvement"
        if "error" in body.lower() or "kritik" in body.lower():
            status = "poor"
        urls = _scrape_url_table(page)
        metric = _metric_from_issue(title)
        amp_issues.append(
            {
                "status": status,
                "title": title,
                "metric": metric,
                "item_key": key,
                "source_url": page.url,
                "url_rows": urls,
                "url_row_count": len(urls),
                "causes": explain_causes(metric, status, title),
            }
        )

    total_amp = sum(int(i.get("url_row_count") or 0) for i in amp_issues)
    return {
        "overview_url": _amp_url(resource_id),
        "overview_body": (meta.get("body_head") or "")[:1200],
        "issues": amp_issues,
        "url_row_count": total_amp,
    }


def scrape_property(page, prop: dict[str, str]) -> dict[str, Any]:
    rid = prop["resource_id"]
    print(f"CWV scrape · {prop.get('label') or rid}", flush=True)
    page.goto(_cwv_url(rid), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(4)
    if not _looks_signed_in(page):
        raise RuntimeError("GSC oturumu yok — scripts/gsc_cwv_scrape.py --login")
    meta = _extract_page_meta(page)
    body = page.inner_text("body")
    overview = _parse_overview_counts(body)
    last_upd = ""
    m = re.search(r"Son güncelleme:\s*([0-9\./]+)", body, re.I)
    if m:
        last_upd = m.group(1)

    mobile = _scrape_device(page, resource_id=rid, device=DEVICE_MOBILE, label="Mobil")
    desktop = _scrape_device(page, resource_id=rid, device=DEVICE_DESKTOP, label="Masaüstü")
    # Prefer overview KPIs when summary parse weak
    if overview["mobile"]["good"] or overview["mobile"]["needs_improvement"]:
        mobile["kpis"] = overview["mobile"]
    if overview["desktop"]["good"] or overview["desktop"]["poor"] or overview["desktop"]["needs_improvement"]:
        desktop["kpis"] = overview["desktop"]
    mobile["last_updated"] = last_upd
    desktop["last_updated"] = last_upd

    amp = _scrape_amp(page, resource_id=rid)

    poor = int(mobile["kpis"].get("poor") or 0) + int(desktop["kpis"].get("poor") or 0)
    ni = int(mobile["kpis"].get("needs_improvement") or 0) + int(desktop["kpis"].get("needs_improvement") or 0)
    good = int(mobile["kpis"].get("good") or 0) + int(desktop["kpis"].get("good") or 0)

    return {
        "site_key": prop.get("site_key") or "",
        "site_domain": prop.get("site_domain") or "",
        "resource_id": rid,
        "label": prop.get("label") or rid,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": last_upd,
        "overview": overview,
        "mobile": mobile,
        "desktop": desktop,
        "amp": amp,
        "totals": {"poor": poor, "needs_improvement": ni, "good": good},
        "source": "gsc_cwv_scrape",
    }


def _post_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        raise RuntimeError("NOTIFICATION_INGEST_TOKEN gerekli")
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Notification-Ingest-Token": token,
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"HTTP {exc.code}: {err}") from exc


def run_sync(*, site_filter: str = "", ingest: bool = True, headed: bool | None = None) -> dict[str, Any]:
    if headed is None:
        env_h = (os.environ.get("GSC_CWV_HEADLESS") or os.environ.get("GSC_LINKS_HEADLESS") or "").strip().lower()
        # Google oturumu için varsayılan headed
        headed = env_h not in ("1", "true", "yes")
    props = PROPERTIES
    if site_filter:
        sk = site_filter.strip().lower()
        props = [p for p in PROPERTIES if p["site_key"] == sk or sk in (p["site_domain"] or "")]
    if not props:
        return {"ok": False, "message": f"site bulunamadı: {site_filter}"}

    pw, context = _launch_context(headed=headed)
    snapshots: list[dict[str, Any]] = []
    try:
        page = context.pages[0] if context.pages else context.new_page()
        for prop in props:
            try:
                snap = scrape_property(page, prop)
                snapshots.append(snap)
                print(
                    f"OK {prop['label']} · poor={snap['totals']['poor']} "
                    f"ni={snap['totals']['needs_improvement']} good={snap['totals']['good']} "
                    f"amp_rows={snap['amp'].get('url_row_count')}",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"FAIL {prop.get('label')}: {exc}", flush=True)
                snapshots.append(
                    {
                        "site_domain": prop.get("site_domain"),
                        "resource_id": prop.get("resource_id"),
                        "ok": False,
                        "error": str(exc)[:300],
                        "source": "gsc_cwv_scrape",
                    }
                )
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass

    payload = {
        "source": "gsc_cwv_scrape",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "snapshots": snapshots,
    }
    result: dict[str, Any] = {"ok": True, "snapshots": len(snapshots), "message": f"{len(snapshots)} property"}
    if ingest:
        ing = _post_ingest(payload)
        result["ingest"] = ing
        result["ok"] = bool(ing.get("ok", True))
        result["message"] = ing.get("message") or result["message"]
        print(f"ingest · {result['message']}", flush=True)
    else:
        out = ROOT / "scratch" / "gsc_cwv_last.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        result["saved"] = str(out)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GSC CWV + AMP scrape")
    parser.add_argument("--login", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--site", default="", help="doviz | sinemalar")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args(argv)
    if args.login:
        out = run_login_interactive()
        print(json.dumps(out, ensure_ascii=False), flush=True)
        return 0 if out.get("ok") else 1
    if not args.sync and not args.ingest:
        parser.print_help()
        return 2
    headed = True if args.headed else (False if args.headless else None)
    out = run_sync(site_filter=args.site, ingest=bool(args.ingest or args.sync), headed=headed)
    print(json.dumps({k: v for k, v in out.items() if k != "snapshots"}, ensure_ascii=False), flush=True)
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
