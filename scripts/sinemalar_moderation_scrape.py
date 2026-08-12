#!/usr/bin/env python3
"""Sinemalar management/getModerationSummary — Mac bridge scrape + ingest.

Örnek:
  .venv/bin/python scripts/sinemalar_moderation_scrape.py --login
  .venv/bin/python scripts/sinemalar_moderation_scrape.py --backfill-2026 --ingest
  .venv/bin/python scripts/sinemalar_moderation_scrape.py --incremental yesterday --ingest
  .venv/bin/python scripts/sinemalar_moderation_scrape.py --incremental today --ingest

Env:
  SINEMALAR_NOADS_PROFILE_DIR / SINEMALAR_ADMIN_* (noAds ile aynı oturum)
  SINEMALAR_MODERATION_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

from backend.services.scrape_browser import sinemalar_profile_dir

TR = ZoneInfo("Europe/Istanbul")
SUMMARY_URL = "https://www.sinemalar.com/management/getModerationSummary"
INGEST_URL = (
    os.environ.get("SINEMALAR_MODERATION_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/sinemalar-moderation/ingest"
).strip()
ADMIN_EMAIL = (os.environ.get("SINEMALAR_ADMIN_EMAIL") or "").strip()
ADMIN_PASSWORD = (os.environ.get("SINEMALAR_ADMIN_PASSWORD") or "").strip()
BACKFILL_START = date(2026, 1, 1)
SCRAPE_DELAY_SEC = float(os.environ.get("SINEMALAR_MODERATION_DELAY_SEC") or "2.0")
BACKFILL_CHUNK_DAYS = int(os.environ.get("SINEMALAR_MODERATION_BACKFILL_CHUNK") or "7")
META_URL = INGEST_URL.rsplit("/", 1)[0] + "/meta"

_EXTRACT_ROWS_JS = r"""
() => {
  const t = document.querySelector('table');
  if (!t) return [];
  const headers = Array.from(t.querySelectorAll('th')).slice(1).map(h => h.textContent.replace(/\s+/g,' ').trim());
  return Array.from(t.querySelectorAll('tr')).slice(1).map(tr => {
    const cells = Array.from(tr.querySelectorAll('td'));
    if (!cells.length) return null;
    const row = { moderator: cells[0].textContent.replace(/\s+/g,' ').trim(), metrics: {} };
    const modLink = cells[0].querySelector('a');
    if (modLink && modLink.href) {
      try {
        const mu = new URL(modLink.href);
        row.moderatorUserId = mu.searchParams.get('userId') || mu.searchParams.get('userid');
      } catch (e) { /* ignore */ }
    }
    cells.slice(1).forEach((td, i) => {
      const a = td.querySelector('a');
      const label = headers[i] || ('c' + i);
      let href = a ? a.href : null;
      let type = null;
      let userId = null;
      if (href) {
        try {
          const u = new URL(href);
          type = u.searchParams.get('type');
          userId = u.searchParams.get('userId') || u.searchParams.get('userid');
        } catch (e) { /* ignore */ }
      }
      row.metrics[label] = {
        count: parseInt((a || td).textContent.trim() || '0', 10) || 0,
        href: href,
        type: type,
        userId: userId,
      };
    });
    return row;
  }).filter(Boolean);
}
"""


def _today_tr() -> date:
    return datetime.now(TR).date()


def _yesterday_tr() -> date:
    return _today_tr() - timedelta(days=1)


def _looks_logged_in(page) -> bool:
    try:
        url = (page.url or "").lower()
        if "login" in url or "giris" in url:
            return False
        return "management" in url
    except Exception:
        return False


def _try_form_login(page) -> bool:
    if not ADMIN_EMAIL or not ADMIN_PASSWORD:
        return False
    try:
        page.goto("https://www.sinemalar.com/", wait_until="domcontentloaded", timeout=60000)
        time.sleep(1.0)
        for sel in ("text=Giriş Yap", "a:has-text('Giriş')", "button:has-text('Giriş')"):
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible():
                    loc.click(timeout=2000)
                    break
            except Exception:
                continue
        time.sleep(0.5)
        for user_sel in (
            "input[name='username']",
            "input[name='email']",
            "input[type='email']",
            "input[placeholder*='E-Posta']",
        ):
            try:
                loc = page.locator(user_sel).first
                if loc.count() and loc.is_visible():
                    loc.fill(ADMIN_EMAIL)
                    break
            except Exception:
                continue
        for pass_sel in ("input[name='password']", "input[type='password']"):
            try:
                loc = page.locator(pass_sel).first
                if loc.count() and loc.is_visible():
                    loc.fill(ADMIN_PASSWORD)
                    break
            except Exception:
                continue
        for btn in ("button:has-text('Giriş Yap')", "button[type='submit']"):
            try:
                loc = page.locator(btn).first
                if loc.count() and loc.is_visible():
                    loc.click(timeout=3000)
                    break
            except Exception:
                continue
        time.sleep(2.0)
        return True
    except Exception as exc:
        print(f"Form login: {exc}", flush=True)
        return False


def summary_url_for_day(day: date) -> str:
    """Sinemalar: startDate dahil, endDate hariç — tek gün için end = start + 1."""
    end = day + timedelta(days=1)
    return f"{SUMMARY_URL}?startDate={day.isoformat()}&endDate={end.isoformat()}"


def _tracked_day_total(rows: list[dict[str, Any]]) -> dict[str, int]:
    from backend.services.sinemalar_moderation import is_tracked_username, parse_summary_rows

    out: dict[str, int] = {}
    for item in parse_summary_rows(rows):
        uname = str(item.get("username") or "")
        if not is_tracked_username(uname):
            continue
        out[uname] = out.get(uname, 0) + int(item.get("count") or 0)
    return out


def fetch_summary_for_day(page, day: date) -> list[dict[str, Any]]:
    url = summary_url_for_day(day)
    page.goto(url, wait_until="networkidle", timeout=90000)
    time.sleep(0.5)
    rows = page.evaluate(_EXTRACT_ROWS_JS) or []
    totals = _tracked_day_total(rows if isinstance(rows, list) else [])
    if totals:
        brief = ", ".join(f"{k}={v}" for k, v in sorted(totals.items()))
        print(f"    tracked totals · {brief}", flush=True)
    return rows if isinstance(rows, list) else []


def scrape_days(
    days: list[date],
    *,
    headed: bool = True,
    delay_sec: float = SCRAPE_DELAY_SEC,
) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    from backend.services.scrape_browser import launch_persistent

    if not days:
        return {"ok": False, "message": "Gün listesi boş", "days": []}

    with sync_playwright() as p:
        context = launch_persistent(
            p, sinemalar_profile_dir(), headed=headed, viewport={"width": 1400, "height": 900}
        )
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(SUMMARY_URL, wait_until="networkidle", timeout=90000)
            time.sleep(1.0)
            if not _looks_logged_in(page):
                _try_form_login(page)
                page.goto(SUMMARY_URL, wait_until="networkidle", timeout=90000)
                time.sleep(1.0)
            if not _looks_logged_in(page):
                return {
                    "ok": False,
                    "needs_login": True,
                    "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
                    "days": [],
                }

            blocks: list[dict[str, Any]] = []
            for i, day in enumerate(days):
                if i > 0 and delay_sec > 0:
                    time.sleep(delay_sec)
                rows = fetch_summary_for_day(page, day)
                blocks.append({"date": day.isoformat(), "rows": rows})
                print(f"  {day.isoformat()} · {len(rows)} moderatör satırı", flush=True)

            return {
                "ok": True,
                "needs_login": False,
                "source": "sinemalar_moderation",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "days": blocks,
                "message": f"{len(blocks)} gün çekildi",
            }
        finally:
            context.close()


def fetch_remote_meta() -> dict[str, Any]:
    req = urllib.request.Request(META_URL, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def _days_from_cursor(cursor: str | None, *, max_days: int) -> list[date]:
    end = _yesterday_tr()
    if end < BACKFILL_START:
        return []
    start = BACKFILL_START
    if cursor:
        try:
            start = max(BACKFILL_START, date.fromisoformat(str(cursor)[:10]))
        except ValueError:
            start = BACKFILL_START
    out: list[date] = []
    d = start
    while d <= end and len(out) < max(1, max_days):
        out.append(d)
        d += timedelta(days=1)
    return out


def run_backfill_chunk(
    *,
    headed: bool = True,
    ingest: bool = False,
    max_days: int | None = None,
    from_date: str | None = None,
) -> dict[str, Any]:
    """2026 backfill — her çağrıda en fazla N gün (management yükünü dağıtır)."""
    chunk = max(1, int(max_days or BACKFILL_CHUNK_DAYS))
    meta = fetch_remote_meta()
    if meta.get("backfill_complete") and not from_date:
        return {"ok": True, "skipped": True, "message": "backfill zaten tamam", "mode": "backfill"}
    cursor = from_date or meta.get("backfill_cursor")
    days = _days_from_cursor(cursor, max_days=chunk)
    if not days:
        return {"ok": True, "skipped": True, "message": "backfill için gün yok", "mode": "backfill"}
    print(
        f"Backfill chunk: {len(days)} gün ({days[0].isoformat()} → {days[-1].isoformat()})",
        flush=True,
    )
    result = scrape_days(days, headed=headed)
    if not result.get("ok"):
        return result
    result["mode"] = "backfill"
    last = days[-1]
    complete = last >= _yesterday_tr()
    result["backfill_complete"] = complete
    result["backfill_cursor"] = None if complete else (last + timedelta(days=1)).isoformat()
    if ingest:
        ing = ingest_result(result, mode="backfill")
        result["ingest"] = ing
        if not ing.get("ok"):
            result["ok"] = False
            result["message"] = ing.get("message") or "ingest failed"
    return result


def ingest_result(result: dict[str, Any], *, mode: str = "incremental") -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    body = {
        "source": result.get("source") or "sinemalar_moderation",
        "mode": mode,
        "scraped_at": result.get("scraped_at") or "",
        "days": result.get("days") or [],
        "backfill_complete": bool(result.get("backfill_complete")),
        "backfill_cursor": result.get("backfill_cursor"),
    }
    req = urllib.request.Request(
        INGEST_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return {"ok": True, "ingest": payload}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def run_backfill_2026(*, headed: bool = True, ingest: bool = False, max_days: int | None = None) -> dict[str, Any]:
    end = _yesterday_tr()
    days: list[date] = []
    d = BACKFILL_START
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    if max_days and max_days > 0:
        days = days[:max_days]

    print(f"Backfill 2026: {len(days)} gün (aralık {days[0] if days else '?'} → {days[-1] if days else '?'})", flush=True)
    result = scrape_days(days, headed=headed)
    if not result.get("ok"):
        return result
    result["mode"] = "backfill"
    result["backfill_complete"] = max_days is None or len(days) >= (_yesterday_tr() - BACKFILL_START).days + 1
    if result.get("backfill_complete"):
        result["backfill_cursor"] = None
    elif days:
        result["backfill_cursor"] = (days[-1] + timedelta(days=1)).isoformat()

    if ingest:
        ing = ingest_result(result, mode="backfill")
        result["ingest"] = ing
        if not ing.get("ok"):
            result["ok"] = False
            result["message"] = ing.get("message") or "ingest failed"
    return result


def run_incremental(which: str = "yesterday", *, headed: bool = True, ingest: bool = False) -> dict[str, Any]:
    day = _yesterday_tr() if which == "yesterday" else _today_tr()
    result = scrape_days([day], headed=headed, delay_sec=0)
    if not result.get("ok"):
        return result
    result["mode"] = "incremental"
    if ingest:
        ing = ingest_result(result, mode="incremental")
        result["ingest"] = ing
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Sinemalar moderasyon özeti scrape")
    parser.add_argument("--login", action="store_true", help="Yalnızca admin oturumu aç")
    parser.add_argument("--backfill-2026", action="store_true", help="2026 tüm günleri tek oturumda çek")
    parser.add_argument(
        "--backfill-chunk",
        action="store_true",
        help="2026 backfill — en fazla N gün (meta cursor; bridge varsayılanı)",
    )
    parser.add_argument("--from-date", help="Backfill chunk başlangıcı YYYY-MM-DD (cursor override)")
    parser.add_argument("--max-days", type=int, default=0, help="Backfill'de en fazla N gün (test)")
    parser.add_argument("--incremental", choices=("yesterday", "today"), help="Tek gün incremental")
    parser.add_argument("--date", help="Tek gün YYYY-MM-DD (endDate otomatik +1 gün)")
    parser.add_argument(
        "--range",
        help="Aralık YYYY-MM-DD:YYYY-MM-DD (tek blok; günlük değil özet — doğrulama için)",
    )
    parser.add_argument("--ingest", action="store_true", help="Railway ingest")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()
    headed = not args.headless

    if args.login:
        from playwright.sync_api import sync_playwright

        from backend.services.scrape_browser import launch_persistent

        with sync_playwright() as p:
            ctx = launch_persistent(p, sinemalar_profile_dir(), headed=True)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(SUMMARY_URL, wait_until="domcontentloaded", timeout=90000)
            print("Tarayıcı açık — giriş yapıp kapatın.", flush=True)
            try:
                page.wait_for_timeout(300_000)
            except Exception:
                pass
            ctx.close()
        return 0

    if args.backfill_chunk:
        out = run_backfill_chunk(
            headed=headed,
            ingest=args.ingest,
            max_days=args.max_days if args.max_days > 0 else None,
            from_date=args.from_date,
        )
        print(json.dumps({k: v for k, v in out.items() if k != "days"}, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.backfill_2026:
        out = run_backfill_2026(
            headed=headed,
            ingest=args.ingest,
            max_days=args.max_days if args.max_days > 0 else None,
        )
        print(json.dumps({k: v for k, v in out.items() if k != "days"}, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.incremental:
        out = run_incremental(args.incremental, headed=headed, ingest=args.ingest)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.range:
        raw = str(args.range).strip()
        if ":" not in raw:
            print("Geçersiz --range (YYYY-MM-DD:YYYY-MM-DD)", file=sys.stderr)
            return 1
        start_s, end_s = raw.split(":", 1)
        try:
            start_d = date.fromisoformat(start_s[:10])
            end_d = date.fromisoformat(end_s[:10])
        except ValueError:
            print("Geçersiz --range tarihleri", file=sys.stderr)
            return 1
        from playwright.sync_api import sync_playwright

        from backend.services.scrape_browser import launch_persistent

        url = f"{SUMMARY_URL}?startDate={start_d.isoformat()}&endDate={end_d.isoformat()}"
        with sync_playwright() as p:
            ctx = launch_persistent(p, sinemalar_profile_dir(), headed=headed)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=90000)
            time.sleep(0.5)
            rows = page.evaluate(_EXTRACT_ROWS_JS) or []
            ctx.close()
        out = {"ok": True, "url": url, "rows": rows, "tracked": _tracked_day_total(rows)}
        if args.ingest:
            out["ingest"] = ingest_result(
                {
                    "source": "sinemalar_moderation",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "days": [{"date": start_d.isoformat(), "rows": rows}],
                },
                mode="range",
            )
        print(json.dumps({k: v for k, v in out.items() if k != "rows"}, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.date:
        try:
            day = date.fromisoformat(args.date[:10])
        except ValueError:
            print("Geçersiz --date", file=sys.stderr)
            return 1
        out = scrape_days([day], headed=headed, delay_sec=0)
        if out.get("ok"):
            day_block = (out.get("days") or [{}])[0]
            out["tracked"] = _tracked_day_total(day_block.get("rows") or [])
        if args.ingest and out.get("ok"):
            out["ingest"] = ingest_result(out, mode="single")
        print(json.dumps(out, ensure_ascii=False, indent=2)[:4000])
        return 0 if out.get("ok") else 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
