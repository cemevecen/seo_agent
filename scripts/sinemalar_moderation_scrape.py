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
import urllib.parse
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
DETAIL_URL = "https://www.sinemalar.com/management/getModerationDetail"
INGEST_URL = (
    os.environ.get("SINEMALAR_MODERATION_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/sinemalar-moderation/ingest"
).strip()
ADMIN_EMAIL = (os.environ.get("SINEMALAR_ADMIN_EMAIL") or "").strip()
ADMIN_PASSWORD = (os.environ.get("SINEMALAR_ADMIN_PASSWORD") or "").strip()
BACKFILL_START = date(2026, 1, 1)
SCRAPE_DELAY_SEC = float(os.environ.get("SINEMALAR_MODERATION_DELAY_SEC") or "2.0")
BACKFILL_CHUNK_DAYS = int(os.environ.get("SINEMALAR_MODERATION_BACKFILL_CHUNK") or "7")
DETAIL_INGEST_CHUNK = int(os.environ.get("SINEMALAR_MODERATION_DETAIL_CHUNK") or "100")
DETAIL_INGEST_CHUNK_MIN = int(os.environ.get("SINEMALAR_MODERATION_DETAIL_CHUNK_MIN") or "25")

# 2026 backfill — aylık pencereler (üst üste binmez; dedup ingest birleştirir)
DETAIL_MONTHLY_WINDOWS_2026: list[tuple[date, date]] = [
    (date(2026, 1, 1), date(2026, 2, 1)),
    (date(2026, 2, 2), date(2026, 3, 1)),
    (date(2026, 3, 2), date(2026, 4, 1)),
    (date(2026, 4, 2), date(2026, 5, 1)),
    (date(2026, 5, 2), date(2026, 6, 1)),
    (date(2026, 6, 2), date(2026, 7, 1)),
    (date(2026, 7, 2), date(2026, 8, 13)),
]
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

_EXTRACT_DETAIL_JS = r"""
() => {
  const t = document.querySelector('table');
  if (!t) return [];
  return Array.from(t.querySelectorAll('tr')).slice(1).map(tr => {
    const cells = Array.from(tr.querySelectorAll('td'));
    if (!cells.length) return null;
    return {
      cells: cells.map(td => {
        const a = td.querySelector('a');
        return {
          text: td.textContent.replace(/\s+/g, ' ').trim(),
          href: a ? a.href : null,
        };
      }),
    };
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


def exclusive_detail_end(day: date) -> date:
    """Tek gün getModerationDetail — endDate hariç (start dahil)."""
    return day + timedelta(days=1)


def summary_url_for_range(start_d: date, end_d: date) -> str:
    """Aralık özeti — getModerationSummary."""
    return f"{SUMMARY_URL}?startDate={start_d.isoformat()}&endDate={end_d.isoformat()}"


def fetch_summary_for_range(page, start_d: date, end_d: date) -> list[dict[str, Any]]:
    url = summary_url_for_range(start_d, end_d)
    page.goto(url, wait_until="networkidle", timeout=120000)
    time.sleep(0.6)
    rows = page.evaluate(_EXTRACT_ROWS_JS) or []
    return rows if isinstance(rows, list) else []


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


_EXTRACT_DETAIL_CHUNK_JS = r"""
({ start, size }) => {
  const trs = Array.from(document.querySelectorAll('table tr')).slice(1);
  return trs.slice(start, start + size).map(tr => {
    const cells = Array.from(tr.querySelectorAll('td'));
    if (!cells.length) return null;
    return {
      cells: cells.map(td => {
        const a = td.querySelector('a');
        return {
          text: td.textContent.replace(/\s+/g, ' ').trim(),
          href: a ? a.href : null,
        };
      }),
    };
  }).filter(Boolean);
}
"""

_DETAIL_ROW_CHUNK = 250


def _safe_close_context(context, *, pw=None, headed: bool = True) -> None:
    """Headed scrapelerde pencereyi kapatma — admin oturumu korunsun."""
    from backend.services.scrape_browser import release_persistent_context

    if context is None and pw is None:
        return
    release_persistent_context(
        "sinemalar",
        pw,
        context,
        headed=headed,
        env_key="SINEMALAR_KEEP_OPEN",
        label="Sinemalar",
    )


def _extract_detail_rows(page) -> list[dict[str, Any]]:
    """Büyük tablolar için parça parça çıkar — tarayıcı çökmesini azaltır."""
    all_rows: list[dict[str, Any]] = []
    start = 0
    while True:
        chunk = page.evaluate(_EXTRACT_DETAIL_CHUNK_JS, {"start": start, "size": _DETAIL_ROW_CHUNK})
        if not isinstance(chunk, list) or not chunk:
            break
        all_rows.extend(chunk)
        if len(chunk) < _DETAIL_ROW_CHUNK:
            break
        start += _DETAIL_ROW_CHUNK
    return all_rows


def fetch_detail_page(
    page,
    *,
    user_id: int,
    username: str,
    metric_type: str,
    start_d: date,
    end_d: date,
) -> dict[str, Any]:
    """getModerationDetail: startDate dahil, endDate hariç (özet API ile aynı)."""
    from backend.services.sinemalar_moderation import detail_url

    url = detail_url(user_id, start=start_d, end=end_d, metric_type=metric_type)
    page.goto(url, wait_until="domcontentloaded", timeout=180000)
    time.sleep(0.6)
    rows = _extract_detail_rows(page)
    count = len(rows)
    print(f"    {username} · {metric_type} · {count} kayıt", flush=True)
    return {
        "user_id": user_id,
        "username": username,
        "metric_type": metric_type,
        "source_url": url,
        "items": rows,
        "item_count": count,
    }


def _open_logged_in_page(headed: bool):
    from backend.services.scrape_browser import (
        acquire_persistent_context,
        release_persistent_context,
    )

    pw, context, _reused = acquire_persistent_context(
        "sinemalar",
        profile=sinemalar_profile_dir(),
        headed=headed,
        env_key="SINEMALAR_KEEP_OPEN",
        label="Sinemalar",
        viewport={"width": 1400, "height": 900},
    )
    page = context.pages[0] if context.pages else context.new_page()
    page.goto(SUMMARY_URL, wait_until="domcontentloaded", timeout=90000)
    time.sleep(0.8)
    if not _looks_logged_in(page):
        _try_form_login(page)
        page.goto(SUMMARY_URL, wait_until="domcontentloaded", timeout=90000)
        time.sleep(0.8)
    if not _looks_logged_in(page):
        release_persistent_context(
            "sinemalar",
            pw,
            context,
            headed=headed,
            env_key="SINEMALAR_KEEP_OPEN",
            label="Sinemalar",
        )
        return None, None, None
    return pw, context, page


def _ingest_detail_chunk(
    batch: dict[str, Any],
    chunk: list[dict[str, Any]],
    *,
    start_d: date,
    end_d: date,
    scraped_at: str,
    backfill_complete: bool,
    mode: str,
    purge_first: bool = False,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "source": "sinemalar_moderation",
        "mode": mode,
        "scraped_at": scraped_at,
        "range_start": start_d.isoformat(),
        "range_end": end_d.isoformat(),
        "detail_batches": [{**batch, "items": chunk, "_recompute_daily": False}],
        "backfill_complete": backfill_complete,
    }
    if purge_first:
        body["purge_first"] = True
    return ingest_result(body, mode=mode)


def _ingest_detail_batch(
    batch: dict[str, Any],
    *,
    start_d: date,
    end_d: date,
    scraped_at: str,
    backfill_complete: bool,
    mode: str = "detail_range",
    purge_first: bool = False,
) -> dict[str, Any]:
    items = list(batch.get("items") or [])
    if not items:
        return {"ok": True, "ingest": {"items_upserted": 0}}

    def ingest_slice(
        slice_items: list[dict[str, Any]],
        *,
        pf: bool,
        complete: bool,
    ) -> dict[str, Any]:
        if not slice_items:
            return {"ok": True}
        if len(slice_items) > DETAIL_INGEST_CHUNK:
            last: dict[str, Any] = {"ok": False}
            for i in range(0, len(slice_items), DETAIL_INGEST_CHUNK):
                part = slice_items[i : i + DETAIL_INGEST_CHUNK]
                is_last = i + DETAIL_INGEST_CHUNK >= len(slice_items)
                last = ingest_slice(part, pf=pf and i == 0, complete=complete and is_last)
                if not last.get("ok"):
                    return last
            return last
        res = _ingest_detail_chunk(
            batch,
            slice_items,
            start_d=start_d,
            end_d=end_d,
            scraped_at=scraped_at,
            backfill_complete=complete,
            mode=mode,
            purge_first=pf,
        )
        if res.get("ok"):
            return res
        msg = str(res.get("message") or "")
        if "500" in msg and len(slice_items) > DETAIL_INGEST_CHUNK_MIN:
            mid = max(1, len(slice_items) // 2)
            left = ingest_slice(slice_items[:mid], pf=pf, complete=False)
            if not left.get("ok"):
                return left
            return ingest_slice(slice_items[mid:], pf=False, complete=complete)
        return res

    return ingest_slice(items, pf=purge_first, complete=backfill_complete)


def fetch_remote_coverage(start_d: date, end_d: date) -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    qs = urllib.parse.urlencode({"start": start_d.isoformat(), "end": end_d.isoformat()})
    url = INGEST_URL.rsplit("/", 1)[0] + "/coverage?" + qs
    req = urllib.request.Request(url, headers=_auth_headers(), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return {"ok": True, **payload}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def post_remote_gaps(
    start_d: date,
    end_d: date,
    expected: dict[str, int],
    *,
    user_ids: list[int] | None = None,
) -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    url = INGEST_URL.rsplit("/", 1)[0] + "/gaps"
    body: dict[str, Any] = {
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "expected": expected,
    }
    if user_ids:
        body["user_ids"] = user_ids
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", **_auth_headers()},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return {"ok": True, **payload}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def _auth_headers() -> dict[str, str]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _parse_remote_body(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return None
    if text.lstrip().startswith("<"):
        raise ValueError("HTML yanıt — endpoint auth middleware arkasında olabilir (deploy bekleyin)")
    return json.loads(text)


def purge_remote_moderation() -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    url = INGEST_URL.rsplit("/", 1)[0] + "/purge"
    req = urllib.request.Request(
        url,
        data=b"{}",
        headers={"Content-Type": "application/json", **_auth_headers()},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw.strip():
                return {"ok": True, "purge": {"ok": True, "message": "empty response"}}
            payload = _parse_remote_body(raw)
            return {"ok": True, "purge": payload if isinstance(payload, dict) else {"result": payload}}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except json.JSONDecodeError as exc:
        return {"ok": False, "message": f"JSON parse: {exc}"}
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}


def scrape_detail_range(
    start_d: date,
    end_d: date,
    *,
    headed: bool = True,
    delay_sec: float = SCRAPE_DELAY_SEC,
    ingest_per_batch: bool = False,
    purge_first: bool = False,
) -> dict[str, Any]:
    from backend.services.sinemalar_moderation import METRIC_TYPE_KEYS, TRACKED_MODERATORS

    if purge_first and ingest_per_batch:
        print("İlk batch ingest ile purge (purge_first)…", flush=True)

    batches: list[dict[str, Any]] = []
    total_items = 0
    total_batches = len(TRACKED_MODERATORS) * len(METRIC_TYPE_KEYS)
    n = 0
    scraped_at = datetime.now(timezone.utc).isoformat()

    for user_id, username in TRACKED_MODERATORS:
        for metric_type in METRIC_TYPE_KEYS:
            if n > 0 and delay_sec > 0:
                time.sleep(delay_sec)
            n += 1
            batch: dict[str, Any] | None = None
            for attempt in range(2):
                pw = ctx = page = None
                try:
                    pw, ctx, page = _open_logged_in_page(headed)
                    if page is None:
                        return {
                            "ok": False,
                            "needs_login": True,
                            "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
                            "detail_batches": batches,
                        }
                    batch = fetch_detail_page(
                        page,
                        user_id=user_id,
                        username=username,
                        metric_type=metric_type,
                        start_d=start_d,
                        end_d=end_d,
                    )
                    break
                except Exception as exc:
                    print(f"    ! {username}/{metric_type} hata (deneme {attempt + 1}): {exc}", flush=True)
                    batch = None
                finally:
                    _safe_close_context(ctx, pw=pw, headed=headed)
            if batch is None:
                batch = {
                    "user_id": user_id,
                    "username": username,
                    "metric_type": metric_type,
                    "source_url": "",
                    "items": [],
                    "item_count": 0,
                }

            batches.append(batch)
            total_items += int(batch.get("item_count") or 0)
            if ingest_per_batch:
                ing = _ingest_detail_batch(
                    batch,
                    start_d=start_d,
                    end_d=end_d,
                    scraped_at=scraped_at,
                    backfill_complete=n >= total_batches,
                    mode="detail_range",
                    purge_first=bool(purge_first and n == 1),
                )
                if not ing.get("ok"):
                    print(f"    ingest hata: {ing.get('message')}", flush=True)

    return {
        "ok": True,
        "needs_login": False,
        "source": "sinemalar_moderation",
        "mode": "detail_range",
        "scraped_at": scraped_at,
        "range_start": start_d.isoformat(),
        "range_end": end_d.isoformat(),
        "detail_batches": batches,
        "item_count": total_items,
        "batch_count": len(batches),
        "message": f"detail_range {start_d} → {end_d} · {total_items} kayıt",
    }


def run_detail_monthly_2026(
    *,
    headed: bool = True,
    ingest: bool = False,
    purge_first: bool = False,
) -> dict[str, Any]:
    """2026 moderasyon — 7 aylık pencere, append-only dedup ingest."""
    purge_via_first_ingest = False
    if purge_first and ingest:
        print("Railway moderasyon verisi siliniyor (detay + günlük)…", flush=True)
        pr = purge_remote_moderation()
        if not pr.get("ok"):
            print(
                f"  purge API başarısız ({pr.get('message')}) — ilk ingest batch purge_first ile denenecek",
                flush=True,
            )
            purge_via_first_ingest = True
        else:
            payload = pr.get("purge") or {}
            print(
                f"  silindi · {payload.get('deleted_details', 0)} detay · "
                f"{payload.get('deleted_daily', 0)} günlük",
                flush=True,
            )

    windows = DETAIL_MONTHLY_WINDOWS_2026
    scraped_total = 0
    window_stats: list[dict[str, Any]] = []
    for idx, (start_d, end_d) in enumerate(windows, start=1):
        print(
            f"\n=== Pencere {idx}/{len(windows)}: {start_d.isoformat()} → {end_d.isoformat()} ===",
            flush=True,
        )
        out = scrape_detail_range(
            start_d,
            end_d,
            headed=headed,
            ingest_per_batch=bool(ingest),
            purge_first=purge_via_first_ingest and idx == 1,
        )
        if not out.get("ok"):
            out["failed_window"] = {"start": start_d.isoformat(), "end": end_d.isoformat()}
            return out
        n = int(out.get("item_count") or 0)
        scraped_total += n
        window_stats.append({"start": start_d.isoformat(), "end": end_d.isoformat(), "item_count": n})

    return {
        "ok": True,
        "needs_login": False,
        "source": "sinemalar_moderation",
        "mode": "detail_monthly_2026",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "range_start": windows[0][0].isoformat(),
        "range_end": windows[-1][1].isoformat(),
        "window_count": len(windows),
        "windows": window_stats,
        "item_count": scraped_total,
        "message": f"detail_monthly_2026 · {len(windows)} pencere · {scraped_total} kayıt çekildi",
    }


def run_purge_only() -> dict[str, Any]:
    print("Railway moderasyon verisi siliniyor…", flush=True)
    pr = purge_remote_moderation()
    if pr.get("ok"):
        payload = pr.get("purge") or {}
        print(
            f"  silindi · {payload.get('deleted_details', 0)} detay · "
            f"{payload.get('deleted_daily', 0)} günlük",
            flush=True,
        )
    return pr


def scrape_fill_gaps(
    start_d: date,
    end_d: date,
    *,
    headed: bool = True,
    delay_sec: float = SCRAPE_DELAY_SEC,
    ingest_per_batch: bool = False,
    user_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Sinemalar özet vs DB karşılaştırması — yalnızca eksik user×type detail çeker (purge yok)."""
    from backend.services.sinemalar_moderation import (
        METRIC_LABEL_BY_TYPE,
        METRIC_TYPE_KEYS,
        parse_summary_rows,
        resolve_username,
        summary_totals_map,
    )

    scraped_at = datetime.now(timezone.utc).isoformat()
    pw = ctx = page = None
    try:
        pw, ctx, page = _open_logged_in_page(headed)
        if page is None:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
            }
        summary_rows = fetch_summary_for_range(page, start_d, end_d)
    finally:
        _safe_close_context(ctx, pw=pw, headed=headed)

    parsed = parse_summary_rows(summary_rows)
    expected = summary_totals_map(parsed)
    gaps_resp = post_remote_gaps(start_d, end_d, expected, user_ids=user_ids)
    gaps = gaps_resp.get("gaps") or [] if gaps_resp.get("ok") else []
    if not gaps_resp.get("ok"):
        from backend.services.sinemalar_moderation import compute_gaps

        cov = fetch_remote_coverage(start_d, end_d)
        actual = cov.get("counts") or {} if cov.get("ok") else {}
        if cov.get("ok"):
            gaps = compute_gaps(expected, actual, user_ids=user_ids)
            print(
                f"gaps API yok ({gaps_resp.get('message')}) — coverage ile {len(gaps)} eksik batch",
                flush=True,
            )
        else:
            print(
                f"gaps/coverage API yok ({gaps_resp.get('message')} / {cov.get('message')}) "
                f"— özet > 0 batch'ler taranacak",
                flush=True,
            )
            allowed = set(user_ids) if user_ids else None
            for key, exp in expected.items():
                if exp <= 0:
                    continue
                parts = key.split("|", 1)
                if len(parts) != 2:
                    continue
                try:
                    uid = int(parts[0])
                except ValueError:
                    continue
                if allowed is not None and uid not in allowed:
                    continue
                gaps.append(
                    {
                        "user_id": uid,
                        "username": resolve_username(uid),
                        "metric_type": parts[1],
                        "metric_label": METRIC_LABEL_BY_TYPE.get(parts[1], parts[1]),
                        "expected": exp,
                        "actual": int(actual.get(key) or 0),
                        "missing": exp - int(actual.get(key) or 0),
                    }
                )
    if not gaps:
        return {
            "ok": True,
            "mode": "fill_gaps",
            "scraped_at": scraped_at,
            "range_start": start_d.isoformat(),
            "range_end": end_d.isoformat(),
            "gap_count": 0,
            "item_count": 0,
            "message": "Eksik batch yok — DB özet ile uyumlu",
        }

    print(f"Eksik batch: {len(gaps)} (purge yok, dedup ingest)", flush=True)
    for g in gaps:
        print(
            f"  · {g.get('username')} / {g.get('metric_type')} "
            f"beklenen {g.get('expected')} · DB {g.get('actual')} · eksik {g.get('missing')}",
            flush=True,
        )

    batches: list[dict[str, Any]] = []
    total_items = 0
    n = 0
    for gap in gaps:
        if n > 0 and delay_sec > 0:
            time.sleep(delay_sec)
        n += 1
        user_id = int(gap.get("user_id") or 0)
        username = str(gap.get("username") or "")
        metric_type = str(gap.get("metric_type") or "")
        if not user_id or not metric_type:
            continue
        batch: dict[str, Any] | None = None
        for attempt in range(2):
            pw = ctx = page = None
            try:
                pw, ctx, page = _open_logged_in_page(headed)
                if page is None:
                    return {
                        "ok": False,
                        "needs_login": True,
                        "message": "Sinemalar admin oturumu yok.",
                        "detail_batches": batches,
                    }
                batch = fetch_detail_page(
                    page,
                    user_id=user_id,
                    username=username,
                    metric_type=metric_type,
                    start_d=start_d,
                    end_d=end_d,
                )
                break
            except Exception as exc:
                print(f"    ! {username}/{metric_type} hata (deneme {attempt + 1}): {exc}", flush=True)
                batch = None
            finally:
                _safe_close_context(ctx, pw=pw, headed=headed)
        if batch is None:
            continue
        batches.append(batch)
        total_items += int(batch.get("item_count") or 0)
        if ingest_per_batch:
            ing = _ingest_detail_batch(
                batch,
                start_d=start_d,
                end_d=end_d,
                scraped_at=scraped_at,
                backfill_complete=n >= len(gaps),
                mode="fill_gaps",
            )
            if not ing.get("ok"):
                print(f"    ingest hata: {ing.get('message')}", flush=True)

    return {
        "ok": True,
        "needs_login": False,
        "source": "sinemalar_moderation",
        "mode": "fill_gaps",
        "scraped_at": scraped_at,
        "range_start": start_d.isoformat(),
        "range_end": end_d.isoformat(),
        "gap_count": len(gaps),
        "detail_batches": batches,
        "item_count": total_items,
        "batch_count": len(batches),
        "message": f"fill_gaps {start_d} → {end_d} · {len(gaps)} eksik batch · {total_items} kayıt",
    }


def scrape_days(
    days: list[date],
    *,
    headed: bool = True,
    delay_sec: float = SCRAPE_DELAY_SEC,
) -> dict[str, Any]:
    if not days:
        return {"ok": False, "message": "Gün listesi boş", "days": []}

    pw, context, page = _open_logged_in_page(headed)
    if page is None:
        return {
            "ok": False,
            "needs_login": True,
            "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
            "days": [],
        }
    try:
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
        _safe_close_context(context, pw=pw, headed=headed)


def fetch_remote_meta() -> dict[str, Any]:
    req = urllib.request.Request(META_URL, headers=_auth_headers(), method="GET")
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
        "detail_batches": result.get("detail_batches") or [],
        "range_start": result.get("range_start"),
        "range_end": result.get("range_end"),
        "backfill_complete": bool(result.get("backfill_complete")),
        "backfill_cursor": result.get("backfill_cursor"),
        "purge_first": bool(result.get("purge_first")),
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
        timeout = 600 if body.get("detail_batches") else 120
        with urllib.request.urlopen(req, timeout=timeout) as resp:
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


def scrape_incremental_detail_day(
    day: date,
    *,
    headed: bool = True,
    delay_sec: float = SCRAPE_DELAY_SEC,
    ingest_per_batch: bool = False,
) -> dict[str, Any]:
    """Tek gün getModerationDetail — append-only ingest, purge yok.

    Sinemalar endDate hariç: tek gün için start=day, end=day+1.
    Tek tarayıcı oturumu ile 6×11 batch (Update page timeout riskini düşürür).
    """
    from backend.services.sinemalar_moderation import METRIC_TYPE_KEYS, TRACKED_MODERATORS

    batches: list[dict[str, Any]] = []
    total_items = 0
    n = 0
    scraped_at = datetime.now(timezone.utc).isoformat()
    # API endDate exclusive — inclusive tek gün
    end_exclusive = exclusive_detail_end(day)

    pw = ctx = page = None
    try:
        pw, ctx, page = _open_logged_in_page(headed)
        if page is None:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
                "detail_batches": batches,
                "report_date": day.isoformat(),
                "range_start": day.isoformat(),
                "range_end": day.isoformat(),
                "item_count": 0,
                "batch_count": 0,
            }

        for user_id, username in TRACKED_MODERATORS:
            for metric_type in METRIC_TYPE_KEYS:
                if n > 0 and delay_sec > 0:
                    time.sleep(delay_sec)
                n += 1
                batch: dict[str, Any] | None = None
                for attempt in range(2):
                    try:
                        batch = fetch_detail_page(
                            page,
                            user_id=user_id,
                            username=username,
                            metric_type=metric_type,
                            start_d=day,
                            end_d=end_exclusive,
                        )
                        break
                    except Exception as exc:
                        print(f"    ! {username}/{metric_type} hata (deneme {attempt + 1}): {exc}", flush=True)
                        batch = None
                        if attempt == 0:
                            # Oturum düşmüş olabilir — yeniden aç
                            _safe_close_context(ctx, pw=pw, headed=headed)
                            pw, ctx, page = _open_logged_in_page(headed)
                            if page is None:
                                return {
                                    "ok": False,
                                    "needs_login": True,
                                    "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
                                    "detail_batches": batches,
                                    "report_date": day.isoformat(),
                                    "range_start": day.isoformat(),
                                    "range_end": day.isoformat(),
                                    "item_count": total_items,
                                    "batch_count": len(batches),
                                }
                if batch is None:
                    continue
                item_count = int(batch.get("item_count") or 0)
                if item_count <= 0:
                    continue
                batches.append(batch)
                total_items += item_count
                if ingest_per_batch:
                    body: dict[str, Any] = {
                        "source": "sinemalar_moderation",
                        "mode": "detail_incremental",
                        "scraped_at": scraped_at,
                        "range_start": day.isoformat(),
                        "range_end": day.isoformat(),
                        "detail_batches": [
                            {
                                **batch,
                                "_sync_daily_date": day.isoformat(),
                                "_recompute_daily": False,
                            }
                        ],
                        "backfill_complete": True,
                    }
                    ing = ingest_result(body, mode="detail_incremental")
                    if not ing.get("ok"):
                        print(f"    ingest hata: {ing.get('message')}", flush=True)
    finally:
        _safe_close_context(ctx, pw=pw, headed=headed)

    return {
        "ok": True,
        "needs_login": False,
        "source": "sinemalar_moderation",
        "mode": "detail_incremental",
        "scraped_at": scraped_at,
        "report_date": day.isoformat(),
        "range_start": day.isoformat(),
        "range_end": day.isoformat(),
        "detail_batches": batches,
        "item_count": total_items,
        "batch_count": len(batches),
        "message": f"detail_incremental {day.isoformat()} · {total_items} kayıt · {len(batches)} batch",
    }


def run_incremental_detail(which: str = "yesterday", *, headed: bool = True, ingest: bool = False) -> dict[str, Any]:
    w = (which or "yesterday").strip().lower()
    if w == "both":
        targets = [_yesterday_tr(), _today_tr()]
    elif w == "today":
        targets = [_today_tr()]
    else:
        targets = [_yesterday_tr()]

    merged: dict[str, Any] = {
        "ok": True,
        "mode": "detail_incremental",
        "which": w,
        "days": [],
        "detail_batches": [],
        "item_count": 0,
        "batch_count": 0,
        "report_dates": [],
        "message": "",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    last: dict[str, Any] = {}
    for day in targets:
        result = scrape_incremental_detail_day(day, headed=headed, ingest_per_batch=bool(ingest))
        last = result
        if result.get("needs_login"):
            return result
        if not result.get("ok"):
            # İlk gün başarısızsa hemen dön; ikinci günde kısmi başarıyı koru
            if not merged["report_dates"]:
                return result
            merged["ok"] = False
            merged["message"] = result.get("message") or f"{day.isoformat()} failed"
            break
        merged["report_dates"].append(day.isoformat())
        merged["days"].extend(result.get("days") or [])
        merged["detail_batches"].extend(result.get("detail_batches") or [])
        merged["item_count"] += int(result.get("item_count") or 0)
        merged["batch_count"] += int(result.get("batch_count") or 0)
        if result.get("scraped_at"):
            merged["scraped_at"] = result["scraped_at"]
        if result.get("ingest"):
            merged["ingest"] = result["ingest"]

    if not merged["report_dates"] and last:
        return last

    dates_label = "+".join(merged["report_dates"]) or "?"
    if ingest and merged.get("ok") and not merged.get("detail_batches") and not merged.get("ingest"):
        # 0 kayıtta bile Last sync güncellensin
        day0 = targets[0]
        day1 = targets[-1]
        merged["message"] = f"detail_incremental {dates_label} · yeni kayıt yok"
        ping = ingest_result(
            {
                "source": "sinemalar_moderation",
                "mode": "detail_incremental",
                "scraped_at": merged.get("scraped_at"),
                "range_start": day0.isoformat(),
                "range_end": day1.isoformat(),
                "detail_batches": [],
                "backfill_complete": True,
                "sync_heartbeat": True,
                "message": merged["message"],
            },
            mode="detail_incremental",
        )
        merged["ingest"] = ping
    elif not merged.get("message"):
        merged["message"] = (
            f"detail_incremental {dates_label} · {merged['item_count']} kayıt · "
            f"{merged['batch_count']} batch"
        )
    merged["range_start"] = targets[0].isoformat()
    merged["range_end"] = targets[-1].isoformat()
    merged["report_date"] = targets[-1].isoformat()
    return merged


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
    parser.add_argument("--incremental", choices=("yesterday", "today", "both"), help="Tek gün özet (legacy)")
    parser.add_argument(
        "--incremental-detail",
        choices=("yesterday", "today", "both"),
        help="getModerationDetail append-only ingest (both = dün+bugün)",
    )
    parser.add_argument("--date", help="Tek gün YYYY-MM-DD (endDate otomatik +1 gün)")
    parser.add_argument(
        "--range",
        help="Aralık YYYY-MM-DD:YYYY-MM-DD (tek blok; günlük değil özet — doğrulama için)",
    )
    parser.add_argument(
        "--detail-range",
        help="getModerationDetail aralığı YYYY-MM-DD:YYYY-MM-DD (6 moderatör × 11 tip)",
    )
    parser.add_argument(
        "--detail-ingest-each",
        action="store_true",
        help="detail-range: her user×type sonrası hemen ingest",
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="detail-range / detail-monthly-2026: ingest öncesi tüm moderasyon verisini sil",
    )
    parser.add_argument(
        "--purge-only",
        action="store_true",
        help="Yalnızca Railway moderasyon verisini sil (scrape yok)",
    )
    parser.add_argument(
        "--detail-monthly-2026",
        action="store_true",
        help="2026 moderasyon — 7 aylık pencere ile detail çek + birleştir (dedup)",
    )
    parser.add_argument(
        "--fill-gaps",
        help="Sinemalar özet vs DB — yalnızca eksik user×type detail çeker YYYY-MM-DD:YYYY-MM-DD",
    )
    parser.add_argument(
        "--user-id",
        type=int,
        action="append",
        dest="user_ids",
        help="fill-gaps: yalnız bu moderatör(ler) (tekrarlanabilir)",
    )
    parser.add_argument("--ingest", action="store_true", help="Railway ingest")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()
    headed = not args.headless

    if args.login:
        from backend.services.scrape_browser import acquire_persistent_context, release_persistent_context

        pw, ctx, _reused = acquire_persistent_context(
            "sinemalar",
            profile=sinemalar_profile_dir(),
            headed=True,
            env_key="SINEMALAR_KEEP_OPEN",
            label="Sinemalar",
        )
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(SUMMARY_URL, wait_until="domcontentloaded", timeout=90000)
            print("Tarayıcı açık — giriş yapın (pencere KEEP_OPEN ile açık kalır).", flush=True)
            try:
                page.wait_for_timeout(300_000)
            except Exception:
                pass
        finally:
            release_persistent_context(
                "sinemalar",
                pw,
                ctx,
                headed=True,
                env_key="SINEMALAR_KEEP_OPEN",
                label="Sinemalar",
            )
        return 0

    if args.purge_only:
        out = run_purge_only()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.detail_monthly_2026:
        out = run_detail_monthly_2026(
            headed=headed,
            ingest=args.ingest,
            purge_first=bool(args.purge),
        )
        print(json.dumps({k: v for k, v in out.items() if k != "detail_batches"}, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

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

    if args.incremental_detail:
        out = run_incremental_detail(args.incremental_detail, headed=headed, ingest=args.ingest)
        print(json.dumps({k: v for k, v in out.items() if k != "detail_batches"}, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.incremental:
        out = run_incremental(args.incremental, headed=headed, ingest=args.ingest)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.fill_gaps:
        raw = str(args.fill_gaps).strip()
        if ":" not in raw:
            print("Geçersiz --fill-gaps (YYYY-MM-DD:YYYY-MM-DD)", file=sys.stderr)
            return 1
        start_s, end_s = raw.split(":", 1)
        try:
            start_d = date.fromisoformat(start_s[:10])
            end_d = date.fromisoformat(end_s[:10])
        except ValueError:
            print("Geçersiz --fill-gaps tarihleri", file=sys.stderr)
            return 1
        user_ids = args.user_ids or None
        print(
            f"Fill gaps: {start_d.isoformat()} → {end_d.isoformat()}"
            + (f" · user_ids={user_ids}" if user_ids else ""),
            flush=True,
        )
        out = scrape_fill_gaps(
            start_d,
            end_d,
            headed=headed,
            ingest_per_batch=bool(args.ingest),
            user_ids=user_ids,
        )
        summary = {k: v for k, v in out.items() if k != "detail_batches"}
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if args.detail_range:
        raw = str(args.detail_range).strip()
        if ":" not in raw:
            print("Geçersiz --detail-range (YYYY-MM-DD:YYYY-MM-DD)", file=sys.stderr)
            return 1
        start_s, end_s = raw.split(":", 1)
        try:
            start_d = date.fromisoformat(start_s[:10])
            end_d = date.fromisoformat(end_s[:10])
        except ValueError:
            print("Geçersiz --detail-range tarihleri", file=sys.stderr)
            return 1
        print(
            f"Detail range: {start_d.isoformat()} → {end_d.isoformat()} "
            f"(6 moderatör × 11 tip = 66 istek)",
            flush=True,
        )
        out = scrape_detail_range(
            start_d,
            end_d,
            headed=headed,
            ingest_per_batch=bool(args.detail_ingest_each and args.ingest),
            purge_first=bool(args.purge),
        )
        if args.ingest and out.get("ok") and not args.detail_ingest_each:
            ing = ingest_result(out, mode="detail_range")
            out["ingest"] = ing
            if not ing.get("ok"):
                out["ok"] = False
                out["message"] = ing.get("message") or "ingest failed"
        summary = {k: v for k, v in out.items() if k != "detail_batches"}
        print(json.dumps(summary, ensure_ascii=False, indent=2))
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
        from backend.services.scrape_browser import acquire_persistent_context, release_persistent_context

        url = f"{SUMMARY_URL}?startDate={start_d.isoformat()}&endDate={end_d.isoformat()}"
        pw, ctx, _reused = acquire_persistent_context(
            "sinemalar",
            profile=sinemalar_profile_dir(),
            headed=headed,
            env_key="SINEMALAR_KEEP_OPEN",
            label="Sinemalar",
            viewport={"width": 1400, "height": 900},
        )
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=90000)
            time.sleep(0.5)
            rows = page.evaluate(_EXTRACT_ROWS_JS) or []
        finally:
            release_persistent_context(
                "sinemalar",
                pw,
                ctx,
                headed=headed,
                env_key="SINEMALAR_KEEP_OPEN",
                label="Sinemalar",
            )
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
