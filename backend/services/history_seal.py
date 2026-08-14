"""Tarihsel gövde mühürü + günlük dilim (dün kaydet, bugünü kaydetme).

Politika (Europe/Istanbul):
- HISTORY_START (varsayılan 2025-01-01) → HISTORY_SEAL (varsayılan 2026-08-13)
  bir kez çekilir / mevcut veri mühürlenir; bir daha full reload yok.
- Mühür sonrası planlı scrape: yalnızca report_calendar_yesterday().
- Bugün (içinde bulunulan gün) kalıcı kayda yazılmaz.
- Seal ile dün arasında boşluk varsa yalnız o günler çekilir.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

from backend.services.timezone_utils import report_calendar_today, report_calendar_yesterday

DEFAULT_HISTORY_START = date(2025, 1, 1)
DEFAULT_HISTORY_SEAL = date(2026, 8, 13)

_STATE_DIR = Path.home() / ".seo-agent"
_META_PATH = _STATE_DIR / "history_seal.json"


def _parse_iso_date(raw: str | None, fallback: date) -> date:
    s = (raw or "").strip()
    if not s:
        return fallback
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return fallback


def history_start() -> date:
    return _parse_iso_date(
        os.environ.get("HISTORY_START") or os.environ.get("PLAY_CONSOLE_STATS_START"),
        DEFAULT_HISTORY_START,
    )


def history_seal() -> date:
    """Mühür günü dahil — bu güne kadar olan veri kalıcı gövde."""
    return _parse_iso_date(
        os.environ.get("HISTORY_SEAL") or os.environ.get("HISTORY_SEAL_DATE"),
        DEFAULT_HISTORY_SEAL,
    )


def calendar_today() -> date:
    return report_calendar_today()


def calendar_yesterday() -> date:
    return report_calendar_yesterday()


def never_store_today(d: date | str | None) -> bool:
    """True = bu tarih kalıcı kayda yazılmamalı (bugün veya gelecek)."""
    if d is None:
        return True
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d[:10])
        except ValueError:
            return True
    return d >= calendar_today()


def load_seal_meta() -> dict[str, Any]:
    try:
        if _META_PATH.is_file():
            data = json.loads(_META_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_seal_meta(patch: dict[str, Any]) -> dict[str, Any]:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    cur = load_seal_meta()
    cur.update(patch or {})
    cur["updated_at"] = calendar_today().isoformat()
    _META_PATH.write_text(json.dumps(cur, ensure_ascii=False, indent=2), encoding="utf-8")
    return cur


def mark_pipeline_sealed(
    pipeline: str,
    *,
    seal: date | None = None,
    note: str = "",
) -> dict[str, Any]:
    """Mevcut veriyi mühürle — full history bir daha çekilmez."""
    seal_d = seal or history_seal()
    pipelines = dict(load_seal_meta().get("pipelines") or {})
    pipelines[pipeline] = {
        "sealed": True,
        "seal_through": seal_d.isoformat(),
        "history_start": history_start().isoformat(),
        "note": (note or "existing data treated as complete")[:240],
        "marked_on": calendar_today().isoformat(),
    }
    return save_seal_meta({"pipelines": pipelines, "seal_through": seal_d.isoformat()})


def mark_all_expensive_pipelines_sealed(*, seal: date | None = None) -> dict[str, Any]:
    seal_d = seal or history_seal()
    for name in (
        "play",
        "asc",
        "gsc_links",
        "search_console",
        "ga4",
        "firebase",
        "empower",
        "empower_sinemalar",
        "notification",
        "doviz_news",
        "sinemalar_moderation",
        # policy bilinçli olarak yok — Ad Manager Policy her turda baştan çekilir
    ):
        mark_pipeline_sealed(name, seal=seal_d, note="bulk seal — panel data complete")
    return load_seal_meta()


def is_pipeline_sealed(pipeline: str) -> bool:
    """Varsayılan True: panel gövdesi HISTORY_SEAL'e kadar tamam kabul edilir.

    Opt-out: HISTORY_SEALED=0 veya {PIPELINE}_HISTORY_SEALED=0 / --force-full.
    """
    raw = (os.environ.get(f"{pipeline.upper()}_HISTORY_SEALED") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    global_raw = (os.environ.get("HISTORY_SEALED") or "").strip().lower()
    if global_raw in ("0", "false", "no", "off"):
        return False
    if global_raw in ("1", "true", "yes", "on"):
        return True
    info = (load_seal_meta().get("pipelines") or {}).get(pipeline) or {}
    if "sealed" in info:
        return bool(info.get("sealed"))
    # Mevcut panel verisi tamam (2026-08-13 mühür) — full reload yok
    return True


def force_full_history(pipeline: str | None = None) -> bool:
    """CLI/env: tek seferlik HISTORY_START → seal backfill."""
    if (os.environ.get("HISTORY_FORCE_FULL") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return True
    if pipeline:
        key = f"{pipeline.upper()}_FORCE_FULL"
        if (os.environ.get(key) or "").strip().lower() in ("1", "true", "yes", "on"):
            return True
    return False


def pipeline_seal_through(pipeline: str) -> date:
    info = (load_seal_meta().get("pipelines") or {}).get(pipeline) or {}
    return _parse_iso_date(str(info.get("seal_through") or ""), history_seal())


def iter_dates_inclusive(start: date, end: date) -> list[date]:
    if end < start:
        return []
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def missing_days(
    have: Iterable[date | str],
    *,
    start: date,
    end: date,
) -> list[date]:
    """[start, end] içinde eksik günler (bugün dahil edilmez)."""
    today = calendar_today()
    end = min(end, today - timedelta(days=1))
    if end < start:
        return []
    have_set: set[date] = set()
    for h in have:
        if isinstance(h, date):
            have_set.add(h)
        else:
            try:
                have_set.add(date.fromisoformat(str(h)[:10]))
            except ValueError:
                continue
    return [d for d in iter_dates_inclusive(start, end) if d not in have_set]


def scheduled_fetch_window(
    pipeline: str,
    *,
    force_full: bool | None = None,
    known_dates: Iterable[date | str] | None = None,
) -> dict[str, Any]:
    """Planlı scrape için çekilecek [start, end] (ikisi de dün veya gap aralığı).

    force_full=True → history_start → min(yesterday, seal) (tek seferlik backfill).
    Mühürlü + gap yok → start=end=yesterday.
    """
    if force_full is None:
        force_full = force_full_history(pipeline)
    yday = calendar_yesterday()
    start_hist = history_start()
    seal = pipeline_seal_through(pipeline)
    sealed = is_pipeline_sealed(pipeline) and not force_full

    if force_full or not sealed:
        end = min(yday, seal)
        start = start_hist
        if start > end:
            start = end
        mode = "backfill_full" if force_full or not sealed else "sealed_idle"
        return {
            "mode": "backfill_full" if (force_full or not sealed) else mode,
            "start": start,
            "end": end,
            "days": (end - start).days + 1,
            "sealed": sealed,
            "seal_through": seal,
            "yesterday": yday,
            "store_dates": True,  # end <= yesterday always
        }

    # Sealed: only yesterday (+ gaps between seal+1 and yesterday)
    gap_start = seal + timedelta(days=1)
    gaps: list[date] = []
    if known_dates is not None and gap_start <= yday:
        gaps = missing_days(known_dates, start=gap_start, end=yday)
    if gaps:
        return {
            "mode": "gap_fill",
            "start": gaps[0],
            "end": gaps[-1],
            "days": (gaps[-1] - gaps[0]).days + 1,
            "gap_dates": [d.isoformat() for d in gaps],
            "sealed": True,
            "seal_through": seal,
            "yesterday": yday,
            "store_dates": True,
        }

    # Normal daily: only previous calendar day
    return {
        "mode": "yesterday_only",
        "start": yday,
        "end": yday,
        "days": 1,
        "sealed": True,
        "seal_through": seal,
        "yesterday": yday,
        "store_dates": True,
    }


def filter_facts_no_today(
    facts: list[dict[str, Any]],
    *,
    date_key: str = "date",
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        raw = f.get(date_key)
        if never_store_today(raw if isinstance(raw, (date, str)) or raw is None else str(raw)):
            continue
        out.append(f)
    return out


def play_qs_date_range(start: date, end: date) -> str:
    def _fmt(d: date) -> str:
        return f"{d.year}_{d.month}_{d.day}"

    if start > end:
        start = end
    return f"{_fmt(start)}-{_fmt(end)}"
