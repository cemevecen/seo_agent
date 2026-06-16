"""GA4 tarih karşılaştırması — eksik günlük KPI serisini API ile tamamlar."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

from backend.collectors.ga4 import _client, _run_daily_kpi_trend
from backend.services.ad_analytics_store import resolve_compare_range
from backend.services.analytics_compare import _ga4_daily_coverage
from backend.services.ga4_auth import get_ga4_credentials_record

LOGGER = logging.getLogger(__name__)

_SERIES_KEYS = ("dates", "sessions", "activeUsers", "engagedSessions", "engagementRate")
_FETCH_CACHE_TTL_SEC = 20 * 60
_fetch_cache: dict[tuple, tuple[float, dict[str, Any]]] = {}


def _parse_iso(d: str | None) -> date | None:
    if not d:
        return None
    try:
        return date.fromisoformat(str(d)[:10])
    except (ValueError, TypeError):
        return None


def merge_ga4_daily_trends(base: dict[str, Any] | None, extra: dict[str, Any] | None) -> dict[str, Any]:
    a = base if isinstance(base, dict) else {}
    b = extra if isinstance(extra, dict) else {}
    by_date: dict[str, dict[str, float]] = {}

    def ingest(src: dict[str, Any]) -> None:
        dates = src.get("dates") or []
        for i, raw in enumerate(dates):
            d = str(raw or "").strip()[:10]
            if not d:
                continue
            row = by_date.setdefault(
                d,
                {"sessions": 0.0, "activeUsers": 0.0, "engagedSessions": 0.0, "engagementRate": 0.0},
            )
            for key in ("sessions", "activeUsers", "engagedSessions", "engagementRate"):
                arr = src.get(key) or []
                if i < len(arr):
                    row[key] = float(arr[i] or 0)

    ingest(a)
    ingest(b)

    sorted_dates = sorted(by_date.keys())
    out: dict[str, Any] = {k: [] for k in _SERIES_KEYS}
    for d in sorted_dates:
        row = by_date[d]
        out["dates"].append(d)
        out["sessions"].append(row["sessions"])
        out["activeUsers"].append(row["activeUsers"])
        out["engagedSessions"].append(row["engagedSessions"])
        out["engagementRate"].append(row["engagementRate"])
    return out


def _range_needs_supplement(daily: dict[str, Any], start: date, end: date) -> bool:
    span = (end - start).days + 1
    return _ga4_daily_coverage(daily, start, end) < span


def _compare_fetch_window(
    compare: dict[str, Any],
    period_payloads: dict[str, dict[str, Any]],
    daily: dict[str, Any],
) -> tuple[date, date] | None:
    mode = compare.get("mode") or "previous_period"
    compare_starts: list[date] = []
    compare_ends: list[date] = []
    for period in period_payloads.values():
        if not isinstance(period, dict):
            continue
        ranges = period.get("ranges") or {}
        ps = str(ranges.get("last_start") or "").strip()[:10]
        pe = str(ranges.get("last_end") or "").strip()[:10]
        if not ps or not pe:
            continue
        cs, ce = resolve_compare_range(
            ps,
            pe,
            mode,
            compare.get("custom_start"),
            compare.get("custom_end"),
        )
        c_start, c_end = _parse_iso(cs), _parse_iso(ce)
        if not c_start or not c_end:
            continue
        if _range_needs_supplement(daily, c_start, c_end):
            compare_starts.append(c_start)
            compare_ends.append(c_end)
    if not compare_starts:
        return None
    return min(compare_starts), max(compare_ends)


def fetch_ga4_daily_kpi(
    property_id: str,
    start: date,
    end: date,
    *,
    client=None,
) -> dict[str, Any]:
    prop = str(property_id or "").strip()
    if not prop:
        return {}
    try:
        ga4_client = client or _client()
        return _run_daily_kpi_trend(
            ga4_client,
            prop,
            start=start.isoformat(),
            end=end.isoformat(),
        )
    except Exception as exc:
        LOGGER.warning(
            "GA4 compare daily fetch failed property=%s %s–%s: %s",
            prop,
            start.isoformat(),
            end.isoformat(),
            exc,
        )
        return {}


def fetch_ga4_daily_kpi_cached(
    site_id: int,
    property_id: str,
    start: date,
    end: date,
    *,
    client=None,
) -> dict[str, Any]:
    key = (int(site_id), str(property_id), start.isoformat(), end.isoformat())
    now = time.monotonic()
    hit = _fetch_cache.get(key)
    if hit and hit[0] > now:
        return dict(hit[1])
    data = fetch_ga4_daily_kpi(property_id, start, end, client=client)
    if data.get("dates"):
        _fetch_cache[key] = (now + _FETCH_CACHE_TTL_SEC, data)
    return data


def apply_compare_daily_to_profiles(
    db,
    site_id: int,
    profiles: dict[str, dict[str, Any]],
    compare: dict[str, Any],
) -> None:
    """Site başına eksik karşılaştırma günlerini (profil başına tek API, önbellek + paralel)."""
    if not compare.get("enabled"):
        return
    if (compare.get("mode") or "previous_period") == "previous_period":
        return
    if get_ga4_credentials_record(db, site_id) is None:
        return

    ref_periods: dict[str, dict[str, Any]] = {}
    for pdata in profiles.values():
        periods = pdata.get("periods") or {}
        ref_periods = {k: periods[k] for k in ("7", "30", "90") if k in periods}
        if ref_periods:
            break
    if not ref_periods:
        return

    jobs: list[tuple[str, str, dict[str, Any]]] = []
    for profile_key, pdata in profiles.items():
        prop_id = str(pdata.get("property_id") or "").strip()
        if not prop_id:
            continue
        daily = (pdata.get("periods") or {}).get("12m") or {}
        daily_long = daily.get("daily_trend") if isinstance(daily, dict) else {}
        daily_long = daily_long if isinstance(daily_long, dict) else {}
        window = _compare_fetch_window(compare, ref_periods, daily_long)
        if not window:
            pdata["compare_daily_long"] = daily_long
            continue
        jobs.append((profile_key, prop_id, daily_long))

    if not jobs:
        return

    fetch_start, fetch_end = _compare_fetch_window(
        compare,
        ref_periods,
        jobs[0][2],
    )
    if not fetch_start or not fetch_end:
        return

    try:
        shared_client = _client()
    except Exception:
        shared_client = None

    def _fetch_one(item: tuple[str, str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        _pk, prop_id, daily_long = item
        extra = fetch_ga4_daily_kpi_cached(
            site_id,
            prop_id,
            fetch_start,
            fetch_end,
            client=shared_client,
        )
        if extra.get("dates"):
            return _pk, merge_ga4_daily_trends(daily_long, extra)
        return _pk, daily_long

    merged_by_profile: dict[str, dict[str, Any]] = {}
    if len(jobs) == 1:
        pk, merged = _fetch_one(jobs[0])
        merged_by_profile[pk] = merged
    else:
        with ThreadPoolExecutor(max_workers=min(4, len(jobs))) as pool:
            futures = {pool.submit(_fetch_one, job): job[0] for job in jobs}
            for fut in as_completed(futures):
                try:
                    pk, merged = fut.result()
                    merged_by_profile[pk] = merged
                except Exception as exc:
                    LOGGER.warning("GA4 compare parallel fetch failed: %s", exc)

    for profile_key, pdata in profiles.items():
        if profile_key in merged_by_profile:
            pdata["compare_daily_long"] = merged_by_profile[profile_key]
        elif "compare_daily_long" not in pdata:
            daily = (pdata.get("periods") or {}).get("12m") or {}
            pdata["compare_daily_long"] = daily.get("daily_trend") if isinstance(daily, dict) else {}


def supplement_ga4_daily_trend(
    db,
    site_id: int,
    property_id: str,
    daily_long: dict[str, Any] | None,
    compare: dict[str, Any],
    period_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not compare.get("enabled"):
        return daily_long if isinstance(daily_long, dict) else {}
    mode = compare.get("mode") or "previous_period"
    if mode == "previous_period":
        return daily_long if isinstance(daily_long, dict) else {}

    record = get_ga4_credentials_record(db, site_id)
    if record is None:
        return daily_long if isinstance(daily_long, dict) else {}

    daily = daily_long if isinstance(daily_long, dict) else {}
    compare_starts: list[date] = []
    compare_ends: list[date] = []

    for _pk, period in period_payloads.items():
        if not isinstance(period, dict):
            continue
        ranges = period.get("ranges") or {}
        ps = str(ranges.get("last_start") or "").strip()[:10]
        pe = str(ranges.get("last_end") or "").strip()[:10]
        if not ps or not pe:
            continue
        cs, ce = resolve_compare_range(
            ps,
            pe,
            mode,
            compare.get("custom_start"),
            compare.get("custom_end"),
        )
        c_start, c_end = _parse_iso(cs), _parse_iso(ce)
        if not c_start or not c_end:
            continue
        if _range_needs_supplement(daily, c_start, c_end):
            compare_starts.append(c_start)
            compare_ends.append(c_end)

    if not compare_starts:
        return daily

    fetch_start = min(compare_starts)
    fetch_end = max(compare_ends)
    extra = fetch_ga4_daily_kpi_cached(site_id, property_id, fetch_start, fetch_end)
    if not (extra.get("dates") or []):
        return daily

    return merge_ga4_daily_trends(daily, extra)
