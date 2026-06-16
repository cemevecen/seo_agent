"""Search Console tarih karşılaştırması — eksik günlük satırları API ile tamamlar."""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

from backend.collectors.search_console import (
    _fetch_search_console_daily_rows,
    _resolve_search_console_targets,
)
from backend.models import Site
from backend.services.ad_analytics_store import resolve_compare_range
from backend.services.analytics_compare import _sc_daily_coverage
from backend.services.search_console_auth import (
    SEARCH_CONSOLE_SCOPES,
    get_search_console_credentials_record,
    load_google_credentials,
)
from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

_FETCH_CACHE_TTL_SEC = 20 * 60
_sc_fetch_cache: dict[tuple, tuple[float, list[dict[str, Any]]]] = {}


def _parse_iso(d: str | None) -> date | None:
    if not d:
        return None
    try:
        return date.fromisoformat(str(d)[:10])
    except (ValueError, TypeError):
        return None


def _merge_daily_rows(*groups: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for rows in groups:
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            raw_d = str(r.get("date") or "").strip()[:10]
            if not raw_d:
                continue
            dev = str(r.get("device") or "ALL").upper()
            out[(raw_d, dev)] = {
                "date": raw_d,
                "device": dev,
                "clicks": float(r.get("clicks") or 0),
                "impressions": float(r.get("impressions") or 0),
                "position": float(r.get("position") or 0),
            }
    return list(out.values())


def _build_search_console_service(credential):
    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return None

    credential_data = load_google_credentials(credential)
    if credential.credential_type == "search_console_oauth":
        credentials = credential_data
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(GoogleAuthRequest())
    else:
        credentials = service_account.Credentials.from_service_account_info(
            credential_data,
            scopes=SEARCH_CONSOLE_SCOPES,
        )
    return build("searchconsole", "v1", credentials=credentials, cache_discovery=False)


def _range_needs_daily_supplement(rows: list[dict[str, Any]], start: date, end: date) -> bool:
    span = (end - start).days + 1
    for dev in ("MOBILE", "DESKTOP"):
        if _sc_daily_coverage(rows, dev, start, end) < span:
            return True
    return False


def fetch_search_console_daily_rows_for_site(
    db: Session,
    site: Site,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    cache_key = (int(site.id), start.isoformat(), end.isoformat())
    now = time.monotonic()
    hit = _sc_fetch_cache.get(cache_key)
    if hit and hit[0] > now:
        return list(hit[1])

    credential = get_search_console_credentials_record(db, site.id)
    if credential is None:
        return []
    service = _build_search_console_service(credential)
    if service is None:
        return []
    try:
        targets = _resolve_search_console_targets(service, site)
    except Exception as exc:
        LOGGER.warning("SC compare: targets failed site_id=%s %s", site.id, exc)
        return []

    span = max(1, (end - start).days + 1)
    row_limit = span + 10
    rows: list[dict[str, Any]] = []
    for target in targets:
        property_url = str(target.get("property_url") or "").strip()
        device = target.get("device")
        if not property_url:
            continue
        try:
            rows.extend(
                _fetch_search_console_daily_rows(
                    service,
                    property_url,
                    start,
                    end,
                    device=device,
                    row_limit=row_limit,
                )
            )
        except Exception as exc:
            LOGGER.warning(
                "SC compare: daily fetch failed site_id=%s %s–%s %s",
                site.id,
                start.isoformat(),
                end.isoformat(),
                exc,
            )
    if rows:
        _sc_fetch_cache[cache_key] = (time.monotonic() + _FETCH_CACHE_TTL_SEC, rows)
    return rows


def supplement_summary_for_compare(
    db: Session,
    site: Site,
    summary_payload: dict[str, Any],
    compare: dict[str, Any],
    period_primary_ranges: dict[str, tuple[str | None, str | None]],
) -> dict[str, Any]:
    """Özet JSON'da olmayan karşılaştırma günlerini (ör. geçen yıl) canlı API ile doldurur."""
    if not compare.get("enabled"):
        return summary_payload
    mode = compare.get("mode") or "previous_period"
    if mode == "previous_period":
        return summary_payload

    base_rows = _merge_daily_rows(
        summary_payload.get("trend_28d_rows"),
        summary_payload.get("trend_12m_rows"),
    )
    compare_starts: list[date] = []
    compare_ends: list[date] = []

    for period_key, pr in period_primary_ranges.items():
        if str(period_key) == "12m":
            continue
        ps = str(pr[0] or "").strip()[:10]
        pe = str(pr[1] or "").strip()[:10]
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
        if _range_needs_daily_supplement(base_rows, c_start, c_end):
            compare_starts.append(c_start)
            compare_ends.append(c_end)

    if not compare_starts:
        return summary_payload

    fetch_start = min(compare_starts)
    fetch_end = max(compare_ends)
    extra = fetch_search_console_daily_rows_for_site(db, site, fetch_start, fetch_end)
    if not extra:
        return summary_payload

    out = dict(summary_payload)
    out["trend_12m_rows"] = _merge_daily_rows(summary_payload.get("trend_12m_rows"), extra)
    out["trend_28d_rows"] = _merge_daily_rows(summary_payload.get("trend_28d_rows"), extra)
    out["compare_daily_supplemented"] = True
    return out
