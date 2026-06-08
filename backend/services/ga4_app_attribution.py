"""GA4 mobil — first_open / eventCount, first user campaign (Exploration «android banner»)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    OrderBy,
    RunReportRequest,
)

from backend.collectors.ga4 import _client
from backend.services.timezone_utils import report_calendar_yesterday

LOGGER = logging.getLogger(__name__)

CAMPAIGN_DIMENSION = "firstUserCampaignName"
_FIRST_OPEN_EVENT = "first_open"
_REPORT_ROW_LIMIT = 25_000


def _ga4_date_to_iso(raw: str) -> str:
    s = (raw or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat((value or "").strip()[:10])


def _first_open_filter() -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value=_FIRST_OPEN_EVENT,
            ),
        )
    )


def _calendar_dates(start: date, end: date) -> list[str]:
    out: list[str] = []
    day = start
    while day <= end:
        out.append(day.isoformat())
        day += timedelta(days=1)
    return out


def _series_from_buckets(
    buckets: dict[str, float],
    *,
    start: date,
    end: date,
) -> dict[str, list]:
    dates = _calendar_dates(start, end)
    return {
        "dates": dates,
        "values": [float(buckets.get(d, 0.0)) for d in dates],
    }


def _aggregate_rows(
    rows: list[Any],
    *,
    start: date,
    end: date,
    top_n: int,
) -> tuple[dict[str, float], dict[str, dict[str, float]]]:
    """Günlük toplam + kampanya→gün→değer; dönem toplamına göre top_n kampanya."""
    total_by_date: dict[str, float] = {}
    by_campaign_date: dict[str, dict[str, float]] = {}
    campaign_totals: dict[str, float] = {}

    for row in rows:
        dims = row.dimension_values or []
        if len(dims) < 2:
            continue
        d_iso = _ga4_date_to_iso(str(dims[0].value or ""))
        if not d_iso:
            continue
        try:
            d_obj = _parse_iso_date(d_iso)
        except ValueError:
            continue
        if d_obj < start or d_obj > end:
            continue
        campaign = str(dims[1].value or "").strip() or "(not set)"
        val = float((row.metric_values or [None])[0].value or 0)
        total_by_date[d_iso] = total_by_date.get(d_iso, 0.0) + val
        bucket = by_campaign_date.setdefault(campaign, {})
        bucket[d_iso] = bucket.get(d_iso, 0.0) + val
        campaign_totals[campaign] = campaign_totals.get(campaign, 0.0) + val

    ranked = sorted(campaign_totals.items(), key=lambda x: x[1], reverse=True)
    keep = {name for name, _ in ranked[: max(1, top_n)]}
    trimmed: dict[str, dict[str, float]] = {k: by_campaign_date[k] for k in keep if k in by_campaign_date}
    return total_by_date, trimmed


def _run_campaign_daily_report(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    start: str,
    end: str,
    metric_mode: str,
) -> list[Any]:
    pid = property_id if str(property_id).startswith("properties/") else f"properties/{property_id}"
    mode = (metric_mode or "first_opens").strip().lower()
    dim_filter = _first_open_filter() if mode == "first_opens" else None
    req = RunReportRequest(
        property=pid,
        dimensions=[
            Dimension(name="date"),
            Dimension(name=CAMPAIGN_DIMENSION),
        ],
        metrics=[Metric(name="eventCount")],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimension_filter=dim_filter,
        limit=_REPORT_ROW_LIMIT,
        order_bys=[
            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date")),
        ],
    )
    resp = client.run_report(req)
    return list(resp.rows or [])


def fetch_app_banner_attribution(
    property_id: str,
    *,
    start: str,
    end: str,
    top_campaigns: int = 10,
    metric_mode: str = "first_opens",
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """
    Exploration «android banner» ile uyumlu: günlük toplam + kampanya kırılımı (ilk N).

    metric_mode: first_opens (eventName=first_open) | event_count (tüm eventler).
    """
    if not str(property_id or "").strip():
        raise ValueError("GA4 property tanımlı değil.")

    start_d = _parse_iso_date(start)
    end_d = _parse_iso_date(end)
    if end_d < start_d:
        raise ValueError("Bitiş tarihi başlangıçtan önce olamaz.")
    span = (end_d - start_d).days + 1
    if span > 366:
        raise ValueError("En fazla 366 günlük aralık desteklenir.")

    ga4_client = client or _client()
    rows = _run_campaign_daily_report(
        ga4_client,
        property_id,
        start=start_d.isoformat(),
        end=end_d.isoformat(),
        metric_mode=metric_mode,
    )
    total_by_date, by_campaign = _aggregate_rows(
        rows,
        start=start_d,
        end=end_d,
        top_n=top_campaigns,
    )

    mode = (metric_mode or "first_opens").strip().lower()
    metric_label = "First opens" if mode == "first_opens" else "Event count"

    campaigns_out: list[dict[str, Any]] = []
    for name, day_map in sorted(
        by_campaign.items(),
        key=lambda item: sum(item[1].values()),
        reverse=True,
    ):
        campaigns_out.append(
            {
                "campaign": name,
                "total": round(sum(day_map.values())),
                "daily": _series_from_buckets(day_map, start=start_d, end=end_d),
            }
        )

    return {
        "metric_mode": mode,
        "metric_label": metric_label,
        "dimension": CAMPAIGN_DIMENSION,
        "dimension_label": "First user campaign",
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "top_campaigns": top_campaigns,
        "total_daily": _series_from_buckets(total_by_date, start=start_d, end=end_d),
        "campaigns": campaigns_out,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }


def default_banner_date_range(*, days: int = 28) -> tuple[str, str]:
    """Son N gün (dün dahil), exploration varsayılanı ile uyumlu."""
    n = max(1, min(int(days), 366))
    end = report_calendar_yesterday()
    start = end - timedelta(days=n - 1)
    return start.isoformat(), end.isoformat()
