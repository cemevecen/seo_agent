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
    FilterExpressionList,
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

MWEB_BANNER_EVENT_NAMES: tuple[str, ...] = (
    "app_download_banner_display",
    "app_download_banner_cd_display",
    "app_download_banner_dismiss",
    "app_download_banner_currency_detail_disp",
    "app_download_banner_click",
    "app_download_banner_cd_click",
    "app_download_banner_currency_detail_clic",
    "app_download_banner_show",
)


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
) -> list[Any]:
    pid = property_id if str(property_id).startswith("properties/") else f"properties/{property_id}"
    req = RunReportRequest(
        property=pid,
        dimensions=[
            Dimension(name="date"),
            Dimension(name=CAMPAIGN_DIMENSION),
        ],
        metrics=[Metric(name="eventCount")],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimension_filter=_first_open_filter(),
        limit=_REPORT_ROW_LIMIT,
        order_bys=[
            OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date")),
        ],
    )
    resp = client.run_report(req)
    return list(resp.rows or [])


def _slice_daily_series(series: dict[str, list] | None, *, from_date: str) -> dict[str, list]:
    if not series or not series.get("dates"):
        return {"dates": [], "values": []}
    dates = list(series["dates"])
    values = list(series.get("values") or [])
    idx = 0
    while idx < len(dates) and dates[idx] < from_date:
        idx += 1
    return {"dates": dates[idx:], "values": values[idx:]}


def _daily_has_signal(series: dict[str, list] | None) -> bool:
    if not series:
        return False
    for v in series.get("values") or []:
        if float(v or 0) > 0:
            return True
    return False


def _drop_zero_daily_series(payload: dict[str, Any]) -> None:
    """Trim sonrası grafik aralığında tamamen sıfır kalan serileri çıkar."""
    camps = [
        c
        for c in (payload.get("campaigns") or [])
        if isinstance(c, dict) and _daily_has_signal(c.get("daily"))
    ]
    payload["campaigns"] = camps

    mw = payload.get("mweb_banner")
    if isinstance(mw, dict):
        mw["events"] = [
            ev
            for ev in (mw.get("events") or [])
            if isinstance(ev, dict) and _daily_has_signal(ev.get("daily"))
        ]

    asc_c = payload.get("app_store_campaign_downloads")
    if isinstance(asc_c, dict) and asc_c.get("ok"):
        asc_c["campaigns"] = [
            c
            for c in (asc_c.get("campaigns") or [])
            if isinstance(c, dict) and _daily_has_signal(c.get("daily"))
        ]


def _first_signal_date(payload: dict[str, Any]) -> str | None:
    """İlk gerçek veri günü (sıfır doldurulmuş başlangıç hariç)."""
    found: list[str] = []

    def _scan(series: dict[str, list] | None) -> None:
        if not series:
            return
        for d, v in zip(series.get("dates") or [], series.get("values") or []):
            if float(v or 0) > 0:
                found.append(str(d)[:10])

    _scan(payload.get("total_daily"))
    for camp in payload.get("campaigns") or []:
        _scan((camp or {}).get("daily"))
    mw = payload.get("mweb_banner") or {}
    for ev in mw.get("events") or []:
        _scan((ev or {}).get("daily"))
    asc = payload.get("app_store_downloads") or {}
    if asc.get("ok") and isinstance(asc.get("daily"), dict):
        ad = asc["daily"]
        for d, v in zip(ad.get("dates") or [], ad.get("total_downloads") or []):
            if float(v or 0) > 0:
                found.append(str(d)[:10])
    asc_c = payload.get("app_store_campaign_downloads") or {}
    if asc_c.get("ok"):
        _scan(asc_c.get("combined_daily"))
        for c in asc_c.get("campaigns") or []:
            _scan((c or {}).get("daily"))
    return min(found) if found else None


def trim_banner_payload_to_observed_start(payload: dict[str, Any]) -> dict[str, Any]:
    """Kullanıcı aralığındaki yapay baştaki sıfırları keser; chart_range döner."""
    req_start = str(payload.get("start") or "")[:10]
    req_end = str(payload.get("end") or "")[:10]
    first = _first_signal_date(payload)
    if not first or (req_start and first <= req_start):
        payload["chart_start"] = req_start
        payload["chart_end"] = req_end
        return payload
    payload["chart_start"] = first
    payload["chart_end"] = req_end
    payload["total_daily"] = _slice_daily_series(payload.get("total_daily"), from_date=first)
    for camp in payload.get("campaigns") or []:
        camp["daily"] = _slice_daily_series(camp.get("daily"), from_date=first)
    mw = payload.get("mweb_banner")
    if isinstance(mw, dict):
        for ev in mw.get("events") or []:
            if isinstance(ev, dict):
                ev["daily"] = _slice_daily_series(ev.get("daily"), from_date=first)
    asc_c = payload.get("app_store_campaign_downloads")
    if isinstance(asc_c, dict) and asc_c.get("ok"):
        if isinstance(asc_c.get("combined_daily"), dict):
            asc_c["combined_daily"] = _slice_daily_series(asc_c["combined_daily"], from_date=first)
        for camp in asc_c.get("campaigns") or []:
            if isinstance(camp, dict):
                camp["daily"] = _slice_daily_series(camp.get("daily"), from_date=first)

    asc = payload.get("app_store_downloads")
    if isinstance(asc, dict) and asc.get("ok") and isinstance(asc.get("daily"), dict):
        ad = asc["daily"]
        asc["daily"] = {
            "dates": [d for d in (ad.get("dates") or []) if str(d)[:10] >= first],
            "total_downloads": [
                float((ad.get("total_downloads") or [0])[i])
                for i, d in enumerate(ad.get("dates") or [])
                if str(d)[:10] >= first
            ],
            "first_time_downloads": [
                float((ad.get("first_time_downloads") or [0])[i])
                for i, d in enumerate(ad.get("dates") or [])
                if str(d)[:10] >= first
            ],
            "redownloads": [
                float((ad.get("redownloads") or [0])[i])
                for i, d in enumerate(ad.get("dates") or [])
                if str(d)[:10] >= first
            ],
        }
    _drop_zero_daily_series(payload)
    return payload


def fetch_app_banner_attribution(
    property_id: str,
    *,
    start: str,
    end: str,
    top_campaigns: int = 10,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """Exploration «android banner»: günlük first_open + kampanya kırılımı (ilk N)."""
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
    )
    total_by_date, by_campaign = _aggregate_rows(
        rows,
        start=start_d,
        end=end_d,
        top_n=top_campaigns,
    )

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
        "metric_mode": "first_opens",
        "metric_label": "First opens",
        "dimension": CAMPAIGN_DIMENSION,
        "dimension_label": "First user campaign",
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "top_campaigns": top_campaigns,
        "total_daily": _series_from_buckets(total_by_date, start=start_d, end=end_d),
        "campaigns": campaigns_out,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }


def _os_filter_for_profile(profile: str) -> FilterExpression:
    prof = (profile or "").strip().lower()
    if prof == "ios":
        values = ["iOS", "Macintosh"]
    elif prof == "android":
        values = ["Android"]
    else:
        values = ["iOS", "Macintosh", "Android"]
    return FilterExpression(
        filter=Filter(
            field_name="operatingSystem",
            in_list_filter=Filter.InListFilter(values=values),
        )
    )


def _banner_events_filter() -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name="eventName",
            in_list_filter=Filter.InListFilter(values=list(MWEB_BANNER_EVENT_NAMES)),
        )
    )


def fetch_mweb_banner_events_daily(
    property_id: str,
    *,
    start: str,
    end: str,
    profile: str,
    client: BetaAnalyticsDataClient | None = None,
) -> dict[str, Any]:
    """mweb GA4 — app_download_banner display / dismiss / click (OS: iOS+Mac veya Android)."""
    if not str(property_id or "").strip():
        raise ValueError("GA4 mweb property tanımlı değil.")

    start_d = _parse_iso_date(start)
    end_d = _parse_iso_date(end)
    ga4_client = client or _client()
    pid = property_id if str(property_id).startswith("properties/") else f"properties/{property_id}"
    dim_filter = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[_os_filter_for_profile(profile), _banner_events_filter()],
        )
    )
    req = RunReportRequest(
        property=pid,
        dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        date_ranges=[DateRange(start_date=start_d.isoformat(), end_date=end_d.isoformat())],
        dimension_filter=dim_filter,
        limit=_REPORT_ROW_LIMIT,
        order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
    )
    resp = ga4_client.run_report(req)

    by_event_date: dict[str, dict[str, float]] = {name: {} for name in MWEB_BANNER_EVENT_NAMES}

    for row in resp.rows or []:
        dims = row.dimension_values or []
        if len(dims) < 2:
            continue
        d_iso = _ga4_date_to_iso(str(dims[0].value or ""))
        ev = str(dims[1].value or "").strip()
        if ev not in by_event_date or not d_iso:
            continue
        val = float((row.metric_values or [None])[0].value or 0)
        bucket = by_event_date[ev]
        bucket[d_iso] = bucket.get(d_iso, 0.0) + val

    events_out: list[dict[str, Any]] = []
    for name in MWEB_BANNER_EVENT_NAMES:
        day_map = by_event_date.get(name) or {}
        total = sum(day_map.values())
        if total <= 0:
            continue
        events_out.append(
            {
                "event_name": name,
                "total": int(round(total)),
                "daily": _series_from_buckets(day_map, start=start_d, end=end_d),
            }
        )

    os_label = "iOS + Macintosh" if (profile or "").lower() == "ios" else "Android"
    return {
        "source": "ga4_mweb",
        "operating_systems": os_label,
        "events": events_out,
    }


def slice_asc_downloads_daily(
    asc_summary: dict[str, Any] | None,
    *,
    start: date,
    end: date,
) -> dict[str, Any] | None:
    """ASC Analytics özetinden seçili tarih aralığına günlük indirme serisi."""
    if not asc_summary or not asc_summary.get("ok"):
        return asc_summary if asc_summary else None
    dates_in = list(asc_summary.get("dates") or [])
    if not dates_in:
        return {
            "ok": False,
            "message": asc_summary.get("message") or "ASC günlük veri yok.",
            "warnings": asc_summary.get("warnings") or [],
        }
    by_date: dict[str, dict[str, float]] = {}
    for i, ds in enumerate(dates_in):
        key = str(ds)[:10]
        by_date[key] = {
            "total_downloads": float((asc_summary.get("total_downloads_series") or [0])[i] or 0),
            "first_time_downloads": float((asc_summary.get("first_downloads_series") or [0])[i] or 0),
            "redownloads": float((asc_summary.get("redownloads_series") or [0])[i] or 0),
        }
    cal_dates = _calendar_dates(start, end)
    dates_out: list[str] = []
    td_out: list[float] = []
    ft_out: list[float] = []
    rd_out: list[float] = []
    for d in cal_dates:
        row = by_date.get(d)
        if not row:
            continue
        if (
            row.get("total_downloads", 0) <= 0
            and row.get("first_time_downloads", 0) <= 0
            and row.get("redownloads", 0) <= 0
        ):
            continue
        dates_out.append(d)
        td_out.append(float(row.get("total_downloads") or 0))
        ft_out.append(float(row.get("first_time_downloads") or 0))
        rd_out.append(float(row.get("redownloads") or 0))
    return {
        "ok": True,
        "source": "app_store_connect",
        "note": "Tüm uygulama Total Downloads (kampanya filtresi yok; ASC Campaigns UI ayrı).",
        "daily": {
            "dates": dates_out,
            "total_downloads": td_out,
            "first_time_downloads": ft_out,
            "redownloads": rd_out,
        },
        "warnings": asc_summary.get("warnings") or [],
    }


def default_banner_date_range(*, days: int = 28) -> tuple[str, str]:
    """Son N gün (dün dahil), exploration varsayılanı ile uyumlu."""
    n = max(1, min(int(days), 366))
    end = report_calendar_yesterday()
    start = end - timedelta(days=n - 1)
    return start.isoformat(), end.isoformat()
