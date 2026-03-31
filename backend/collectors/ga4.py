"""GA4 (Google Analytics Data API): son N gün vs önceki N gün — kanal, KPI, sayfa (haber hariç), kaynak."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

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
from google.oauth2 import service_account
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site
from backend.services.ga4_auth import GA4_SCOPES, get_ga4_credentials_record, load_ga4_properties, load_ga4_service_account_info
from backend.services.metric_store import save_metrics
from backend.services.warehouse import finish_collector_run, save_ga4_report_snapshot, start_collector_run

LOGGER = logging.getLogger(__name__)

KPI_METRIC_NAMES = (
    "sessions",
    "totalUsers",
    "newUsers",
    "engagedSessions",
    "engagementRate",
    "averageSessionDuration",
    "screenPageViews",
)


def _client() -> BetaAnalyticsDataClient:
    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def _calendar_windows(days: int) -> tuple[tuple[str, str], tuple[str, str]]:
    """İki N günlük pencere: (son N gün), (onun hemen önceki N günü)."""
    n = int(days) if int(days) > 0 else 30
    yesterday = date.today() - timedelta(days=1)
    last_end = yesterday
    last_start = yesterday - timedelta(days=n - 1)
    prev_end = last_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=n - 1)
    return (
        (last_start.isoformat(), last_end.isoformat()),
        (prev_start.isoformat(), prev_end.isoformat()),
    )


def _exclude_path_substrings() -> list[str]:
    raw = (getattr(settings, "ga4_exclude_path_substrings", None) or "").strip()
    if not raw:
        return ["/haber/", "/news/", "/gundem/"]
    return [p.strip() for p in raw.split(",") if p.strip()]


def _landing_exclude_filter(field_name: str = "landingPagePlusQueryString") -> FilterExpression | None:
    parts = _exclude_path_substrings()
    expressions: list[FilterExpression] = []
    for sub in parts:
        expressions.append(
            FilterExpression(
                filter=Filter(
                    field_name=field_name,
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.CONTAINS,
                        value=sub,
                        case_sensitive=False,
                    ),
                ),
            )
        )
    if not expressions:
        return None
    return FilterExpression(
        not_expression=FilterExpression(
            or_group=FilterExpressionList(expressions=expressions),
        ),
    )


def _pair_metric_values(metric_values: list, n_metrics: int) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for i in range(n_metrics):
        lo = 2 * i
        last = float(metric_values[lo].value or 0.0) if lo < len(metric_values) else 0.0
        prev = float(metric_values[lo + 1].value or 0.0) if lo + 1 < len(metric_values) else 0.0
        out.append((last, prev))
    return out


def _run_kpi_totals(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    last_start: str,
    last_end: str,
    prev_start: str,
    prev_end: str,
) -> tuple[dict[str, float], dict[str, float]]:
    names = list(KPI_METRIC_NAMES)
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[],
        metrics=[Metric(name=n) for n in names],
        date_ranges=[
            DateRange(name="last", start_date=last_start, end_date=last_end),
            DateRange(name="prev", start_date=prev_start, end_date=prev_end),
        ],
    )
    response = client.run_report(request)
    z = {k: 0.0 for k in names}
    if not response.rows:
        return z.copy(), z.copy()
    row = response.rows[0]
    pairs = _pair_metric_values(list(row.metric_values), len(names))
    last_d = {names[i]: pairs[i][0] for i in range(len(names))}
    prev_d = {names[i]: pairs[i][1] for i in range(len(names))}
    return last_d, prev_d


def _run_landing_pages_excl_news(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    last_start: str,
    last_end: str,
    prev_start: str,
    prev_end: str,
    limit: int = 100,
) -> list[dict]:
    filt = _landing_exclude_filter("landingPagePlusQueryString")
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [Dimension(name="landingPagePlusQueryString")],
        "metrics": [Metric(name="sessions")],
        "date_ranges": [
            DateRange(name="last", start_date=last_start, end_date=last_end),
            DateRange(name="prev", start_date=prev_start, end_date=prev_end),
        ],
        "limit": max(10, min(int(limit), 250)),
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
    }
    if filt is not None:
        req_kwargs["dimension_filter"] = filt
    response = client.run_report(RunReportRequest(**req_kwargs))

    rows: list[dict] = []
    for row in response.rows:
        page = str(row.dimension_values[0].value or "")
        last_v = float(row.metric_values[0].value or 0.0) if len(row.metric_values) > 0 else 0.0
        prev_v = float(row.metric_values[1].value or 0.0) if len(row.metric_values) > 1 else 0.0
        delta = last_v - prev_v
        delta_pct = (delta / prev_v * 100.0) if prev_v > 0 else (100.0 if last_v > 0 else 0.0)
        rows.append(
            {
                "page": page,
                "last_total": last_v,
                "prev_total": prev_v,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        )
    rows.sort(key=lambda item: item["last_total"], reverse=True)
    return rows[:50]


def _run_session_source_medium(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    last_start: str,
    last_end: str,
    prev_start: str,
    prev_end: str,
    limit: int = 60,
) -> list[dict]:
    response = client.run_report(
        RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="sessionSourceMedium")],
            metrics=[Metric(name="sessions")],
            date_ranges=[
                DateRange(name="last", start_date=last_start, end_date=last_end),
                DateRange(name="prev", start_date=prev_start, end_date=prev_end),
            ],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
            limit=max(10, min(int(limit), 250)),
        )
    )
    rows: list[dict] = []
    for row in response.rows:
        sm = str(row.dimension_values[0].value or "")
        last_v = float(row.metric_values[0].value or 0.0) if len(row.metric_values) > 0 else 0.0
        prev_v = float(row.metric_values[1].value or 0.0) if len(row.metric_values) > 1 else 0.0
        delta = last_v - prev_v
        delta_pct = (delta / prev_v * 100.0) if prev_v > 0 else (100.0 if last_v > 0 else 0.0)
        rows.append(
            {
                "source_medium": sm,
                "last_total": last_v,
                "prev_total": prev_v,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        )
    return rows[:50]


def collect_ga4_channel_sessions(db: Session, site: Site, *, profile: str | None = None, days: int = 30) -> dict:
    """Son N gün vs önceki N gün: kanal özeti, KPI, haber hariç sayfalar, session kaynak/ortam.

    Skaler metrikler (Metric tablosu) kanal + oturum toplamları için korunur.
    Detay tabloları `ga4_report_snapshots` içinde JSON olarak saklanır.
    """

    safe_days = int(days) if int(days) > 0 else 30
    run = start_collector_run(
        db,
        site_id=site.id,
        provider="ga4",
        strategy=f"ga4_{safe_days}d"[:20],
        target_url=site.domain,
    )
    collected_at = datetime.utcnow()

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    if not properties:
        finish_collector_run(
            db,
            run,
            status="failed",
            error_message="GA4 property tanımlı değil.",
            summary={"state": "failed", "error": "property_missing"},
        )
        return {"state": "failed", "error": "GA4 property tanımlı değil."}

    def _profiles_to_fetch() -> list[tuple[str, str]]:
        if profile:
            key = str(profile).strip().lower()
            if not key:
                return []
            prop = str(properties.get(key) or "").strip()
            return [(key, prop)] if prop else []
        return sorted([(k, v) for k, v in properties.items() if v], key=lambda item: item[0])

    try:
        client = _client()

        def slugify(value: str) -> str:
            safe = (value or "").strip().lower()
            safe = safe.replace(" ", "_").replace("-", "_")
            safe = "".join(ch for ch in safe if ch.isalnum() or ch == "_")
            return safe or "unknown"

        metrics: dict[str, float] = {}
        summaries: dict[str, dict] = {}
        total_rows = 0

        (last_start, last_end), (prev_start, prev_end) = _calendar_windows(safe_days)

        for profile_key, property_id in _profiles_to_fetch():
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name="sessionDefaultChannelGroup")],
                metrics=[Metric(name="sessions")],
                date_ranges=[
                    DateRange(name=f"last{safe_days}d", start_date=last_start, end_date=last_end),
                    DateRange(name=f"prev{safe_days}d", start_date=prev_start, end_date=prev_end),
                ],
                order_bys=[],
                limit=100,
            )
            response = client.run_report(request)

            last_by_channel: dict[str, float] = {}
            prev_by_channel: dict[str, float] = {}
            for row in response.rows:
                channel = str(row.dimension_values[0].value or "")
                last_value = float(row.metric_values[0].value or 0.0) if len(row.metric_values) > 0 else 0.0
                prev_value = float(row.metric_values[1].value or 0.0) if len(row.metric_values) > 1 else 0.0
                last_by_channel[channel] = last_value
                prev_by_channel[channel] = prev_value

            last_total = sum(last_by_channel.values())
            prev_total = sum(prev_by_channel.values())
            wow_pct = ((last_total - prev_total) / prev_total * 100.0) if prev_total > 0 else 0.0

            prefix = f"ga4_{profile_key}_sessions_"
            metrics[f"{prefix}last{safe_days}d_total"] = float(last_total)
            metrics[f"{prefix}prev{safe_days}d_total"] = float(prev_total)
            metrics[f"{prefix}wow_change_pct"] = float(wow_pct)
            for channel, value in last_by_channel.items():
                metrics[f"{prefix}last{safe_days}d_channel__{slugify(channel)}"] = float(value)
            for channel, value in prev_by_channel.items():
                metrics[f"{prefix}prev{safe_days}d_channel__{slugify(channel)}"] = float(value)

            # KPI + tablolar
            try:
                last_kpi, prev_kpi = _run_kpi_totals(
                    client,
                    property_id,
                    last_start=last_start,
                    last_end=last_end,
                    prev_start=prev_start,
                    prev_end=prev_end,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 KPI raporu başarısız (%s / %s): %s", site.domain, profile_key, exc)
                last_kpi = {k: 0.0 for k in KPI_METRIC_NAMES}
                prev_kpi = {k: 0.0 for k in KPI_METRIC_NAMES}

            try:
                pages_rows = _run_landing_pages_excl_news(
                    client,
                    property_id,
                    last_start=last_start,
                    last_end=last_end,
                    prev_start=prev_start,
                    prev_end=prev_end,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 sayfa (haber hariç) raporu başarısız (%s / %s): %s", site.domain, profile_key, exc)
                pages_rows = []

            try:
                sources_rows = _run_session_source_medium(
                    client,
                    property_id,
                    last_start=last_start,
                    last_end=last_end,
                    prev_start=prev_start,
                    prev_end=prev_end,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 kaynak/ortam raporu başarısız (%s / %s): %s", site.domain, profile_key, exc)
                sources_rows = []

            payload = {
                "summary": {"last": last_kpi, "prev": prev_kpi},
                "pages_no_news": pages_rows,
                "sources": sources_rows,
                "exclude_path_substrings": _exclude_path_substrings(),
            }
            save_ga4_report_snapshot(
                db,
                site_id=site.id,
                profile=profile_key,
                period_days=safe_days,
                last_start=last_start,
                last_end=last_end,
                prev_start=prev_start,
                prev_end=prev_end,
                payload=payload,
                collected_at=collected_at,
                collector_run_id=run.id,
            )

            for key in KPI_METRIC_NAMES:
                metrics[f"ga4_{profile_key}_kpi_last_{key}"] = float(last_kpi.get(key, 0.0))
                metrics[f"ga4_{profile_key}_kpi_prev_{key}"] = float(prev_kpi.get(key, 0.0))

            summaries[profile_key] = {
                "property_id": property_id,
                "channels": len(last_by_channel),
                "days": safe_days,
                "last_total": last_total,
                "prev_total": prev_total,
                "wow_change_pct": wow_pct,
                "kpi": {"last": last_kpi, "prev": prev_kpi},
                "pages_no_news_count": len(pages_rows),
                "sources_count": len(sources_rows),
            }
            total_rows += len(last_by_channel) + len(pages_rows) + len(sources_rows)

        save_metrics(db, site.id, metrics, collected_at=collected_at)

        summary = {
            "state": "success",
            "profiles": summaries,
            "ranges": {
                "last_start": last_start,
                "last_end": last_end,
                "prev_start": prev_start,
                "prev_end": prev_end,
            },
        }
        finish_collector_run(db, run, status="success", summary=summary, row_count=total_rows)
        return summary
    except Exception as exc:  # noqa: BLE001
        finish_collector_run(
            db,
            run,
            status="failed",
            error_message=str(exc),
            summary={"state": "failed", "error": str(exc), "properties": properties},
        )
        return {"state": "failed", "error": str(exc)}


def fetch_ga4_landing_pages(
    *,
    property_id: str,
    days: int = 30,
    limit: int = 50,
    exclude_news: bool = True,
) -> list[dict]:
    """Landing page kırılımı: son N gün vs önceki N gün sessions."""

    safe_days = int(days) if int(days) > 0 else 30
    safe_limit = max(5, min(int(limit or 50), 200))
    (last_start, last_end), (prev_start, prev_end) = _calendar_windows(safe_days)

    client = _client()
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [Dimension(name="landingPagePlusQueryString")],
        "metrics": [Metric(name="sessions")],
        "date_ranges": [
            DateRange(name=f"last{safe_days}d", start_date=last_start, end_date=last_end),
            DateRange(name=f"prev{safe_days}d", start_date=prev_start, end_date=prev_end),
        ],
        "limit": safe_limit,
    }
    if exclude_news:
        filt = _landing_exclude_filter("landingPagePlusQueryString")
        if filt is not None:
            req_kwargs["dimension_filter"] = filt
    response = client.run_report(RunReportRequest(**req_kwargs))

    rows: list[dict] = []
    for row in response.rows:
        page = str(row.dimension_values[0].value or "")
        last_value = float(row.metric_values[0].value or 0.0) if len(row.metric_values) > 0 else 0.0
        prev_value = float(row.metric_values[1].value or 0.0) if len(row.metric_values) > 1 else 0.0
        delta = last_value - prev_value
        delta_pct = (delta / prev_value * 100.0) if prev_value > 0 else (100.0 if last_value > 0 else 0.0)
        rows.append(
            {
                "page": page,
                "last_total": last_value,
                "prev_total": prev_value,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        )

    rows.sort(key=lambda item: item["last_total"], reverse=True)
    return rows
