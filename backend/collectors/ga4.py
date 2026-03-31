"""GA4 (Google Analytics Data API) collector: kanal bazlı sessions (WoW)."""

from __future__ import annotations

from datetime import datetime

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.ga4_auth import GA4_SCOPES, get_ga4_credentials_record, load_ga4_properties, load_ga4_service_account_info
from backend.services.metric_store import save_metrics
from backend.services.warehouse import finish_collector_run, start_collector_run


def _client() -> BetaAnalyticsDataClient:
    info = load_ga4_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=GA4_SCOPES)
    return BetaAnalyticsDataClient(credentials=creds)


def collect_ga4_channel_sessions(db: Session, site: Site, *, profile: str | None = None, days: int = 30) -> dict:
    """Son N gün vs önceki N gün: channel group bazlı sessions.

    Kaydedilen metrikler:
    - ga4_<profile>_sessions_last{N}d_total
    - ga4_<profile>_sessions_prev{N}d_total
    - ga4_<profile>_sessions_wow_change_pct
    - ga4_<profile>_sessions_last{N}d_channel__<slug>
    - ga4_<profile>_sessions_prev{N}d_channel__<slug>
    """

    run = start_collector_run(
        db,
        site_id=site.id,
        provider="ga4",
        strategy=f"channel_sessions:{profile or 'all'}:{int(days)}d",
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

        for profile_key, property_id in _profiles_to_fetch():
            safe_days = int(days) if int(days) > 0 else 30
            # lastNd: N days ago -> yesterday
            # prevNd: 2N days ago -> (N+1) days ago
            last_start = f"{safe_days}daysAgo"
            last_end = "yesterday"
            prev_start = f"{safe_days * 2}daysAgo"
            prev_end = f"{safe_days + 1}daysAgo"
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
                # metric_values: 2 date ranges -> [last7d, prev7d]
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

            summaries[profile_key] = {
                "property_id": property_id,
                "channels": len(last_by_channel),
                "days": safe_days,
                "last_total": last_total,
                "prev_total": prev_total,
                "wow_change_pct": wow_pct,
            }
            total_rows += len(last_by_channel)

        save_metrics(db, site.id, metrics, collected_at=collected_at)

        summary = {
            "state": "success",
            "profiles": summaries,
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
) -> list[dict]:
    """Landing page kırılımı: son N gün vs önceki N gün sessions."""

    safe_days = int(days) if int(days) > 0 else 30
    safe_limit = max(5, min(int(limit or 50), 200))
    last_start = f"{safe_days}daysAgo"
    last_end = "yesterday"
    prev_start = f"{safe_days * 2}daysAgo"
    prev_end = f"{safe_days + 1}daysAgo"

    client = _client()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="landingPagePlusQueryString")],
        metrics=[Metric(name="sessions")],
        date_ranges=[
            DateRange(name=f"last{safe_days}d", start_date=last_start, end_date=last_end),
            DateRange(name=f"prev{safe_days}d", start_date=prev_start, end_date=prev_end),
        ],
        limit=safe_limit,
    )
    response = client.run_report(request)

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

