"""GA4 (Google Analytics Data API): son N gün vs önceki N gün — kanal, KPI, sayfa (haber hariç), kaynak."""

from __future__ import annotations

import logging
import re
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

from backend.locale.tr import weekday_tr
from backend.models import Site
from backend.services.ga4_auth import GA4_SCOPES, get_ga4_credentials_record, load_ga4_properties, load_ga4_service_account_info
from backend.services.ga4_page_urls import ga4_canonical_page_url
from backend.services.metric_store import save_metrics
from backend.services.timezone_utils import report_calendar_yesterday
from backend.services.warehouse import finish_collector_run, save_ga4_report_snapshot, start_collector_run

LOGGER = logging.getLogger(__name__)

# Haber landing raporu: sonsuz bekleme yerine makul üst süre (özellikle local / zayıf ağ).
_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC = 120.0


def _channel_pct_change(last_v: float, prev_v: float) -> float:
    """Önceki döneme göre % değişim (main._ga4_period_pct_change ile aynı mantık)."""
    try:
        lv = float(last_v or 0.0)
        pv = float(prev_v or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if pv > 0.0:
        return (lv - pv) / pv * 100.0
    return 100.0 if lv > 0.0 else 0.0


# GA4 Data API — Traffic acquisition vb. arayüzdeki "Users" = activeUsers ile daha uyumlu.
KPI_METRIC_NAMES = (
    "sessions",
    "totalUsers",
    "activeUsers",
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
    yesterday = report_calendar_yesterday()
    last_end = yesterday
    last_start = yesterday - timedelta(days=n - 1)
    prev_end = last_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=n - 1)
    return (
        (last_start.isoformat(), last_end.isoformat()),
        (prev_start.isoformat(), prev_end.isoformat()),
    )


def _same_weekday_day_windows() -> tuple[tuple[str, str], tuple[str, str]]:
    """Son tam gün (dün) vs 7 gün önceki aynı takvim günü — same_weekday KPI / 1g kart ile uyumlu."""
    yesterday = report_calendar_yesterday()
    last_start = yesterday.isoformat()
    last_end = yesterday.isoformat()
    wow_prev = yesterday - timedelta(days=7)
    prev_start = wow_prev.isoformat()
    prev_end = wow_prev.isoformat()
    return (
        (last_start, last_end),
        (prev_start, prev_end),
    )


def _exclude_path_substrings() -> list[str]:
    from backend.config import settings

    raw = (getattr(settings, "ga4_exclude_path_substrings", None) or "").strip()
    if not raw:
        return ["/haber/", "/news/", "/gundem/"]
    return [p.strip() for p in raw.split(",") if p.strip()]


# Son path segmenti sayısal ID olan haber/makale detay sayfalarını tespit et.
# Örnek: /gundem-haberleri/baslik/837872  →  haber detayı (çıkar)
#         /gundem-haberleri               →  kategori sayfası (koru)
_NEWS_DETAIL_PATH_RE = re.compile(r"/\d+(?:[/?#].*)?$")


def _is_news_detail_path(path: str) -> bool:
    """Path'in son segmenti sayısal ID ise haber detay sayfasıdır."""
    return bool(_NEWS_DETAIL_PATH_RE.search(path))


def _path_contains_news_marker(path: str) -> bool:
    """Yapılandırılmış haber path alt dizeleri (ga4_exclude_path_substrings ile uyumlu)."""
    low = (path or "").lower().replace("\\", "/")
    for sub in _exclude_path_substrings():
        frag = (sub or "").strip().lower().replace("\\", "/")
        if frag and frag in low:
            return True
    return False


def _is_news_article_path(path: str) -> bool:
    """Haber detayı (sayısal ID), haber bölümü path'i veya *-haberleri yapısı."""
    from backend.services.realtime_news_paths import is_news_detail_path, path_has_haberleri_segment

    if is_news_detail_path(path):
        return True
    if _path_contains_news_marker(path):
        return True
    if path_has_haberleri_segment(path):
        return True
    parts = [p for p in (path or "").split("/") if p]
    if len(parts) == 2 and parts[-1].isdigit() and len(parts[0]) >= 3 and not parts[0].isdigit():
        return True
    return False


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


def _landing_news_include_filter(field_name: str = "landingPagePlusQueryString") -> FilterExpression | None:
    """Haber landing'leri: API tarafında süz — tüm siteden top-N çekip sonra filtrelemek web'de satır kaçırır."""
    exprs: list[FilterExpression] = []
    # Son segment sayısal ID (_NEWS_DETAIL_PATH_RE ile aynı mantık: .../847860, .../847860/amp...)
    exprs.append(
        FilterExpression(
            filter=Filter(
                field_name=field_name,
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.FULL_REGEXP,
                    value=r"^.*/[0-9]+(?:[/?#].*)?$",
                    case_sensitive=False,
                ),
            ),
        )
    )
    for sub in _exclude_path_substrings():
        frag = (sub or "").strip()
        if not frag:
            continue
        exprs.append(
            FilterExpression(
                filter=Filter(
                    field_name=field_name,
                    string_filter=Filter.StringFilter(
                        match_type=Filter.StringFilter.MatchType.CONTAINS,
                        value=frag,
                        case_sensitive=False,
                    ),
                ),
            )
        )
    exprs.append(
        FilterExpression(
            filter=Filter(
                field_name=field_name,
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.CONTAINS,
                    value="-haberleri",
                    case_sensitive=False,
                ),
            ),
        )
    )
    if not exprs:
        return None
    if len(exprs) == 1:
        return exprs[0]
    return FilterExpression(or_group=FilterExpressionList(expressions=exprs))


def _metrics_from_report_row(response) -> dict[str, float]:
    """RunReport cevabının ilk satırından metrik adı -> değer (metric_headers ile; sıra kayması yok)."""
    headers = list(response.metric_headers or [])
    names = [str(getattr(h, "name", "") or "").strip() for h in headers if str(getattr(h, "name", "") or "").strip()]
    rows = list(response.rows or [])
    if not rows or not names:
        return {}
    vals = list(rows[0].metric_values or [])
    out: dict[str, float] = {}
    for i, name in enumerate(names):
        if i < len(vals):
            out[name] = float(vals[i].value or 0.0)
    return out


def _run_kpi_for_single_range(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    start: str,
    end: str,
) -> dict[str, float]:
    """Tek tarih aralığında KPI toplamları (çoklu dateRange parse hatası yok)."""
    names = list(KPI_METRIC_NAMES)
    response = client.run_report(
        RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[],
            metrics=[Metric(name=n) for n in names],
            date_ranges=[DateRange(start_date=start, end_date=end)],
        )
    )
    z = {k: 0.0 for k in names}
    if not response.rows:
        return z
    parsed = _metrics_from_report_row(response)
    if parsed and "sessions" in parsed:
        for k in names:
            z[k] = float(parsed.get(k, 0.0))
        return z
    row = response.rows[0]
    for i, name in enumerate(names):
        if i < len(row.metric_values):
            z[name] = float(row.metric_values[i].value or 0.0)
    return z


def _run_kpi_totals(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    last_start: str,
    last_end: str,
    prev_start: str,
    prev_end: str,
) -> tuple[dict[str, float], dict[str, float]]:
    last_d = _run_kpi_for_single_range(client, property_id, start=last_start, end=last_end)
    prev_d = _run_kpi_for_single_range(client, property_id, start=prev_start, end=prev_end)
    return last_d, prev_d


def _empty_daily_trend() -> dict[str, list]:
    return {
        "dates": [],
        "sessions": [],
        "activeUsers": [],
        "engagedSessions": [],
        "engagementRate": [],
    }


def _run_daily_kpi_trend(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    start: str,
    end: str,
) -> dict[str, list]:
    """Son tarih aralığında günlük: sessions, activeUsers, engagedSessions, engagementRate (0–100)."""
    trend_metrics = ("sessions", "activeUsers", "engagedSessions", "engagementRate")
    response = client.run_report(
        RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name=n) for n in trend_metrics],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            limit=5000,
            order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        )
    )
    m_headers = [str(getattr(h, "name", "") or "").strip() for h in (response.metric_headers or []) if getattr(h, "name", None)]
    dates: list[str] = []
    sessions: list[float] = []
    users: list[float] = []
    engaged: list[float] = []
    er_pct: list[float] = []
    for row in response.rows or []:
        raw = str(row.dimension_values[0].value or "")
        if len(raw) == 8 and raw.isdigit():
            d_iso = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
        else:
            d_iso = raw
        dates.append(d_iso)
        vals = list(row.metric_values or [])
        if m_headers and len(m_headers) == len(vals):
            m: dict[str, float] = {m_headers[i]: float(vals[i].value or 0) for i in range(len(m_headers))}
            sessions.append(float(m.get("sessions", 0.0)))
            users.append(float(m.get("activeUsers", 0.0)))
            engaged.append(float(m.get("engagedSessions", 0.0)))
            er_raw = float(m.get("engagementRate", 0.0))
        else:
            sessions.append(float(vals[0].value or 0) if len(vals) > 0 else 0.0)
            users.append(float(vals[1].value or 0) if len(vals) > 1 else 0.0)
            engaged.append(float(vals[2].value or 0) if len(vals) > 2 else 0.0)
            er_raw = float(vals[3].value or 0) if len(vals) > 3 else 0.0
        er_pct.append(er_raw * 100.0 if er_raw <= 1.0 else er_raw)
    return {
        "dates": dates,
        "sessions": sessions,
        "activeUsers": users,
        "engagedSessions": engaged,
        "engagementRate": er_pct,
    }


def _fill_daily_trend_calendar(
    daily: dict[str, list],
    *,
    start: date,
    end: date,
) -> dict[str, list]:
    """API'de eksik günleri sıfırla doldurur (12 ay trend grafiği için)."""
    by_date: dict[str, dict[str, float]] = {}
    dates_in = list(daily.get("dates") or [])
    for idx, key in enumerate(dates_in):
        if not key:
            continue
        by_date[key] = {
            "sessions": float((daily.get("sessions") or [0])[idx] if idx < len(daily.get("sessions") or []) else 0),
            "activeUsers": float(
                (daily.get("activeUsers") or [0])[idx] if idx < len(daily.get("activeUsers") or []) else 0
            ),
            "engagedSessions": float(
                (daily.get("engagedSessions") or [0])[idx] if idx < len(daily.get("engagedSessions") or []) else 0
            ),
            "engagementRate": float(
                (daily.get("engagementRate") or [0])[idx] if idx < len(daily.get("engagementRate") or []) else 0
            ),
        }
    out_dates: list[str] = []
    out_sessions: list[float] = []
    out_users: list[float] = []
    out_engaged: list[float] = []
    out_er: list[float] = []
    day = start
    while day <= end:
        key = day.isoformat()
        bucket = by_date.get(key) or {}
        out_dates.append(key)
        out_sessions.append(float(bucket.get("sessions") or 0.0))
        out_users.append(float(bucket.get("activeUsers") or 0.0))
        out_engaged.append(float(bucket.get("engagedSessions") or 0.0))
        out_er.append(float(bucket.get("engagementRate") or 0.0))
        day += timedelta(days=1)
    return {
        "mode": "last_12m",
        "dates": out_dates,
        "sessions": out_sessions,
        "activeUsers": out_users,
        "engagedSessions": out_engaged,
        "engagementRate": out_er,
    }


def ga4_scheduled_kpi_period_days() -> tuple[int, ...]:
    """Gece job ve toplu yenileme için KPI gün aralıkları (varsayılan: 1/7/30/60/90)."""
    from backend.config import settings

    raw = str(settings.ga4_scheduled_kpi_period_days or "1,7,30,60,90")
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            days = int(part)
        except ValueError:
            continue
        if days > 0 and days not in out:
            out.append(days)
    return tuple(out or (1, 7, 30, 60, 90))


def collect_ga4_scheduled_site_metrics(db: Session, site: Site) -> None:
    """Gece/manuel toplu GA4: KPI dönemleri + 12 ay günlük trend."""
    for days in ga4_scheduled_kpi_period_days():
        collect_ga4_channel_sessions(db, site, days=days)
    collect_ga4_12m_daily_trend(db, site)


def collect_ga4_12m_daily_trend(db: Session, site: Site, *, profile: str | None = None) -> dict:
    """Son 12 ay günlük KPI trendi (karşılaştırma / kanal / sayfa yok)."""
    from backend.config import settings
    from backend.services.warehouse import save_ga4_report_snapshot

    trend_days = max(30, int(settings.ga4_trend_12m_days))
    period_days = int(settings.ga4_trend_12m_period_days)
    collected_at = datetime.utcnow()
    run = start_collector_run(
        db,
        site_id=site.id,
        provider="ga4",
        strategy="ga4_12m_trend",
        target_url=site.domain,
    )

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)
    if not properties:
        finish_collector_run(
            db,
            run,
            status="failed",
            error_message="GA4 property tanımlı değil.",
            summary={"state": "failed"},
        )
        return {"state": "failed", "error": "GA4 property tanımlı değil."}

    def _profiles_to_fetch() -> list[tuple[str, str]]:
        if profile:
            key = str(profile).strip().lower()
            prop = str(properties.get(key) or "").strip()
            return [(key, prop)] if prop else []
        return sorted([(k, v) for k, v in properties.items() if v], key=lambda item: item[0])

    yesterday = report_calendar_yesterday()
    last_end = yesterday
    last_start = yesterday - timedelta(days=trend_days - 1)
    last_start_s = last_start.isoformat()
    last_end_s = last_end.isoformat()

    summaries: dict[str, dict] = {}
    try:
        client = _client()
        for profile_key, property_id in _profiles_to_fetch():
            try:
                daily = _run_daily_kpi_trend(
                    client,
                    property_id,
                    start=last_start_s,
                    end=last_end_s,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 12m trend başarısız (%s / %s): %s", site.domain, profile_key, exc)
                daily = _empty_daily_trend()
            filled = _fill_daily_trend_calendar(daily, start=last_start, end=last_end)
            payload = {
                "trend_only": True,
                "daily_trend": filled,
            }
            save_ga4_report_snapshot(
                db,
                site_id=site.id,
                profile=profile_key,
                period_days=period_days,
                last_start=last_start_s,
                last_end=last_end_s,
                prev_start=last_start_s,
                prev_end=last_end_s,
                payload=payload,
                collected_at=collected_at,
                collector_run_id=run.id,
            )
            summaries[profile_key] = {
                "property_id": property_id,
                "days": len(filled.get("dates") or []),
                "has_signal": any(float(v or 0) > 0 for v in (filled.get("sessions") or []))
                or any(float(v or 0) > 0 for v in (filled.get("activeUsers") or [])),
            }
        finish_collector_run(
            db,
            run,
            status="success",
            finished_at=collected_at,
            summary={"state": "success", "profiles": summaries, "trend_days": trend_days},
            row_count=len(summaries),
        )
        return {"state": "success", "profiles": summaries}
    except Exception as exc:  # noqa: BLE001
        finish_collector_run(
            db,
            run,
            status="failed",
            finished_at=datetime.utcnow(),
            error_message=str(exc),
            summary={"state": "failed", "error": str(exc)},
        )
        return {"state": "failed", "error": str(exc)}


def _run_dim_sessions_single_range(
    client: BetaAnalyticsDataClient,
    property_id: str,
    dimension_name: str,
    *,
    start: str,
    end: str,
    limit: int,
    dimension_filter: FilterExpression | None = None,
) -> dict[str, float]:
    """Tek dönem: boyut başına oturum sayısı."""
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [Dimension(name=dimension_name)],
        "metrics": [Metric(name="sessions")],
        "date_ranges": [DateRange(start_date=start, end_date=end)],
        "limit": max(10, min(int(limit), 250)),
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
    }
    if dimension_filter is not None:
        req_kwargs["dimension_filter"] = dimension_filter
    response = client.run_report(RunReportRequest(**req_kwargs))
    out: dict[str, float] = {}
    for row in response.rows:
        key = str(row.dimension_values[0].value or "")
        val = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        out[key] = val
    return out


def _run_landing_host_path_sessions_single_range(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    start: str,
    end: str,
    limit: int,
    dimension_filter: FilterExpression | None = None,
) -> dict[str, float]:
    """hostName + landingPagePlusQueryString -> sessions (anahtar: host\\x1fpath)."""
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [
            Dimension(name="hostName"),
            Dimension(name="landingPagePlusQueryString"),
        ],
        "metrics": [Metric(name="sessions")],
        "date_ranges": [DateRange(start_date=start, end_date=end)],
        "limit": max(10, min(int(limit), 250)),
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
    }
    if dimension_filter is not None:
        req_kwargs["dimension_filter"] = dimension_filter
    response = client.run_report(RunReportRequest(**req_kwargs))
    out: dict[str, float] = {}
    for row in response.rows:
        if len(row.dimension_values) < 2:
            continue
        host = str(row.dimension_values[0].value or "").strip()
        path = str(row.dimension_values[1].value or "").strip()
        key = f"{host}\x1f{path}"
        val = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        out[key] = val
    return out


def _run_landing_host_path_metric_single_range(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    metric_name: str,
    path_dimension: str = "landingPagePlusQueryString",
    start: str,
    end: str,
    limit: int,
    dimension_filter: FilterExpression | None = None,
    timeout: float | None = None,
) -> dict[str, float]:
    """hostName + path_dimension -> seçilen metric (anahtar: host\\x1fpath)."""
    metric = str(metric_name or "sessions").strip() or "sessions"
    dim = str(path_dimension or "landingPagePlusQueryString").strip() or "landingPagePlusQueryString"
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [
            Dimension(name="hostName"),
            Dimension(name=dim),
        ],
        "metrics": [Metric(name=metric)],
        "date_ranges": [DateRange(start_date=start, end_date=end)],
        "limit": max(10, min(int(limit), 250)),
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name=metric), desc=True)],
    }
    if dimension_filter is not None:
        req_kwargs["dimension_filter"] = dimension_filter
    call_kw: dict = {}
    if timeout is not None:
        call_kw["timeout"] = float(timeout)
    response = client.run_report(RunReportRequest(**req_kwargs), **call_kw)
    out: dict[str, float] = {}
    for row in response.rows:
        if len(row.dimension_values) < 2:
            continue
        host = str(row.dimension_values[0].value or "").strip()
        path = str(row.dimension_values[1].value or "").strip()
        key = f"{host}\x1f{path}"
        val = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        out[key] = val
    return out


def _merge_period_maps(
    last_map: dict[str, float],
    prev_map: dict[str, float],
) -> dict[str, tuple[float, float]]:
    keys = set(last_map) | set(prev_map)
    return {k: (float(last_map.get(k, 0.0)), float(prev_map.get(k, 0.0))) for k in keys}


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
    # Analytics "Pages and screens" ile uyum: pagePath + screenPageViews
    filt = _landing_exclude_filter("pagePath")
    lim = max(10, min(int(limit), 250))
    last_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=last_start,
        end=last_end,
        limit=lim,
        dimension_filter=filt,
    )
    prev_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=prev_start,
        end=prev_end,
        limit=lim,
        dimension_filter=filt,
    )
    merged = _merge_period_maps(last_map, prev_map)
    rows: list[dict] = []
    for key, (last_v, prev_v) in merged.items():
        host, sep, path = key.partition("\x1f")
        if not sep:
            path = host
            host = ""
        host = host.strip()
        path = path.strip()
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        delta = last_v - prev_v
        delta_pct = (delta / prev_v * 100.0) if prev_v > 0 else (100.0 if last_v > 0 else 0.0)
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "last_total": last_v,
                "prev_total": prev_v,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        )
    # Haber detay sayfalarını çıkar (son segment sayısal ID olanlar)
    rows = [r for r in rows if not _is_news_detail_path(r["page"])]
    rows.sort(key=lambda item: item["last_total"], reverse=True)
    return rows[:50]


def _run_landing_pages_news_only(
    client: BetaAnalyticsDataClient,
    property_id: str,
    *,
    last_start: str,
    last_end: str,
    prev_start: str,
    prev_end: str,
    limit: int = 250,
    top_n: int = 30,
) -> list[dict]:
    """En çok görüntülenen haber URL'leri (API'de tüm sayfalar, sonra haber filtresi)."""
    lim = max(30, min(int(limit), 250))
    n = max(1, min(int(top_n), 50))
    last_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=last_start,
        end=last_end,
        limit=lim,
        dimension_filter=None,
    )
    prev_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=prev_start,
        end=prev_end,
        limit=lim,
        dimension_filter=None,
    )
    merged = _merge_period_maps(last_map, prev_map)
    rows: list[dict] = []
    for key, (last_v, prev_v) in merged.items():
        host, sep, path = key.partition("\x1f")
        if not sep:
            path = host
            host = ""
        host = host.strip()
        path = path.strip()
        if not _is_news_article_path(path):
            continue
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        delta = last_v - prev_v
        delta_pct = (delta / prev_v * 100.0) if prev_v > 0 else (100.0 if last_v > 0 else 0.0)
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "last_total": last_v,
                "prev_total": prev_v,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        )
    rows.sort(key=lambda item: item["last_total"], reverse=True)
    return rows[:n]


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
    lim = max(10, min(int(limit), 250))
    last_map = _run_dim_sessions_single_range(
        client,
        property_id,
        "sessionSourceMedium",
        start=last_start,
        end=last_end,
        limit=lim,
        dimension_filter=None,
    )
    prev_map = _run_dim_sessions_single_range(
        client,
        property_id,
        "sessionSourceMedium",
        start=prev_start,
        end=prev_end,
        limit=lim,
        dimension_filter=None,
    )
    merged = _merge_period_maps(last_map, prev_map)
    rows: list[dict] = []
    for sm, (last_v, prev_v) in merged.items():
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
    rows.sort(key=lambda item: item["last_total"], reverse=True)
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
            last_map_ch = _run_dim_sessions_single_range(
                client,
                property_id,
                "sessionDefaultChannelGroup",
                start=last_start,
                end=last_end,
                limit=100,
                dimension_filter=None,
            )
            prev_map_ch = _run_dim_sessions_single_range(
                client,
                property_id,
                "sessionDefaultChannelGroup",
                start=prev_start,
                end=prev_end,
                limit=100,
                dimension_filter=None,
            )
            merged_ch = _merge_period_maps(last_map_ch, prev_map_ch)
            last_by_channel: dict[str, float] = {}
            prev_by_channel: dict[str, float] = {}
            for channel, (last_value, prev_value) in merged_ch.items():
                last_by_channel[channel] = last_value
                prev_by_channel[channel] = prev_value

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

            # Sessions tek kaynaktan gelsin: GA4 KPI toplamı (UI/GA4 Total satırıyla birebir).
            last_total = float(last_kpi.get("sessions") or 0.0)
            prev_total = float(prev_kpi.get("sessions") or 0.0)
            wow_pct = ((last_total - prev_total) / prev_total * 100.0) if prev_total > 0 else 0.0

            prefix = f"ga4_{profile_key}_sessions_"
            metrics[f"{prefix}last{safe_days}d_total"] = float(last_total)
            metrics[f"{prefix}prev{safe_days}d_total"] = float(prev_total)
            ds = f"_{safe_days}d"
            metrics[f"{prefix}wow_change_pct{ds}"] = float(wow_pct)
            if safe_days == 30:
                metrics[f"{prefix}wow_change_pct"] = float(wow_pct)
            for channel, value in last_by_channel.items():
                metrics[f"{prefix}last{safe_days}d_channel__{slugify(channel)}"] = float(value)
            for channel, value in prev_by_channel.items():
                metrics[f"{prefix}prev{safe_days}d_channel__{slugify(channel)}"] = float(value)

            try:
                daily_trend = _run_daily_kpi_trend(
                    client,
                    property_id,
                    start=last_start,
                    end=last_end,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 günlük KPI trend başarısız (%s / %s): %s", site.domain, profile_key, exc)
                daily_trend = _empty_daily_trend()

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

            # Haberler sekmesi karşılaştırma/snapshot tutmaz; her zaman /ga4/pages?news=1 ile tek dönem canlı çeker.

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

            wow_ref = date.fromisoformat(last_end)
            wow_prev = wow_ref - timedelta(days=7)
            try:
                wow_last_kpi = _run_kpi_for_single_range(client, property_id, start=last_end, end=last_end)
                wow_prev_kpi = _run_kpi_for_single_range(
                    client, property_id, start=wow_prev.isoformat(), end=wow_prev.isoformat()
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 same_weekday KPI başarısız (%s / %s): %s", site.domain, profile_key, exc)
                wow_last_kpi = {k: 0.0 for k in KPI_METRIC_NAMES}
                wow_prev_kpi = {k: 0.0 for k in KPI_METRIC_NAMES}
            try:
                wow_last_ch_raw = _run_dim_sessions_single_range(
                    client,
                    property_id,
                    "sessionDefaultChannelGroup",
                    start=last_end,
                    end=last_end,
                    limit=100,
                    dimension_filter=None,
                )
                wow_prev_ch_raw = _run_dim_sessions_single_range(
                    client,
                    property_id,
                    "sessionDefaultChannelGroup",
                    start=wow_prev.isoformat(),
                    end=wow_prev.isoformat(),
                    limit=100,
                    dimension_filter=None,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("GA4 same_weekday channel başarısız (%s / %s): %s", site.domain, profile_key, exc)
                wow_last_ch_raw = {}
                wow_prev_ch_raw = {}
            wow_channels_last = {slugify(k): float(v) for k, v in (wow_last_ch_raw or {}).items()}
            wow_channels_prev = {slugify(k): float(v) for k, v in (wow_prev_ch_raw or {}).items()}

            # UI kanal özeti: ham kanal adıyla last/prev eşleştirilir (DB metric anahtarı / JSON slug hatası yok).
            channel_summary_rows: list[dict] = []
            for ch_name, last_v in sorted(last_by_channel.items(), key=lambda x: -x[1])[:4]:
                prev_v = float(prev_by_channel.get(ch_name, 0.0))
                channel_summary_rows.append(
                    {
                        "label": ch_name,
                        "value": float(last_v),
                        "pct_change": _channel_pct_change(float(last_v), prev_v),
                    }
                )
            org_last_sess = 0.0
            org_prev_sess = 0.0
            for ch_name, v in last_by_channel.items():
                if slugify(ch_name) == "organic_search":
                    org_last_sess = float(v)
                    break
            for ch_name, v in prev_by_channel.items():
                if slugify(ch_name) == "organic_search":
                    org_prev_sess = float(v)
                    break
            organic_share_pct = (org_last_sess / last_total * 100.0) if last_total > 0 else 0.0
            organic_share_prev_pct = (org_prev_sess / prev_total * 100.0) if prev_total > 0 else 0.0
            organic_share_pct_change = _channel_pct_change(organic_share_pct, organic_share_prev_pct)

            payload = {
                "summary": {"last": last_kpi, "prev": prev_kpi},
                "daily_trend": daily_trend,
                "pages_no_news": pages_rows,
                "sources": sources_rows,
                "channel_summary_rows": channel_summary_rows,
                "organic_share_pct": float(organic_share_pct),
                "organic_share_pct_change": float(organic_share_pct_change),
                # Kanal kırılımı (slug -> oturum); eski yol / fallback.
                "channels_last": {slugify(k): float(v) for k, v in last_by_channel.items()},
                "channels_prev": {slugify(k): float(v) for k, v in prev_by_channel.items()},
                "exclude_path_substrings": _exclude_path_substrings(),
                "same_weekday_kpi": {
                    "reference_date": last_end,
                    "previous_week_date": wow_prev.isoformat(),
                    "weekday_label_tr": weekday_tr(wow_ref),
                    "last": wow_last_kpi,
                    "prev": wow_prev_kpi,
                    "channels_last": wow_channels_last,
                    "channels_prev": wow_channels_prev,
                    "property_id": property_id,
                },
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
                metrics[f"ga4_{profile_key}_kpi_last_{key}{ds}"] = float(last_kpi.get(key, 0.0))
                metrics[f"ga4_{profile_key}_kpi_prev_{key}{ds}"] = float(prev_kpi.get(key, 0.0))
                if safe_days == 30:
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


def fetch_ga4_top_landing_audit(
    *,
    property_id: str,
    days: int = 30,
    limit: int = 500,
    exclude_news: bool = True,
) -> list[dict]:
    """Tek dönem: son N gün host + landing path, sessions sıralı (link denetimi / rapor).

    GA4 RunReport limit üst sınırı 500 (tek istek).
    """
    safe_days = max(1, int(days))
    safe_limit = max(5, min(int(limit or 500), 500))
    end = report_calendar_yesterday()
    start = end - timedelta(days=safe_days - 1)
    client = _client()
    filt = _landing_exclude_filter("landingPagePlusQueryString") if exclude_news else None
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [
            Dimension(name="hostName"),
            Dimension(name="landingPagePlusQueryString"),
        ],
        "metrics": [Metric(name="sessions")],
        "date_ranges": [DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
        "limit": safe_limit,
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
    }
    if filt is not None:
        req_kwargs["dimension_filter"] = filt
    response = client.run_report(RunReportRequest(**req_kwargs))
    rows: list[dict] = []
    for row in response.rows or []:
        if len(row.dimension_values) < 2:
            continue
        host = str(row.dimension_values[0].value or "").strip()
        path = str(row.dimension_values[1].value or "").strip()
        sessions = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "sessions": sessions,
            }
        )
    if exclude_news:
        rows = [r for r in rows if not _is_news_detail_path(r["page"])]
    return rows


def fetch_ga4_landing_pages(
    *,
    property_id: str,
    days: int = 30,
    limit: int = 50,
    exclude_news: bool = True,
    news_only: bool = False,
    same_weekday_day: bool = False,
) -> list[dict]:
    """Sayfa kırılımı (pagePath/screenPageViews): son N gün vs önceki N gün.

    same_weekday_day=True: 1g modu — son tam gün vs bir önceki haftanın aynı günü (7g snapshot listesiyle karıştırma).
    news_only=True: en çok oturum alan haber URL'leri (üst sınır 30 satır).
    """

    if news_only:
        exclude_news = False

    safe_days = int(days) if int(days) > 0 else 30
    if news_only:
        safe_limit = max(30, min(int(limit or 250), 250))
    else:
        safe_limit = max(5, min(int(limit or 50), 200))
    if same_weekday_day:
        (last_start, last_end), (prev_start, prev_end) = _same_weekday_day_windows()
    else:
        (last_start, last_end), (prev_start, prev_end) = _calendar_windows(safe_days)

    client = _client()
    filt = _landing_exclude_filter("pagePath") if exclude_news else None
    last_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=last_start,
        end=last_end,
        limit=safe_limit,
        dimension_filter=filt,
    )
    prev_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=prev_start,
        end=prev_end,
        limit=safe_limit,
        dimension_filter=filt,
    )
    merged = _merge_period_maps(last_map, prev_map)
    rows: list[dict] = []
    for key, (last_value, prev_value) in merged.items():
        host, sep, path = key.partition("\x1f")
        if not sep:
            path = host
            host = ""
        host = host.strip()
        path = path.strip()
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        delta = last_value - prev_value
        delta_pct = (delta / prev_value * 100.0) if prev_value > 0 else (100.0 if last_value > 0 else 0.0)
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "last_total": last_value,
                "prev_total": prev_value,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        )

    if news_only:
        rows = [r for r in rows if _is_news_article_path(r["page"])]
    elif exclude_news:
        # Haber detay sayfalarını çıkar (son segment sayısal ID olanlar)
        rows = [r for r in rows if not _is_news_detail_path(r["page"])]
    rows.sort(key=lambda item: item["last_total"], reverse=True)
    cap = 30 if news_only else 50
    return rows[:cap]


def article_id_path_filter(field_name: str, article_id: str) -> FilterExpression | None:
    """pagePath — haber makalesi ID'si içeren URL'ler (ör. .../705471)."""
    aid = re.sub(r"\D", "", str(article_id or "").strip())
    if not aid:
        return None
    return FilterExpression(
        filter=Filter(
            field_name=str(field_name or "pagePath"),
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.CONTAINS,
                value=f"/{aid}",
                case_sensitive=False,
            ),
        ),
    )


def fetch_ga4_news_detail_pages_metrics(
    *,
    property_id: str,
    start: str,
    end: str,
    limit: int = 500,
) -> list[dict]:
    """Tarih aralığında haber detay sayfaları (path + title + views + sessions)."""
    if not str(property_id or "").strip() or not start or not end:
        return []
    news_filt = _landing_news_include_filter("pagePath")
    client = _client()
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [
            Dimension(name="hostName"),
            Dimension(name="pagePath"),
            Dimension(name="pageTitle"),
        ],
        "metrics": [Metric(name="screenPageViews"), Metric(name="sessions")],
        "date_ranges": [DateRange(start_date=start, end_date=end)],
        "limit": max(10, min(int(limit or 500), 500)),
        "order_bys": [
            OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True),
        ],
    }
    if news_filt is not None:
        req_kwargs["dimension_filter"] = news_filt
    try:
        response = client.run_report(
            RunReportRequest(**req_kwargs),
            timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
        )
    except Exception:
        LOGGER.warning("GA4 haber detay listesi başarısız property=%s", property_id, exc_info=True)
        return []
    rows: list[dict] = []
    for row in response.rows or []:
        if len(row.dimension_values) < 3:
            continue
        host = str(row.dimension_values[0].value or "").strip()
        path = str(row.dimension_values[1].value or "").strip()
        title = str(row.dimension_values[2].value or "").strip()
        if not path or not _is_news_article_path(path):
            continue
        views = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        sessions = float(row.metric_values[1].value or 0.0) if len(row.metric_values or []) > 1 else 0.0
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "page_title": title,
                "views": views,
                "sessions": sessions,
            }
        )
    return rows


def fetch_ga4_article_paths_metrics(
    *,
    property_id: str,
    article_id: str,
    days: int = 7,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    """Tek dönem: makale ID'sine uyan tüm pagePath satırları (screenPageViews + sessions)."""
    aid = re.sub(r"\D", "", str(article_id or "").strip())
    if not aid or not str(property_id or "").strip():
        return []
    if start and end:
        last_start, last_end = start, end
    else:
        safe_days = max(1, min(int(days or 7), 90))
        (last_start, last_end), _ = _calendar_windows(safe_days)
    filt = article_id_path_filter("pagePath", aid)
    if filt is None:
        return []

    client = _client()
    views_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="screenPageViews",
        path_dimension="pagePath",
        start=last_start,
        end=last_end,
        limit=50,
        dimension_filter=filt,
        timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
    )
    sess_map = _run_landing_host_path_metric_single_range(
        client,
        property_id,
        metric_name="sessions",
        path_dimension="pagePath",
        start=last_start,
        end=last_end,
        limit=50,
        dimension_filter=filt,
        timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
    )
    keys = set(views_map) | set(sess_map)
    rows: list[dict] = []
    for key in keys:
        host, sep, path = key.partition("\x1f")
        if not sep:
            path = host
            host = ""
        host = host.strip()
        path = path.strip()
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "views": float(views_map.get(key, 0.0)),
                "sessions": float(sess_map.get(key, 0.0)),
            }
        )
    rows.sort(key=lambda item: float(item.get("views") or 0.0), reverse=True)
    return rows


def _run_dim_metrics_single_range(
    client: BetaAnalyticsDataClient,
    property_id: str,
    dimension_name: str,
    *,
    start: str,
    end: str,
    limit: int,
    dimension_filter: FilterExpression | None = None,
    timeout: float | None = None,
) -> list[dict[str, float | str]]:
    """Tek dönem: boyut başına oturum + görüntüleme."""
    req_kwargs: dict = {
        "property": f"properties/{property_id}",
        "dimensions": [Dimension(name=dimension_name)],
        "metrics": [Metric(name="sessions"), Metric(name="screenPageViews")],
        "date_ranges": [DateRange(start_date=start, end_date=end)],
        "limit": max(10, min(int(limit), 250)),
        "order_bys": [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)],
    }
    if dimension_filter is not None:
        req_kwargs["dimension_filter"] = dimension_filter
    response = client.run_report(
        RunReportRequest(**req_kwargs),
        timeout=timeout or _GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
    )
    rows: list[dict[str, float | str]] = []
    for row in response.rows or []:
        dim = str(row.dimension_values[0].value or "")
        sessions = float(row.metric_values[0].value or 0.0) if row.metric_values else 0.0
        views = float(row.metric_values[1].value or 0.0) if len(row.metric_values or []) > 1 else 0.0
        rows.append({"dimension": dim, "sessions": sessions, "views": views})
    return rows


def fetch_ga4_article_traffic_sources(
    *,
    property_id: str,
    article_id: str,
    start: str,
    end: str,
    limit: int = 50,
) -> dict[str, list[dict]]:
    """Makale pagePath filtresiyle oturum kaynağı (kanal + source/medium)."""
    aid = re.sub(r"\D", "", str(article_id or "").strip())
    if not aid or not str(property_id or "").strip() or not start or not end:
        return {"channels": [], "source_medium": []}
    filt = article_id_path_filter("pagePath", aid)
    if filt is None:
        return {"channels": [], "source_medium": []}

    client = _client()
    lim = max(10, min(int(limit or 50), 100))
    empty: dict[str, list[dict]] = {"channels": [], "source_medium": []}
    try:
        ch_rows = _run_dim_metrics_single_range(
            client,
            property_id,
            "sessionDefaultChannelGroup",
            start=start,
            end=end,
            limit=lim,
            dimension_filter=filt,
            timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
        )
        sm_rows = _run_dim_metrics_single_range(
            client,
            property_id,
            "sessionSourceMedium",
            start=start,
            end=end,
            limit=lim,
            dimension_filter=filt,
            timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
        )
    except Exception:
        LOGGER.warning(
            "GA4 makale kaynak kırılımı başarısız property=%s article=%s",
            property_id,
            aid,
            exc_info=True,
        )
        return empty

    channels = [
        {
            "channel": str(r["dimension"]),
            "sessions": float(r["sessions"]),
            "views": float(r["views"]),
        }
        for r in ch_rows
        if float(r.get("sessions") or 0) > 0 or float(r.get("views") or 0) > 0
    ]
    source_medium = [
        {
            "source_medium": str(r["dimension"]),
            "sessions": float(r["sessions"]),
            "views": float(r["views"]),
        }
        for r in sm_rows
        if float(r.get("sessions") or 0) > 0 or float(r.get("views") or 0) > 0
    ]
    return {"channels": channels, "source_medium": source_medium}


def fetch_ga4_news_landing_pages_total(
    *,
    property_id: str,
    days: int = 7,
    limit: int = 30,
) -> list[dict]:
    """Tek dönem: en çok görüntülenen haber landing sayfaları (karşılaştırma yok)."""
    safe_days = int(days) if int(days) > 0 else 7
    # Rapor: GA4 sayfa başına en fazla 250 satır; haber filtresi sonrası da yeterli kapsama için tam kullan.
    api_lim = 250
    (last_start, last_end), _ = _calendar_windows(safe_days)

    client = _client()
    news_filt = _landing_news_include_filter("pagePath")
    try:
        last_map = _run_landing_host_path_metric_single_range(
            client,
            property_id,
            metric_name="screenPageViews",
            path_dimension="pagePath",
            start=last_start,
            end=last_end,
            limit=api_lim,
            dimension_filter=news_filt,
            timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
        )
    except Exception:
        LOGGER.warning(
            "GA4 haber landing raporu (filtreli) başarısız, filtresiz tekrar deneniyor — property=%s",
            property_id,
            exc_info=True,
        )
        last_map = _run_landing_host_path_metric_single_range(
            client,
            property_id,
            metric_name="screenPageViews",
            path_dimension="pagePath",
            start=last_start,
            end=last_end,
            limit=api_lim,
            dimension_filter=None,
            timeout=_GA4_NEWS_RUN_REPORT_TIMEOUT_SEC,
        )
    rows: list[dict] = []
    for key, sess in last_map.items():
        host, sep, path = key.partition("\x1f")
        if not sep:
            path = host
            host = ""
        host = host.strip()
        path = path.strip()
        if not _is_news_article_path(path):
            continue
        page_url = ga4_canonical_page_url(host, path)
        ph = host if host.lower() not in ("(not set)", "not set") else ""
        rows.append(
            {
                "page": path,
                "page_host": ph,
                "page_url": page_url,
                "views": float(sess or 0.0),
            }
        )
    rows.sort(key=lambda item: float(item.get("views") or 0.0), reverse=True)
    cap = max(1, min(int(limit or 30), 250))
    return rows[:cap]


def fetch_ga4_session_source_medium(
    *,
    property_id: str,
    days: int = 30,
    limit: int = 50,
    same_weekday_day: bool = False,
) -> list[dict]:
    """sessionSourceMedium: son N gün vs önceki N gün veya 1g same-weekday çifti."""

    safe_days = int(days) if int(days) > 0 else 30
    lim = max(10, min(int(limit or 50), 250))
    if same_weekday_day:
        (last_start, last_end), (prev_start, prev_end) = _same_weekday_day_windows()
    else:
        (last_start, last_end), (prev_start, prev_end) = _calendar_windows(safe_days)
    client = _client()
    return _run_session_source_medium(
        client,
        property_id,
        last_start=last_start,
        last_end=last_end,
        prev_start=prev_start,
        prev_end=prev_end,
        limit=lim,
    )


def fetch_ga4_same_weekday_channel_maps(*, property_id: str) -> dict[str, dict[str, float]]:
    """Son tam gün vs geçen haftanın aynı günü için kanal map'i."""
    (last_start, last_end), (prev_start, prev_end) = _same_weekday_day_windows()
    client = _client()
    last_raw = _run_dim_sessions_single_range(
        client,
        property_id,
        "sessionDefaultChannelGroup",
        start=last_start,
        end=last_end,
        limit=100,
        dimension_filter=None,
    )
    prev_raw = _run_dim_sessions_single_range(
        client,
        property_id,
        "sessionDefaultChannelGroup",
        start=prev_start,
        end=prev_end,
        limit=100,
        dimension_filter=None,
    )

    def _slugify(value: str) -> str:
        safe = (value or "").strip().lower()
        safe = safe.replace(" ", "_").replace("-", "_")
        safe = "".join(ch for ch in safe if ch.isalnum() or ch == "_")
        return safe or "unknown"

    return {
        "channels_last": {_slugify(k): float(v or 0.0) for k, v in (last_raw or {}).items()},
        "channels_prev": {_slugify(k): float(v or 0.0) for k, v in (prev_raw or {}).items()},
    }


def fetch_ga4_app_screens(
    *,
    property_id: str,
    days: int = 30,
    limit: int = 50,
) -> list[dict]:
    """Mobil uygulama ekran kırılımı (unifiedScreenName / screenPageViews): son N gün vs önceki N gün."""
    safe_days = max(1, int(days))
    safe_limit = max(10, min(int(limit), 200))
    (last_start, last_end), (prev_start, prev_end) = _calendar_windows(safe_days)
    client = _client()

    def _fetch(start: str, end: str) -> dict[str, float]:
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="unifiedScreenName")],
            metrics=[Metric(name="screenPageViews")],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            limit=safe_limit,
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)],
        )
        resp = client.run_report(req)
        return {
            str(row.dimension_values[0].value or "").strip(): float(row.metric_values[0].value or 0.0)
            for row in resp.rows if row.dimension_values
        }

    last_map = _fetch(last_start, last_end)
    prev_map = _fetch(prev_start, prev_end)
    merged = _merge_period_maps(last_map, prev_map)
    rows = [
        {
            "screen": k,
            "views": last_val,
            "views_prev": prev_val,
            "change_pct": round((last_val - prev_val) / prev_val * 100, 1) if prev_val else None,
        }
        for k, (last_val, prev_val) in merged.items()
        if last_val > 0 or prev_val > 0
    ]
    rows.sort(key=lambda r: r["views"], reverse=True)
    return rows[:safe_limit]


def fetch_ga4_app_events(
    *,
    property_id: str,
    days: int = 30,
    limit: int = 50,
) -> list[dict]:
    """Mobil uygulama etkinlik kırılımı (eventName / eventCount): son N gün vs önceki N gün."""
    safe_days = max(1, int(days))
    safe_limit = max(10, min(int(limit), 200))
    (last_start, last_end), (prev_start, prev_end) = _calendar_windows(safe_days)
    client = _client()

    def _fetch(start: str, end: str) -> dict[str, float]:
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="eventName")],
            metrics=[Metric(name="eventCount")],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            limit=safe_limit,
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="eventCount"), desc=True)],
        )
        resp = client.run_report(req)
        return {
            str(row.dimension_values[0].value or "").strip(): float(row.metric_values[0].value or 0.0)
            for row in resp.rows if row.dimension_values
        }

    last_map = _fetch(last_start, last_end)
    prev_map = _fetch(prev_start, prev_end)
    merged = _merge_period_maps(last_map, prev_map)
    rows = [
        {
            "event_name": k,
            "count": last_val,
            "count_prev": prev_val,
            "change_pct": round((last_val - prev_val) / prev_val * 100, 1) if prev_val else None,
        }
        for k, (last_val, prev_val) in merged.items()
        if last_val > 0 or prev_val > 0
    ]
    rows.sort(key=lambda r: r["count"], reverse=True)
    return rows[:safe_limit]
