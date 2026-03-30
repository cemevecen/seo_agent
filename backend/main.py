"""FastAPI uygulama giriş noktası."""
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from ipaddress import ip_address, ip_network
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Localhost development için insecure OAuth transport'u allow et
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import Environment, FileSystemLoader
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler

from backend.api.alerts import router as alerts_router
from backend.api.metrics import router as metrics_router
from backend.api.sites import router as sites_router
from backend.collectors.crawler import collect_crawler_metrics
from backend.collectors.crux_history import collect_crux_history
from backend.collectors.pagespeed import (
    STRATEGY_METRIC_MAP,
    collect_pagespeed_metrics,
    fetch_live_lighthouse_category_scores,
    get_latest_pagespeed_audit_snapshot,
)
from backend.collectors.search_console import collect_search_console_alert_metrics, collect_search_console_metrics, get_top_queries
from backend.collectors.url_inspection import collect_url_inspection
from backend.config import settings
from backend.database import SessionLocal, init_db
from backend.models import CollectorRun, ExternalSite, PageSpeedPayloadSnapshot, Site
from backend.rate_limiter import limiter
from backend.services.alert_engine import ensure_site_alerts, get_alert_rules, get_recent_alerts
from backend.services.metric_store import get_latest_metrics, get_metric_history
from backend.services.quota_guard import get_quota_status
from backend.services.search_console_auth import build_oauth_flow, decode_oauth_state, delete_oauth_credentials, encode_oauth_state, get_search_console_connection_status, oauth_is_configured, save_oauth_credentials
from backend.services.pagespeed_analyzer import analyze_pagespeed_alerts
from backend.services.pagespeed_detailed import analyze_pagespeed_detailed
from backend.services.lighthouse_analyzer import get_lighthouse_analysis
from backend.services.operations_notifier import (
    notify_crawler_audit_emails,
    notify_missed_scheduled_refreshes,
    notify_result_map,
    notify_system_trigger,
)
from backend.services.timezone_utils import format_datetime_like, format_local_datetime
from backend.services.warehouse import (
    get_latest_crux_snapshot,
    get_latest_search_console_rows,
    get_latest_url_inspection_snapshot,
    get_site_warehouse_summary,
)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
LOGGER = logging.getLogger(__name__)
DAILY_REFRESH_LOCK = threading.Lock()
SCHEDULER: BackgroundScheduler | None = None

# Create Jinja2Templates with cache disabled for Python 3.14 compatibility
from jinja2 import Environment, FileSystemLoader
jinja_env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    cache_size=0,
    auto_reload=True
)


def _format_exact(value) -> str:
    if value is None or value == "":
        return "N/A"
    if isinstance(value, str):
        return value
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return str(value)
    normalized = format(decimal_value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in {"", "-0"}:
        return "0"
    return normalized


def _format_max_two_decimals(value) -> str:
    if value is None or value == "":
        return "N/A"
    if isinstance(value, str):
        return value
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return str(value)
    clipped = decimal_value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    normalized = format(clipped.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in {"", "-0"}:
        return "0"
    return normalized


def _format_exact_signed(value) -> str:
    if value is None or value == "":
        return "N/A"
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        text = str(value)
        return text if text.startswith(("+", "-")) else f"+{text}"
    sign = "+" if decimal_value >= 0 else ""
    return f"{sign}{_format_exact(decimal_value)}"


def _ms_to_exact_seconds(value) -> str:
    if value is None:
        return "N/A"
    try:
        seconds = Decimal(str(value)) / Decimal("1000")
    except (InvalidOperation, ValueError, TypeError):
        return str(value)
    return _format_exact(seconds)


jinja_env.filters["exact"] = _format_exact
jinja_env.filters["max_two_decimals"] = _format_max_two_decimals
jinja_env.filters["exact_signed"] = _format_exact_signed
jinja_env.filters["seconds_exact"] = _ms_to_exact_seconds
templates = Jinja2Templates(env=jinja_env)
app = FastAPI(title="SEO Agent Dashboard")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

PERIOD_DAYS_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
}


def _resolve_period(raw_period: str | None) -> tuple[str, int]:
    normalized = (raw_period or "monthly").strip().lower()
    aliases = {
        "day": "daily",
        "week": "weekly",
        "month": "monthly",
        "daily": "daily",
        "weekly": "weekly",
        "monthly": "monthly",
    }
    period = aliases.get(normalized, "monthly")
    return period, PERIOD_DAYS_MAP[period]


def _format_trend_label(timestamp: str, period: str) -> str:
    if period == "daily":
        return timestamp[11:16]
    return timestamp[5:10]


def _latest_value_from_history(history: dict[str, list[dict]], metric_type: str, fallback: float = 0.0) -> float:
    items = history.get(metric_type, [])
    if not items:
        return fallback
    return float(items[-1]["value"])


def _format_metric_timestamp(metric) -> str:
    if metric is None:
        return "Henüz veri yok"
    return format_local_datetime(metric.collected_at, fallback="Henüz veri yok")


def _extract_client_ip(request: Request) -> str:
    # Proxy arkasında çalışırken gerçek istemci IP'sini alır.
    if settings.trust_proxy_headers:
        forwarded = request.headers.get("x-forwarded-for", "").strip()
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else ""


def _is_ip_allowed(client_ip: str, allowlist: list[str]) -> bool:
    # Tek IP ve CIDR formatlarını destekleyen allowlist kontrolü.
    if not client_ip:
        return False
    for entry in allowlist:
        try:
            if "/" in entry:
                if ip_address(client_ip) in ip_network(entry, strict=False):
                    return True
            elif client_ip == entry:
                return True
        except ValueError:
            continue
    return False


@app.middleware("http")
async def ip_allowlist_middleware(request: Request, call_next):
    # ALLOWED_CLIENT_IPS doluysa sadece listedeki IP'lere erişim izni verir.
    allowlist = [item.strip() for item in settings.allowed_client_ips.split(",") if item.strip()]
    if not allowlist:
        return await call_next(request)

    client_ip = _extract_client_ip(request)
    if _is_ip_allowed(client_ip, allowlist):
        return await call_next(request)

    return JSONResponse(
        status_code=403,
        content={
            "detail": "IP erişim izni yok.",
            "client_ip": client_ip,
        },
    )

# Site yönetimi endpoint'leri JSON API altında toplanır.
app.include_router(sites_router, prefix="/api")
app.include_router(metrics_router, prefix="/api")
app.include_router(alerts_router, prefix="/api")


@app.on_event("startup")
def on_startup() -> None:
    # Uygulama açılışında tablolar create_all ile hazırlanır.
    global SCHEDULER
    init_db()
    if SCHEDULER is None:
        SCHEDULER = _build_daily_refresh_scheduler()
        if SCHEDULER is not None:
            SCHEDULER.start()
            LOGGER.info(
                "Scheduled jobs started. Search Console=%02d:%02d, full refresh=%02d:%02d %s.",
                int(settings.search_console_scheduled_refresh_hour),
                int(settings.search_console_scheduled_refresh_minute),
                int(settings.scheduled_refresh_hour),
                int(settings.scheduled_refresh_minute),
                settings.scheduled_refresh_timezone,
            )


@app.on_event("shutdown")
def on_shutdown() -> None:
    global SCHEDULER
    if SCHEDULER is not None:
        SCHEDULER.shutdown(wait=False)
        SCHEDULER = None


def _preferred_site_order_key(domain: str | None, display_name: str | None = None) -> tuple[int, str]:
    normalized_domain = str(domain or "").lower()
    preferred_domains = {
        "doviz.com": 0,
        "www.doviz.com": 0,
        "sinemalar.com": 1,
        "www.sinemalar.com": 1,
    }
    return (
        preferred_domains.get(normalized_domain, 99),
        str(display_name or "").lower(),
    )


def get_sidebar_sites() -> list[dict]:
    # Sidebar için aktif siteler veritabanından okunur.
    with SessionLocal() as db:
        external_site_ids = {
            int(row.site_id)
            for row in db.query(ExternalSite.site_id).all()
        }
        sites = (
            db.query(Site)
            .filter(Site.is_active.is_(True))
            .order_by(Site.created_at.desc())
            .all()
        )
        rows = []
        for site in sites:
            if site.id in external_site_ids:
                continue
            connection = get_search_console_connection_status(db, site.id)
            is_public = not bool(connection.get("connected"))
            rows.append(
                {
                    "domain": site.domain,
                    "label": site.display_name,
                    "profile": "public" if is_public else "verified",
                    "href": f"/external-explorer/{site.domain}" if is_public else f"/data-explorer/{site.domain}",
                }
            )
        rows.sort(key=lambda site: _preferred_site_order_key(site.get("domain"), site.get("label")))
        return rows


def _external_site_ids(db) -> set[int]:
    return {
        int(row.site_id)
        for row in db.query(ExternalSite.site_id).all()
    }


def _is_external_site(db, site_id: int) -> bool:
    return (
        db.query(ExternalSite.id)
        .filter(ExternalSite.site_id == site_id)
        .first()
        is not None
    )


def _settings_sites_payload(db) -> list[dict]:
    external_site_ids = _external_site_ids(db)
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    rows: list[dict] = []
    for site in sites:
        if site.id in external_site_ids:
            continue
        rows.append(
            {
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
                "is_active": site.is_active,
                "search_console": get_search_console_connection_status(db, site.id),
            }
        )
    return rows


def _format_optional_datetime(value: datetime | None) -> str:
    if value is None:
        return "Henüz tetiklenmedi"
    return format_local_datetime(value, fallback="Henüz tetiklenmedi")


def _latest_provider_run(db, *, site_id: int, provider: str, strategy: str | None = None) -> CollectorRun | None:
    query = db.query(CollectorRun).filter(CollectorRun.site_id == site_id, CollectorRun.provider == provider)
    if strategy is not None:
        query = query.filter(CollectorRun.strategy == strategy)
    return query.order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc()).first()


def _latest_successful_provider_summary(db, *, site_id: int, provider: str, strategy: str | None = None) -> dict:
    query = db.query(CollectorRun).filter(
        CollectorRun.site_id == site_id,
        CollectorRun.provider == provider,
        CollectorRun.status == "success",
    )
    if strategy is not None:
        query = query.filter(CollectorRun.strategy == strategy)
    run = query.order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc()).first()
    if run is None:
        return {}
    try:
        return json.loads(run.summary_json or "{}")
    except json.JSONDecodeError:
        return {}


def _summarize_search_console_rows(rows: list[dict]) -> dict[str, float]:
    total_clicks = sum(float(row.get("clicks", 0.0)) for row in rows)
    total_impressions = sum(float(row.get("impressions", 0.0)) for row in rows)
    avg_ctr = (total_clicks / total_impressions * 100.0) if total_impressions > 0 else 0.0

    weighted_position_total = 0.0
    weighted_position_weight = 0.0
    fallback_position_total = 0.0
    fallback_position_count = 0
    for row in rows:
        position = float(row.get("position", 0.0))
        impressions = float(row.get("impressions", 0.0))
        if impressions > 0:
            weighted_position_total += position * impressions
            weighted_position_weight += impressions
        elif position > 0:
            fallback_position_total += position
            fallback_position_count += 1

    if weighted_position_weight > 0:
        avg_position = weighted_position_total / weighted_position_weight
    elif fallback_position_count > 0:
        avg_position = fallback_position_total / fallback_position_count
    else:
        avg_position = 0.0

    return {
        "clicks": total_clicks,
        "impressions": total_impressions,
        "ctr": avg_ctr,
        "position": avg_position,
    }


def _filter_search_console_rows_by_device(rows: list[dict], device: str) -> list[dict]:
    normalized_device = str(device or "").upper().strip()
    return [
        row
        for row in rows
        if str(row.get("device") or "ALL").upper().strip() == normalized_device
    ]


def _aggregate_search_console_queries(rows: list[dict]) -> dict[str, dict]:
    aggregated: dict[str, dict] = {}
    for row in rows:
        query = str(row.get("query") or "").strip()
        if not query:
            continue
        item = aggregated.setdefault(
            query,
            {
                "query": query,
                "clicks": 0.0,
                "impressions": 0.0,
                "position_weighted_total": 0.0,
                "position_weighted_impressions": 0.0,
                "fallback_position_total": 0.0,
                "fallback_position_count": 0,
            },
        )
        clicks = float(row.get("clicks", 0.0))
        impressions = float(row.get("impressions", 0.0))
        position = float(row.get("position", 0.0))
        item["clicks"] += clicks
        item["impressions"] += impressions
        if impressions > 0:
            item["position_weighted_total"] += position * impressions
            item["position_weighted_impressions"] += impressions
        elif position > 0:
            item["fallback_position_total"] += position
            item["fallback_position_count"] += 1

    normalized: dict[str, dict] = {}
    for query, item in aggregated.items():
        impressions = float(item["impressions"])
        if item["position_weighted_impressions"] > 0:
            position = item["position_weighted_total"] / item["position_weighted_impressions"]
        elif item["fallback_position_count"] > 0:
            position = item["fallback_position_total"] / item["fallback_position_count"]
        else:
            position = 0.0
        normalized[query] = {
            "query": query,
            "clicks": float(item["clicks"]),
            "impressions": impressions,
            "ctr": (float(item["clicks"]) / impressions * 100.0) if impressions > 0 else 0.0,
            "position": position,
        }
    return normalized


def _build_search_console_top_queries(current_rows: list[dict], previous_rows: list[dict], *, limit: int = 50) -> list[dict]:
    current_map = _aggregate_search_console_queries(current_rows)
    previous_map = _aggregate_search_console_queries(previous_rows)
    items: list[dict] = []
    for query, current in sorted(current_map.items(), key=lambda item: item[1]["clicks"], reverse=True)[:limit]:
        previous = previous_map.get(query, {})
        previous_position = float(previous.get("position", current["position"]))
        current_position = float(current["position"])
        items.append(
            {
                "query": query,
                "clicks_current": float(current.get("clicks", 0.0)),
                "clicks_previous": float(previous.get("clicks", 0.0)),
                "clicks_diff": float(current.get("clicks", 0.0)) - float(previous.get("clicks", 0.0)),
                "position_current": current_position,
                "position_previous": previous_position,
                "position_diff": current_position - previous_position,
            }
        )
    return items


def _sanitize_search_console_trend(trend: dict) -> dict:
    sanitized = dict(trend or {})
    if str(sanitized.get("mode") or "") == "last_28d":
        clicks = list(sanitized.get("clicks") or [])
        positions = list(sanitized.get("position") or [])
        for index in range(min(len(clicks), len(positions))):
            if float(clicks[index] or 0.0) == 0.0 and float(positions[index] or 0.0) == 0.0:
                clicks[index] = None
                positions[index] = None
        sanitized["clicks"] = clicks
        sanitized["position"] = positions
        return sanitized
    for prefix in ("current", "previous"):
        clicks_key = f"{prefix}_clicks"
        position_key = f"{prefix}_position"
        clicks = list(sanitized.get(clicks_key) or [])
        positions = list(sanitized.get(position_key) or [])
        for index in range(min(len(clicks), len(positions))):
            if float(clicks[index] or 0.0) == 0.0 and float(positions[index] or 0.0) == 0.0:
                clicks[index] = None
                positions[index] = None
        sanitized[clicks_key] = clicks
        sanitized[position_key] = positions
    return sanitized


def _search_console_report_payload(db, *, site_id: int) -> dict:
    current_rows = get_latest_search_console_rows(db, site_id=site_id, data_scope="current_7d")
    previous_rows = get_latest_search_console_rows(db, site_id=site_id, data_scope="previous_7d")
    summary_payload = _latest_successful_provider_summary(
        db,
        site_id=site_id,
        provider="search_console",
        strategy="all",
    )
    current_summary = summary_payload.get("current_7d_summary") or _summarize_search_console_rows(current_rows)
    previous_summary = summary_payload.get("previous_7d_summary") or _summarize_search_console_rows(previous_rows)
    current_summary_by_device = summary_payload.get("current_7d_summary_by_device") or {}
    previous_summary_by_device = summary_payload.get("previous_7d_summary_by_device") or {}
    trend_summary = _sanitize_search_console_trend(
        summary_payload.get("trend_28d_summary")
        or summary_payload.get("trend_7d_summary")
        or {
            "mode": "last_28d",
            "labels": [],
            "dates": [],
            "clicks": [],
            "position": [],
        }
    )
    top_queries = _build_search_console_top_queries(current_rows, previous_rows, limit=50)
    trend_summary_by_device = (
        summary_payload.get("trend_28d_summary_by_device")
        or summary_payload.get("trend_7d_summary_by_device")
        or {}
    )

    views: dict[str, dict] = {}
    for device_key, device_label in (("mobile", "Mobile"), ("desktop", "Desktop")):
        device_code = device_key.upper()
        filtered_current_rows = _filter_search_console_rows_by_device(current_rows, device_code)
        filtered_previous_rows = _filter_search_console_rows_by_device(previous_rows, device_code)
        device_trend = _sanitize_search_console_trend(trend_summary_by_device.get(device_code) or {
            "mode": "last_28d",
            "labels": [],
            "dates": [],
            "clicks": [],
            "position": [],
        })
        device_top_queries = _build_search_console_top_queries(filtered_current_rows, filtered_previous_rows, limit=50)
        views[device_key] = {
            "device_code": device_code,
            "device_label": device_label,
            "has_data": bool(filtered_current_rows or filtered_previous_rows or device_top_queries),
            "summary_current": current_summary_by_device.get(device_code) or _summarize_search_console_rows(filtered_current_rows),
            "summary_previous": previous_summary_by_device.get(device_code) or _summarize_search_console_rows(filtered_previous_rows),
            "trend": device_trend,
            "top_queries": device_top_queries,
        }

    return {
        "has_data": bool(current_rows or previous_rows or top_queries),
        "summary_current": current_summary,
        "summary_previous": previous_summary,
        "trend": trend_summary,
        "top_queries": top_queries,
        "default_device": "mobile",
        "views": views,
    }


def _search_console_sites_payload(db) -> list[dict]:
    external_site_ids = _external_site_ids(db)
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    rows: list[dict] = []
    schedule_label = (
        f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
        f"{int(settings.search_console_scheduled_refresh_minute):02d}"
    )
    for site in sites:
        if site.id in external_site_ids:
            continue
        latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
        status = _search_console_status(db, latest, site.id)
        connection = get_search_console_connection_status(db, site.id)
        last_run = _latest_provider_run(db, site_id=site.id, provider="search_console", strategy="all")
        cooldown_active = _latest_collector_run_recent(
            db,
            site_id=site.id,
            provider="search_console",
            cooldown_seconds=settings.search_console_refresh_cooldown_seconds,
        )
        rows.append(
            {
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
                "is_active": site.is_active,
                "connection": connection,
                "status": status,
                "last_run_status": str(last_run.status or "").upper() if last_run and last_run.status else "NEVER",
                "last_run_at": _format_optional_datetime(last_run.requested_at if last_run else None),
                "last_run_error": str(last_run.error_message or "") if last_run else "",
                "cooldown_active": cooldown_active,
                "manual_mode_label": f"{schedule_label} otomatik + manuel",
                "report": _search_console_report_payload(db, site_id=site.id),
            }
        )
    rows.sort(key=lambda row: _preferred_site_order_key(row.get("domain"), row.get("display_name")))
    return rows


def _active_sites(db) -> list[Site]:
    return (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.asc(), Site.id.asc())
        .all()
    )


def _is_search_console_connected(db, site_id: int) -> bool:
    connection = get_search_console_connection_status(db, site_id)
    return bool(connection.get("connected"))


def _refresh_public_site_measurements(db, site: Site, *, force: bool = True) -> dict[str, dict]:
    # Search Console yetkisi gerektirmeyen collector akisi.
    results = _refresh_site_detail_measurements(
        db,
        site,
        include_pagespeed=True,
        include_crawler=True,
        include_search_console=False,
        force=force,
    )

    try:
        results["crux_history"] = collect_crux_history(db, site)
    except Exception as exc:  # noqa: BLE001
        results["crux_history"] = {"state": "failed", "error": str(exc)}

    results["url_inspection"] = {
        "state": "skipped",
        "reason": "URL Inspection için Search Console property yetkisi gerekiyor.",
    }
    return results


def _run_daily_search_console_refresh_job() -> None:
    if not DAILY_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Daily Search Console refresh skipped because another scheduled job is still in progress.")
        return

    try:
        LOGGER.info("Daily Search Console refresh started.")
        with SessionLocal() as db:
            connected_sites = [
                site
                for site in _active_sites(db)
                if get_search_console_connection_status(db, site.id).get("connected")
            ]

            for index, site in enumerate(connected_sites):
                LOGGER.info("Daily Search Console refresh processing site=%s", site.domain)
                try:
                    result = collect_search_console_metrics(db, site)
                    db.commit()
                    notify_system_trigger(
                        trigger_source="system",
                        system_key="search_console",
                        site=site,
                        result=result,
                        action_label="Günlük Search Console yenilemesi",
                    )
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily Search Console refresh failed for %s: %s", site.domain, exc)
                    notify_system_trigger(
                        trigger_source="system",
                        system_key="search_console",
                        site=site,
                        result={"state": "failed", "error": str(exc)},
                        action_label="Günlük Search Console yenilemesi",
                    )

                if index < len(connected_sites) - 1:
                    time.sleep(max(0, int(settings.search_console_scheduled_refresh_site_spacing_seconds)))

        LOGGER.info("Daily Search Console refresh completed.")
    finally:
        DAILY_REFRESH_LOCK.release()


def _run_daily_alert_refresh_job() -> None:
    if not DAILY_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Daily alert refresh skipped because another scheduled job is still in progress.")
        return

    try:
        LOGGER.info("Daily alert refresh started.")
        with SessionLocal() as db:
            sites = _active_sites(db)

            for index, site in enumerate(sites):
                LOGGER.info("Daily alert refresh processing site=%s", site.domain)
                try:
                    result = collect_search_console_alert_metrics(db, site, send_notifications=True)
                    db.commit()
                    notify_system_trigger(
                        trigger_source="system",
                        system_key="search_console_alerts",
                        site=site,
                        result=result,
                        action_label="Günlük alert yenilemesi",
                    )
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily alert refresh failed for %s: %s", site.domain, exc)
                    notify_system_trigger(
                        trigger_source="system",
                        system_key="search_console_alerts",
                        site=site,
                        result={"state": "failed", "error": str(exc)},
                        action_label="Günlük alert yenilemesi",
                    )

                if index < len(sites) - 1:
                    time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))

        LOGGER.info("Daily alert refresh completed.")
    finally:
        DAILY_REFRESH_LOCK.release()


def _run_daily_refresh_job() -> None:
    if not DAILY_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Daily refresh skipped because a previous run is still in progress.")
        return

    try:
        LOGGER.info("Daily refresh started.")
        with SessionLocal() as db:
            sites = _active_sites(db)

            for index, site in enumerate(sites):
                connection = get_search_console_connection_status(db, site.id)
                LOGGER.info(
                    "Daily refresh processing site=%s search_console_connected=%s",
                    site.domain,
                    bool(connection.get("connected")),
                )

                results = _refresh_site_detail_measurements(
                    db,
                    site,
                    include_pagespeed=True,
                    include_crawler=True,
                    include_search_console=False,
                    force=True,
                )
                db.commit()
                notify_result_map(
                    trigger_source="system",
                    site=site,
                    results=results,
                    action_label="Günlük site yenilemesi",
                )
                if isinstance(results.get("crawler"), dict):
                    notify_crawler_audit_emails(
                        db=db,
                        site=site,
                        result=results.get("crawler"),
                        trigger_source="system",
                    )

                try:
                    crux_result = collect_crux_history(db, site)
                    db.commit()
                    notify_system_trigger(
                        trigger_source="system",
                        system_key="crux_history",
                        site=site,
                        result=crux_result,
                        action_label="Günlük CrUX yenilemesi",
                    )
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily refresh CrUX failed for %s: %s", site.domain, exc)
                    notify_system_trigger(
                        trigger_source="system",
                        system_key="crux_history",
                        site=site,
                        result={"state": "failed", "error": str(exc)},
                        action_label="Günlük CrUX yenilemesi",
                    )

                if connection.get("connected"):
                    try:
                        inspection_result = collect_url_inspection(db, site)
                        db.commit()
                        notify_system_trigger(
                            trigger_source="system",
                            system_key="url_inspection",
                            site=site,
                            result=inspection_result,
                            action_label="Günlük URL Inspection yenilemesi",
                        )
                    except Exception as exc:  # noqa: BLE001
                        db.rollback()
                        LOGGER.warning("Daily refresh URL Inspection failed for %s: %s", site.domain, exc)
                        notify_system_trigger(
                            trigger_source="system",
                            system_key="url_inspection",
                            site=site,
                            result={"state": "failed", "error": str(exc)},
                            action_label="Günlük URL Inspection yenilemesi",
                        )

                if index < len(sites) - 1:
                    time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))

        LOGGER.info("Daily refresh completed.")
    finally:
        DAILY_REFRESH_LOCK.release()


def _run_scheduled_refresh_monitor_job() -> None:
    with SessionLocal() as db:
        sent_subjects = notify_missed_scheduled_refreshes(db)
    for subject in sent_subjects:
        LOGGER.warning("Scheduled refresh monitor sent operations email: %s", subject)


def _build_daily_refresh_scheduler() -> BackgroundScheduler | None:
    try:
        timezone = ZoneInfo(settings.scheduled_refresh_timezone)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Invalid scheduled refresh timezone %s: %s", settings.scheduled_refresh_timezone, exc)
        timezone = ZoneInfo("UTC")

    scheduler = BackgroundScheduler(timezone=timezone)
    job_count = 0

    if settings.alerts_scheduled_refresh_enabled:
        scheduler.add_job(
            _run_daily_alert_refresh_job,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.alerts_scheduled_refresh_hour))),
                minute=max(0, min(59, int(settings.alerts_scheduled_refresh_minute))),
                timezone=timezone,
            ),
            id="daily-alert-refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if settings.search_console_scheduled_refresh_enabled:
        scheduler.add_job(
            _run_daily_search_console_refresh_job,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.search_console_scheduled_refresh_hour))),
                minute=max(0, min(59, int(settings.search_console_scheduled_refresh_minute))),
                timezone=timezone,
            ),
            id="daily-search-console-refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if settings.scheduled_refresh_enabled:
        scheduler.add_job(
            _run_daily_refresh_job,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.scheduled_refresh_hour))),
                minute=max(0, min(59, int(settings.scheduled_refresh_minute))),
                timezone=timezone,
            ),
            id="daily-site-refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if settings.scheduled_refresh_monitor_enabled:
        scheduler.add_job(
            _run_scheduled_refresh_monitor_job,
            trigger="interval",
            minutes=max(5, int(settings.scheduled_refresh_monitor_interval_minutes)),
            id="scheduled-refresh-monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if job_count == 0:
        LOGGER.info("All scheduled refresh jobs are disabled via settings.")
        return None

    return scheduler


def _metric_map(site_id: int) -> dict[str, object]:
    # Son metric kayıtlarını hızlı erişim için sözlüğe dönüştürür.
    with SessionLocal() as db:
        latest = get_latest_metrics(db, site_id)
        return {metric.metric_type: metric for metric in latest}


def _score_color(score: float) -> str:
    # PageSpeed skorunu renk sınıfına çevirir.
    if score >= 90:
        return "text-emerald-600"
    if score >= 50:
        return "text-amber-500"
    return "text-rose-600"


def _metric_value(latest: dict[str, object], metric_type: str, default: float = 0.0) -> float:
    metric = latest.get(metric_type)
    if metric is None:
        return default
    return float(metric.value)


def _metric_is_stale(latest: dict[str, object], metric_type: str, max_age_minutes: int = 30) -> bool:
    metric = latest.get(metric_type)
    if metric is None:
        return True
    return metric.collected_at < datetime.utcnow() - timedelta(minutes=max_age_minutes)


def _metric_age_seconds(latest: dict[str, object], metric_type: str) -> float | None:
    metric = latest.get(metric_type)
    if metric is None or metric.collected_at is None:
        return None
    return max(0.0, (datetime.utcnow() - metric.collected_at).total_seconds())


def _metrics_fresh_within(latest: dict[str, object], metric_types: tuple[str, ...], max_age_seconds: int) -> bool:
    if max_age_seconds <= 0:
        return False
    for metric_type in metric_types:
        age_seconds = _metric_age_seconds(latest, metric_type)
        if age_seconds is None or age_seconds > max_age_seconds:
            return False
    return True


def _latest_collector_run_recent(
    db,
    *,
    site_id: int,
    provider: str,
    strategy: str | None = None,
    cooldown_seconds: int,
) -> bool:
    if cooldown_seconds <= 0:
        return False
    query = db.query(CollectorRun).filter(CollectorRun.site_id == site_id, CollectorRun.provider == provider)
    if strategy is not None:
        query = query.filter(CollectorRun.strategy == strategy)
    run = query.order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc()).first()
    if run is None or run.requested_at is None:
        return False
    return run.requested_at >= datetime.utcnow() - timedelta(seconds=cooldown_seconds)


def _cached_pagespeed_scores(latest: dict[str, object], strategy: str) -> dict[str, float] | None:
    prefix = f"pagespeed_{strategy}_"
    scores = {
        "performance": _metric_value(latest, f"{prefix}score", -1.0),
        "accessibility": _metric_value(latest, f"{prefix}accessibility_score", -1.0),
        "best_practices": _metric_value(latest, f"{prefix}best_practices_score", -1.0),
        "seo": _metric_value(latest, f"{prefix}seo_score", -1.0),
    }
    if any(value < 0 for value in scores.values()):
        return None
    return scores


def _pagespeed_strategy_is_complete(latest: dict[str, object], strategy: str) -> bool:
    metric_names = STRATEGY_METRIC_MAP[strategy]
    required_positive = (
        "performance_score",
        "accessibility_score",
        "best_practices_score",
        "seo_score",
        "lcp",
        "fcp",
    )
    for key in required_positive:
        metric = latest.get(metric_names[key])
        if metric is None or float(metric.value) <= 0:
            return False

    cls_metric = latest.get(metric_names["cls"])
    return cls_metric is not None


def _site_detail_should_refresh(latest: dict[str, object]) -> bool:
    required_metrics = (
        "pagespeed_mobile_score",
        "pagespeed_desktop_score",
        "pagespeed_mobile_accessibility_score",
        "pagespeed_desktop_accessibility_score",
        "pagespeed_mobile_best_practices_score",
        "pagespeed_desktop_best_practices_score",
        "pagespeed_mobile_seo_score",
        "pagespeed_desktop_seo_score",
        "pagespeed_mobile_fcp",
        "pagespeed_desktop_fcp",
        "pagespeed_mobile_ttfb",
        "pagespeed_desktop_ttfb",
        "pagespeed_mobile_inp",
        "pagespeed_desktop_inp",
        "search_console_clicks_28d",
        "crawler_robots_accessible",
    )
    return (
        any(_metric_is_stale(latest, metric_type) for metric_type in required_metrics)
        or not _pagespeed_strategy_is_complete(latest, "mobile")
        or not _pagespeed_strategy_is_complete(latest, "desktop")
    )


def _refresh_site_detail_measurements(
    db,
    site: Site,
    *,
    include_pagespeed: bool = True,
    include_crawler: bool = False,
    include_search_console: bool = False,
    force: bool = False,
) -> dict[str, dict]:
    if not settings.live_refresh_enabled:
        return {}
    results: dict[str, dict] = {}
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    if include_pagespeed:
        pagespeed_recent = _latest_collector_run_recent(
            db,
            site_id=site.id,
            provider="pagespeed",
            cooldown_seconds=settings.pagespeed_refresh_cooldown_seconds,
        )
        pagespeed_cached = _metrics_fresh_within(
            latest,
            ("pagespeed_mobile_score", "pagespeed_desktop_score"),
            settings.pagespeed_refresh_cooldown_seconds,
        )
        if not force and (pagespeed_recent or pagespeed_cached):
            results["pagespeed"] = {
                "state": "skipped",
                "reason": "PageSpeed ölçümü yakın zamanda alındığı için yeniden tetiklenmedi.",
            }
        else:
            try:
                results["pagespeed"] = collect_pagespeed_metrics(db, site)
            except Exception as exc:  # noqa: BLE001
                results["pagespeed"] = {"errors": {"exception": str(exc)}}
    if include_crawler:
        crawler_recent = _latest_collector_run_recent(
            db,
            site_id=site.id,
            provider="crawler",
            cooldown_seconds=settings.crawler_refresh_cooldown_seconds,
        )
        crawler_cached = _metrics_fresh_within(
            latest,
            (
                "crawler_robots_accessible",
                "crawler_sitemap_exists",
                "crawler_schema_found",
                "crawler_canonical_found",
                "crawler_broken_links_count",
                "crawler_redirect_chain_count",
            ),
            settings.crawler_refresh_cooldown_seconds,
        )
        if not force and (crawler_recent or crawler_cached):
            results["crawler"] = {
                "state": "skipped",
                "reason": "Crawler kontrolleri yakın zamanda çalıştığı için yeniden istek atılmadı.",
            }
        else:
            try:
                results["crawler"] = collect_crawler_metrics(db, site)
            except Exception as exc:  # noqa: BLE001
                results["crawler"] = {"errors": {"exception": str(exc)}}
    if include_search_console:
        search_console_recent = _latest_collector_run_recent(
            db,
            site_id=site.id,
            provider="search_console",
            cooldown_seconds=settings.search_console_refresh_cooldown_seconds,
        )
        search_console_cached = _metrics_fresh_within(
            latest,
            ("search_console_clicks_28d",),
            settings.search_console_refresh_cooldown_seconds,
        )
        if not force and (search_console_recent or search_console_cached):
            results["search_console"] = {
                "state": "skipped",
                "reason": "Search Console verisi yakın zamanda yenilendiği için yeniden sorgulanmadı.",
            }
        else:
            try:
                results["search_console"] = collect_search_console_metrics(db, site)
            except Exception as exc:  # noqa: BLE001
                results["search_console"] = {"errors": {"exception": str(exc)}}
    return results


def _pagespeed_progress(value: float, good_threshold: float, poor_threshold: float) -> int:
    if value <= 0:
        return 0
    if value <= good_threshold:
        return 100
    if value >= poor_threshold:
        return 15
    ratio = (value - good_threshold) / (poor_threshold - good_threshold)
    return max(15, min(100, int(round(100 - ratio * 85))))


def _cwv_status(lcp_ms: float, inp_ms: float, cls: float) -> dict[str, object]:
    good = lcp_ms <= 2500 and inp_ms <= 200 and cls <= 0.1
    needs_improvement = lcp_ms <= 4000 and inp_ms <= 500 and cls <= 0.25
    if good:
        return {
            "passed": True,
            "title": "Core Web Vitals Assessment: Passed",
            "icon": "✓",
        }
    if needs_improvement:
        return {
            "passed": False,
            "title": "Core Web Vitals Assessment: Needs Improvement",
            "icon": "!",
        }
    return {
        "passed": False,
        "title": "Core Web Vitals Assessment: Failed",
        "icon": "✗",
    }


def _ms_to_seconds(value: float) -> float:
    return round((value or 0.0) / 1000, 1)


def _has_metric_value(value: float) -> bool:
    return (value or 0.0) > 0


def _build_lighthouse_score(key: str, label: str, subtitle: str, value: float, scope: str) -> dict[str, object]:
    score = int(round(max(0.0, min(100.0, value))))
    circumference = 2 * 3.141592653589793 * 37
    dash = circumference * (score / 100)
    if score >= 90:
        stroke_color = "#10b981"
        text_class = "text-emerald-600"
    elif score >= 50:
        stroke_color = "#f59e0b"
        text_class = "text-amber-500"
    else:
        stroke_color = "#f43f5e"
        text_class = "text-rose-500"
    return {
        "key": key,
        "detail_id": f"lighthouse-details-{scope}-{key}",
        "label": label,
        "subtitle": subtitle,
        "value": score,
        "dasharray": f"{dash:.0f} {circumference:.0f}",
        "stroke_color": stroke_color,
        "text_class": text_class,
    }


def _analysis_category_score(analysis: dict | None, key: str) -> float:
    if not analysis:
        return 0.0
    category = (analysis.get("categories") or {}).get(key) or {}
    return float(category.get("score") or 0.0)


def _normalize_lighthouse_issue_order(analysis: dict | None) -> dict | None:
    if not analysis or not analysis.get("issues"):
        return analysis

    def issue_sort_key(issue: dict) -> tuple[int, int, str]:
        issue_id = str(issue.get("id") or "")
        title = str(issue.get("title_en") or issue.get("title") or "").lower()
        priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        insight_like = (
            "insight" in issue_id
            or any(
                token in issue_id
                for token in (
                    "unused",
                    "render-blocking",
                    "cache",
                    "image",
                    "document-latency",
                    "server-response",
                    "network",
                    "redirect",
                    "font-display",
                )
            )
            or any(
                phrase in title
                for phrase in (
                    "use ",
                    "reduce ",
                    "eliminate ",
                    "improve ",
                    "avoid ",
                    "serve ",
                    "defer ",
                )
            )
        )
        metric_like = issue_id in {
            "largest-contentful-paint",
            "first-contentful-paint",
            "speed-index",
            "interactive",
            "max-potential-fid",
            "total-blocking-time",
            "cumulative-layout-shift",
        }
        bucket = 0 if insight_like else 2 if metric_like else 1
        return (bucket, priority_order.get(str(issue.get("priority") or "").upper(), 9), issue_id)

    normalized = dict(analysis)
    normalized["issues"] = sorted(list(analysis.get("issues") or []), key=issue_sort_key)
    return normalized


def _fallback_lighthouse_analysis(
    scope: str,
    *,
    accessibility_score: int,
    practices_score: int,
    seo_score: int,
    lcp_ms: float,
    fcp_ms: float,
    cls: float,
) -> dict:
    scope_label = "Mobile" if scope == "mobile" else "Desktop"
    issues: list[dict] = []

    if accessibility_score < 95:
        issues.append(
            {
                "id": f"{scope}-accessibility-primary",
                "title": "Dokunulabilirlik ve okunabilirlik iyileştirilmeli" if scope == "mobile" else "Masaüstü okunabilirlik iyileştirilmeli",
                "category": "Accessibility",
                "priority": "HIGH" if accessibility_score < 85 else "MEDIUM",
                "problem": (
                    f"{scope_label} audit skorunda erişilebilirlik {accessibility_score}. "
                    + (
                        "Mobil ekranda dokunma alanları, küçük metinler veya kontrast sorunları olası görünüyor."
                        if scope == "mobile"
                        else "Büyük ekran düzeninde kontrast, tablo yoğunluğu veya odak görünürlüğü sorunları olası görünüyor."
                    )
                ),
                "impact": f"{scope_label} kullanıcılarında okunabilirlik ve etkileşim kalitesi düşebilir.",
                "solution": [],
                "expected_result": f"{scope_label} erişilebilirlik skorunda artış beklenir.",
                "timeline": None,
                "examples": [
                    f"{scope_label} accessibility score: {accessibility_score}",
                    f"LCP: {_ms_to_seconds(lcp_ms)} s",
                    f"FCP: {_ms_to_seconds(fcp_ms)} s",
                ],
                "source_strategy": scope,
            }
        )

    if practices_score < 95:
        issues.append(
            {
                "id": f"{scope}-best-practices-primary",
                "title": "Mobil yükleme pratikleri optimize edilmeli" if scope == "mobile" else "Desktop bundle ve tarayıcı davranışı optimize edilmeli",
                "category": "Best Practices",
                "priority": "HIGH" if practices_score < 75 else "MEDIUM",
                "problem": (
                    "Mobil tarafta ağır görsel/script kullanımı ya da gereksiz kaynaklar görünüyor."
                    if scope == "mobile"
                    else "Desktop tarafta gereksiz bundle, console warning ya da tarayıcı uyumluluk konusu görünüyor."
                ),
                "impact": f"{scope_label} best practices skoru {practices_score} seviyesinde kalir.",
                "solution": [],
                "expected_result": f"{scope_label} için daha stabil sayfa davranışı.",
                "timeline": None,
                "examples": [
                    f"{scope_label} best practices score: {practices_score}",
                    f"CLS: {cls:.2f}",
                ],
                "source_strategy": scope,
            }
        )

    if seo_score < 100:
        issues.append(
            {
                "id": f"{scope}-seo-primary",
                "title": "Mobil SERP görünümü düzenlenmeli" if scope == "mobile" else "Desktop SEO sinyalleri güçlendirilmeli",
                "category": "SEO",
                "priority": "LOW",
                "problem": (
                    "Mobil arama sonucunda başlık veya snippet görünümü iyileştirilebilir."
                    if scope == "mobile"
                    else "Desktop arama sonucunda teknik sinyaller daha da güçlendirilebilir."
                ),
                "impact": f"{scope_label} SEO skoru {seo_score}.",
                "solution": [],
                "expected_result": f"{scope_label} SEO skorunda marjinal artış beklenir.",
                "timeline": None,
                "examples": [f"{scope_label} SEO score: {seo_score}"],
                "source_strategy": scope,
            }
        )

    return {
        "strategy": scope,
        "issues": issues,
        "categories": {
            "accessibility": {"score": accessibility_score, "issues_count": len([i for i in issues if i["category"] == "Accessibility"]), "title": "Accessibility"},
            "best_practices": {"score": practices_score, "issues_count": len([i for i in issues if i["category"] == "Best Practices"]), "title": "Best Practices"},
            "seo": {"score": seo_score, "issues_count": len([i for i in issues if i["category"] == "SEO"]), "title": "SEO"},
        },
        "summary": f"{scope_label} fallback audit özeti",
    }


def _average_available(values: list[float]) -> float:
    available = [value for value in values if value > 0]
    if not available:
        return 0.0
    return sum(available) / len(available)


def _crawler_checks_from_metrics(latest: dict[str, object]) -> list[dict]:
    checks = [
        (
            "robots.txt",
            _metric_value(latest, "crawler_robots_accessible", 0.0) >= 1.0,
            "Arama motoru bot erişimi",
        ),
        (
            "sitemap.xml",
            _metric_value(latest, "crawler_sitemap_exists", 0.0) >= 1.0,
            "Sitemap erişilebilirliği",
        ),
        (
            "Canonical",
            _metric_value(latest, "crawler_canonical_found", 0.0) >= 1.0,
            "Canonical etiketi",
        ),
        (
            "Schema Markup",
            _metric_value(latest, "crawler_schema_found", 0.0) >= 1.0,
            "Yapısal veri varlığı",
        ),
        (
            "Kırık İç Link",
            latest.get("crawler_broken_links_count") is None or _metric_value(latest, "crawler_broken_links_count", 0.0) <= 0.0,
            "Site içi taranan URL'lerde hata sayısı",
        ),
        (
            "Redirect Zinciri",
            latest.get("crawler_redirect_chain_count") is None or _metric_value(latest, "crawler_redirect_chain_count", 0.0) <= 0.0,
            "Birden fazla yönlendirme adımı kullanan iç linkler",
        ),
    ]
    items: list[dict] = []
    for name, passed, label in checks:
        items.append(
            {
                "check": name,
                "passed": passed,
                "status": "✓ Başarılı" if passed else "✗ Eksik / Hatalı",
                "reason": f"{label} son kaydedilmiş crawler ölçümünden okunuyor.",
                "impact": "İyi durumda." if passed else "Bu alanda teknik SEO kaybı yaşanabilir.",
                "action": "Canlı yenileme sonrası detaylı teknik analiz tekrar üretilecektir.",
            }
        )
    return items


def _latest_crawler_link_audit_summary(db, *, site_id: int) -> dict:
    summary = _latest_successful_provider_summary(db, site_id=site_id, provider="crawler")
    link_audit = dict(summary.get("link_audit") or {})
    return {
        "source_pages": int(link_audit.get("source_pages") or 0),
        "audited_urls": int(link_audit.get("audited_urls") or link_audit.get("sampled_links") or 0),
        "redirect_links": int(link_audit.get("redirect_links") or 0),
        "redirect_301_links": int(link_audit.get("redirect_301_links") or 0),
        "redirect_302_links": int(link_audit.get("redirect_302_links") or 0),
        "redirect_chains": int(link_audit.get("redirect_chains") or 0),
        "broken_links": int(link_audit.get("broken_links") or 0),
        "max_hops": int(link_audit.get("max_hops") or 0),
        "source_strategy": str(link_audit.get("source_strategy") or "URL listesi"),
        "source_pages_sample": list(link_audit.get("source_pages_sample") or []),
        "redirect_samples": list(link_audit.get("redirect_samples") or []),
        "broken_samples": list(link_audit.get("broken_samples") or []),
        "has_data": bool(link_audit),
    }


def _pagespeed_strategy_status(latest: dict[str, object], strategy: str, alert_messages: list[str]) -> dict[str, object]:
    metric = latest.get(f"pagespeed_{strategy}_score")
    has_metric = metric is not None
    is_stale = _metric_is_stale(latest, f"pagespeed_{strategy}_score") if has_metric else True
    has_fetch_error = any(f"{strategy} PageSpeed" in message for message in alert_messages)

    if has_metric and not is_stale and not has_fetch_error:
        state = "live"
        label = "Live"
        badge_class = "border-emerald-200 bg-emerald-50 text-emerald-700"
        description = "Canlı ve güncel veri"
    elif has_metric:
        state = "stale"
        label = "Güncel değil"
        badge_class = "border-amber-200 bg-amber-50 text-amber-800"
        description = "Son başarılı ölçüm gösteriliyor"
    else:
        state = "failed"
        label = "Failed"
        badge_class = "border-rose-200 bg-rose-50 text-rose-700"
        description = "Veri alinamadi"

    return {
        "state": state,
        "label": label,
        "badge_class": badge_class,
        "description": description,
        "updated_at": _format_metric_timestamp(metric),
    }


def _search_console_status(db, latest: dict[str, object], site_id: int) -> dict[str, object]:
    connection = get_search_console_connection_status(db, site_id)
    clicks_metric = latest.get("search_console_clicks_28d")
    has_metric = clicks_metric is not None
    is_stale = _metric_is_stale(latest, "search_console_clicks_28d") if has_metric else True
    has_rows = bool(get_latest_search_console_rows(db, site_id=site_id, data_scope="current_28d"))

    if connection.get("connected") and has_metric and not is_stale:
        state = "live"
        label = "Live"
        badge_class = "border-emerald-200 bg-emerald-50 text-emerald-700"
        description = "Search Console canli veri"
    elif connection.get("connected") and (has_metric or has_rows):
        state = "stale"
        label = "Güncel değil"
        badge_class = "border-amber-200 bg-amber-50 text-amber-800"
        description = "Son başarılı Search Console kaydı gösteriliyor"
    else:
        state = "failed"
        label = "Failed"
        badge_class = "border-rose-200 bg-rose-50 text-rose-700"
        description = "Search Console verisi alinamadi"

    return {
        "state": state,
        "label": label,
        "badge_class": badge_class,
        "description": description,
        "updated_at": _format_metric_timestamp(clicks_metric),
        "connection_label": connection.get("label", "Baglanti yok"),
        "has_rows": has_rows,
    }


def _data_state_badge(state: str, live_text: str, stale_text: str, failed_text: str) -> dict[str, str]:
    if state == "live":
        return {
            "label": "Live",
            "badge_class": "border-emerald-200 bg-emerald-50 text-emerald-700",
            "description": live_text,
        }
    if state == "stale":
        return {
            "label": "Güncel değil",
            "badge_class": "border-amber-200 bg-amber-50 text-amber-800",
            "description": stale_text,
        }
    return {
        "label": "Failed",
        "badge_class": "border-rose-200 bg-rose-50 text-rose-700",
        "description": failed_text,
    }


PAGESPEED_FIELD_METRIC_MAP = {
    "LARGEST_CONTENTFUL_PAINT_MS": "largest_contentful_paint",
    "INTERACTION_TO_NEXT_PAINT": "interaction_to_next_paint",
    "CUMULATIVE_LAYOUT_SHIFT_SCORE": "cumulative_layout_shift",
    "FIRST_CONTENTFUL_PAINT_MS": "first_contentful_paint",
    "EXPERIMENTAL_TIME_TO_FIRST_BYTE": "experimental_time_to_first_byte",
}


def _latest_pagespeed_field_metrics(db, site_id: int, strategy: str) -> dict[str, dict]:
    row = (
        db.query(PageSpeedPayloadSnapshot)
        .filter(PageSpeedPayloadSnapshot.site_id == site_id, PageSpeedPayloadSnapshot.strategy == strategy)
        .order_by(PageSpeedPayloadSnapshot.collected_at.desc(), PageSpeedPayloadSnapshot.id.desc())
        .first()
    )
    if row is None:
        return {}
    try:
        payload = json.loads(row.payload_json or "{}")
    except json.JSONDecodeError:
        return {}

    loading_metrics = (payload.get("loadingExperience") or {}).get("metrics") or {}
    origin_metrics = (payload.get("originLoadingExperience") or {}).get("metrics") or {}
    source_metrics = loading_metrics or origin_metrics
    if not source_metrics:
        return {}

    output: dict[str, dict] = {}
    for payload_key, metric_key in PAGESPEED_FIELD_METRIC_MAP.items():
        metric_payload = source_metrics.get(payload_key) or {}
        percentile = metric_payload.get("percentile")
        if percentile is None:
            continue
        good_share = None
        distributions = metric_payload.get("distributions") or []
        if distributions and isinstance(distributions, list):
            proportion = (distributions[0] or {}).get("proportion")
            try:
                good_share = float(proportion) * 100.0 if proportion is not None else None
            except (TypeError, ValueError):
                good_share = None

        latest_value = float(percentile)
        if payload_key == "CUMULATIVE_LAYOUT_SHIFT_SCORE":
            latest_value = latest_value / 100.0

        output[metric_key] = {
            "latest": latest_value,
            "good_share": good_share,
        }
    return output


def _latest_pagespeed_category_scores(db, site_id: int, strategy: str) -> dict[str, float]:
    row = (
        db.query(PageSpeedPayloadSnapshot)
        .filter(PageSpeedPayloadSnapshot.site_id == site_id, PageSpeedPayloadSnapshot.strategy == strategy)
        .order_by(PageSpeedPayloadSnapshot.collected_at.desc(), PageSpeedPayloadSnapshot.id.desc())
        .first()
    )
    if row is None:
        return {}
    try:
        payload = json.loads(row.payload_json or "{}")
    except json.JSONDecodeError:
        return {}

    categories = ((payload.get("lighthouseResult") or {}).get("categories") or {})
    output: dict[str, float] = {}
    for payload_key, normalized_key in {
        "performance": "performance",
        "accessibility": "accessibility",
        "best-practices": "best_practices",
        "seo": "seo",
    }.items():
        category = categories.get(payload_key) or {}
        score = category.get("score")
        if score is None:
            continue
        output[normalized_key] = float(score) * 100.0
    return output


def _latest_pagespeed_payload_snapshot(db, site_id: int, strategy: str) -> tuple[dict, datetime | None]:
    row = (
        db.query(PageSpeedPayloadSnapshot)
        .filter(PageSpeedPayloadSnapshot.site_id == site_id, PageSpeedPayloadSnapshot.strategy == strategy)
        .order_by(PageSpeedPayloadSnapshot.collected_at.desc(), PageSpeedPayloadSnapshot.id.desc())
        .first()
    )
    if row is None:
        return {}, None
    try:
        return json.loads(row.payload_json or "{}"), row.collected_at
    except json.JSONDecodeError:
        return {}, row.collected_at


def _format_pagespeed_report_time(value: datetime | None, fallback_iso: str = "") -> str:
    if fallback_iso:
        try:
            return format_datetime_like(fallback_iso)
        except Exception:
            pass
    if value is None:
        return "N/A"
    return format_local_datetime(value)


def _pagespeed_overall_category_label(value: str) -> tuple[str, str]:
    normalized = str(value or "").strip().upper()
    if normalized == "FAST":
        return "Passed", "border-emerald-200 bg-emerald-50 text-emerald-700"
    if normalized == "AVERAGE":
        return "Needs Attention", "border-amber-200 bg-amber-50 text-amber-800"
    if normalized == "SLOW":
        return "Failed", "border-rose-200 bg-rose-50 text-rose-700"
    return "N/A", "border-slate-200 bg-slate-50 text-slate-600"


def _pagespeed_metric_numeric_value(audit: dict, fallback_value: float | None = None) -> float | None:
    numeric_value = audit.get("numericValue")
    if numeric_value is not None:
        return float(numeric_value)
    if fallback_value is not None:
        return float(fallback_value)
    return None


def _pagespeed_metric_tone(metric_key: str, numeric_value: float | None) -> dict[str, str]:
    if numeric_value is None:
        return {
            "shell_class": "border-slate-200 bg-slate-50",
            "label_class": "text-slate-500",
            "value_class": "text-slate-900",
            "badge_class": "bg-slate-100 text-slate-600",
            "status_label": "N/A",
        }

    thresholds = {
        "fcp": (1800.0, 3000.0),
        "lcp": (2500.0, 4000.0),
        "tbt": (200.0, 600.0),
        "speed_index": (3400.0, 5800.0),
        "cls": (0.1, 0.25),
    }
    good_threshold, needs_attention_threshold = thresholds.get(metric_key, (0.0, 0.0))

    if numeric_value <= good_threshold:
        return {
            "shell_class": "border-emerald-200 bg-emerald-50/70",
            "label_class": "text-emerald-700",
            "value_class": "text-emerald-700",
            "badge_class": "bg-emerald-100 text-emerald-700",
            "status_label": "Iyi",
        }
    if numeric_value <= needs_attention_threshold:
        return {
            "shell_class": "border-amber-200 bg-amber-50/70",
            "label_class": "text-amber-700",
            "value_class": "text-amber-700",
            "badge_class": "bg-amber-100 text-amber-700",
            "status_label": "Izlenmeli",
        }
    return {
        "shell_class": "border-rose-200 bg-rose-50/70",
        "label_class": "text-rose-700",
        "value_class": "text-rose-700",
        "badge_class": "bg-rose-100 text-rose-700",
        "status_label": "Zayif",
    }


def _format_pagespeed_metric_display(audit: dict, fallback_value: float | None = None, metric_type: str = "timing") -> str:
    display_value = str(audit.get("displayValue") or "").strip()
    if display_value:
        return display_value
    if fallback_value is None:
        return "N/A"
    if metric_type == "cls":
        return _format_max_two_decimals(fallback_value)
    seconds = float(fallback_value) / 1000.0
    return f"{_format_max_two_decimals(seconds)} s"


def _build_pagespeed_report_panel(db, site_id: int, strategy: str, analysis: dict | None) -> dict:
    payload, collected_at = _latest_pagespeed_payload_snapshot(db, site_id, strategy)
    lighthouse = (payload.get("lighthouseResult") or {})
    categories = lighthouse.get("categories") or {}
    loading_experience = payload.get("loadingExperience") or {}
    origin_loading_experience = payload.get("originLoadingExperience") or {}
    environment = lighthouse.get("environment") or {}
    config_settings = lighthouse.get("configSettings") or {}
    audits = lighthouse.get("audits") or {}
    field_metrics = _latest_pagespeed_field_metrics(db, site_id, strategy)
    category_scores = _latest_pagespeed_category_scores(db, site_id, strategy)
    overall_category = loading_experience.get("overall_category") or origin_loading_experience.get("overall_category") or ""
    cwv_label, cwv_badge_class = _pagespeed_overall_category_label(overall_category)
    analysis = _normalize_lighthouse_issue_order(analysis)

    metric_tiles = [
        {
            "key": "fcp",
            "label": "First Contentful Paint",
            "value": _format_pagespeed_metric_display(
                audits.get("first-contentful-paint") or {},
                field_metrics.get("first_contentful_paint", {}).get("latest"),
            ),
            "tone": _pagespeed_metric_tone(
                "fcp",
                _pagespeed_metric_numeric_value(
                    audits.get("first-contentful-paint") or {},
                    field_metrics.get("first_contentful_paint", {}).get("latest"),
                ),
            ),
        },
        {
            "key": "lcp",
            "label": "Largest Contentful Paint",
            "value": _format_pagespeed_metric_display(
                audits.get("largest-contentful-paint") or {},
                field_metrics.get("largest_contentful_paint", {}).get("latest"),
            ),
            "tone": _pagespeed_metric_tone(
                "lcp",
                _pagespeed_metric_numeric_value(
                    audits.get("largest-contentful-paint") or {},
                    field_metrics.get("largest_contentful_paint", {}).get("latest"),
                ),
            ),
        },
        {
            "key": "tbt",
            "label": "Total Blocking Time",
            "value": _format_pagespeed_metric_display(audits.get("total-blocking-time") or {}),
            "tone": _pagespeed_metric_tone(
                "tbt",
                _pagespeed_metric_numeric_value(audits.get("total-blocking-time") or {}),
            ),
        },
        {
            "key": "cls",
            "label": "Cumulative Layout Shift",
            "value": _format_pagespeed_metric_display(
                audits.get("cumulative-layout-shift") or {},
                field_metrics.get("cumulative_layout_shift", {}).get("latest"),
                metric_type="cls",
            ),
            "tone": _pagespeed_metric_tone(
                "cls",
                _pagespeed_metric_numeric_value(
                    audits.get("cumulative-layout-shift") or {},
                    field_metrics.get("cumulative_layout_shift", {}).get("latest"),
                ),
            ),
        },
        {
            "key": "speed_index",
            "label": "Speed Index",
            "value": _format_pagespeed_metric_display(audits.get("speed-index") or {}),
            "tone": _pagespeed_metric_tone(
                "speed_index",
                _pagespeed_metric_numeric_value(audits.get("speed-index") or {}),
            ),
        },
    ]

    score_tiles = []
    for category_key, label in (
        ("performance", "Performance"),
        ("accessibility", "Accessibility"),
        ("best_practices", "Best Practices"),
        ("seo", "SEO"),
    ):
        value = round(float(category_scores.get(category_key, 0.0)))
        score_tiles.append(
            {
                "label": label,
                "value": value,
                "tone": _score_color(value),
            }
        )

    sections = []
    for category_key, category_label in (
        ("performance", "Performance"),
        ("accessibility", "Accessibility"),
        ("best_practices", "Best Practices"),
        ("seo", "SEO"),
    ):
        category_sections = ((analysis or {}).get("sections") or {}).get(category_key) or []
        if not category_sections:
            continue
        sections.append(
            {
                "key": category_key,
                "label": category_label,
                "items_count": sum(len(section.get("items") or []) for section in category_sections),
                "sections": category_sections,
            }
        )

    return {
        "has_data": bool(payload),
        "strategy": strategy,
        "strategy_label": "Mobile" if strategy == "mobile" else "Desktop",
        "report_time": _format_pagespeed_report_time(collected_at, str(lighthouse.get("fetchTime") or "")),
        "requested_url": str(payload.get("id") or lighthouse.get("requestedUrl") or loading_experience.get("initial_url") or ""),
        "final_url": str(lighthouse.get("finalDisplayedUrl") or lighthouse.get("finalUrl") or origin_loading_experience.get("id") or ""),
        "environment": str(environment.get("benchmarkIndex") or ""),
        "emulation": str(config_settings.get("emulatedFormFactor") or strategy),
        "cwv_label": cwv_label,
        "cwv_badge_class": cwv_badge_class,
        "metric_tiles": metric_tiles,
        "score_tiles": score_tiles,
        "sections": sections,
    }


def _format_crux_series(snapshot: dict | None, current_override: dict[str, dict] | None = None) -> dict[str, dict]:
    summary = (snapshot or {}).get("summary") or {}
    series = summary.get("series") or {}
    current = summary.get("current") or {}
    current_override = current_override or {}
    formatted: dict[str, dict] = {}
    metric_keys = list(dict.fromkeys([*series.keys(), *current.keys(), *current_override.keys()]))
    for metric_key in metric_keys:
        item = series.get(metric_key) or {}
        current_item = current.get(metric_key) or {}
        override_item = current_override.get(metric_key) or {}
        points = item.get("points") or []
        latest_value = override_item.get("latest")
        if latest_value is None:
            latest_value = current_item.get("latest") if current_item.get("latest") is not None else item.get("latest")
        good_share = override_item.get("good_share")
        if good_share is None:
            good_share = current_item.get("good_share") if current_item.get("good_share") is not None else item.get("good_share")
        formatted[metric_key] = {
            "label": current_item.get("label") or item.get("label") or metric_key.upper(),
            "latest": latest_value,
            "good_share": good_share,
            "chart": {
                "x": [point.get("label") for point in points],
                "y": [point.get("value") for point in points],
            },
        }
    return formatted


def _data_explorer_context(domain: str) -> dict:
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            raise ValueError("Site bulunamadı.")

        warehouse = get_site_warehouse_summary(db, site_id=site.id)
        crawler_link_audit = _latest_crawler_link_audit_summary(db, site_id=site.id)
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")
        mobile_pagespeed_current = _latest_pagespeed_field_metrics(db, site.id, "mobile")
        desktop_pagespeed_current = _latest_pagespeed_field_metrics(db, site.id, "desktop")
        mobile_lighthouse_analysis = get_latest_pagespeed_audit_snapshot(db, site.id, "mobile")
        desktop_lighthouse_analysis = get_latest_pagespeed_audit_snapshot(db, site.id, "desktop")
        inspection = get_latest_url_inspection_snapshot(db, site_id=site.id)
        if inspection and inspection.get("summary"):
            inspection_summary = dict(inspection.get("summary") or {})
            inspection_summary["last_crawl_time"] = format_datetime_like(
                inspection_summary.get("last_crawl_time"),
                fallback="N/A",
            )
            inspection = {**inspection, "summary": inspection_summary}

        mobile_state = _data_state_badge(
            "live" if mobile_crux else "failed",
            "CrUX güncel kaydı ve history serisi mevcut",
            "Son başarılı CrUX kaydı gösteriliyor",
            "CrUX geçmiş verisi henüz yok",
        )
        desktop_state = _data_state_badge(
            "live" if desktop_crux else "failed",
            "CrUX güncel kaydı ve history serisi mevcut",
            "Son başarılı CrUX kaydı gösteriliyor",
            "CrUX geçmiş verisi henüz yok",
        )
        inspection_state = _data_state_badge(
            "live" if inspection else "failed",
            "URL Inspection kaydı mevcut",
            "Son başarılı URL Inspection kaydı gösteriliyor",
            "URL Inspection verisi henüz yok",
        )

        return {
            "site_name": f"PageSpeed - {site.display_name}",
            "sites": get_sidebar_sites(),
            "domain": site.domain,
            "warehouse_summary": warehouse,
            "crawler_link_audit": crawler_link_audit,
            "crux_mobile": mobile_crux,
            "crux_desktop": desktop_crux,
            "crux_mobile_series": _format_crux_series(mobile_crux, mobile_pagespeed_current),
            "crux_desktop_series": _format_crux_series(desktop_crux, desktop_pagespeed_current),
            "pagespeed_report_mobile": _build_pagespeed_report_panel(db, site.id, "mobile", mobile_lighthouse_analysis),
            "pagespeed_report_desktop": _build_pagespeed_report_panel(db, site.id, "desktop", desktop_lighthouse_analysis),
            "url_inspection": inspection,
            "crux_mobile_status": mobile_state,
            "crux_desktop_status": desktop_state,
            "url_inspection_status": inspection_state,
        }


def _public_sites_payload(db) -> list[dict]:
    sites = (
        db.query(Site)
        .join(ExternalSite, ExternalSite.site_id == Site.id)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.desc())
        .all()
    )
    rows: list[dict] = []
    for site in sites:
        latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
        warehouse = get_site_warehouse_summary(db, site_id=site.id)
        crawler_link_audit = _latest_crawler_link_audit_summary(db, site_id=site.id)
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")
        last_updated = max((metric.collected_at for metric in latest.values()), default=site.created_at)

        rows.append(
            {
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
                "last_updated": format_local_datetime(last_updated, fallback="Henüz veri yok"),
                "pagespeed_mobile": round(_metric_value(latest, "pagespeed_mobile_score", 0.0)) if latest.get("pagespeed_mobile_score") else None,
                "pagespeed_desktop": round(_metric_value(latest, "pagespeed_desktop_score", 0.0)) if latest.get("pagespeed_desktop_score") else None,
                "crawler_broken_links": int(_metric_value(latest, "crawler_broken_links_count", 0.0)),
                "crawler_redirect_chains": int(_metric_value(latest, "crawler_redirect_chain_count", 0.0)),
                "crawler_audited_urls": crawler_link_audit.get("audited_urls", 0),
                "crux_ready": bool(mobile_crux or desktop_crux),
                "warehouse": warehouse,
            }
        )

    rows.sort(key=lambda row: _preferred_site_order_key(row.get("domain"), row.get("display_name")))
    return rows


def _public_explorer_context(domain: str) -> dict:
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            raise ValueError("Site bulunamadı.")
        if not _is_external_site(db, site.id):
            raise ValueError("Site external profilinde değil.")

        connection = get_search_console_connection_status(db, site.id)
        latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
        warehouse = get_site_warehouse_summary(db, site_id=site.id)
        crawler_link_audit = _latest_crawler_link_audit_summary(db, site_id=site.id)
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")

        cwv_rows = []
        mobile_series = _format_crux_series(mobile_crux)
        desktop_series = _format_crux_series(desktop_crux)
        for metric_key, label in (
            ("largest_contentful_paint", "LCP"),
            ("interaction_to_next_paint", "INP"),
            ("cumulative_layout_shift", "CLS"),
            ("first_contentful_paint", "FCP"),
            ("experimental_time_to_first_byte", "TTFB"),
        ):
            cwv_rows.append(
                {
                    "label": label,
                    "mobile": (mobile_series.get(metric_key) or {}).get("latest"),
                    "desktop": (desktop_series.get(metric_key) or {}).get("latest"),
                }
            )

        return {
            "site_name": f"External - {site.display_name}",
            "sites": get_sidebar_sites(),
            "domain": site.domain,
            "display_name": site.display_name,
            "is_public_only": not bool(connection.get("connected")),
            "search_console_label": connection.get("label", "Bağlantı yok"),
            "pagespeed_mobile": round(_metric_value(latest, "pagespeed_mobile_score", 0.0)) if latest.get("pagespeed_mobile_score") else None,
            "pagespeed_desktop": round(_metric_value(latest, "pagespeed_desktop_score", 0.0)) if latest.get("pagespeed_desktop_score") else None,
            "crawler_robots": _metric_value(latest, "crawler_robots_accessible", 0.0) >= 1.0,
            "crawler_sitemap": _metric_value(latest, "crawler_sitemap_exists", 0.0) >= 1.0,
            "crawler_schema": _metric_value(latest, "crawler_schema_found", 0.0) >= 1.0,
            "crawler_canonical": _metric_value(latest, "crawler_canonical_found", 0.0) >= 1.0,
            "crawler_broken_links": int(_metric_value(latest, "crawler_broken_links_count", 0.0)),
            "crawler_redirect_chains": int(_metric_value(latest, "crawler_redirect_chain_count", 0.0)),
            "crawler_link_audit": crawler_link_audit,
            "crux_mobile": mobile_crux,
            "crux_desktop": desktop_crux,
            "crux_rows": cwv_rows,
            "warehouse_summary": warehouse,
        }


def _dashboard_cards() -> list[dict]:
    # Dashboard için tüm site kartı özetlerini üretir.
    with SessionLocal() as db:
        recent_alerts = get_recent_alerts(db, limit=100)
        return _dashboard_cards_with_db(db, recent_alerts_cache=recent_alerts)


def _dashboard_cards_with_db(db, *, recent_alerts_cache: list[dict] | None = None) -> list[dict]:
    external_site_ids = _external_site_ids(db)
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    sites = [site for site in sites if site.id not in external_site_ids]
    cards = [_build_dashboard_card(db, site, recent_alerts_cache=recent_alerts_cache) for site in sites]
    cards.sort(key=lambda card: _preferred_site_order_key(card.get("domain"), card.get("display_name")))
    return cards


def _build_dashboard_card(
    db,
    site: Site,
    flash_message: str | None = None,
    recent_alerts_cache: list[dict] | None = None,
) -> dict:
    ensure_site_alerts(db, site)
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    mobile_pagespeed_metric = latest.get("pagespeed_mobile_score")
    desktop_pagespeed_metric = latest.get("pagespeed_desktop_score")
    pagespeed_metric = latest.get("pagespeed_mobile_score") or latest.get("pagespeed_desktop_score")
    crawler_link_audit = _latest_crawler_link_audit_summary(db, site_id=site.id)
    available_metrics = [metric for metric in latest.values()]
    last_updated = max((metric.collected_at for metric in available_metrics), default=site.created_at)
    pagespeed_score = float(pagespeed_metric.value) if pagespeed_metric else 0.0
    alert_rows = recent_alerts_cache if recent_alerts_cache is not None else get_recent_alerts(db, limit=100)
    recent_site_alerts = [alert for alert in alert_rows if alert["domain"] == site.domain][:6]
    pagespeed_status_alerts = [
        alert["message"]
        for alert in recent_site_alerts
        if alert["alert_type"] in {"pagespeed_mobile_fetch_error", "pagespeed_desktop_fetch_error"}
    ]
    mobile_status = _pagespeed_strategy_status(latest, "mobile", pagespeed_status_alerts)
    desktop_status = _pagespeed_strategy_status(latest, "desktop", pagespeed_status_alerts)
    base_crawler_issues: list[str] = []
    if _metric_value(latest, "crawler_robots_accessible", 0.0) < 1.0:
        base_crawler_issues.append("robots.txt")
    if _metric_value(latest, "crawler_sitemap_exists", 0.0) < 1.0:
        base_crawler_issues.append("sitemap")
    if _metric_value(latest, "crawler_schema_found", 0.0) < 1.0:
        base_crawler_issues.append("schema")
    if _metric_value(latest, "crawler_canonical_found", 0.0) < 1.0:
        base_crawler_issues.append("canonical")
    base_crawler_ok = all(
        _metric_value(latest, metric_name, 0.0) >= 1.0
        for metric_name in (
            "crawler_robots_accessible",
            "crawler_sitemap_exists",
            "crawler_schema_found",
            "crawler_canonical_found",
        )
    )
    broken_links_ok = crawler_link_audit["broken_links"] <= 0
    redirect_chain_ok = crawler_link_audit["redirect_chains"] <= 0
    redirect_301_ok = crawler_link_audit["redirect_301_links"] <= 0
    redirect_302_ok = crawler_link_audit["redirect_302_links"] <= 0
    crawler_ok = base_crawler_ok and broken_links_ok and redirect_chain_ok and redirect_301_ok and redirect_302_ok
    crawler_status_parts: list[str] = list(base_crawler_issues)
    if crawler_link_audit["audited_urls"] > 0:
        crawler_status_parts.append(f'{crawler_link_audit["audited_urls"]} URL tarandı')
    if crawler_link_audit["redirect_301_links"] > 0:
        crawler_status_parts.append(f'{crawler_link_audit["redirect_301_links"]} adet 301')
    if crawler_link_audit["redirect_302_links"] > 0:
        crawler_status_parts.append(f'{crawler_link_audit["redirect_302_links"]} adet 302')
    if crawler_link_audit["broken_links"] > 0:
        crawler_status_parts.append(f'{crawler_link_audit["broken_links"]} kırık link')
    if crawler_link_audit["redirect_chains"] > 0:
        crawler_status_parts.append(f'{crawler_link_audit["redirect_chains"]} redirect zinciri')
    crawler_label = "Crawler sağlıklı" if crawler_ok else "Crawler kontrol gerekli"
    crawler_detail = ", ".join(crawler_status_parts) if crawler_status_parts else "robots, sitemap, schema ve site içi link denetimi iyi durumda"
    search_console_status = _search_console_status(db, latest, site.id)
    search_console_report = _search_console_report_payload(db, site_id=site.id)
    search_console_summary = search_console_report.get("summary_current") or {}
    search_console_run = _latest_provider_run(db, site_id=site.id, provider="search_console", strategy="all")
    mobile_pagespeed_score = float(mobile_pagespeed_metric.value) if mobile_pagespeed_metric is not None else None
    desktop_pagespeed_score = float(desktop_pagespeed_metric.value) if desktop_pagespeed_metric is not None else None
    return {
        "id": site.id,
        "display_name": site.display_name,
        "domain": site.domain,
        "pagespeed_score": round(pagespeed_score),
        "pagespeed_color": _score_color(pagespeed_score),
        "pagespeed_mobile_score": round(mobile_pagespeed_score) if mobile_pagespeed_score is not None else None,
        "pagespeed_mobile_label": str(round(mobile_pagespeed_score)) if mobile_pagespeed_score is not None else "Veri yok",
        "pagespeed_mobile_color": _score_color(mobile_pagespeed_score) if mobile_pagespeed_score is not None else "text-slate-400",
        "pagespeed_desktop_score": round(desktop_pagespeed_score) if desktop_pagespeed_score is not None else None,
        "pagespeed_desktop_label": str(round(desktop_pagespeed_score)) if desktop_pagespeed_score is not None else "Veri yok",
        "pagespeed_desktop_color": _score_color(desktop_pagespeed_score) if desktop_pagespeed_score is not None else "text-slate-400",
        "crawler_ok": crawler_ok,
        "crawler_label": crawler_label,
        "crawler_detail": crawler_detail,
        "crawler_link_audit": crawler_link_audit,
        "check_count": len(available_metrics),
        "last_updated": format_local_datetime(last_updated, fallback="Henüz veri yok"),
        "alert_count": len(recent_site_alerts),
        "recent_alerts": recent_site_alerts[:3],
        "top_queries": search_console_report.get("top_queries") or [],
        "search_console": {
            "clicks": float(search_console_summary.get("clicks", 0.0)),
            "clicks_label": _format_compact_number(search_console_summary.get("clicks", 0.0)),
            "ctr": float(search_console_summary.get("ctr", 0.0)),
            "ctr_label": f"{_format_max_two_decimals(search_console_summary.get('ctr', 0.0))}%",
            "position": float(search_console_summary.get("position", 0.0)),
            "position_label": _format_max_two_decimals(search_console_summary.get("position", 0.0)),
            "status": search_console_status,
            "last_run_status": str(search_console_run.status or "").upper() if search_console_run and search_console_run.status else "NEVER",
            "last_run_at": _format_optional_datetime(search_console_run.requested_at if search_console_run else None),
            "last_run_dt": search_console_run.requested_at if search_console_run else None,
        },
        "pagespeed_status": {
            "mobile_updated_at": mobile_status["updated_at"],
            "desktop_updated_at": desktop_status["updated_at"],
            "mobile": mobile_status,
            "desktop": desktop_status,
            "messages": pagespeed_status_alerts,
        },
        "flash_message": flash_message,
    }


def _summarize_manual_measurement(results: dict[str, dict]) -> str:
    pagespeed_result = results.get("pagespeed", {})
    crawler_result = results.get("crawler", {})
    search_console_result = results.get("search_console", {})
    parts: list[str] = []

    if pagespeed_result.get("saved_metric_count"):
        parts.append("PageSpeed ölçümü tamamlandı")
    elif pagespeed_result.get("state") == "skipped":
        parts.append("PageSpeed yeniden tetiklenmedi")
    elif pagespeed_result.get("errors"):
        parts.append("PageSpeed kısmi olarak güncellendi")

    if crawler_result.get("metrics"):
        parts.append("Crawler kontrolleri yenilendi")
    elif crawler_result.get("state") == "skipped":
        parts.append("Crawler kontrolleri tekrar çalıştırılmadı")

    if search_console_result.get("blocked"):
        parts.append("Search Console kota nedeniyle atlandi")
    elif search_console_result.get("summary"):
        parts.append("Search Console verisi yenilendi")
    elif search_console_result.get("state") == "skipped":
        parts.append("Search Console yeniden sorgulanmadı")

    return ". ".join(parts) + "." if parts else "Ölçüm tetiklendi."


def _format_compact_number(value) -> str:
    if value is None or value == "":
        return "0"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    abs_numeric = abs(numeric)
    for threshold, suffix in (
        (1_000_000_000, "B"),
        (1_000_000, "M"),
        (1_000, "K"),
    ):
        if abs_numeric >= threshold:
            compact = f"{numeric / threshold:.1f}".rstrip("0").rstrip(".")
            return f"{compact}{suffix}"
    if float(numeric).is_integer():
        return f"{int(numeric):,}"
    return _format_max_two_decimals(numeric)


def _dashboard_tone_classes(tone: str) -> dict[str, str]:
    palette = {
        "rose": {
            "badge": "border-rose-200 bg-rose-50 text-rose-700",
            "title": "text-rose-700",
            "accent": "text-rose-600",
            "panel": "border-rose-100 bg-rose-50/60",
        },
        "amber": {
            "badge": "border-amber-200 bg-amber-50 text-amber-700",
            "title": "text-amber-700",
            "accent": "text-amber-700",
            "panel": "border-amber-100 bg-amber-50/60",
        },
        "sky": {
            "badge": "border-sky-200 bg-sky-50 text-sky-700",
            "title": "text-sky-700",
            "accent": "text-sky-700",
            "panel": "border-sky-100 bg-sky-50/70",
        },
        "emerald": {
            "badge": "border-emerald-200 bg-emerald-50 text-emerald-700",
            "title": "text-emerald-700",
            "accent": "text-emerald-700",
            "panel": "border-emerald-100 bg-emerald-50/70",
        },
    }
    return palette.get(tone, {
        "badge": "border-slate-200 bg-slate-50 text-slate-700",
        "title": "text-slate-800",
        "accent": "text-slate-700",
        "panel": "border-slate-200 bg-slate-50",
    })


def _dashboard_device_label(device_code: str) -> str:
    if device_code == "M":
        return "Mobile"
    if device_code == "D":
        return "Desktop"
    return ""


def _build_dashboard_overview(site_cards: list[dict], recent_alerts: list[dict]) -> list[dict]:
    total_sites = len(site_cards)
    highlighted_alert_count = min(len(recent_alerts), 12)
    ready_search_console = sum(
        1 for card in site_cards if card.get("search_console", {}).get("status", {}).get("state") != "failed"
    )
    attention_sites = sum(
        1
        for card in site_cards
        if card.get("alert_count", 0) > 0 or not card.get("crawler_ok") or card.get("pagespeed_score", 0) < 50
    )
    latest_snapshot = max(
        (card.get("search_console", {}).get("last_run_dt") for card in site_cards if card.get("search_console", {}).get("last_run_dt")),
        default=None,
    )
    return [
        {
            "eyebrow": "Aktif Site",
            "value": str(total_sites),
            "note": "dashboard takibi açık proje",
        },
        {
            "eyebrow": "Kritik Uyarı",
            "value": str(highlighted_alert_count),
            "note": "öne çıkan alarm kaydı",
        },
        {
            "eyebrow": "Search Console Güncel",
            "value": f"{ready_search_console}/{total_sites}" if total_sites else "0/0",
            "note": "sabah verisi hazır",
        },
        {
            "eyebrow": "Dikkat Gereken Site",
            "value": str(attention_sites),
            "note": format_local_datetime(latest_snapshot) if latest_snapshot else "henüz otomatik çekim yok",
        },
    ]


def _build_dashboard_top_drops(site_cards: list[dict], *, limit: int = 6) -> list[dict]:
    candidates: list[dict] = []
    seen_queries: set[tuple[str, str]] = set()
    for card in site_cards:
        for query in card.get("top_queries") or []:
            clicks_current = float(query.get("clicks_current", 0.0))
            clicks_previous = float(query.get("clicks_previous", 0.0))
            clicks_diff = float(query.get("clicks_diff", 0.0))
            position_current = float(query.get("position_current", 0.0))
            position_previous = float(query.get("position_previous", position_current))
            position_diff = float(query.get("position_diff", 0.0))
            if clicks_diff >= 0 and position_diff <= 0.15:
                continue

            reason = "Tıklama düşüşü"
            tone = "rose"
            metric = f"{_format_compact_number(clicks_previous)} -> {_format_compact_number(clicks_current)} tıklama"
            secondary = ""
            impact = abs(clicks_diff)

            if clicks_diff < 0 and position_diff > 0.15:
                reason = "Tıklama + pozisyon kaybı"
                metric = f"{_format_compact_number(clicks_previous)} -> {_format_compact_number(clicks_current)} tıklama"
                secondary = f"Pozisyon {_format_max_two_decimals(position_previous)} -> {_format_max_two_decimals(position_current)}"
                impact = abs(clicks_diff) + (position_diff * 1000)
            elif position_diff > 0.15:
                reason = "Pozisyon düşüşü"
                tone = "rose"
                metric = f"Pozisyon {_format_max_two_decimals(position_previous)} -> {_format_max_two_decimals(position_current)}"
                secondary = f"{_format_compact_number(clicks_current)} tıklama"
                impact = max(clicks_current, 1.0) + (position_diff * 1000)

            key = (card.get("domain", ""), str(query.get("query") or "").strip().lower())
            if not key[1] or key in seen_queries:
                continue
            seen_queries.add(key)
            candidates.append(
                {
                    "domain": card.get("domain"),
                    "query": query.get("query"),
                    "reason": reason,
                    "metric": metric,
                    "secondary": secondary,
                    "impact": impact,
                    "classes": _dashboard_tone_classes(tone),
                }
            )

    candidates.sort(key=lambda item: item.get("impact", 0.0), reverse=True)
    return candidates[:limit]


def _build_dashboard_opportunities(site_cards: list[dict], *, limit: int = 4) -> list[dict]:
    candidates: list[dict] = []
    seen_queries: set[tuple[str, str]] = set()
    for card in site_cards:
        for query in card.get("top_queries") or []:
            clicks_current = float(query.get("clicks_current", 0.0))
            clicks_previous = float(query.get("clicks_previous", 0.0))
            position_current = float(query.get("position_current", 0.0))
            position_previous = float(query.get("position_previous", position_current))
            position_diff = float(query.get("position_diff", 0.0))
            title = ""
            tone = "emerald"
            detail = ""
            action = ""
            score = 0.0

            if clicks_current >= 2000 and 3.0 <= position_current <= 8.0 and position_diff <= 0.25:
                title = "İlk sayfa fırsatı"
                detail = f"Pozisyon {_format_max_two_decimals(position_current)} · {_format_compact_number(clicks_current)} tıklama"
                action = "İçerik, başlık ve iç link güçlendirmesiyle daha yukarı taşınabilir."
                tone = "sky"
                score = clicks_current / max(position_current, 1.0)
            elif clicks_current >= 800 and position_diff < -0.15 and clicks_current >= clicks_previous:
                title = "Momentum yakalayan sorgu"
                detail = f"Pozisyon {_format_max_two_decimals(position_previous)} -> {_format_max_two_decimals(position_current)}"
                action = "Kazanım yeni ise landing sayfayı desteklemek için iyi bir zaman."
                tone = "emerald"
                score = clicks_current + (abs(position_diff) * 1000)
            elif clicks_current >= 3000 and position_current <= 3.0:
                title = "CTR optimizasyon adayı"
                detail = f"Pozisyon {_format_max_two_decimals(position_current)} · {_format_compact_number(clicks_current)} tıklama"
                action = "Snippet ve title testi ile ekstra tıklama alınabilir."
                tone = "amber"
                score = clicks_current

            key = (card.get("domain", ""), str(query.get("query") or "").strip().lower())
            if not title or not key[1] or key in seen_queries:
                continue
            seen_queries.add(key)
            candidates.append(
                {
                    "domain": card.get("domain"),
                    "query": query.get("query"),
                    "title": title,
                    "detail": detail,
                    "action": action,
                    "score": score,
                    "classes": _dashboard_tone_classes(tone),
                }
            )

    candidates.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    return candidates[:limit]


def _dashboard_comparison_data(period_days: int) -> dict:
    # Dashboard karşılaştırma grafikleri için çoklu site verisi üretir.
    with SessionLocal() as db:
        sites = db.query(Site).order_by(Site.created_at.asc()).all()
        labels: list[str] = []
        mobile_scores: list[float] = []
        desktop_scores: list[float] = []
        crawler_health: list[float] = []
        search_clicks: list[float] = []
        search_impressions: list[float] = []
        search_ctr: list[float] = []
        search_position: list[float] = []
        for site in sites:
            history = get_metric_history(db, site.id, days=period_days)
            labels.append(site.domain)
            mobile_scores.append(_latest_value_from_history(history, "pagespeed_mobile_score", 0.0))
            desktop_scores.append(_latest_value_from_history(history, "pagespeed_desktop_score", 0.0))
            crawler_values = [
                _latest_value_from_history(history, "crawler_robots_accessible", 0.0),
                _latest_value_from_history(history, "crawler_sitemap_exists", 0.0),
                _latest_value_from_history(history, "crawler_schema_found", 0.0),
                _latest_value_from_history(history, "crawler_canonical_found", 0.0),
            ]
            crawler_health.append(round(sum(crawler_values) / 4 * 100, 2))
            search_clicks.append(_latest_value_from_history(history, "search_console_clicks_28d", 0.0))
            search_impressions.append(_latest_value_from_history(history, "search_console_impressions_28d", 0.0))
            search_ctr.append(_latest_value_from_history(history, "search_console_avg_ctr_28d", 0.0))
            search_position.append(_latest_value_from_history(history, "search_console_avg_position_28d", 0.0))
        return {
            "labels": labels,
            "mobile_scores": mobile_scores,
            "desktop_scores": desktop_scores,
            "crawler_health": crawler_health,
            "search_clicks": search_clicks,
            "search_impressions": search_impressions,
            "search_ctr": search_ctr,
            "search_position": search_position,
        }


def _dashboard_trend_data(period: str, period_days: int) -> dict:
    # Çoklu site trend grafiği için son 7 mobile PageSpeed noktasını döndürür.
    with SessionLocal() as db:
        sites = db.query(Site).order_by(Site.domain.asc()).all()
        series: list[dict] = []
        for site in sites:
            history = get_metric_history(db, site.id, days=period_days)
            mobile_history = history.get("pagespeed_mobile_score", [])
            series.append(
                {
                    "name": site.domain,
                    "x": [_format_trend_label(item["collected_at"], period) for item in mobile_history],
                    "y": [item["value"] for item in mobile_history],
                }
            )
        return {"series": series}


def _site_detail_context(domain: str, period: str, period_days: int) -> dict:
    # Site detay görünümü için gerekli tüm veriyi hazırlar.
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            raise ValueError("Site bulunamadı.")

        latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
        if settings.live_refresh_enabled and settings.pagespeed_auto_collect_on_page_load and (
            not _pagespeed_strategy_is_complete(latest, "mobile")
            or not _pagespeed_strategy_is_complete(latest, "desktop")
        ) and not _latest_collector_run_recent(
            db,
            site_id=site.id,
            provider="pagespeed",
            cooldown_seconds=settings.pagespeed_refresh_cooldown_seconds,
        ):
            _refresh_site_detail_measurements(db, site, include_pagespeed=True)
            latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}

        history = get_metric_history(db, site.id, days=period_days)
        top_queries: list[dict] = []

        mobile_score = _latest_value_from_history(
            history,
            "pagespeed_mobile_score",
            float((latest.get("pagespeed_mobile_score").value if latest.get("pagespeed_mobile_score") else 0.0)),
        )
        desktop_score = _latest_value_from_history(
            history,
            "pagespeed_desktop_score",
            float((latest.get("pagespeed_desktop_score").value if latest.get("pagespeed_desktop_score") else 0.0)),
        )
        trend_labels = [_format_trend_label(item["collected_at"], period) for item in history.get("pagespeed_mobile_score", [])]
        mobile_trend = [item["value"] for item in history.get("pagespeed_mobile_score", [])]
        desktop_trend = [item["value"] for item in history.get("pagespeed_desktop_score", [])]
        search_clicks_history = history.get("search_console_clicks_28d", [])
        search_impressions_history = history.get("search_console_impressions_28d", [])
        search_ctr_history = history.get("search_console_avg_ctr_28d", [])
        search_position_history = history.get("search_console_avg_position_28d", [])
        search_trend_labels = [_format_trend_label(item["collected_at"], period) for item in search_clicks_history]

        crawler_checks = _crawler_checks_from_metrics(latest)
        
        # PageSpeed comprehensive analysis
        pagespeed_analysis = analyze_pagespeed_detailed(int(mobile_score), int(desktop_score))

        mobile_lcp = _metric_value(latest, "pagespeed_mobile_lcp")
        mobile_cls = _metric_value(latest, "pagespeed_mobile_cls")
        mobile_inp = _metric_value(latest, "pagespeed_mobile_inp")
        mobile_fcp = _metric_value(latest, "pagespeed_mobile_fcp")
        mobile_ttfb = _metric_value(latest, "pagespeed_mobile_ttfb")
        desktop_lcp = _metric_value(latest, "pagespeed_desktop_lcp")
        desktop_cls = _metric_value(latest, "pagespeed_desktop_cls")
        desktop_inp = _metric_value(latest, "pagespeed_desktop_inp")
        desktop_fcp = _metric_value(latest, "pagespeed_desktop_fcp")
        desktop_ttfb = _metric_value(latest, "pagespeed_desktop_ttfb")

        mobile_lighthouse_analysis = get_latest_pagespeed_audit_snapshot(db, site.id, "mobile") or _fallback_lighthouse_analysis(
            "mobile",
            accessibility_score=round(_metric_value(latest, "pagespeed_mobile_accessibility_score", 0.0)),
            practices_score=round(_metric_value(latest, "pagespeed_mobile_best_practices_score", 0.0)),
            seo_score=round(_metric_value(latest, "pagespeed_mobile_seo_score", 0.0)),
            lcp_ms=mobile_lcp,
            fcp_ms=mobile_fcp,
            cls=mobile_cls,
        )
        desktop_lighthouse_analysis = get_latest_pagespeed_audit_snapshot(db, site.id, "desktop") or _fallback_lighthouse_analysis(
            "desktop",
            accessibility_score=round(_metric_value(latest, "pagespeed_desktop_accessibility_score", 0.0)),
            practices_score=round(_metric_value(latest, "pagespeed_desktop_best_practices_score", 0.0)),
            seo_score=round(_metric_value(latest, "pagespeed_desktop_seo_score", 0.0)),
            lcp_ms=desktop_lcp,
            fcp_ms=desktop_fcp,
            cls=desktop_cls,
        )
        mobile_lighthouse_analysis = _normalize_lighthouse_issue_order(mobile_lighthouse_analysis)
        desktop_lighthouse_analysis = _normalize_lighthouse_issue_order(desktop_lighthouse_analysis)
        latest_mobile_category_scores = _latest_pagespeed_category_scores(db, site.id, "mobile")
        latest_desktop_category_scores = _latest_pagespeed_category_scores(db, site.id, "desktop")

        category_mobile_scores = latest_mobile_category_scores
        category_desktop_scores = latest_desktop_category_scores

        mobile_lighthouse_performance = round(category_mobile_scores.get("performance", 0.0)) or round(mobile_score) or round(_analysis_category_score(mobile_lighthouse_analysis, "performance"))
        mobile_lighthouse_accessibility = round(category_mobile_scores.get("accessibility", 0.0)) or round(_metric_value(latest, "pagespeed_mobile_accessibility_score", 0.0)) or round(_analysis_category_score(mobile_lighthouse_analysis, "accessibility"))
        mobile_lighthouse_best_practices = round(category_mobile_scores.get("best_practices", 0.0)) or round(_metric_value(latest, "pagespeed_mobile_best_practices_score", 0.0)) or round(_analysis_category_score(mobile_lighthouse_analysis, "best_practices"))
        mobile_lighthouse_seo = round(category_mobile_scores.get("seo", 0.0)) or round(_metric_value(latest, "pagespeed_mobile_seo_score", 0.0)) or round(_analysis_category_score(mobile_lighthouse_analysis, "seo"))

        desktop_lighthouse_performance = round(category_desktop_scores.get("performance", 0.0)) or round(desktop_score) or round(_analysis_category_score(desktop_lighthouse_analysis, "performance"))
        desktop_lighthouse_accessibility = round(category_desktop_scores.get("accessibility", 0.0)) or round(_metric_value(latest, "pagespeed_desktop_accessibility_score", 0.0)) or round(_analysis_category_score(desktop_lighthouse_analysis, "accessibility"))
        desktop_lighthouse_best_practices = round(category_desktop_scores.get("best_practices", 0.0)) or round(_metric_value(latest, "pagespeed_desktop_best_practices_score", 0.0)) or round(_analysis_category_score(desktop_lighthouse_analysis, "best_practices"))
        desktop_lighthouse_seo = round(category_desktop_scores.get("seo", 0.0)) or round(_metric_value(latest, "pagespeed_desktop_seo_score", 0.0)) or round(_analysis_category_score(desktop_lighthouse_analysis, "seo"))

        lighthouse_accessibility = round(
            _average_available([mobile_lighthouse_accessibility, desktop_lighthouse_accessibility])
        )
        lighthouse_best_practices = round(
            _average_available([mobile_lighthouse_best_practices, desktop_lighthouse_best_practices])
        )
        lighthouse_seo = round(_average_available([mobile_lighthouse_seo, desktop_lighthouse_seo]))

        # Lighthouse comprehensive analysis
        lighthouse_analysis = get_lighthouse_analysis(
            accessible_score=lighthouse_accessibility,
            practices_score=lighthouse_best_practices,
            seo_score=lighthouse_seo,
        )
        
        recent_site_alerts = [alert for alert in get_recent_alerts(db, limit=20) if alert["domain"] == site.domain][:5]
        pagespeed_status_alerts = [
            alert["message"]
            for alert in recent_site_alerts
            if alert["alert_type"] in {"pagespeed_mobile_fetch_error", "pagespeed_desktop_fetch_error"}
        ]
        mobile_status = _pagespeed_strategy_status(latest, "mobile", pagespeed_status_alerts)
        desktop_status = _pagespeed_strategy_status(latest, "desktop", pagespeed_status_alerts)
        search_console_status = _search_console_status(db, latest, site.id)

        top_queries = get_top_queries(db, site, limit=50, device="all") if search_console_status["state"] != "failed" or search_console_status.get("has_rows") else []
        has_search_console_queries = bool(top_queries)
        has_search_console_trend = bool(search_trend_labels)

        if search_console_status["state"] != "failed" or has_search_console_queries or has_search_console_trend:
            search_summary = {
                "clicks": _latest_value_from_history(
                    history,
                    "search_console_clicks_28d",
                    float((latest.get("search_console_clicks_28d").value if latest.get("search_console_clicks_28d") else 0.0)),
                ),
                "impressions": _latest_value_from_history(
                    history,
                    "search_console_impressions_28d",
                    float((latest.get("search_console_impressions_28d").value if latest.get("search_console_impressions_28d") else 0.0)),
                ),
                "avg_ctr": _latest_value_from_history(
                    history,
                    "search_console_avg_ctr_28d",
                    float((latest.get("search_console_avg_ctr_28d").value if latest.get("search_console_avg_ctr_28d") else 0.0)),
                ),
                "avg_position": _latest_value_from_history(
                    history,
                    "search_console_avg_position_28d",
                    float((latest.get("search_console_avg_position_28d").value if latest.get("search_console_avg_position_28d") else 0.0)),
                ),
                "dropped_queries": _latest_value_from_history(
                    history,
                    "search_console_dropped_queries",
                    float((latest.get("search_console_dropped_queries").value if latest.get("search_console_dropped_queries") else 0.0)),
                ),
                "biggest_drop": _latest_value_from_history(
                    history,
                    "search_console_biggest_drop",
                    float((latest.get("search_console_biggest_drop").value if latest.get("search_console_biggest_drop") else 0.0)),
                ),
            }
            search_trend_data = {
                "labels": search_trend_labels,
                "clicks": [item["value"] for item in search_clicks_history],
                "impressions": [item["value"] for item in search_impressions_history],
                "avg_ctr": [item["value"] for item in search_ctr_history],
                "avg_position": [item["value"] for item in search_position_history],
            }
            has_search_console_queries = bool(top_queries)
            has_search_console_trend = bool(search_trend_labels)
        else:
            top_queries = []
            search_summary = {
                "clicks": 0.0,
                "impressions": 0.0,
                "avg_ctr": 0.0,
                "avg_position": 0.0,
                "dropped_queries": 0.0,
                "biggest_drop": 0.0,
            }
            search_trend_data = {
                "labels": [],
                "clicks": [],
                "impressions": [],
                "avg_ctr": [],
                "avg_position": [],
            }
            has_search_console_queries = False
            has_search_console_trend = False

        return {
            "site_name": site.display_name,
            "sites": get_sidebar_sites(),
            "domain": site.domain,
            "period": period,
            "mobile_score": mobile_score,
            "mobile_color": _score_color(mobile_score),
            "mobile_lcp": mobile_lcp,
            "mobile_cls": mobile_cls,
            "mobile_inp": mobile_inp,
            "mobile_fcp": mobile_fcp,
            "mobile_ttfb": mobile_ttfb,
            "mobile_lcp_seconds": _ms_to_seconds(mobile_lcp),
            "mobile_fcp_seconds": _ms_to_seconds(mobile_fcp),
            "mobile_ttfb_seconds": _ms_to_seconds(mobile_ttfb),
            "mobile_lcp_progress": _pagespeed_progress(mobile_lcp, 2500, 4000),
            "mobile_inp_progress": _pagespeed_progress(mobile_inp, 200, 500),
            "mobile_cls_progress": _pagespeed_progress(mobile_cls, 0.1, 0.25),
            "mobile_inp_available": _has_metric_value(mobile_inp),
            "mobile_ttfb_available": _has_metric_value(mobile_ttfb),
            "mobile_cwv": _cwv_status(mobile_lcp, mobile_inp, mobile_cls),
            "desktop_score": desktop_score,
            "desktop_color": _score_color(desktop_score),
            "desktop_lcp": desktop_lcp,
            "desktop_cls": desktop_cls,
            "desktop_inp": desktop_inp,
            "desktop_fcp": desktop_fcp,
            "desktop_ttfb": desktop_ttfb,
            "desktop_lcp_seconds": _ms_to_seconds(desktop_lcp),
            "desktop_fcp_seconds": _ms_to_seconds(desktop_fcp),
            "desktop_ttfb_seconds": _ms_to_seconds(desktop_ttfb),
            "desktop_lcp_progress": _pagespeed_progress(desktop_lcp, 2500, 4000),
            "desktop_inp_progress": _pagespeed_progress(desktop_inp, 200, 500),
            "desktop_cls_progress": _pagespeed_progress(desktop_cls, 0.1, 0.25),
            "desktop_inp_available": _has_metric_value(desktop_inp),
            "desktop_ttfb_available": _has_metric_value(desktop_ttfb),
            "desktop_cwv": _cwv_status(desktop_lcp, desktop_inp, desktop_cls),
            "pagespeed_status": {
                "mobile_updated_at": mobile_status["updated_at"],
                "desktop_updated_at": desktop_status["updated_at"],
                "mobile": mobile_status,
                "desktop": desktop_status,
                "messages": pagespeed_status_alerts,
            },
            "mobile_lighthouse_scores": [
                _build_lighthouse_score("performance", "Performance", "Performans", mobile_lighthouse_performance, "mobile"),
                _build_lighthouse_score("accessibility", "Accessibility", "Erişilebilirlik", mobile_lighthouse_accessibility, "mobile"),
                _build_lighthouse_score("practices", "Best Practices", "En İyi Uygulamalar", mobile_lighthouse_best_practices, "mobile"),
                _build_lighthouse_score("seo", "SEO", "Arama Motoru", mobile_lighthouse_seo, "mobile"),
            ],
            "desktop_lighthouse_scores": [
                _build_lighthouse_score("performance", "Performance", "Performans", desktop_lighthouse_performance, "desktop"),
                _build_lighthouse_score("accessibility", "Accessibility", "Erişilebilirlik", desktop_lighthouse_accessibility, "desktop"),
                _build_lighthouse_score("practices", "Best Practices", "En İyi Uygulamalar", desktop_lighthouse_best_practices, "desktop"),
                _build_lighthouse_score("seo", "SEO", "Arama Motoru", desktop_lighthouse_seo, "desktop"),
            ],
            "crawler_checks": crawler_checks,
            "pagespeed_analysis": pagespeed_analysis,
            "lighthouse_analysis": lighthouse_analysis,
            "mobile_lighthouse_analysis": mobile_lighthouse_analysis,
            "desktop_lighthouse_analysis": desktop_lighthouse_analysis,
            "site_alerts": recent_site_alerts,
            "top_queries": top_queries,
            "search_console_status": search_console_status,
            "search_summary": search_summary,
            "search_console_has_queries": has_search_console_queries,
            "search_console_has_trend": has_search_console_trend,
            "allow_live_lighthouse_sync": settings.pagespeed_live_sync_on_page_load,
            "trend_data": {
                "labels": trend_labels,
                "mobile": mobile_trend,
                "desktop": desktop_trend,
            },
            "search_trend_data": search_trend_data,
            "should_background_refresh": False,
        }


@app.get("/")
def dashboard(request: Request):
    # Jinja2 template'i başlangıç ekranını render eder.
    with SessionLocal() as db:
        recent_alerts = get_recent_alerts(db, limit=100)
        site_cards = _dashboard_cards_with_db(db, recent_alerts_cache=recent_alerts)
        payload = {
            "site_name": "SEO Agent Dashboard",
            "sites": get_sidebar_sites(),
            "period": _resolve_period(request.query_params.get("period"))[0],
            "overview_items": _build_dashboard_overview(site_cards, recent_alerts),
            "critical_alerts": recent_alerts[:6],
            "site_cards": site_cards,
            "top_drop_items": _build_dashboard_top_drops(site_cards, limit=6),
            "opportunity_items": _build_dashboard_opportunities(site_cards, limit=4),
        }
    return templates.TemplateResponse(request, "dashboard.html", context={"request": request, **payload})


@app.get("/design/lighthouse-minimal-options", response_class=HTMLResponse)
def design_lighthouse_minimal_options():
    design_path = BASE_DIR / "design" / "lighthouse_minimal_options.html"
    if not design_path.exists():
        return HTMLResponse("Design preview bulunamadi.", status_code=404)
    return HTMLResponse(design_path.read_text(encoding="utf-8"))


@app.post("/dashboard/cards/{site_id}/measure", response_class=HTMLResponse)
def dashboard_measure_site(request: Request, site_id: int):
    period, _ = _resolve_period(request.query_params.get("period"))
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

        try:
            search_console_connected = _is_search_console_connected(db, site.id)
            results = _refresh_site_detail_measurements(
                db,
                site,
                include_pagespeed=True,
                include_crawler=True,
                include_search_console=search_console_connected,
                force=True,
            )
            if not search_console_connected:
                try:
                    results["crux_history"] = collect_crux_history(db, site)
                except Exception as exc:  # noqa: BLE001
                    results["crux_history"] = {"state": "failed", "error": str(exc)}
            db.commit()
            notify_result_map(
                trigger_source="manual",
                site=site,
                results=results,
                action_label="Dashboard manuel ölçüm",
            )
            if isinstance(results.get("crawler"), dict):
                notify_crawler_audit_emails(
                    db=db,
                    site=site,
                    result=results.get("crawler"),
                    trigger_source="manual",
                )
            flash_message = _summarize_manual_measurement(results)
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            LOGGER.warning("Dashboard manual measure failed for site_id=%s: %s", site_id, exc)
            site = db.query(Site).filter(Site.id == site_id).first()
            if site is None:
                return HTMLResponse("Site bulunamadı.", status_code=404)
            flash_message = f"Ölçüm tamamlanamadı. Mevcut kayıtlı veri gösteriliyor. Detay: {exc}"

        card = _build_dashboard_card(db, site, flash_message=flash_message)
    return templates.TemplateResponse(
        request,
        "partials/dashboard_site_card.html",
        context={"request": request, "card": card, "period": period},
    )


@app.get("/site/{domain}", response_class=HTMLResponse)
def site_detail(request: Request, domain: str):
    # Lighthouse içeriği Data Explorer'a taşındığı için eski rota güvenli şekilde yönlendirilir.
    return RedirectResponse(url=f"/data-explorer/{domain}", status_code=307)


@app.get("/data-explorer/{domain}", response_class=HTMLResponse)
def data_explorer(request: Request, domain: str):
    try:
        payload = _data_explorer_context(domain)
    except ValueError:
        return HTMLResponse("Site bulunamadı.", status_code=404)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "partials/data_explorer_content.html", context={"request": request, **payload})
    return templates.TemplateResponse(request, "data_explorer.html", context={"request": request, **payload})


@app.get("/external")
@app.get("/public-sites")
def public_sites_page(request: Request):
    with SessionLocal() as db:
        payload = {
            "site_name": "External",
            "sites": get_sidebar_sites(),
            "public_sites": _public_sites_payload(db),
        }
    template_name = "partials/public_sites_content.html" if request.headers.get("HX-Request") == "true" else "public_sites.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.get("/external-explorer/{domain}", response_class=HTMLResponse)
@app.get("/public-explorer/{domain}", response_class=HTMLResponse)
def public_explorer(request: Request, domain: str):
    try:
        payload = _public_explorer_context(domain)
    except ValueError:
        return HTMLResponse("Site bulunamadı.", status_code=404)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "partials/public_explorer_content.html", context={"request": request, **payload})
    return templates.TemplateResponse(request, "public_explorer.html", context={"request": request, **payload})


@app.get("/external/site-list")
@app.get("/public-sites/site-list")
def public_sites_list(request: Request):
    with SessionLocal() as db:
        return templates.TemplateResponse(
            request,
            "partials/public_site_cards.html",
            context={
                "request": request,
                "public_sites": _public_sites_payload(db),
            },
        )


def _normalize_external_domain(raw_value: str) -> str:
    candidate = str(raw_value or "").strip().lower()
    if not candidate:
        return ""
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if "/" in host:
        host = host.split("/", 1)[0]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host.rstrip(".")


@app.post("/external/sites")
async def public_sites_create_site(request: Request):
    with SessionLocal() as db:
        content_type = (request.headers.get("content-type") or "").lower()
        if "application/json" in content_type:
            payload = await request.json()
            domain_input = payload.get("domain") or payload.get("url") or ""
            display_name = (payload.get("display_name") or "").strip()
            is_active = bool(payload.get("is_active", True))
        else:
            form = await request.form()
            domain_input = form.get("domain") or form.get("url") or ""
            display_name = str(form.get("display_name", "")).strip()
            is_active = str(form.get("is_active", "true")).lower() in {"true", "1", "on", "yes"}

        domain = _normalize_external_domain(str(domain_input))
        if not domain:
            return JSONResponse({"ok": False, "error": "Geçerli bir domain veya URL girin."}, status_code=422)

        if not display_name:
            display_name = domain

        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            site = Site(domain=domain, display_name=display_name, is_active=is_active)
        else:
            site.display_name = display_name or site.display_name
            site.is_active = is_active

        db.add(site)
        db.commit()
        db.refresh(site)

        if not _is_external_site(db, site.id):
            db.add(ExternalSite(site_id=site.id))
            db.commit()

        results = _refresh_public_site_measurements(db, site, force=True)
        db.commit()
        notify_result_map(
            trigger_source="manual",
            site=site,
            results=results,
            action_label="External site ekleme ve ilk tarama",
        )
        if isinstance(results.get("crawler"), dict):
            notify_crawler_audit_emails(
                db=db,
                site=site,
                result=results.get("crawler"),
                trigger_source="manual",
            )

        return JSONResponse(
            {
                "ok": True,
                "site": {
                    "id": site.id,
                    "domain": site.domain,
                    "display_name": site.display_name,
                },
                "summary": _summarize_manual_measurement(results),
            }
        )


@app.delete("/external/sites/{site_id}")
def public_sites_delete_site(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"ok": False, "error": "Site bulunamadı."}, status_code=404)
        marker = db.query(ExternalSite).filter(ExternalSite.site_id == site.id).first()
        if marker is None:
            return JSONResponse({"ok": False, "error": "Site external profilinde değil."}, status_code=404)

        db.delete(site)
        db.commit()
        if request.headers.get("HX-Request") == "true":
            return templates.TemplateResponse(
                request,
                "partials/public_site_cards.html",
                context={
                    "request": request,
                    "public_sites": _public_sites_payload(db),
                },
            )
        return JSONResponse({"ok": True, "deleted_id": site_id})


@app.post("/external/refresh/{site_id}")
@app.post("/public-sites/refresh/{site_id}")
def public_sites_refresh_site(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        if not _is_external_site(db, site.id):
            return HTMLResponse("Site external profilinde değil.", status_code=404)

        results = _refresh_public_site_measurements(db, site, force=True)
        db.commit()
        notify_result_map(
            trigger_source="manual",
            site=site,
            results=results,
            action_label="Public crawl verisini yenile",
        )
        if isinstance(results.get("crawler"), dict):
            notify_crawler_audit_emails(
                db=db,
                site=site,
                result=results.get("crawler"),
                trigger_source="manual",
            )
        return templates.TemplateResponse(
            request,
            "partials/public_site_cards.html",
            context={
                "request": request,
                "public_sites": _public_sites_payload(db),
            },
        )


@app.post("/external/refresh-all")
@app.post("/public-sites/refresh-all")
def public_sites_refresh_all(request: Request):
    with SessionLocal() as db:
        sites = (
            db.query(Site)
            .join(ExternalSite, ExternalSite.site_id == Site.id)
            .filter(Site.is_active.is_(True))
            .order_by(Site.created_at.asc(), Site.id.asc())
            .all()
        )
        for index, site in enumerate(sites):
            results = _refresh_public_site_measurements(db, site, force=True)
            db.commit()
            notify_result_map(
                trigger_source="manual",
                site=site,
                results=results,
                action_label="Public crawl tum siteler yenile",
            )
            if isinstance(results.get("crawler"), dict):
                notify_crawler_audit_emails(
                    db=db,
                    site=site,
                    result=results.get("crawler"),
                    trigger_source="manual",
                )
            if index < len(sites) - 1:
                time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))
        return templates.TemplateResponse(
            request,
            "partials/public_site_cards.html",
            context={
                "request": request,
                "public_sites": _public_sites_payload(db),
            },
        )


@app.get("/api/site/{domain}/top-queries")
def api_get_top_queries(domain: str, device: str = "all", limit: int = 10):
    """API endpoint to get filtered top queries by device and limit."""
    try:
        with SessionLocal() as db:
            site = db.query(Site).filter(Site.domain == domain).first()
            if site is None:
                return JSONResponse({"error": "Site not found"}, status_code=404)

            latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
            search_console_status = _search_console_status(db, latest, site.id)
            queries = get_top_queries(db, site, limit=limit, device=device)

            if not queries and search_console_status["state"] == "failed":
                return JSONResponse(
                    {
                        "queries": [],
                        "summary": {
                            "clicks": 0,
                            "impressions": 0,
                            "ctr": 0.0,
                            "position": 0.0,
                            "biggest_drop": 0.0,
                        },
                        "status": search_console_status,
                    }
                )

            # Calculate summary from returned queries
            total_clicks = sum(float(q.get("clicks", 0)) for q in queries)
            total_impressions = sum(float(q.get("impressions", 0)) for q in queries)
            total_ctr = sum(float(q.get("ctr", 0)) for q in queries)
            max_delta = max((float(q.get("delta", 0)) for q in queries), default=0.0)
            total_position = sum(float(q.get("position", 0)) for q in queries)
            
            avg_ctr = (total_ctr / len(queries)) if queries else 0.0
            avg_position = (total_position / len(queries)) if queries else 0.0
            
            return JSONResponse({
                "queries": queries,
                "status": search_console_status,
                "summary": {
                    "clicks": total_clicks,
                    "impressions": total_impressions,
                    "ctr": avg_ctr,
                    "position": avg_position,
                    "biggest_drop": max_delta
                }
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/site/{domain}/warehouse-summary")
def api_get_site_warehouse_summary(domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)
        return JSONResponse(
            {
                "site": site.domain,
                "warehouse": get_site_warehouse_summary(db, site_id=site.id),
            }
        )


@app.get("/api/site/{domain}/lighthouse-live-scores")
@limiter.limit("20/hour")
def api_get_live_lighthouse_scores(request: Request, domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
        mobile_scores = None
        desktop_scores = None
        source = "live"
        if _metrics_fresh_within(
            latest,
            ("pagespeed_mobile_score", "pagespeed_desktop_score"),
            settings.lighthouse_live_score_cache_seconds,
        ):
            mobile_scores = _cached_pagespeed_scores(latest, "mobile")
            desktop_scores = _cached_pagespeed_scores(latest, "desktop")
            if mobile_scores and desktop_scores:
                source = "cache"

        if mobile_scores is None or desktop_scores is None:
            try:
                mobile_scores = fetch_live_lighthouse_category_scores(site, "mobile")
                desktop_scores = fetch_live_lighthouse_category_scores(site, "desktop")
            except Exception as exc:
                return JSONResponse(
                    {"error": f"Live Lighthouse fetch failed: {exc}"},
                    status_code=502,
                )

        return JSONResponse(
            {
                "site": site.domain,
                "source": source,
                "mobile": {
                    "performance": _build_lighthouse_score("performance", "Performance", "Performans", mobile_scores.get("performance", 0.0), "mobile"),
                    "accessibility": _build_lighthouse_score("accessibility", "Accessibility", "Erişilebilirlik", mobile_scores.get("accessibility", 0.0), "mobile"),
                    "practices": _build_lighthouse_score("practices", "Best Practices", "En İyi Uygulamalar", mobile_scores.get("best_practices", 0.0), "mobile"),
                    "seo": _build_lighthouse_score("seo", "SEO", "Arama Motoru", mobile_scores.get("seo", 0.0), "mobile"),
                },
                "desktop": {
                    "performance": _build_lighthouse_score("performance", "Performance", "Performans", desktop_scores.get("performance", 0.0), "desktop"),
                    "accessibility": _build_lighthouse_score("accessibility", "Accessibility", "Erişilebilirlik", desktop_scores.get("accessibility", 0.0), "desktop"),
                    "practices": _build_lighthouse_score("practices", "Best Practices", "En İyi Uygulamalar", desktop_scores.get("best_practices", 0.0), "desktop"),
                    "seo": _build_lighthouse_score("seo", "SEO", "Arama Motoru", desktop_scores.get("seo", 0.0), "desktop"),
                },
            }
        )


@app.get("/api/site/{domain}/lighthouse-live-scores/{strategy}")
@limiter.limit("20/hour")
def api_get_live_lighthouse_scores_by_strategy(request: Request, domain: str, strategy: str):
    normalized_strategy = (strategy or "").strip().lower()
    if normalized_strategy not in {"mobile", "desktop"}:
        return JSONResponse({"error": "Invalid strategy"}, status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
        scores = None
        source = "live"
        score_metric = f"pagespeed_{normalized_strategy}_score"
        if _metrics_fresh_within(latest, (score_metric,), settings.lighthouse_live_score_cache_seconds):
            scores = _cached_pagespeed_scores(latest, normalized_strategy)
            if scores is not None:
                source = "cache"
        if scores is None:
            try:
                scores = fetch_live_lighthouse_category_scores(site, normalized_strategy)
            except Exception as exc:
                return JSONResponse(
                    {"error": f"Live Lighthouse fetch failed: {exc}"},
                    status_code=502,
                )

        return JSONResponse(
            {
                "site": site.domain,
                "strategy": normalized_strategy,
                "source": source,
                "scores": {
                    "performance": _build_lighthouse_score("performance", "Performance", "Performans", scores.get("performance", 0.0), normalized_strategy),
                    "accessibility": _build_lighthouse_score("accessibility", "Accessibility", "Erişilebilirlik", scores.get("accessibility", 0.0), normalized_strategy),
                    "practices": _build_lighthouse_score("practices", "Best Practices", "En İyi Uygulamalar", scores.get("best_practices", 0.0), normalized_strategy),
                    "seo": _build_lighthouse_score("seo", "SEO", "Arama Motoru", scores.get("seo", 0.0), normalized_strategy),
                },
            }
        )


@app.post("/api/site/{domain}/data-explorer/refresh")
def api_refresh_data_explorer(request: Request, domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        results: dict[str, dict] = {}
        try:
            results["crux_history"] = collect_crux_history(db, site)
        except Exception as exc:  # noqa: BLE001
            results["crux_history"] = {"state": "failed", "error": str(exc)}
        try:
            results["url_inspection"] = collect_url_inspection(db, site)
        except Exception as exc:  # noqa: BLE001
            results["url_inspection"] = {"state": "failed", "error": str(exc)}
        db.commit()
        notify_result_map(
            trigger_source="manual",
            site=site,
            results=results,
            action_label="Data Explorer manuel refresh",
        )
        return JSONResponse(
            {
                "site": site.domain,
                "refreshed": True,
                "results": results,
                "warehouse": get_site_warehouse_summary(db, site_id=site.id),
            }
        )


@app.post("/api/site/{domain}/refresh")
def api_refresh_site_metrics(request: Request, domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        search_console_connected = _is_search_console_connected(db, site.id)
        results = _refresh_site_detail_measurements(
            db,
            site,
            include_pagespeed=True,
            include_crawler=True,
            include_search_console=search_console_connected,
            force=True,
        )
        try:
            results["crux_history"] = collect_crux_history(db, site)
        except Exception as exc:  # noqa: BLE001
            results["crux_history"] = {"state": "failed", "error": str(exc)}
        if search_console_connected:
            try:
                results["url_inspection"] = collect_url_inspection(db, site)
            except Exception as exc:  # noqa: BLE001
                results["url_inspection"] = {"state": "failed", "error": str(exc)}
        else:
            results["url_inspection"] = {
                "state": "skipped",
                "reason": "URL Inspection için Search Console property yetkisi gerekiyor.",
            }
        db.commit()
        notify_result_map(
            trigger_source="manual",
            site=site,
            results=results,
            action_label="Site metriklerini manuel yenile",
        )
        if isinstance(results.get("crawler"), dict):
            notify_crawler_audit_emails(
                db=db,
                site=site,
                result=results.get("crawler"),
                trigger_source="manual",
            )
        return JSONResponse(
            {
                "site": site.domain,
                "refreshed": True,
                "summary": _summarize_manual_measurement(results),
                "results": results,
            }
        )


@app.get("/alerts")
def alerts_page(request: Request):
    # Son alarm kayıtlarını listeler.
    with SessionLocal() as db:
        payload = {
            "site_name": "Uyarılar",
            "sites": get_sidebar_sites(),
            "recent_alerts": get_recent_alerts(db, limit=100),
            "selected_alert_id": request.query_params.get("selected_alert", "").strip(),
        }
    template_name = "partials/alerts_content.html" if request.headers.get("HX-Request") == "true" else "alerts.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.post("/alerts/refresh")
def alerts_refresh(request: Request):
    summaries: list[dict[str, object]] = []
    with SessionLocal() as db:
        sites = _active_sites(db)
        for index, site in enumerate(sites):
            try:
                results = {
                    "search_console": collect_search_console_alert_metrics(
                        db,
                        site,
                        send_notifications=False,
                    )
                }
                db.commit()
                notify_system_trigger(
                    trigger_source="manual",
                    system_key="search_console_alerts",
                    site=site,
                    result=results["search_console"],
                    action_label="Uyarıları yenile",
                )
                summaries.append({"site": site.domain, "results": results})
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                notify_system_trigger(
                    trigger_source="manual",
                    system_key="search_console_alerts",
                    site=site,
                    result={"state": "failed", "error": str(exc)},
                    action_label="Uyarıları yenile",
                )
                summaries.append({"site": site.domain, "error": str(exc)})

            if index < len(sites) - 1:
                time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))

        return JSONResponse(
            {
                "refreshed": True,
                "sites": summaries,
                "recent_alerts": get_recent_alerts(db, limit=100),
            }
        )


@app.get("/settings")
def settings_page(request: Request):
    # Settings ekranı site yönetimi arayüzünü gösterir.
    with SessionLocal() as db:
        payload = {
            "site_name": "Ayarlar",
            "sites": get_sidebar_sites(),
            "alert_rules": get_alert_rules(db),
            "quota_status": get_quota_status(db),
            "oauth_ready": oauth_is_configured(),
            "oauth_redirect_uri": settings.google_oauth_redirect_uri,
        }
    return templates.TemplateResponse(request, "settings.html", context={"request": request, **payload})


@app.get("/settings/site-list")
def settings_site_list(request: Request):
    # HTMX istekleri için sadece site listesini döner.
    with SessionLocal() as db:
        sites = _settings_sites_payload(db)
        return templates.TemplateResponse(
            request,
            "partials/site_list.html",
            context={"request": request, "sites": sites, "oauth_ready": oauth_is_configured()},
        )


@app.get("/settings/alert-thresholds")
def settings_alert_thresholds(request: Request):
    # HTMX ile alert threshold tablosunu yeniler.
    with SessionLocal() as db:
        alert_rules = get_alert_rules(db)
    return templates.TemplateResponse(request, "partials/alert_thresholds.html", context={"request": request, "alert_rules": alert_rules})


@app.get("/search-console")
def search_console_page(request: Request):
    with SessionLocal() as db:
        search_console_schedule_label = (
            f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
            f"{int(settings.search_console_scheduled_refresh_minute):02d}"
        )
        payload = {
            "site_name": "Search Console",
            "sites": get_sidebar_sites(),
            "oauth_ready": oauth_is_configured(),
            "oauth_redirect_uri": settings.google_oauth_redirect_uri,
            "search_console_sites": _search_console_sites_payload(db),
            "search_console_schedule_label": search_console_schedule_label,
        }
    return templates.TemplateResponse(request, "search_console.html", context={"request": request, **payload})


@app.get("/search-console/site-list")
def search_console_site_list(request: Request):
    with SessionLocal() as db:
        return templates.TemplateResponse(
            request,
            "partials/search_console_site_cards.html",
            context={
                "request": request,
                "search_console_sites": _search_console_sites_payload(db),
                "oauth_ready": oauth_is_configured(),
            },
        )


@app.post("/search-console/refresh-all")
def search_console_refresh_all(request: Request):
    with SessionLocal() as db:
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
        for index, site in enumerate(sites):
            connection = get_search_console_connection_status(db, site.id)
            if not connection.get("connected"):
                continue
            try:
                results = _refresh_site_detail_measurements(
                    db,
                    site,
                    include_pagespeed=False,
                    include_crawler=False,
                    include_search_console=True,
                    force=True,
                )
                db.commit()
                notify_result_map(
                    trigger_source="manual",
                    site=site,
                    results=results,
                    action_label="Tüm Search Console sitelerini yenile",
                )
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                notify_system_trigger(
                    trigger_source="manual",
                    system_key="search_console",
                    site=site,
                    result={"state": "failed", "error": str(exc)},
                    action_label="Tüm Search Console sitelerini yenile",
                )
        return templates.TemplateResponse(
            request,
            "partials/search_console_site_cards.html",
            context={
                "request": request,
                "search_console_sites": _search_console_sites_payload(db),
                "oauth_ready": oauth_is_configured(),
            },
        )


@app.post("/search-console/refresh/{site_id}")
def search_console_manual_refresh(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        results = _refresh_site_detail_measurements(
            db,
            site,
            include_pagespeed=False,
            include_crawler=False,
            include_search_console=True,
            force=True,
        )
        db.commit()
        notify_result_map(
            trigger_source="manual",
            site=site,
            results=results,
            action_label="Search Console verisini yenile",
        )
        return templates.TemplateResponse(
            request,
            "partials/search_console_site_cards.html",
            context={
                "request": request,
                "search_console_sites": _search_console_sites_payload(db),
                "oauth_ready": oauth_is_configured(),
            },
        )


@app.post("/search-console/disconnect/{site_id}")
def search_console_disconnect_from_header(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)
        delete_oauth_credentials(db, site_id)
        return templates.TemplateResponse(
            request,
            "partials/search_console_site_cards.html",
            context={
                "request": request,
                "search_console_sites": _search_console_sites_payload(db),
                "oauth_ready": oauth_is_configured(),
            },
        )


@app.get("/api/search-console/oauth/start/{site_id}")
def search_console_oauth_start(site_id: int, next: str = "/settings"):
    if not oauth_is_configured():
        return HTMLResponse("Google OAuth ayarlari eksik. GOOGLE_CLIENT_ID ve GOOGLE_CLIENT_SECRET gerekli.", status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

    state = encode_oauth_state(site_id, return_path=next)
    flow = build_oauth_flow(state=state)
    authorization_url, _ = flow.authorization_url(access_type="offline", prompt="consent", include_granted_scopes="true")
    return RedirectResponse(authorization_url, status_code=302)


@app.get("/api/search-console/oauth/callback")
def search_console_oauth_callback(request: Request):
    error = request.query_params.get("error")
    if error:
        return HTMLResponse(f"Google OAuth reddedildi: {error}", status_code=400)

    state = request.query_params.get("state")
    if not state:
        return HTMLResponse("OAuth state eksik.", status_code=400)

    try:
        payload = decode_oauth_state(state)
    except ValueError as exc:
        return HTMLResponse(str(exc), status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == int(payload["site_id"])).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

        flow = build_oauth_flow(state=state)
        flow.fetch_token(authorization_response=str(request.url))
        save_oauth_credentials(db, site.id, flow.credentials)
    return RedirectResponse(str(payload.get("return_path") or "/settings"), status_code=302)


@app.post("/api/search-console/oauth/disconnect/{site_id}")
def search_console_oauth_disconnect(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)
        delete_oauth_credentials(db, site_id)
        sites = _settings_sites_payload(db)
    return templates.TemplateResponse(
        "partials/site_list.html",
        context={"request": request, "sites": sites, "oauth_ready": oauth_is_configured()},
    )


@app.get("/health")
def health_check():
    # Basit sağlık kontrol endpoint'i JSON döner.
    return JSONResponse({"status": "ok", "host": settings.app_host})
