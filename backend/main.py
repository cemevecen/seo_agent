"""FastAPI uygulama giriş noktası."""
import json
import logging
import os
import re
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
from ipaddress import ip_address, ip_network
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote, urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Localhost development için insecure OAuth transport'u allow et
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from fastapi import BackgroundTasks, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import Environment, FileSystemLoader
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.exc import OperationalError
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler

from backend.api.alerts import router as alerts_router
from backend.api.ga4 import router as ga4_router
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
from backend.database import SessionLocal, _IS_SQLITE, init_db
from backend.models import (
    Alert, AlertLog, CollectorRun, CruxHistorySnapshot, ExternalOnboardingJob,
    ExternalSite, Ga4ReportSnapshot, LighthouseAuditRecord, Metric,
    NotificationDeliveryLog, PageSpeedAuditSnapshot, PageSpeedPayloadSnapshot,
    SearchConsoleQuerySnapshot, Site, UrlAuditRecord, UrlInspectionSnapshot,
)
from backend.rate_limiter import limiter
from backend.services.alert_engine import ensure_site_alerts, get_alert_rules, get_recent_alerts, get_site_alerts
from backend.services.metric_store import get_latest_metrics, get_metric_history, get_metric_day_over_day_score
from backend.services.quota_guard import get_quota_status
from backend.services.search_console_auth import build_oauth_flow, decode_oauth_state, delete_oauth_credentials, encode_oauth_state, get_search_console_connection_status, oauth_is_configured, save_oauth_credentials
from backend.services.ga4_auth import ga4_is_configured, get_ga4_connection_status
from backend.services.pagespeed_analyzer import analyze_pagespeed_alerts
from backend.services.pagespeed_detailed import analyze_pagespeed_detailed
from backend.services.lighthouse_analyzer import get_lighthouse_analysis
from backend.services.ga4_digest_email import ga4_digest_bucket_for_domain, send_ga4_weekly_digest_emails
from backend.services.ga4_page_urls import (
    enrich_ga4_page_rows as _enrich_ga4_page_rows,
    ga4_fallback_page_url as _ga4_fallback_page_url,
    ga4_row_page_href as _ga4_row_page_href,
    ga4_row_page_label as _ga4_row_page_label,
    ga4_site_host as _ga4_site_host,
)
from backend.services.operations_notifier import (
    notify_crawler_audit_emails,
    notify_crawler_audit_emails_batch,
    notify_missed_scheduled_refreshes,
    notify_result_map,
    notify_system_trigger,
    send_consolidated_system_email,
)
from backend.services.timezone_utils import format_datetime_like, format_local_datetime
from backend.services.warehouse import (
    get_latest_crux_snapshot,
    get_latest_ga4_report_snapshot,
    get_latest_search_console_rows,
    get_latest_search_console_rows_batch,
    get_site_warehouse_summary,
)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
GSC_SCREENSHOT_DIR = STATIC_DIR / "gsc"
LOGGER = logging.getLogger(__name__)
DAILY_REFRESH_LOCK = threading.Lock()
SCHEDULER: BackgroundScheduler | None = None
EXTERNAL_ONBOARDING_JOB_TTL_SECONDS = 1800
EXTERNAL_ONBOARDING_MAX_RUNNING_SECONDS = 180

#
# Not: GSC CWV ekran görüntüsü otomasyonu (Playwright) kaldırıldı.
# Sistem yalnızca manuel upload ile statik görsel gösterir.
#

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


def _format_signed_max_two_decimals(value) -> str:
    """Δ % gibi yüzde farkları: en fazla 2 ondalık, yuvarlanmış, işaretli (+/-)."""
    if value is None or value == "":
        return "N/A"
    if isinstance(value, str):
        return value
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return str(value)
    clipped = decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    normalized = format(clipped.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in {"", "-0"}:
        normalized = "0"
    if clipped > 0:
        return f"+{normalized}"
    if clipped < 0:
        return normalized
    return "0"


def _ms_to_exact_seconds(value) -> str:
    if value is None:
        return "N/A"
    try:
        seconds = Decimal(str(value)) / Decimal("1000")
    except (InvalidOperation, ValueError, TypeError):
        return str(value)
    return _format_exact(seconds)


def _format_tr_int(value) -> str:
    """3.729.980 gibi TR binlik ayıracı ile formatlar."""
    if value is None or value == "":
        return "0"
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return "0"
        try:
            value = float(value.replace(".", "").replace(",", "."))
        except ValueError:
            return value
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        return str(value)
    # Python: 1,234,567 -> TR: 1.234.567
    return f"{number:,}".replace(",", ".")


def _filter_ga4_abs_page_url(path, domain) -> str:
    """Jinja: path + site domain (GA4 host yoksa)."""
    return _ga4_fallback_page_url(path, str(domain) if domain is not None else None)


def _filter_ga4_site_root(domain) -> str:
    """Jinja: {{ site.domain | ga4_site_root }}"""
    d = _ga4_site_host(str(domain) if domain is not None else None)
    return f"https://{d}/" if d else ""


def _filter_ga4_source_href(sm) -> str:
    """Jinja: {{ row.source_medium | ga4_source_href }} — referral host veya tam URL."""
    s = (sm or "").strip() if sm is not None else ""
    if not s:
        return ""
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return s.split()[0]
    if " / " in s:
        left = s.split(" / ")[0].strip()
        if re.fullmatch(r"[\w.-]+\.[a-zA-Z]{2,}", left):
            return f"https://{left}"
    return ""


def _filter_ga4_iso_ddmmyy(value) -> str:
    """ISO tarih (YYYY-MM-DD) → dd.mm.yy; GA4 aralık etiketleri için."""
    if value is None or value == "":
        return "—"
    s = str(value).strip()
    try:
        d = date.fromisoformat(s[:10])
        return d.strftime("%d.%m.%y")
    except (ValueError, TypeError, OSError):
        return s[:10] if s else "—"


def _filter_ga4_iso_ddmmyyyy(value) -> str:
    """ISO tarih (YYYY-MM-DD) → dd.mm.yyyy (tam tarih)."""
    if value is None or value == "":
        return "—"
    s = str(value).strip()
    try:
        d = date.fromisoformat(s[:10])
        return d.strftime("%d.%m.%Y")
    except (ValueError, TypeError, OSError):
        return s[:10] if s else "—"


jinja_env.filters["exact"] = _format_exact
jinja_env.filters["max_two_decimals"] = _format_max_two_decimals
jinja_env.filters["exact_signed"] = _format_exact_signed
jinja_env.filters["signed_max_two_decimals"] = _format_signed_max_two_decimals
jinja_env.filters["seconds_exact"] = _ms_to_exact_seconds
jinja_env.filters["tr_int"] = _format_tr_int
jinja_env.filters["ga4_abs_page_url"] = _filter_ga4_abs_page_url
jinja_env.filters["ga4_site_root"] = _filter_ga4_site_root
jinja_env.filters["ga4_source_href"] = _filter_ga4_source_href
jinja_env.filters["ga4_iso_ddmmyy"] = _filter_ga4_iso_ddmmyy
jinja_env.filters["ga4_iso_ddmmyyyy"] = _filter_ga4_iso_ddmmyyyy
jinja_env.filters["ga4_row_page_href"] = _ga4_row_page_href
jinja_env.filters["ga4_row_page_label"] = _ga4_row_page_label


def _ai_brief_sites_filter(value: str | None) -> dict:
    from backend.services.ai_daily_brief import parse_stored_brief_section_for_ui

    return parse_stored_brief_section_for_ui(value)


def _ai_brief_normalize_breaks(raw: str) -> str:
    """LLM bazen <br> yazar; kaçışlı metin olarak görünmesin diye satır sonuna çevir."""
    import re

    s = str(raw)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)&lt;br\s*/?&gt;", "\n", s)
    return s


def _ai_brief_nl2br(text: str):
    """Kaçış + satır içi güvenli <br />."""
    from markupsafe import Markup, escape

    parts = escape(text).splitlines()
    if not parts:
        return Markup("")
    return Markup("<br />\n").join(parts)


def _ai_brief_stacked_p(body: str, *, p_class: str):
    """\\n\\n ile ayrılmış paragrafları <p> olarak döndür; tek paragraf içi \\n → <br />."""
    from markupsafe import Markup

    chunks = [c.strip() for c in body.split("\n\n") if c.strip()]
    if not chunks:
        return Markup("")
    out: list[str] = []
    for chunk in chunks:
        inner = _ai_brief_nl2br(chunk)
        out.append(f'<p class="{p_class}">{inner}</p>')
    return Markup("".join(out))


def _ai_brief_html_paragraphs(value: str | None):
    """AI özet metnini çift satır sonundan paragraflara böler; light/dark uyumlu kutular."""
    from markupsafe import Markup, escape

    if not value or not str(value).strip():
        return Markup("")
    normalized = _ai_brief_normalize_breaks(str(value))
    parts = [p.strip() for p in normalized.split("\n\n") if p.strip()]
    blocks: list[str] = []
    label_cls = (
        "mb-1.5 text-[11px] font-bold uppercase tracking-wide text-sky-800 dark:text-sky-200"
    )
    body_cls = "text-[13px] leading-relaxed text-slate-800 dark:text-slate-100"
    stacked_p_cls = (
        "mb-2.5 last:mb-0 text-[13px] leading-relaxed text-slate-800 dark:text-slate-100"
    )
    plain_cls = f"mb-3 {body_cls}"
    # Yarı saydam değil: dark modda metin/beyaz karışımı ve düşük kontrast önlenir.
    card_durum = (
        "mb-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2.5 "
        "dark:border-slate-600 dark:bg-slate-800"
    )
    card_neutral = (
        "mb-3 rounded-lg border border-slate-200 bg-white px-3 py-2.5 shadow-sm shadow-slate-200/40 "
        "dark:border-slate-600 dark:bg-slate-900 dark:shadow-none"
    )
    for p in parts:
        head = p.lstrip()
        up = head.upper()
        with_br = _ai_brief_nl2br(p)
        if up.startswith("DURUM:") or up.startswith("RAKAMLAR:"):
            first_ln, _, rest = p.partition("\n")
            if rest.strip():
                lbl = escape(first_ln.strip())
                if up.startswith("RAKAMLAR:"):
                    body = _ai_brief_nl2br(rest.strip())
                    blocks.append(
                        f'<div class="{card_durum}">'
                        f'<p class="{label_cls}">{lbl}</p><div class="{body_cls}">{body}</div></div>'
                    )
                else:
                    body = _ai_brief_stacked_p(rest.strip(), p_class=stacked_p_cls)
                    blocks.append(
                        f'<div class="{card_durum}">'
                        f'<p class="{label_cls}">{lbl}</p><div class="{body_cls} space-y-0">{body}</div></div>'
                    )
            else:
                blocks.append(f'<div class="{plain_cls}">{with_br}</div>')
        elif up.startswith("NE ANLAMA GELİYOR:") or up.startswith("ÖNCELİK:"):
            first_ln, _, rest = p.partition("\n")
            if rest.strip():
                lbl = escape(first_ln.strip())
                body = _ai_brief_stacked_p(rest.strip(), p_class=stacked_p_cls)
                blocks.append(
                    f'<div class="{card_neutral}">'
                    f'<p class="{label_cls}">{lbl}</p><div class="{body_cls} space-y-0">{body}</div></div>'
                )
            else:
                blocks.append(f'<div class="{plain_cls}">{with_br}</div>')
        else:
            blocks.append(f'<div class="{plain_cls}">{with_br}</div>')
    return Markup("".join(blocks))


jinja_env.filters["ai_brief_sites"] = _ai_brief_sites_filter
jinja_env.filters["ai_brief_html_paragraphs"] = _ai_brief_html_paragraphs
templates = Jinja2Templates(env=jinja_env)
app = FastAPI(title="SEO Agent Dashboard")


@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico() -> RedirectResponse:
    # Tarayıcılar çoğunlukla kökte /favicon.ico ister; repoda yalnızca static/favicon.svg var.
    return RedirectResponse(url="/static/favicon.svg", status_code=307)


@app.get("/apple-touch-icon.png", include_in_schema=False)
def apple_touch_icon() -> RedirectResponse:
    return RedirectResponse(url="/static/favicon.svg", status_code=307)


# Static mount dosya sonunda (Starlette: Mount genelde en sonda; aksi halde bazı rotalar 404 dönebilir).
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# API routers
app.include_router(alerts_router, prefix="/api")
app.include_router(metrics_router, prefix="/api")
app.include_router(sites_router, prefix="/api")
app.include_router(ga4_router, prefix="/api")

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


def _dashboard_sc_scopes_for_url_period(period: str) -> tuple[str, str]:
    """Dashboard `period` (daily|weekly|monthly) → Search Console snapshot scope çifti."""
    if period == "daily":
        return "current_day", "previous_week_same_weekday"
    if period == "weekly":
        return "current_7d", "previous_7d"
    return "current_30d", "previous_30d"


def _dashboard_period_to_sc_segment(period: str) -> str:
    return {"daily": "1", "weekly": "7", "monthly": "30"}.get(period, "30")


def _dashboard_ga4_period_caption(period: str) -> str:
    if period == "daily":
        return "1 günlük · haftanın aynı günü"
    if period == "weekly":
        return "7 günlük · önceki dönem"
    return "30 günlük · önceki dönem"


def _dashboard_pagespeed_compare_blurb(dash_period: str) -> str:
    if dash_period == "daily":
        return "Önceki güne göre"
    if dash_period == "weekly":
        return "Son 7 gündeki ölçümlere göre"
    return "Son 30 gündeki ölçümlere göre"


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


def _format_status_chip_date(value: datetime | None) -> str:
    """Durum chip'i için takvim tarihi (örn. 06.04.2026)."""
    return format_local_datetime(value, fmt="%d.%m.%Y", fallback="—", include_suffix=False)


def _search_console_latest_snapshot_collected_at(db, site_id: int) -> datetime | None:
    from sqlalchemy import func

    ts = (
        db.query(func.max(SearchConsoleQuerySnapshot.collected_at))
        .filter(SearchConsoleQuerySnapshot.site_id == site_id)
        .scalar()
    )
    return ts if isinstance(ts, datetime) else None


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

# (legacy) Router include blokları yukarı taşındı.


@app.on_event("startup")
def on_startup() -> None:
    # Uygulama açılışında tablolar create_all ile hazırlanır.
    global SCHEDULER
    init_db()
    if SCHEDULER is None:
        SCHEDULER = _build_daily_refresh_scheduler()
        if SCHEDULER is not None:
            SCHEDULER.start()
            ga4_sched = (
                f", GA4={int(settings.ga4_scheduled_refresh_hour):02d}:{int(settings.ga4_scheduled_refresh_minute):02d}"
                if settings.ga4_scheduled_refresh_enabled
                else ""
            )
            LOGGER.info(
                "Scheduled jobs started. Search Console=%02d:%02d%s, full refresh=%02d:%02d %s.",
                int(settings.search_console_scheduled_refresh_hour),
                int(settings.search_console_scheduled_refresh_minute),
                ga4_sched,
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


def _dashboard_spotlight_card_limit(domain: str | None) -> int:
    """Öne çıkan sorgu kartı sayısı: döviz daha kompakt; sinemalar vb. tam liste."""
    d = str(domain or "").strip().lower()
    if d in ("doviz.com", "www.doviz.com"):
        return 20
    return 24


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


def _external_site_domains(db) -> set[str]:
    rows = (
        db.query(Site.domain)
        .join(ExternalSite, ExternalSite.site_id == Site.id)
        .all()
    )
    return {str(row[0] or "").lower() for row in rows if row and row[0]}


def _exclude_external_alerts(db, alerts: list[dict]) -> list[dict]:
    external_site_ids = _external_site_ids(db)
    if external_site_ids:
        alert_ids = [int(alert.get("alert_id")) for alert in alerts if alert.get("alert_id") is not None]
        if alert_ids:
            external_alert_ids = {
                int(row.id)
                for row in db.query(Alert.id)
                .filter(Alert.id.in_(alert_ids), Alert.site_id.in_(external_site_ids))
                .all()
            }
            if external_alert_ids:
                alerts = [
                    alert
                    for alert in alerts
                    if int(alert.get("alert_id") or -1) not in external_alert_ids
                ]

    external_domains = _external_site_domains(db)
    if not external_domains:
        return alerts
    return [
        alert
        for alert in alerts
        if str(alert.get("domain") or "").lower() not in external_domains
    ]


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
                "ga4": get_ga4_connection_status(db, site.id),
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

    # current verisi varsa: normal karşılaştırma
    if current_map:
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
    elif previous_map:
        # current boşsa previous verisini göster (henüz veri toplanmamış dönemler için)
        for query, prev in sorted(previous_map.items(), key=lambda item: item[1]["clicks"], reverse=True)[:limit]:
            prev_position = float(prev.get("position", 0.0))
            items.append(
                {
                    "query": query,
                    "clicks_current": 0.0,
                    "clicks_previous": float(prev.get("clicks", 0.0)),
                    "clicks_diff": -float(prev.get("clicks", 0.0)),
                    "position_current": 0.0,
                    "position_previous": prev_position,
                    "position_diff": -prev_position,
                }
            )
    return items


def _sanitize_search_console_trend(trend: dict) -> dict:
    sanitized = dict(trend or {})
    if str(sanitized.get("mode") or "") == "last_28d":
        clicks = list(sanitized.get("clicks") or [])
        impressions = list(sanitized.get("impressions") or [])
        ctr = list(sanitized.get("ctr") or [])
        positions = list(sanitized.get("position") or [])
        for index in range(min(len(clicks), len(positions))):
            if float(clicks[index] or 0.0) == 0.0 and float(positions[index] or 0.0) == 0.0:
                clicks[index] = None
                positions[index] = None
                if index < len(impressions):
                    impressions[index] = None
                if index < len(ctr):
                    ctr[index] = None
        sanitized["clicks"] = clicks
        sanitized["impressions"] = impressions
        sanitized["ctr"] = ctr
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


def _format_sc_tr_date(iso_date: str) -> str:
    raw = str(iso_date or "").strip()
    if not raw:
        return ""
    try:
        d = datetime.fromisoformat(raw[:10]).date()
        return f"{d.day:02d}.{d.month:02d}.{d.year}"
    except (ValueError, TypeError, OSError):
        return raw


def _format_sc_tr_date_range(start_iso: str, end_iso: str) -> str:
    a = _format_sc_tr_date(start_iso)
    b = _format_sc_tr_date(end_iso)
    if not a and not b:
        return ""
    if a == b:
        return a
    return f"{a} – {b}"


def _scope_range_from_rows(rows: list[dict]) -> tuple[str, str]:
    if not rows:
        return ("", "")
    first = rows[0]
    return (str(first.get("start_date") or ""), str(first.get("end_date") or ""))


def _slice_search_console_trend_last_days(trend: dict, last_n: int) -> dict:
    t = dict(trend or {})
    if str(t.get("mode") or "") != "last_28d":
        return t
    dates = list(t.get("dates") or [])
    labels = list(t.get("labels") or [])
    clicks = list(t.get("clicks") or [])
    impressions = list(t.get("impressions") or [])
    ctr = list(t.get("ctr") or [])
    position = list(t.get("position") or [])
    L = min(len(dates), len(clicks), len(position))
    if L == 0:
        return t
    take = max(0, min(int(last_n), L))
    if take == 0:
        return t
    return {
        **t,
        "mode": "last_28d",
        "dates": dates[-take:],
        "labels": labels[-take:] if len(labels) >= take else labels,
        "clicks": clicks[-take:],
        "impressions": impressions[-take:] if len(impressions) >= take else impressions,
        "ctr": ctr[-take:] if len(ctr) >= take else ctr,
        "position": position[-take:],
    }


def _search_console_report_payload(db, *, site_id: int) -> dict:
    _sc_batch = get_latest_search_console_rows_batch(
        db,
        site_id=site_id,
        scopes=["current_7d", "previous_7d", "current_30d", "previous_30d", "current_day", "previous_week_same_weekday"],
    )
    current_rows_7 = _sc_batch.get("current_7d", [])
    previous_rows_7 = _sc_batch.get("previous_7d", [])
    current_rows_30 = _sc_batch.get("current_30d", [])
    previous_rows_30 = _sc_batch.get("previous_30d", [])
    current_rows_1 = _sc_batch.get("current_day", [])
    previous_rows_wow = _sc_batch.get("previous_week_same_weekday", [])
    summary_payload = _latest_successful_provider_summary(
        db,
        site_id=site_id,
        provider="search_console",
        strategy="all",
    )
    current_summary = summary_payload.get("current_7d_summary") or _summarize_search_console_rows(current_rows_7)
    previous_summary = summary_payload.get("previous_7d_summary") or _summarize_search_console_rows(previous_rows_7)
    current_7d_by_device = summary_payload.get("current_7d_summary_by_device") or {}
    previous_7d_by_device = summary_payload.get("previous_7d_summary_by_device") or {}
    current_30d_by_device = summary_payload.get("current_30d_summary_by_device") or {}
    previous_30d_by_device = summary_payload.get("previous_30d_summary_by_device") or {}
    sw_day = summary_payload.get("same_weekday_day") if isinstance(summary_payload.get("same_weekday_day"), dict) else {}
    sw_by_device = sw_day.get("by_device") if isinstance(sw_day.get("by_device"), dict) else {}

    _raw_trend_summary = (
        summary_payload.get("trend_28d_summary")
        or summary_payload.get("trend_7d_summary")
        or {}
    )
    _raw_trend_by_device = (
        summary_payload.get("trend_28d_summary_by_device")
        or summary_payload.get("trend_7d_summary_by_device")
        or {}
    )
    # Eski veriye impressions/ctr yoksa trend_28d_rows'dan in-flight recompute et
    _stored_trend_rows = summary_payload.get("trend_28d_rows") or []
    if _stored_trend_rows and (not _raw_trend_summary.get("impressions") or not _raw_trend_summary.get("ctr")):
        from backend.collectors.search_console import _build_recent_trend_summary, _build_recent_trend_summary_by_device
        try:
            _dates = [r.get("date") for r in _stored_trend_rows if r.get("date")]
            if _dates:
                from datetime import date as _date_cls
                _start = _date_cls.fromisoformat(min(_dates))
                _end = _date_cls.fromisoformat(max(_dates))
                _raw_trend_summary = _build_recent_trend_summary(_stored_trend_rows, start_date=_start, end_date=_end)
                _raw_trend_by_device = _build_recent_trend_summary_by_device(_stored_trend_rows, start_date=_start, end_date=_end)
        except Exception:
            pass
    trend_summary = _sanitize_search_console_trend(_raw_trend_summary or {
        "mode": "last_28d",
        "labels": [],
        "dates": [],
        "clicks": [],
        "impressions": [],
        "ctr": [],
        "position": [],
    })
    top_queries = _build_search_console_top_queries(current_rows_7, previous_rows_7, limit=50)
    trend_summary_by_device = _raw_trend_by_device

    range_7_last = _scope_range_from_rows(current_rows_7)
    range_7_prev = _scope_range_from_rows(previous_rows_7)
    range_30_last = _scope_range_from_rows(current_rows_30)
    range_30_prev = _scope_range_from_rows(previous_rows_30)
    # 1g kartları: referans ve WoW tarihleri (özet JSON veya snapshot satırlarından)
    ref_d_global = str(sw_day.get("reference_date") or "").strip()
    if not ref_d_global:
        _s1, _e1 = _scope_range_from_rows(current_rows_1)
        ref_d_global = (_e1 or _s1 or range_7_last[1] or "").strip()
    prev_wow_d_global = str(sw_day.get("previous_week_date") or "").strip()
    if not prev_wow_d_global:
        _sws, _swe = _scope_range_from_rows(previous_rows_wow)
        prev_wow_d_global = (_swe or _sws or "").strip()
    if not prev_wow_d_global and ref_d_global:
        try:
            prev_wow_d_global = (date.fromisoformat(ref_d_global[:10]) - timedelta(days=7)).isoformat()
        except (ValueError, TypeError, OSError):
            prev_wow_d_global = ""

    periods: dict[str, dict] = {}
    for period_key, pd_days, cur_lbl, prev_lbl, trend_days in (
        # 1g: tablo etiketleri aşağıda range_last/range_prev (kesin tarih) ile doldurulur
        ("1", 1, "Son tam gün", "Geçen haftanın aynı günü", 7),
        ("7", 7, "Son 7 gün", "Önceki 7 gün", 7),
        ("30", 30, "Son 30 gün", "Önceki 30 gün", 30),
    ):
        views: dict[str, dict] = {}
        for device_key, device_label in (("mobile", "Mobile"), ("desktop", "Desktop")):
            device_code = device_key.upper()
            empty_trend = {
                "mode": "last_28d",
                "labels": [],
                "dates": [],
                "clicks": [],
                "position": [],
            }
            base_trend = _sanitize_search_console_trend(trend_summary_by_device.get(device_code) or empty_trend)

            if period_key == "1":
                fc = _filter_search_console_rows_by_device(current_rows_1, device_code)
                fp = _filter_search_console_rows_by_device(previous_rows_wow, device_code)
                sw_dev = sw_by_device.get(device_code) if isinstance(sw_by_device.get(device_code), dict) else {}
                sc = sw_dev.get("current_day_summary") if isinstance(sw_dev.get("current_day_summary"), dict) else None
                sp = (
                    sw_dev.get("previous_week_same_weekday_summary")
                    if isinstance(sw_dev.get("previous_week_same_weekday_summary"), dict)
                    else None
                )
                q_cur = _summarize_search_console_rows(fc)
                q_prev = _summarize_search_console_rows(fp)
                # Sorgu satırları varsa özetleri API/query toplamından al; günlük trend özeti sıfır dönebiliyor
                summary_current = q_cur if fc else (sc or q_cur)
                summary_previous = q_prev if fp else (sp or q_prev)
                device_top = _build_search_console_top_queries(fc, fp, limit=50)
                chart_trend = _slice_search_console_trend_last_days(base_trend, trend_days)
                range_last = (
                    _format_sc_tr_date(ref_d_global)
                    or _format_sc_tr_date_range(*_scope_range_from_rows(fc))
                )
                range_prev = (
                    _format_sc_tr_date(prev_wow_d_global)
                    or _format_sc_tr_date_range(*_scope_range_from_rows(fp))
                )
            elif period_key == "7":
                fc = _filter_search_console_rows_by_device(current_rows_7, device_code)
                fp = _filter_search_console_rows_by_device(previous_rows_7, device_code)
                summary_current = current_7d_by_device.get(device_code) or _summarize_search_console_rows(fc)
                summary_previous = previous_7d_by_device.get(device_code) or _summarize_search_console_rows(fp)
                device_top = _build_search_console_top_queries(fc, fp, limit=50)
                chart_trend = _slice_search_console_trend_last_days(base_trend, trend_days)
                range_last = _format_sc_tr_date_range(*range_7_last)
                range_prev = _format_sc_tr_date_range(*range_7_prev)
            else:
                fc = _filter_search_console_rows_by_device(current_rows_30, device_code)
                fp = _filter_search_console_rows_by_device(previous_rows_30, device_code)
                summary_current = current_30d_by_device.get(device_code) or _summarize_search_console_rows(fc)
                summary_previous = previous_30d_by_device.get(device_code) or _summarize_search_console_rows(fp)
                device_top = _build_search_console_top_queries(fc, fp, limit=50)
                chart_trend = _slice_search_console_trend_last_days(base_trend, trend_days)
                range_last = _format_sc_tr_date_range(*range_30_last)
                range_prev = _format_sc_tr_date_range(*range_30_prev)

            _cur = summary_current if isinstance(summary_current, dict) else {}
            _prev = summary_previous if isinstance(summary_previous, dict) else {}
            if period_key == "1":
                tbl_cur = (range_last or "").strip() or cur_lbl
                tbl_prev = (range_prev or "").strip() or prev_lbl
            else:
                tbl_cur = cur_lbl
                tbl_prev = prev_lbl
            views[device_key] = {
                "device_code": device_code,
                "device_label": device_label,
                "has_data": bool(fc or fp or device_top),
                "summary_current": summary_current,
                "summary_previous": summary_previous,
                "trend": chart_trend,
                "top_queries": device_top,
                "table_label_current": tbl_cur,
                "table_label_previous": tbl_prev,
                "range_last": range_last,
                "range_prev": range_prev,
                "clicks_pct_change": _ga4_period_pct_change(
                    float(_cur.get("clicks") or 0),
                    float(_prev.get("clicks") or 0),
                ),
                "impressions_pct_change": _ga4_period_pct_change(
                    float(_cur.get("impressions") or 0),
                    float(_prev.get("impressions") or 0),
                ),
                "ctr_pct_change": _ga4_period_pct_change(
                    float(_cur.get("ctr") or 0),
                    float(_prev.get("ctr") or 0),
                ),
                "position_pct_change": _ga4_period_pct_change(
                    float(_cur.get("position") or 0),
                    float(_prev.get("position") or 0),
                ),
            }

        mv = views.get("mobile") or {}
        _rl = (mv.get("range_last") or "").strip()
        _rp = (mv.get("range_prev") or "").strip()
        if period_key == "1" and _rl and _rp:
            _heading = f"{_rl} · {_rp}"
            _subtitle = f"Güncel: {_rl} · Karşılaştırma: {_rp}"
            _lc = _rl
            _lp = _rp
        else:
            _heading = f"{cur_lbl} ve {prev_lbl.lower()}"
            _subtitle = (
                f"Güncel dönem: {mv.get('range_last') or '—'} · "
                f"Karşılaştırma: {mv.get('range_prev') or '—'}"
            )
            _lc = cur_lbl
            _lp = prev_lbl
        periods[period_key] = {
            "period_days": pd_days,
            "heading": _heading,
            "subtitle": _subtitle,
            "label_current": _lc,
            "label_previous": _lp,
            "trend_caption": "Son 7 günün günlük trendi"
            if pd_days in (1, 7)
            else "Son 30 günün günlük trendi",
            "views": views,
        }

    legacy_views = periods["7"]["views"]

    # 1g tüm-cihaz özeti (kart deltaleri için)
    sw_cur = sw_day.get("current_day_summary") if isinstance(sw_day.get("current_day_summary"), dict) else {}
    sw_prev = sw_day.get("previous_week_same_weekday_summary") if isinstance(sw_day.get("previous_week_same_weekday_summary"), dict) else {}
    summary_1d_cur = _summarize_search_console_rows(current_rows_1) if current_rows_1 else sw_cur
    summary_1d_prev = _summarize_search_console_rows(previous_rows_wow) if previous_rows_wow else sw_prev
    summary_current_30d = summary_payload.get("current_30d_summary") or _summarize_search_console_rows(
        current_rows_30
    )
    summary_previous_30d = summary_payload.get("previous_30d_summary") or _summarize_search_console_rows(
        previous_rows_30
    )

    return {
        "has_data": bool(current_rows_7 or previous_rows_7 or top_queries),
        "summary_current": current_summary,
        "summary_previous": previous_summary,
        "trend": trend_summary,
        "top_queries": top_queries,
        "default_device": "mobile",
        "views": legacy_views,
        "periods": periods,
        "summary_current_1d": summary_1d_cur,
        "summary_previous_1d": summary_1d_prev,
        "summary_current_30d": summary_current_30d,
        "summary_previous_30d": summary_previous_30d,
        "range_current_1d": _format_sc_tr_date(ref_d_global) or "",
        "range_previous_1d": _format_sc_tr_date(prev_wow_d_global) or "",
        "range_current_7d": _format_sc_tr_date_range(*range_7_last),
        "range_previous_7d": _format_sc_tr_date_range(*range_7_prev),
        "range_current_30d": _format_sc_tr_date_range(*range_30_last),
        "range_previous_30d": _format_sc_tr_date_range(*range_30_prev),
    }


def _search_console_single_site_data(db, site, schedule_label: str) -> dict:
    """Tek bir site için tam Search Console kart verisi üretir."""
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
    # GSC CWV screenshot'ları (yerel script üretir): static/gsc/<domain>-cwv-*.png
    import re as _re
    from urllib.parse import quote as _quote

    raw_domain = str(site.domain or "").strip()
    domain_for_property = _re.sub(r"^https?://", "", raw_domain, flags=_re.I).strip().strip("/")
    domain_slug = _re.sub(r"^https?://", "", raw_domain.strip().lower())
    domain_slug = domain_slug.strip("/").replace("/", "-")
    domain_slug = _re.sub(r"[^a-z0-9._-]+", "-", domain_slug)
    domain_slug = _re.sub(r"-{2,}", "-", domain_slug).strip("-") or "site"

    mobile_path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-mobile.png"
    desktop_path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-desktop.png"
    full_path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-full.png"
    extra_path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-extra.png"

    resource_id = f"sc-domain:{domain_for_property}" if domain_for_property else ""
    resource_param = _quote(resource_id, safe="") if resource_id else ""
    def _static_url_if_exists(path: Path) -> str:
        if not path.exists():
            return ""
        try:
            v = int(path.stat().st_mtime)
        except OSError:
            v = int(time.time())
        return f"/static/gsc/{path.name}?v={v}"

    gsc_cwv = {
        "resource_url": (
            f"https://search.google.com/search-console/core-web-vitals?resource_id={resource_param}&hl=en"
            if resource_param
            else ""
        ),
        "mobile_url": _static_url_if_exists(mobile_path),
        "desktop_url": _static_url_if_exists(desktop_path),
        "full_url": _static_url_if_exists(full_path),
        "extra_url": _static_url_if_exists(extra_path),
    }

    return {
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
        "gsc_cwv": gsc_cwv,
    }


def _gsc_domain_slug(domain: str) -> str:
    d = str(domain or "").strip().lower()
    d = re.sub(r"^https?://", "", d, flags=re.I).strip()
    d = d.strip("/").replace("/", "-")
    d = re.sub(r"[^a-z0-9._-]+", "-", d)
    d = re.sub(r"-{2,}", "-", d).strip("-")
    return d or "site"


def _search_console_sites_payload(db) -> list[dict]:
    external_site_ids = _external_site_ids(db)
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    schedule_label = (
        f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
        f"{int(settings.search_console_scheduled_refresh_minute):02d}"
    )
    rows = [
        _search_console_single_site_data(db, site, schedule_label)
        for site in sites
        if site.id not in external_site_ids
    ]
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


def _collect_pagespeed_external_fast(db, site: Site) -> dict:
    return collect_pagespeed_metrics(
        db,
        site,
        request_timeout=12,
        max_retries=1,
        retry_backoff_seconds=1.0,
    )


def _collect_crux_external_fast(db, site: Site) -> dict:
    return collect_crux_history(
        db,
        site,
        request_timeout=3,
        max_identifier_attempts=1,
        form_factors=("mobile",),
        include_current=False,
    )


def _collect_crawler_external_fast(db, site: Site) -> dict:
    return collect_crawler_metrics(
        db,
        site,
        source_page_limit=3,
        target_url_limit=6,
        links_per_page_limit=3,
        issue_sample_limit=2,
        sitemap_url_limit=12,
        request_timeout_seconds=2,
    )


def _refresh_public_site_measurements(db, site: Site, *, force: bool = True) -> dict[str, dict]:
    # Search Console yetkisi gerektirmeyen collector akisi.
    results: dict[str, dict] = {}

    try:
        results["pagespeed"] = _collect_pagespeed_external_fast(db, site)
    except Exception as exc:  # noqa: BLE001
        results["pagespeed"] = {"errors": {"exception": str(exc)}}

    try:
        results["crawler"] = _collect_crawler_external_fast(db, site)
    except Exception as exc:  # noqa: BLE001
        results["crawler"] = {"errors": {"exception": str(exc)}}

    try:
        results["crux_history"] = _collect_crux_external_fast(db, site)
    except Exception as exc:  # noqa: BLE001
        results["crux_history"] = {"state": "failed", "error": str(exc)}

    results["url_inspection"] = {
        "state": "skipped",
        "reason": "URL Inspection için Search Console property yetkisi gerekiyor.",
    }
    return results


def _cleanup_external_onboarding_jobs(db) -> None:
    cutoff = datetime.utcnow() - timedelta(seconds=EXTERNAL_ONBOARDING_JOB_TTL_SECONDS)
    try:
        (
            db.query(ExternalOnboardingJob)
            .filter(ExternalOnboardingJob.updated_at < cutoff)
            .delete(synchronize_session=False)
        )
    except OperationalError as exc:
        db.rollback()
        if _is_sqlite_lock_error(exc):
            LOGGER.warning("External onboarding cleanup skipped due lock.")
            return
        raise


def _job_to_dict(job: ExternalOnboardingJob) -> dict:
    return {
        "job_id": job.job_id,
        "site_id": int(job.site_id),
        "domain": str(job.domain or ""),
        "status": str(job.status or "running"),
        "percent": int(job.percent or 0),
        "title": str(job.title or ""),
        "detail": str(job.detail or ""),
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }


def _create_external_onboarding_job(db, *, site_id: int, domain: str) -> tuple[str, bool]:
    now = datetime.utcnow()
    _cleanup_external_onboarding_jobs(db)

    stale_cutoff = now - timedelta(seconds=EXTERNAL_ONBOARDING_MAX_RUNNING_SECONDS)
    stale_running = (
        db.query(ExternalOnboardingJob)
        .filter(ExternalOnboardingJob.status == "running", ExternalOnboardingJob.updated_at < stale_cutoff)
        .all()
    )
    for row in stale_running:
        row.status = "failed"
        row.percent = 100
        row.title = "Onboarding zaman aşımına uğradı"
        row.detail = "İşlem beklenenden uzun sürdü. Yeniden deneyin veya logları kontrol edin."
        row.finished_at = now
        row.updated_at = now

    existing_running = (
        db.query(ExternalOnboardingJob)
        .filter(ExternalOnboardingJob.site_id == site_id, ExternalOnboardingJob.status == "running")
        .order_by(ExternalOnboardingJob.updated_at.desc(), ExternalOnboardingJob.id.desc())
        .first()
    )
    if existing_running:
        return str(existing_running.job_id), False

    job_id = uuid4().hex
    db.add(
        ExternalOnboardingJob(
            job_id=job_id,
            site_id=site_id,
            domain=domain,
            status="running",
            percent=3,
            title="Onboarding başlatıldı",
            detail="External ölçüm kuyruğa alındı.",
            created_at=now,
            updated_at=now,
            finished_at=None,
        )
    )
    db.commit()
    return job_id, True


def _find_running_external_onboarding_job_id(db, site_id: int) -> str | None:
    timeout_cutoff = datetime.utcnow() - timedelta(seconds=EXTERNAL_ONBOARDING_MAX_RUNNING_SECONDS)
    running = (
        db.query(ExternalOnboardingJob)
        .filter(
            ExternalOnboardingJob.site_id == site_id,
            ExternalOnboardingJob.status == "running",
            ExternalOnboardingJob.updated_at >= timeout_cutoff,
        )
        .order_by(ExternalOnboardingJob.updated_at.desc(), ExternalOnboardingJob.id.desc())
        .first()
    )
    return str(running.job_id) if running else None


def _is_sqlite_lock_error(exc: Exception) -> bool:
    return "database is locked" in str(exc).lower()


def _commit_with_lock_retry(db, *, attempts: int = 6, base_wait: float = 0.25) -> None:
    for attempt in range(1, attempts + 1):
        try:
            db.commit()
            return
        except OperationalError as exc:
            db.rollback()
            if not _is_sqlite_lock_error(exc) or attempt >= attempts:
                raise
            time.sleep(base_wait * attempt)


def _set_external_onboarding_job(job_id: str, **updates) -> None:
    with SessionLocal() as db:
        job = db.query(ExternalOnboardingJob).filter(ExternalOnboardingJob.job_id == job_id).first()
        if job is None:
            return
        for key, value in updates.items():
            if hasattr(job, key):
                setattr(job, key, value)
        job.updated_at = datetime.utcnow()
        for attempt in range(1, 6):
            try:
                db.commit()
                return
            except OperationalError as exc:
                db.rollback()
                if not _is_sqlite_lock_error(exc):
                    raise
                if attempt >= 5:
                    LOGGER.warning("External onboarding job update skipped due lock: job_id=%s", job_id)
                    return
                time.sleep(0.08 * attempt)


def _run_external_pagespeed_detached(site_id: int) -> None:
    # Onboarding akisina takilmamasi icin PageSpeed olcumunu ayrik thread'de calistirir.
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return
        try:
            collect_pagespeed_metrics(
                db,
                site,
                request_timeout=8,
                max_retries=0,
                retry_backoff_seconds=0,
            )
            _commit_with_lock_retry(db)
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            LOGGER.warning("Detached external PageSpeed run failed for site_id=%s: %s", site_id, exc)


def _run_external_crawler_detached(site_id: int) -> None:
    # Crawler denetimi nispeten agir oldugu icin onboarding tamamlanmasini bloklamaz.
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return
        try:
            _collect_crawler_external_fast(db, site)
            _commit_with_lock_retry(db)
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            LOGGER.warning("Detached external crawler run failed for site_id=%s: %s", site_id, exc)


def _get_external_onboarding_job(job_id: str) -> dict | None:
    with SessionLocal() as db:
        job = db.query(ExternalOnboardingJob).filter(ExternalOnboardingJob.job_id == job_id).first()
        if job is None:
            return None

        if job.status == "running":
            timeout_cutoff = datetime.utcnow() - timedelta(seconds=EXTERNAL_ONBOARDING_MAX_RUNNING_SECONDS)
            if job.updated_at and job.updated_at < timeout_cutoff:
                job.status = "failed"
                job.percent = 100
                job.title = "Onboarding zaman aşımına uğradı"
                job.detail = "İşlem beklenenden uzun sürdü. Yeniden deneyin veya logları kontrol edin."
                job.finished_at = datetime.utcnow()
                job.updated_at = datetime.utcnow()
                try:
                    db.commit()
                except OperationalError as exc:
                    db.rollback()
                    if _is_sqlite_lock_error(exc):
                        pass
                    else:
                        raise

        snapshot = _job_to_dict(job)
        return snapshot


def _run_external_onboarding_background(site_id: int, job_id: str) -> None:
    # External onboarding UI'ini bloklamamak için olcumleri arka planda calistirir.
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            _set_external_onboarding_job(
                job_id,
                status="failed",
                percent=100,
                title="Onboarding başarısız",
                detail="Site kaydı bulunamadı.",
                finished_at=datetime.utcnow(),
            )
            return

        results: dict[str, dict] = {}
        has_error = False
        warnings: list[str] = []
        try:
            _set_external_onboarding_job(
                job_id,
                percent=12,
                title="PageSpeed ölçümü çalışıyor",
                detail="Hızlı onboarding için PageSpeed ölçümü ayrı kuyruğa alındı.",
            )
            results["pagespeed"] = {
                "state": "queued",
                "message": "PageSpeed ölçümü arka planda devam ediyor.",
            }

            _set_external_onboarding_job(
                job_id,
                percent=48,
                title="CrUX geçmişi güncelleniyor",
                detail="Chrome UX Report verileri çekiliyor.",
            )
            try:
                results["crux_history"] = _collect_crux_external_fast(db, site)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"CrUX: {exc}")
                results["crux_history"] = {"state": "failed", "error": str(exc)}

            _set_external_onboarding_job(
                job_id,
                percent=76,
                title="Crawler analizi çalışıyor",
                detail="Kartların boş gelmemesi için ilk crawler denetimi yazılıyor.",
            )
            try:
                results["crawler"] = _collect_crawler_external_fast(db, site)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Crawler: {exc}")
                results["crawler"] = {"state": "failed", "error": str(exc)}

            results["url_inspection"] = {
                "state": "skipped",
                "reason": "URL Inspection için Search Console property yetkisi gerekiyor.",
            }

            try:
                db.commit()
            except OperationalError as exc:
                db.rollback()
                if _is_sqlite_lock_error(exc):
                    has_error = True
                    results["onboarding"] = {"state": "failed", "error": "database is locked"}
                else:
                    raise

            if has_error:
                _set_external_onboarding_job(
                    job_id,
                    status="failed",
                    percent=100,
                    title="Onboarding tamamlandı (kısmi hata)",
                    detail="Bazı adımlar hata verdi. Kartlardaki durum ve log detaylarını kontrol edin.",
                    finished_at=datetime.utcnow(),
                )
            else:
                _set_external_onboarding_job(
                    job_id,
                    status="completed",
                    percent=100,
                    title="Onboarding tamamlandı",
                    detail=(
                        "External ölçümler tamamlandı, kartlar güncellendi."
                        if not warnings
                        else "Onboarding tamamlandı. Bazı veri adımları arka planda yeniden denenecek."
                    ),
                    finished_at=datetime.utcnow(),
                )

            # Onboarding ekranini hizla sonlandirip agir adimlari ayrik calistir.
            threading.Thread(
                target=_run_external_pagespeed_detached,
                args=(site.id,),
                daemon=True,
            ).start()
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            LOGGER.warning("External onboarding background run failed for site_id=%s: %s", site_id, exc)
            _set_external_onboarding_job(
                job_id,
                status="failed",
                percent=100,
                title="Onboarding başarısız",
                detail=str(exc),
                finished_at=datetime.utcnow(),
            )


def _run_daily_search_console_refresh_job() -> None:
    if not DAILY_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Daily Search Console refresh skipped because another scheduled job is still in progress.")
        return

    try:
        LOGGER.info("Daily Search Console refresh started.")
        with SessionLocal() as db:
            external = _external_site_ids(db)
            connected_sites = [
                site
                for site in _active_sites(db)
                if site.id not in external and get_search_console_connection_status(db, site.id).get("connected")
            ]
            sc_batch: list[tuple[Site, dict]] = []

            for index, site in enumerate(connected_sites):
                LOGGER.info("Daily Search Console refresh processing site=%s", site.domain)
                try:
                    result = collect_search_console_metrics(db, site)
                    db.commit()
                    sc_batch.append((site, result))
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily Search Console refresh failed for %s: %s", site.domain, exc)
                    sc_batch.append((site, {"state": "failed", "error": str(exc)}))

                if index < len(connected_sites) - 1:
                    time.sleep(max(0, int(settings.search_console_scheduled_refresh_site_spacing_seconds)))

            if sc_batch:
                send_consolidated_system_email(
                    system_key="search_console",
                    trigger_source="system",
                    action_label="Günlük Search Console yenilemesi",
                    items=sc_batch,
                )

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
            external = _external_site_ids(db)
            sites = [s for s in _active_sites(db) if s.id not in external]
            alert_batch: list[tuple[Site, dict]] = []

            for index, site in enumerate(sites):
                LOGGER.info("Daily alert refresh processing site=%s", site.domain)
                try:
                    result = collect_search_console_alert_metrics(db, site, send_notifications=True)
                    db.commit()
                    alert_batch.append((site, result))
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily alert refresh failed for %s: %s", site.domain, exc)
                    alert_batch.append((site, {"state": "failed", "error": str(exc)}))

                if index < len(sites) - 1:
                    time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))

            if alert_batch:
                send_consolidated_system_email(
                    system_key="search_console_alerts",
                    trigger_source="system",
                    action_label="Günlük alert yenilemesi",
                    items=alert_batch,
                )

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
            external = _external_site_ids(db)
            sites = [s for s in _active_sites(db) if s.id not in external]
            pagespeed_batch: list[tuple[Site, dict]] = []
            crawler_batch: list[tuple[Site, dict]] = []
            crux_batch: list[tuple[Site, dict]] = []
            url_batch: list[tuple[Site, dict]] = []

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
                if isinstance(results.get("pagespeed"), dict):
                    pagespeed_batch.append((site, results["pagespeed"]))
                if isinstance(results.get("crawler"), dict):
                    crawler_batch.append((site, results["crawler"]))

                try:
                    crux_result = collect_crux_history(db, site)
                    db.commit()
                    crux_batch.append((site, crux_result))
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily refresh CrUX failed for %s: %s", site.domain, exc)
                    crux_batch.append((site, {"state": "failed", "error": str(exc)}))

                if connection.get("connected"):
                    try:
                        inspection_result = collect_url_inspection(db, site)
                        db.commit()
                        url_batch.append((site, inspection_result))
                    except Exception as exc:  # noqa: BLE001
                        db.rollback()
                        LOGGER.warning("Daily refresh URL Inspection failed for %s: %s", site.domain, exc)
                        url_batch.append((site, {"state": "failed", "error": str(exc)}))

                if index < len(sites) - 1:
                    time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))

            if pagespeed_batch:
                send_consolidated_system_email(
                    system_key="pagespeed",
                    trigger_source="system",
                    action_label="Günlük site yenilemesi (PageSpeed)",
                    items=pagespeed_batch,
                )
            if crawler_batch:
                notify_crawler_audit_emails_batch(db, crawler_batch, "system")
            if crux_batch:
                send_consolidated_system_email(
                    system_key="crux_history",
                    trigger_source="system",
                    action_label="Günlük CrUX yenilemesi",
                    items=crux_batch,
                )
            if url_batch:
                send_consolidated_system_email(
                    system_key="url_inspection",
                    trigger_source="system",
                    action_label="Günlük URL Inspection yenilemesi",
                    items=url_batch,
                )

        LOGGER.info("Daily refresh completed.")
    finally:
        DAILY_REFRESH_LOCK.release()


def _run_daily_ga4_refresh_job() -> None:
    if not ga4_is_configured():
        LOGGER.info("Daily GA4 refresh skipped: GA4 service account not configured.")
        return
    if not DAILY_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Daily GA4 refresh skipped because another scheduled job is still in progress.")
        return

    try:
        LOGGER.info("Daily GA4 refresh started.")
        from backend.collectors.ga4 import collect_ga4_channel_sessions

        with SessionLocal() as db:
            external_site_ids = _external_site_ids(db)
            sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
            eligible = [
                site
                for site in sites
                if site.id not in external_site_ids and get_ga4_connection_status(db, site.id).get("connected")
            ]
            any_ga4_ok = False
            ga4_failures: list[tuple[str, str]] = []
            for index, site in enumerate(eligible):
                LOGGER.info("Daily GA4 refresh processing site=%s", site.domain)
                try:
                    collect_ga4_channel_sessions(db, site, days=30)
                    collect_ga4_channel_sessions(db, site, days=7)
                    db.commit()
                    any_ga4_ok = True
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    LOGGER.warning("Daily GA4 refresh failed for %s: %s", site.domain, exc)
                    ga4_failures.append((site.domain, str(exc)))
                if index < len(eligible) - 1:
                    time.sleep(max(0, int(settings.ga4_scheduled_refresh_site_spacing_seconds)))

            if any_ga4_ok or ga4_failures:
                send_ga4_weekly_digest_emails(
                    db,
                    trigger_source="system",
                    action_label="Günlük GA4 yenilemesi",
                    collect_failures=ga4_failures,
                )

        LOGGER.info("Daily GA4 refresh completed.")
    finally:
        DAILY_REFRESH_LOCK.release()


def _run_scheduled_refresh_monitor_job() -> None:
    with SessionLocal() as db:
        sent_subjects = notify_missed_scheduled_refreshes(db)
    for subject in sent_subjects:
        LOGGER.warning("Scheduled refresh monitor sent operations email: %s", subject)


def _run_ai_daily_brief_scheduled() -> None:
    try:
        from backend.services.ai_daily_brief import run_ai_daily_brief_job

        run_ai_daily_brief_job()
    except Exception:  # noqa: BLE001
        LOGGER.exception("Scheduled AI daily brief job failed.")


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

    if settings.ga4_scheduled_refresh_enabled:
        scheduler.add_job(
            _run_daily_ga4_refresh_job,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.ga4_scheduled_refresh_hour))),
                minute=max(0, min(59, int(settings.ga4_scheduled_refresh_minute))),
                timezone=timezone,
            ),
            id="daily-ga4-refresh",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if settings.ai_daily_brief_enabled:
        try:
            ai_tz = ZoneInfo(settings.ai_daily_brief_timezone)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Invalid ai_daily_brief_timezone %s: %s", settings.ai_daily_brief_timezone, exc)
            ai_tz = ZoneInfo("Europe/Istanbul")
        scheduler.add_job(
            _run_ai_daily_brief_scheduled,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.ai_daily_brief_hour))),
                minute=max(0, min(59, int(settings.ai_daily_brief_minute))),
                timezone=ai_tz,
            ),
            id="daily-ai-brief",
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

    # Her gün gece 03:30'da eski verileri temizle (her zaman aktif)
    scheduler.add_job(
        _run_scheduled_db_cleanup_job,
        trigger=CronTrigger(hour=3, minute=30, timezone=timezone),
        id="daily-db-retention-cleanup",
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
        return "text-emerald-600 dark:text-emerald-400"
    if score >= 50:
        return "text-amber-500 dark:text-amber-400"
    return "text-rose-600 dark:text-rose-400"


def _metric_value(latest: dict[str, object], metric_type: str, default: float = 0.0) -> float:
    metric = latest.get(metric_type)
    if metric is None:
        return default
    return float(metric.value)


def _metric_is_stale(
    latest: dict[str, object], metric_type: str, max_age_minutes: int = 30
) -> bool:
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


def _crawler_link_audit_with_metric_fallback(crawler_link_audit: dict, latest: dict[str, object]) -> dict:
    merged = dict(crawler_link_audit or {})
    merged["source_pages"] = int(merged.get("source_pages") or _metric_value(latest, "crawler_source_pages_count", 0.0))
    merged["audited_urls"] = int(merged.get("audited_urls") or _metric_value(latest, "crawler_audited_urls_count", 0.0))
    merged["redirect_links"] = int(merged.get("redirect_links") or _metric_value(latest, "crawler_redirect_links_count", 0.0))
    merged["redirect_301_links"] = int(merged.get("redirect_301_links") or _metric_value(latest, "crawler_redirect_301_count", 0.0))
    merged["redirect_302_links"] = int(merged.get("redirect_302_links") or _metric_value(latest, "crawler_redirect_302_count", 0.0))
    merged["redirect_chains"] = int(merged.get("redirect_chains") or _metric_value(latest, "crawler_redirect_chain_count", 0.0))
    merged["broken_links"] = int(merged.get("broken_links") or _metric_value(latest, "crawler_broken_links_count", 0.0))
    merged["max_hops"] = int(merged.get("max_hops") or _metric_value(latest, "crawler_redirect_max_hops", 0.0))
    merged["source_strategy"] = str(merged.get("source_strategy") or "URL listesi")
    merged["source_pages_sample"] = list(merged.get("source_pages_sample") or [])
    merged["redirect_samples"] = list(merged.get("redirect_samples") or [])
    merged["broken_samples"] = list(merged.get("broken_samples") or [])
    merged["has_data"] = bool(
        merged.get("has_data")
        or merged["audited_urls"]
        or merged["source_pages"]
        or merged["redirect_links"]
        or merged["redirect_chains"]
        or merged["broken_links"]
    )
    return merged


_KEYWORD_STOPWORDS = {
    "www",
    "http",
    "https",
    "com",
    "net",
    "org",
    "html",
    "php",
    "asp",
    "aspx",
    "index",
    "page",
    "pages",
    "blog",
    "kategori",
    "category",
    "tag",
    "utm",
    "source",
    "medium",
    "campaign",
    "ref",
    "amp",
}


def _tokenize_keyword_text(text: str) -> list[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").lower())
    tokens: list[str] = []
    for token in normalized.split():
        if len(token) < 3:
            continue
        if token in _KEYWORD_STOPWORDS:
            continue
        if token.isdigit():
            continue
        tokens.append(token)
    return tokens


def _tokens_from_url(url: str) -> list[str]:
    if not url:
        return []
    try:
        parsed = urlparse(url)
    except Exception:
        return []

    parts: list[str] = []
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    host_parts = [item for item in host.split(".") if item]
    if len(host_parts) >= 2:
        host_parts = host_parts[:-1]
    parts.extend(host_parts)

    path = unquote(parsed.path or "")
    for segment in path.split("/"):
        if not segment:
            continue
        parts.extend(segment.replace("-", " ").replace("_", " ").split())

    for key, value in parse_qsl(parsed.query or "", keep_blank_values=False):
        if key:
            parts.extend(key.replace("-", " ").replace("_", " ").split())
        if value:
            parts.extend(unquote(value).replace("-", " ").replace("_", " ").split())

    tokens: list[str] = []
    for part in parts:
        tokens.extend(_tokenize_keyword_text(part))
    return tokens


def _build_external_keyword_insights(domain: str, crawler_link_audit: dict, *, limit: int = 20) -> dict:
    weighted_tokens: Counter[str] = Counter()
    keyword_sources: dict[str, set[str]] = defaultdict(set)

    def add_url_tokens(url: str, *, weight: int) -> None:
        if not url:
            return
        for token in _tokens_from_url(url):
            weighted_tokens[token] += weight
            keyword_sources[token].add(url)

    for source_url in list(crawler_link_audit.get("source_pages_sample") or []):
        add_url_tokens(str(source_url), weight=4)

    for sample in list(crawler_link_audit.get("redirect_samples") or []):
        add_url_tokens(str(sample.get("url") or ""), weight=3)
        add_url_tokens(str(sample.get("final_url") or ""), weight=2)
        for src in list(sample.get("source_urls") or []):
            add_url_tokens(str(src), weight=2)

    for sample in list(crawler_link_audit.get("broken_samples") or []):
        add_url_tokens(str(sample.get("url") or ""), weight=4)
        add_url_tokens(str(sample.get("final_url") or ""), weight=2)
        for src in list(sample.get("source_urls") or []):
            add_url_tokens(str(src), weight=2)

    if not weighted_tokens:
        domain_tokens = _tokenize_keyword_text((domain or "").replace(".", " "))
        top_keywords = [
            {
                "keyword": token,
                "score": 1,
                "source_count": 0,
                "sample_sources": [],
            }
            for token in domain_tokens[: max(3, min(limit, 6))]
        ]
        return {
            "has_data": bool(top_keywords),
            "total_keywords": len(top_keywords),
            "top_keywords": top_keywords,
            "source": "domain-fallback",
            "note": "Crawler URL örneklerinden yeterli token çıkmadığı için domain bazlı fallback kullanıldı.",
        }

    ranked: list[dict] = []
    for token, score in weighted_tokens.most_common(limit):
        sources = sorted(keyword_sources.get(token) or set())
        ranked.append(
            {
                "keyword": token,
                "score": int(score),
                "source_count": len(sources),
                "sample_sources": sources[:3],
            }
        )

    return {
        "has_data": bool(ranked),
        "total_keywords": len(ranked),
        "top_keywords": ranked,
        "source": "crawler-url-signals",
        "note": "Bu anahtar kelimeler Search Console sorgusu değil; crawler URL/path/query sinyallerinden türetilen query fallback içgörüsüdür.",
    }


def _pagespeed_strategy_status(
    latest: dict[str, object],
    strategy: str,
    alert_messages: list[str],
    *,
    max_age_minutes: int = 30,
) -> dict[str, object]:
    metric = latest.get(f"pagespeed_{strategy}_score")
    has_metric = metric is not None
    is_stale = (
        _metric_is_stale(latest, f"pagespeed_{strategy}_score", max_age_minutes=max_age_minutes)
        if has_metric
        else True
    )
    has_fetch_error = any(f"{strategy} PageSpeed" in message for message in alert_messages)

    if has_metric and not is_stale and not has_fetch_error:
        state = "live"
        label = "Live"
        badge_class = "border-emerald-200 bg-emerald-50 text-emerald-700"
        description = "Canlı ve güncel veri"
    elif has_metric:
        state = "stale"
        label = _format_status_chip_date(metric.collected_at if metric else None)
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
        snap_ts = clicks_metric.collected_at if clicks_metric else None
        if snap_ts is None and has_rows:
            snap_ts = _search_console_latest_snapshot_collected_at(db, site_id)
        label = _format_status_chip_date(snap_ts)
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


def _search_console_status_from_cache(
    latest: dict[str, object],
    connection: dict[str, object],
    has_rows_28d: bool,
) -> dict[str, object]:
    """Pre-fetched verilerle SC durumunu hesaplar — ek DB sorgusu yok."""
    clicks_metric = latest.get("search_console_clicks_28d")
    has_metric = clicks_metric is not None
    is_stale = _metric_is_stale(latest, "search_console_clicks_28d") if has_metric else True

    if connection.get("connected") and has_metric and not is_stale:
        state = "live"
        label = "Live"
        badge_class = "border-emerald-200 bg-emerald-50 text-emerald-700"
        description = "Search Console canli veri"
    elif connection.get("connected") and (has_metric or has_rows_28d):
        state = "stale"
        label = _format_status_chip_date(clicks_metric.collected_at if clicks_metric else None)
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
        "connection_label": connection.get("label", "Bağlantı yok"),
        "has_rows": has_rows_28d,
    }


def _get_latest_provider_runs_batch(
    db,
    site_ids: list[int],
    provider: str,
    strategy: str | None = None,
) -> "dict[int, CollectorRun | None]":
    """Multiple sites için latest provider run — tek GROUP BY sorgusu."""
    from sqlalchemy import func, and_

    if not site_ids:
        return {}
    subq_q = (
        db.query(CollectorRun.site_id, func.max(CollectorRun.requested_at).label("max_ts"))
        .filter(CollectorRun.site_id.in_(site_ids), CollectorRun.provider == provider)
    )
    if strategy:
        subq_q = subq_q.filter(CollectorRun.strategy == strategy)
    subq = subq_q.group_by(CollectorRun.site_id).subquery("_lpr_batch")

    runs_q = (
        db.query(CollectorRun)
        .join(
            subq,
            and_(CollectorRun.site_id == subq.c.site_id, CollectorRun.requested_at == subq.c.max_ts),
        )
        .filter(CollectorRun.provider == provider)
    )
    if strategy:
        runs_q = runs_q.filter(CollectorRun.strategy == strategy)

    runs = runs_q.all()
    result: dict[int, CollectorRun | None] = {sid: None for sid in site_ids}
    for run in runs:
        existing = result[run.site_id]
        if existing is None or run.id > existing.id:
            result[run.site_id] = run
    return result


def _build_dashboard_slim_cards_batch(
    db,
    sites: list,
    *,
    recent_alerts_cache: list[dict],
    period: str = "monthly",
) -> list[dict]:
    """Tüm siteler için slim cards — ~5 toplam sorgu (N+1 yerine).

    Tek tek `_build_dashboard_card_slim` çağırmak yerine batched queries kullanır:
    * get_latest_metrics_batch    : 1 sorgu (N yerine)
    * get_latest_sc_rows_multi_site: 2 sorgu (6N yerine)
    * get_sc_connections_batch    : 1 sorgu (2N yerine)
    * _get_latest_provider_runs_batch: 1 sorgu (N yerine)
    """
    if not sites:
        return []

    from backend.services.metric_store import get_latest_metrics_batch
    from backend.services.warehouse import get_latest_sc_rows_multi_site
    from backend.services.search_console_auth import get_sc_connections_batch

    site_ids = [s.id for s in sites]

    # --- 1. Batch: latest metrics (1 subquery) ---
    all_latest: dict[int, dict] = get_latest_metrics_batch(db, site_ids)

    # --- 2. Batch: SC rows — seçilen döneme göre 2 scope × tüm siteler ---
    # current_28d sadece has_rows kontrolü için lazım → metrics'ten türetilir (SC sorgusu yok)
    cur_scope, prev_scope = _dashboard_sc_scopes_for_url_period(period)
    sc_batch = get_latest_sc_rows_multi_site(
        db,
        site_ids=site_ids,
        scopes=list(dict.fromkeys([cur_scope, prev_scope])),
    )

    # --- 3. Batch: SC connection status (1 query) ---
    sc_connections = get_sc_connections_batch(db, site_ids)

    # --- 4. Batch: latest SC provider runs (1 query) ---
    sc_runs = _get_latest_provider_runs_batch(db, site_ids, "search_console", "all")

    cards: list[dict] = []
    for site in sites:
        latest = all_latest.get(site.id, {})
        site_sc = sc_batch.get(site.id, {})
        current_rows_sc = site_sc.get(cur_scope, [])
        previous_rows_sc = site_sc.get(prev_scope, [])
        has_rows_28d = latest.get("search_console_clicks_28d") is not None
        connection = sc_connections.get(
            site.id, {"connected": False, "method": "none", "label": "Bağlantı yok"}
        )
        sc_run = sc_runs.get(site.id)

        mobile_score = latest.get("pagespeed_mobile_score")
        pagespeed_score = float(mobile_score.value) if mobile_score else 0.0
        crawler_ok = all(
            _metric_value(latest, m, 0.0) >= 1.0
            for m in (
                "crawler_robots_accessible",
                "crawler_sitemap_exists",
                "crawler_schema_found",
                "crawler_canonical_found",
            )
        )
        search_console_status = _search_console_status_from_cache(latest, connection, has_rows_28d)
        site_alerts = [a for a in recent_alerts_cache if a.get("domain") == site.domain]
        top_queries = _build_search_console_top_queries(current_rows_sc, previous_rows_sc, limit=50)

        cards.append(
            {
                "id": site.id,
                "display_name": site.display_name,
                "domain": site.domain,
                "pagespeed_score": round(pagespeed_score),
                "crawler_ok": crawler_ok,
                "alert_count": len(site_alerts),
                "top_queries": top_queries,
                "search_console": {
                    "status": search_console_status,
                    "last_run_dt": sc_run.requested_at if sc_run else None,
                },
            }
        )
    return cards


def _data_state_badge(
    state: str,
    live_text: str,
    stale_text: str,
    failed_text: str,
    *,
    stale_collected_at: datetime | None = None,
) -> dict[str, str]:
    if state == "live":
        return {
            "label": "Live",
            "badge_class": "border-emerald-200 bg-emerald-50 text-emerald-700",
            "description": live_text,
        }
    if state == "stale":
        return {
            "label": _format_status_chip_date(stale_collected_at),
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


_CWV_METRIC_INFO: dict[str, dict] = {
    "largest_contentful_paint":       {"label": "LCP",  "good": "≤ 2.5s",   "ni": "≤ 4s",    "poor": "> 4s"},
    "interaction_to_next_paint":      {"label": "INP",  "good": "≤ 200ms",  "ni": "≤ 500ms", "poor": "> 500ms"},
    "cumulative_layout_shift":        {"label": "CLS",  "good": "≤ 0.1",    "ni": "≤ 0.25",  "poor": "> 0.25"},
    "first_contentful_paint":         {"label": "FCP",  "good": "≤ 1.8s",   "ni": "≤ 3s",    "poor": "> 3s"},
    "experimental_time_to_first_byte":{"label": "TTFB", "good": "≤ 0.8s",   "ni": "≤ 1.8s",  "poor": "> 1.8s"},
}


def _safe_pct(v) -> float | None:
    try:
        f = float(v)
        import math as _math
        if _math.isnan(f) or _math.isinf(f):
            return None
        return round(f * 100.0, 1)
    except (TypeError, ValueError):
        return None


def _build_crux_cwv_chart(payload: dict) -> dict | None:
    """CrUX histogramTimeseries'ten GSC tarzı good/needs_improvement/poor zaman serisi.

    Payload DB'de {'history': {'record': ...}, 'current': ...} olarak saklanır.
    """
    # history.record veya doğrudan record'dan al
    history_block = payload.get("history") or {}
    record = history_block.get("record") or payload.get("record") or {}
    metrics_data = record.get("metrics") or {}
    periods = record.get("collectionPeriods") or []
    if not periods or not metrics_data:
        return None

    labels: list[str] = []
    for period in periods:
        last_date = (period if isinstance(period, dict) else {}).get("lastDate") or {}
        y, m, d = last_date.get("year"), last_date.get("month"), last_date.get("day")
        labels.append(f"{y:04d}-{m:02d}-{d:02d}" if (y and m and d) else "")

    n = len(labels)
    metric_series: dict[str, dict] = {}
    all_good: list[list] = []
    all_poor: list[list] = []

    for metric_key, info in _CWV_METRIC_INFO.items():
        mp = metrics_data.get(metric_key) or {}
        hist = mp.get("histogramTimeseries") or []
        if len(hist) < 3:
            continue
        good_raw  = hist[0].get("densities") or []
        ni_raw    = hist[1].get("densities") or []
        poor_raw  = hist[2].get("densities") or []
        good_pct  = [_safe_pct(v) for v in good_raw]
        ni_pct    = [_safe_pct(v) for v in ni_raw]
        poor_pct  = [_safe_pct(v) for v in poor_raw]
        metric_series[metric_key] = {
            "label": info["label"],
            "good_threshold": info["good"],
            "ni_threshold":   info["ni"],
            "poor_threshold": info["poor"],
            "good":             {"series": good_pct, "latest": next((v for v in reversed(good_pct) if v is not None), None)},
            "needs_improvement":{"series": ni_pct,   "latest": next((v for v in reversed(ni_pct)   if v is not None), None)},
            "poor":             {"series": poor_pct, "latest": next((v for v in reversed(poor_pct)  if v is not None), None)},
        }
        if good_pct: all_good.append(good_pct)
        if poor_pct: all_poor.append(poor_pct)

    if not metric_series:
        return None

    # Overall classification: good = min(all metrics good); poor = max(all metrics poor)
    overall_good: list = []
    overall_poor: list = []
    overall_ni:   list = []
    for i in range(n):
        gv = [s[i] for s in all_good if i < len(s) and s[i] is not None]
        pv = [s[i] for s in all_poor if i < len(s) and s[i] is not None]
        if gv and pv:
            g = round(min(gv), 1)
            p = round(max(pv), 1)
            ni = round(max(0.0, 100.0 - g - p), 1)
            overall_good.append(g); overall_poor.append(p); overall_ni.append(ni)
        else:
            overall_good.append(None); overall_poor.append(None); overall_ni.append(None)

    # Issue rows for breakdown table
    issue_rows: list[dict] = []
    for metric_key, mdata in metric_series.items():
        p = mdata["poor"]["latest"]
        ni = mdata["needs_improvement"]["latest"]
        if p is not None and p > 0:
            issue_rows.append({"metric": mdata["label"], "severity": "poor",             "threshold": mdata["poor_threshold"], "share": p})
        if ni is not None and ni > 0:
            issue_rows.append({"metric": mdata["label"], "severity": "needs_improvement","threshold": mdata["ni_threshold"],   "share": ni})
    issue_rows.sort(key=lambda r: (0 if r["severity"] == "poor" else 1, -(r["share"] or 0)))

    def _last(lst): return next((v for v in reversed(lst) if v is not None), None)

    # GSC tarzı stacked bar için overall'ı da metric_series formatında ver
    # metric_series'e de x/labels ekle (JS tarafı için)
    for ms in metric_series.values():
        ms["labels"] = labels

    return {
        "labels": labels,
        "overall": {
            "good":             {"series": overall_good, "latest": _last(overall_good)},
            "needs_improvement":{"series": overall_ni,   "latest": _last(overall_ni)},
            "poor":             {"series": overall_poor, "latest": _last(overall_poor)},
        },
        "metrics": metric_series,
        "issue_rows": issue_rows,
        "has_data": True,
    }


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
        mobile_cwv  = _build_crux_cwv_chart(mobile_crux.get("payload") or {})  if mobile_crux  else None
        desktop_cwv = _build_crux_cwv_chart(desktop_crux.get("payload") or {}) if desktop_crux else None

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
            "cwv_mobile": mobile_cwv,
            "cwv_desktop": desktop_cwv,
            "pagespeed_report_mobile": _build_pagespeed_report_panel(db, site.id, "mobile", mobile_lighthouse_analysis),
            "pagespeed_report_desktop": _build_pagespeed_report_panel(db, site.id, "desktop", desktop_lighthouse_analysis),
            "crux_mobile_status": mobile_state,
            "crux_desktop_status": desktop_state,
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
        crawler_link_audit = _crawler_link_audit_with_metric_fallback(
            _latest_crawler_link_audit_summary(db, site_id=site.id),
            latest,
        )
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")
        crawler_run = _latest_provider_run(db, site_id=site.id, provider="crawler", strategy="sitewide")
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
                "crawler_status": str(crawler_run.status or "").lower() if crawler_run and crawler_run.status else "never",
                "crux_ready": bool(mobile_crux or desktop_crux),
                "warehouse": warehouse,
                "alerts": get_site_alerts(db, site_id=site.id, limit=1000),
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
        crawler_link_audit = _crawler_link_audit_with_metric_fallback(
            _latest_crawler_link_audit_summary(db, site_id=site.id),
            latest,
        )
        keyword_insights = _build_external_keyword_insights(site.domain, crawler_link_audit)
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")
        mobile_lighthouse_analysis = get_latest_pagespeed_audit_snapshot(db, site.id, "mobile")
        desktop_lighthouse_analysis = get_latest_pagespeed_audit_snapshot(db, site.id, "desktop")

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
            "keyword_insights": keyword_insights,
            "crux_mobile": mobile_crux,
            "crux_desktop": desktop_crux,
            "crux_rows": cwv_rows,
            "pagespeed_report_mobile": _build_pagespeed_report_panel(db, site.id, "mobile", mobile_lighthouse_analysis),
            "pagespeed_report_desktop": _build_pagespeed_report_panel(db, site.id, "desktop", desktop_lighthouse_analysis),
            "warehouse_summary": warehouse,
        }


def _build_dashboard_card_slim(db, site, *, recent_alerts_cache: list[dict]) -> dict:
    """Dashboard ilk yüklemesi için hafif kart verisi (overview/drops/opportunities).

    Tam kart yerine sadece 4 DB sorgusu: latest_metrics, sc_run, 7d_current, 7d_previous.
    Tam kart HTMX ile lazy load edilir.
    """
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    mobile_score = latest.get("pagespeed_mobile_score")
    pagespeed_score = float(mobile_score.value) if mobile_score else 0.0
    crawler_ok = all(
        _metric_value(latest, m, 0.0) >= 1.0
        for m in ("crawler_robots_accessible", "crawler_sitemap_exists", "crawler_schema_found", "crawler_canonical_found")
    )
    search_console_status = _search_console_status(db, latest, site.id)
    sc_run = _latest_provider_run(db, site_id=site.id, provider="search_console", strategy="all")
    site_alerts = [a for a in recent_alerts_cache if a.get("domain") == site.domain]
    current_rows_7 = get_latest_search_console_rows(db, site_id=site.id, data_scope="current_7d")
    previous_rows_7 = get_latest_search_console_rows(db, site_id=site.id, data_scope="previous_7d")
    top_queries = _build_search_console_top_queries(current_rows_7, previous_rows_7, limit=50)
    return {
        "id": site.id,
        "display_name": site.display_name,
        "domain": site.domain,
        "pagespeed_score": round(pagespeed_score),
        "crawler_ok": crawler_ok,
        "alert_count": len(site_alerts),
        "top_queries": top_queries,
        "search_console": {
            "status": search_console_status,
            "last_run_dt": sc_run.requested_at if sc_run else None,
        },
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


def _normalize_dashboard_platform(raw: str | None) -> str:
    p = (raw or "web").lower().strip().replace("-", "_")
    if p in ("mobile_web", "mobil_web", "mobilweb", "mweb"):
        return "mobile_web"
    if p == "android":
        return "android"
    if p == "ios":
        return "ios"
    return "web"


def _dashboard_sc_device_from_platform(platform: str) -> tuple[str, str | None]:
    """Search Console `periods.views` anahtarı (desktop|mobile) ve isteğe bağlı kullanıcı notu."""
    norm = _normalize_dashboard_platform(platform)
    if norm == "web":
        return "desktop", None
    if norm in ("android", "ios"):
        return (
            "mobile",
            "Search Console bu görünümde iOS/Android ayrımı sunmaz; mobil arama verisi gösterilir.",
        )
    return "mobile", None


def _dashboard_platform_label(platform: str) -> str:
    return _DASHBOARD_PLATFORM_LABELS.get(_normalize_dashboard_platform(platform), "Web")


_DASHBOARD_PLATFORM_LABELS = {
    "web": "Web",
    "mobile_web": "Mobil web",
    "android": "Android",
    "ios": "iOS",
}


def _dashboard_sc_period_metrics(cur: dict | None, prev: dict | None) -> dict:
    """Search Console kartı için tek dönem (1g/7g/30g) etiket ve % değişimleri."""
    c = cur if isinstance(cur, dict) else {}
    p = prev if isinstance(prev, dict) else {}
    cc = float(c.get("clicks") or 0.0)
    cp = float(p.get("clicks") or 0.0)
    tc = float(c.get("ctr") or 0.0)
    tp = float(p.get("ctr") or 0.0)
    npc = float(c.get("position") or 0.0)
    npp = float(p.get("position") or 0.0)
    ic = float(c.get("impressions") or 0.0)
    ip = float(p.get("impressions") or 0.0)
    has = bool(cc or cp or tc or tp or npc or npp or ic or ip)
    clicks_cur = _format_compact_number(cc) if cc else "—"
    clicks_prev = _format_compact_number(cp) if cp else "—"
    ctr_cur = f"{_format_max_two_decimals(tc)}%" if tc else "—"
    ctr_prev = f"{_format_max_two_decimals(tp)}%" if tp else "—"
    pos_cur = _format_max_two_decimals(npc) if npc else "—"
    pos_prev = _format_max_two_decimals(npp) if npp else "—"
    return {
        "clicks_label": clicks_cur,
        "clicks_prev_label": clicks_prev,
        "clicks_compare_line": f"{clicks_prev} → {clicks_cur}",
        "clicks_change": _ga4_period_pct_change(cc, cp),
        "ctr_label": ctr_cur,
        "ctr_prev_label": ctr_prev,
        "ctr_compare_line": f"{ctr_prev} → {ctr_cur}",
        "ctr_change": _ga4_period_pct_change(tc, tp),
        "position_label": pos_cur,
        "position_prev_label": pos_prev,
        "position_compare_line": f"{pos_prev} → {pos_cur}",
        "position_change": _ga4_period_pct_change(npp, npc),
        "has_data": has,
    }


def _dashboard_period_pack_for_device(
    search_console_report: dict,
    device_key: str,
    *,
    active_summary_key: str = "30",
) -> tuple[dict, bool, dict, list]:
    """Seçilen cihaza göre 1g/7g/30g blokları, alt özet, top_queries.

    active_summary_key: dashboard URL `period` ile hizalı '1' | '7' | '30' (footer + top sorgular).
    """
    periods = search_console_report.get("periods") or {}

    def _vw(pk: str) -> dict:
        return ((periods.get(pk) or {}).get("views") or {}).get(device_key) or {}

    v1, v7, v30 = _vw("1"), _vw("7"), _vw("30")
    s1c = v1.get("summary_current") if isinstance(v1.get("summary_current"), dict) else {}
    s1p = v1.get("summary_previous") if isinstance(v1.get("summary_previous"), dict) else {}
    s7c = v7.get("summary_current") if isinstance(v7.get("summary_current"), dict) else {}
    s7p = v7.get("summary_previous") if isinstance(v7.get("summary_previous"), dict) else {}
    s30c = v30.get("summary_current") if isinstance(v30.get("summary_current"), dict) else {}
    s30p = v30.get("summary_previous") if isinstance(v30.get("summary_previous"), dict) else {}

    pc1 = _dashboard_sc_period_metrics(s1c, s1p)
    pc7 = _dashboard_sc_period_metrics(s7c, s7p)
    pc30 = _dashboard_sc_period_metrics(s30c, s30p)
    r1c = (str(v1.get("range_last") or "").strip() or search_console_report.get("range_current_1d", ""))
    r1p = (str(v1.get("range_prev") or "").strip() or search_console_report.get("range_previous_1d", ""))

    period_compare = {
        "1": {
            **pc1,
            "range_cur": r1c,
            "range_prev": r1p,
            "compare_badge": "1g",
            "delta_caption": "dün",
            "position_subcaption": "dün · ort. sıra",
            "footer_hint": "Son tam gün",
        },
        "7": {
            **pc7,
            "range_cur": search_console_report.get("range_current_7d", ""),
            "range_prev": search_console_report.get("range_previous_7d", ""),
            "compare_badge": "7g",
            "delta_caption": "son 7 gün",
            "position_subcaption": "ort. sıra · son 7 gün",
            "footer_hint": "7 günlük özet",
        },
        "30": {
            **pc30,
            "range_cur": search_console_report.get("range_current_30d", ""),
            "range_prev": search_console_report.get("range_previous_30d", ""),
            "compare_badge": "30g",
            "delta_caption": "son 30 gün",
            "position_subcaption": "ort. sıra · son 30 gün",
            "footer_hint": "30 günlük özet",
        },
    }
    has_compare_data = bool(pc1["has_data"] or pc7["has_data"] or pc30["has_data"])
    _ak = active_summary_key if active_summary_key in {"1", "7", "30"} else "30"
    v_active = v1 if _ak == "1" else (v30 if _ak == "30" else v7)
    top_queries = v_active.get("top_queries") if isinstance(v_active.get("top_queries"), list) else []
    if not top_queries:
        top_queries = list(search_console_report.get("top_queries") or [])

    if _ak == "1":
        footer_summary = s1c if s1c else (search_console_report.get("summary_current_1d") or {})
    elif _ak == "30":
        footer_summary = s30c if s30c else (search_console_report.get("summary_current_30d") or {})
    else:
        footer_summary = s7c if s7c else (search_console_report.get("summary_current") or {})
    return period_compare, has_compare_data, footer_summary, top_queries


def _dashboard_pagespeed_primary(platform: str) -> str:
    return "mobile" if _normalize_dashboard_platform(platform) != "web" else "desktop"


_DASHBOARD_GA4_PROFILE_LABELS = {
    "web": "Web",
    "mweb": "Mobil web",
    "android": "Android",
    "ios": "iOS",
}


def _dashboard_ga4_profile_key(platform_norm: str) -> str:
    mapping = {"web": "web", "mobile_web": "mweb", "android": "android", "ios": "ios"}
    return mapping.get(platform_norm, "web")


def _dashboard_ga4_compact_block(
    db,
    *,
    site_id: int,
    profile: str,
    latest_metrics: dict[str, float],
    properties: dict,
    period_days: int = 30,
) -> dict | None:
    prop_id = str(properties.get(profile, "") or "").strip()
    if not prop_id:
        return None
    pl = _ga4_profile_payload_for_period(
        db,
        site_id=site_id,
        profile=profile,
        period_days=int(period_days),
        latest=latest_metrics,
        prop_id=prop_id,
    )
    return {
        "profile": profile,
        "label": _DASHBOARD_GA4_PROFILE_LABELS.get(profile, profile),
        "has_data": bool(pl.get("has_period_data")),
        "sessions_display": _format_compact_number(float(pl.get("last_total") or 0.0)),
        "sessions_change": pl.get("sessions_pct_change"),
        "organic_display": f"{_format_max_two_decimals(float(pl.get('organic_share_pct') or 0.0))}%",
        "organic_change": pl.get("organic_share_pct_change"),
    }


def _dashboard_ga4_layout_cell(
    db,
    *,
    site_id: int,
    profile: str,
    latest_metrics: dict[str, float],
    properties: dict,
    period_days: int = 30,
) -> dict:
    """Tek GA4 hücresi; property yoksa yine de etiketli placeholder döner (2x2 kutu boş kalmaz)."""
    b = _dashboard_ga4_compact_block(
        db,
        site_id=site_id,
        profile=profile,
        latest_metrics=latest_metrics,
        properties=properties,
        period_days=period_days,
    )
    if b:
        return b
    return {
        "profile": profile,
        "label": _DASHBOARD_GA4_PROFILE_LABELS.get(profile, profile),
        "has_data": False,
        "missing_property": True,
        "sessions_display": "—",
        "sessions_change": None,
        "organic_display": "—",
    }


def _dashboard_ga4_layout(
    db,
    site: Site,
    platform_norm: str,
    latest_metrics: dict[str, float],
    ga4_status: dict,
    *,
    period_days: int = 30,
) -> dict:
    """GA4 kart yerleşimi: doviz.com → 2x2 (sol web+mweb, sağ android+ios); diğer siteler → sol seçilen profil, sağ eş profil."""
    if not ga4_status.get("connected"):
        return {"mode": "none", "col_left": [], "col_right": []}
    props = ga4_status.get("properties") or {}
    domain_l = (site.domain or "").strip().lower()
    pd = int(period_days) if int(period_days) > 0 else 30

    if domain_l == "doviz.com":
        return {
            "mode": "quad",
            "col_left": [
                _dashboard_ga4_layout_cell(
                    db, site_id=site.id, profile="web", latest_metrics=latest_metrics, properties=props, period_days=pd
                ),
                _dashboard_ga4_layout_cell(
                    db, site_id=site.id, profile="mweb", latest_metrics=latest_metrics, properties=props, period_days=pd
                ),
            ],
            "col_right": [
                _dashboard_ga4_layout_cell(
                    db, site_id=site.id, profile="android", latest_metrics=latest_metrics, properties=props, period_days=pd
                ),
                _dashboard_ga4_layout_cell(
                    db, site_id=site.id, profile="ios", latest_metrics=latest_metrics, properties=props, period_days=pd
                ),
            ],
        }

    primary = _dashboard_ga4_profile_key(platform_norm)
    col_left = [
        _dashboard_ga4_layout_cell(
            db, site_id=site.id, profile=primary, latest_metrics=latest_metrics, properties=props, period_days=pd
        )
    ]
    col_right: list[dict] = []
    counterpart = {"web": "mweb", "mobile_web": "web", "android": "ios", "ios": "android"}.get(primary)
    if counterpart:
        col_right.append(
            _dashboard_ga4_layout_cell(
                db, site_id=site.id, profile=counterpart, latest_metrics=latest_metrics, properties=props, period_days=pd
            )
        )
    return {"mode": "pair", "col_left": col_left, "col_right": col_right}


def _build_dashboard_card(
    db,
    site: Site,
    flash_message: str | None = None,
    recent_alerts_cache: list[dict] | None = None,
    dashboard_platform: str | None = None,
    dashboard_period: str | None = None,
) -> dict:
    dash_period, period_days = _resolve_period(dashboard_period)
    sc_segment = _dashboard_period_to_sc_segment(dash_period)
    platform_norm = _normalize_dashboard_platform(dashboard_platform)
    device_key, sc_platform_note = _dashboard_sc_device_from_platform(platform_norm)
    pagespeed_primary = _dashboard_pagespeed_primary(platform_norm)
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
    _ps_fresh_minutes = 60 * 32
    mobile_status = _pagespeed_strategy_status(
        latest, "mobile", pagespeed_status_alerts, max_age_minutes=_ps_fresh_minutes
    )
    desktop_status = _pagespeed_strategy_status(
        latest, "desktop", pagespeed_status_alerts, max_age_minutes=_ps_fresh_minutes
    )
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
    period_compare, has_compare_data, footer_summary, device_top_queries = _dashboard_period_pack_for_device(
        search_console_report,
        device_key,
        active_summary_key=sc_segment,
    )
    pc1 = period_compare["1"]
    search_console_summary = footer_summary
    search_console_run = _latest_provider_run(db, site_id=site.id, provider="search_console", strategy="all")
    mobile_pagespeed_score = float(mobile_pagespeed_metric.value) if mobile_pagespeed_metric is not None else None
    desktop_pagespeed_score = float(desktop_pagespeed_metric.value) if desktop_pagespeed_metric is not None else None
    # PageSpeed: her zaman son ölçüm vs bir önceki yerel takvim günü (dashboard dönümünden bağımsız)
    pagespeed_compare_blurb = _dashboard_pagespeed_compare_blurb("daily")
    pm_prev, _pm_last, pm_prev_date = get_metric_day_over_day_score(db, site.id, "pagespeed_mobile_score")
    pd_prev, _pd_last, pd_prev_date = get_metric_day_over_day_score(db, site.id, "pagespeed_desktop_score")
    pagespeed_mobile_change = (
        _ga4_period_pct_change(float(mobile_pagespeed_score), float(pm_prev))
        if mobile_pagespeed_score is not None and pm_prev is not None
        else None
    )
    pagespeed_desktop_change = (
        _ga4_period_pct_change(float(desktop_pagespeed_score), float(pd_prev))
        if desktop_pagespeed_score is not None and pd_prev is not None
        else None
    )
    pagespeed_mobile_prev_score = round(pm_prev) if pm_prev is not None else None
    pagespeed_desktop_prev_score = round(pd_prev) if pd_prev is not None else None
    pagespeed_mobile_prev_date = pm_prev_date.strftime("%d.%m.%Y") if pm_prev_date is not None else None
    pagespeed_desktop_prev_date = pd_prev_date.strftime("%d.%m.%Y") if pd_prev_date is not None else None
    latest_ga4_floats = {k: float(v.value) for k, v in latest.items()}
    ga4_conn = get_ga4_connection_status(db, site.id)
    ga4_layout = _dashboard_ga4_layout(
        db, site, platform_norm, latest_ga4_floats, ga4_conn, period_days=period_days
    )
    spotlight_queries_all = _dashboard_spotlight_queries(
        device_top_queries, recent_site_alerts[:3], limit=_dashboard_spotlight_card_limit(site.domain)
    )
    spotlight_split = (len(spotlight_queries_all) + 1) // 2
    return {
        "id": site.id,
        "display_name": site.display_name,
        "domain": site.domain,
        "dashboard_platform": platform_norm,
        "dashboard_platform_label": _dashboard_platform_label(platform_norm),
        "ga4_period_caption": _dashboard_ga4_period_caption(dash_period),
        "sc_platform_note": sc_platform_note,
        "pagespeed_primary": pagespeed_primary,
        "pagespeed_score": round(pagespeed_score),
        "pagespeed_color": _score_color(pagespeed_score),
        "pagespeed_mobile_score": round(mobile_pagespeed_score) if mobile_pagespeed_score is not None else None,
        "pagespeed_mobile_label": str(round(mobile_pagespeed_score)) if mobile_pagespeed_score is not None else "Veri yok",
        "pagespeed_mobile_color": _score_color(mobile_pagespeed_score)
        if mobile_pagespeed_score is not None
        else "text-slate-400 dark:text-slate-500",
        "pagespeed_desktop_score": round(desktop_pagespeed_score) if desktop_pagespeed_score is not None else None,
        "pagespeed_desktop_label": str(round(desktop_pagespeed_score)) if desktop_pagespeed_score is not None else "Veri yok",
        "pagespeed_desktop_color": _score_color(desktop_pagespeed_score)
        if desktop_pagespeed_score is not None
        else "text-slate-400 dark:text-slate-500",
        "pagespeed_mobile_change": pagespeed_mobile_change,
        "pagespeed_desktop_change": pagespeed_desktop_change,
        "pagespeed_mobile_prev_score": pagespeed_mobile_prev_score,
        "pagespeed_desktop_prev_score": pagespeed_desktop_prev_score,
        "pagespeed_mobile_prev_date": pagespeed_mobile_prev_date,
        "pagespeed_desktop_prev_date": pagespeed_desktop_prev_date,
        "pagespeed_compare_blurb": pagespeed_compare_blurb,
        "ga4_connected": bool(ga4_conn.get("connected")),
        "ga4_layout": ga4_layout,
        "crawler_ok": crawler_ok,
        "crawler_label": crawler_label,
        "crawler_detail": crawler_detail,
        "crawler_link_audit": crawler_link_audit,
        "check_count": len(available_metrics),
        "last_updated": format_local_datetime(last_updated, fallback="Henüz veri yok"),
        "alert_count": len(recent_site_alerts),
        "recent_alerts": recent_site_alerts[:3],
        "top_queries": device_top_queries,
        "spotlight_queries": spotlight_queries_all,
        "spotlight_queries_left": spotlight_queries_all[:spotlight_split],
        "spotlight_queries_right": spotlight_queries_all[spotlight_split:],
        "search_console": {
            "clicks": float(search_console_summary.get("clicks", 0.0)),
            "clicks_label": _format_compact_number(search_console_summary.get("clicks", 0.0)),
            "ctr": float(search_console_summary.get("ctr", 0.0)),
            "ctr_label": f"{_format_max_two_decimals(search_console_summary.get('ctr', 0.0))}%",
            "position": float(search_console_summary.get("position", 0.0)),
            "position_label": _format_max_two_decimals(search_console_summary.get("position", 0.0)),
            "status": search_console_status,
            "has_rows": bool(search_console_status.get("has_rows")),
            "last_run_status": str(search_console_run.status or "").upper() if search_console_run and search_console_run.status else "NEVER",
            "last_run_at": _format_optional_datetime(search_console_run.requested_at if search_console_run else None),
            "last_run_dt": search_console_run.requested_at if search_console_run else None,
            "period_compare": period_compare,
            "has_compare_data": has_compare_data,
            # Geriye dönük (şablon dışı tüketim)
            "clicks_1d_label": pc1["clicks_label"],
            "clicks_1d_change": pc1["clicks_change"],
            "ctr_1d_label": pc1["ctr_label"],
            "ctr_1d_change": pc1["ctr_change"],
            "position_1d_label": pc1["position_label"],
            "position_1d_change": pc1["position_change"],
            "range_1d": period_compare["1"]["range_cur"],
            "range_1d_prev": period_compare["1"]["range_prev"],
            "has_1d_data": bool(pc1["has_data"]),
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


def _build_alert_query_lookup(recent_alerts: list[dict]) -> dict[tuple[str, str], int]:
    """(domain, query_lower) → AlertLog id eşlemesi; SC kayıp alertleri için."""
    _sc_drop_types = {
        "search_console_ctr_drop",
        "search_console_position_drop",
        "search_console_impression_drop",
    }
    lookup: dict[tuple[str, str], int] = {}
    for a in recent_alerts:
        if a.get("alert_type") not in _sc_drop_types:
            continue
        dq = (a.get("display_query") or "").strip().lower()
        if not dq:
            continue
        key = ((a.get("domain") or "").lower(), dq)
        if key not in lookup:
            lookup[key] = a["id"]
    return lookup


def _build_dashboard_top_drops(
    site_cards: list[dict],
    *,
    limit: int = 6,
    recent_alerts: list[dict] | None = None,
) -> list[dict]:
    alert_lookup = _build_alert_query_lookup(recent_alerts or [])
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
            alert_key = ((card.get("domain") or "").lower(), str(query.get("query") or "").strip().lower())
            candidates.append(
                {
                    "domain": card.get("domain"),
                    "site_id": card.get("id"),
                    "query": query.get("query"),
                    "reason": reason,
                    "metric": metric,
                    "secondary": secondary,
                    "impact": impact,
                    "classes": _dashboard_tone_classes(tone),
                    "alert_id": alert_lookup.get(alert_key),
                }
            )

    candidates.sort(key=lambda item: item.get("impact", 0.0), reverse=True)
    return candidates[:limit]


def _dashboard_spotlight_row_from_sc_query(row: dict) -> dict:
    """Tek SC top_query satırını dashboard spotlight kartına çevirir."""
    q = str(row.get("query") or "").strip()
    cc = float(row.get("clicks_current") or 0.0)
    cp = float(row.get("clicks_previous") or 0.0)
    diff = float(row.get("clicks_diff") or 0.0)
    return {
        "query": q,
        "subtitle": f"{_format_compact_number(cp)} → {_format_compact_number(cc)} tıklama",
        "diff": diff,
        "tone": "emerald" if diff >= 0 else "rose",
    }


def _dashboard_spotlight_queries(
    top_queries: list[dict] | None,
    recent_alert_slice: list[dict],
    *,
    limit: int = 10,
) -> list[dict]:
    """Site kartı sağ sütununu dolduran SC sorgu satırları; uyarılardaki sorguları önce çıkarır."""
    rows = list(top_queries or [])
    alert_q = {
        (a.get("display_query") or "").strip().lower()
        for a in recent_alert_slice
        if (a.get("display_query") or "").strip()
    }
    ranked = sorted(rows, key=lambda r: float(r.get("clicks_current") or 0.0), reverse=True)

    out: list[dict] = []
    seen: set[str] = set()

    def push(row: dict, *, skip_alerts: bool) -> None:
        q = str(row.get("query") or "").strip()
        if not q or q.lower() in seen:
            return
        if skip_alerts and q.lower() in alert_q:
            return
        out.append(_dashboard_spotlight_row_from_sc_query(row))
        seen.add(q.lower())

    for row in ranked:
        if len(out) >= limit:
            break
        push(row, skip_alerts=True)
    if len(out) < limit:
        for row in ranked:
            if len(out) >= limit:
                break
            push(row, skip_alerts=False)
    return out


def _build_dashboard_critical_panel(
    site_cards: list[dict],
    recent_alerts: list[dict],
    *,
    limit: int = 6,
) -> list[dict]:
    """Önce bakılması gerekenler: `recent_alerts` içinden seçilir; sıra seçilen döneme göre SC kayıp
    etkisiyle güçlendirilir (Hızlı kayıp ile aynı slim/top_queries penceresi).

    Böylece dönem filtresi değişince sol panel de sağdaki gibi güncellenir; eşleşme yoksa
    yine zaman sırasına yakın davranır.
    """
    if not recent_alerts:
        return []
    drops = _build_dashboard_top_drops(site_cards, limit=80, recent_alerts=recent_alerts)
    impact_by_key: dict[tuple[str, str], float] = {}
    for d in drops:
        dom = (d.get("domain") or "").lower()
        q = (str(d.get("query") or "")).strip().lower()
        if not q:
            continue
        imp = float(d.get("impact") or 0.0)
        impact_by_key[(dom, q)] = max(impact_by_key.get((dom, q), 0.0), imp)

    def sort_key(a: dict) -> tuple[float, str]:
        dom = (a.get("domain") or "").lower()
        dq = (a.get("display_query") or "").strip().lower()
        bonus = float(impact_by_key.get((dom, dq), 0.0)) if dq else 0.0
        return bonus, a.get("triggered_at_iso") or ""

    ranked = sorted(recent_alerts, key=lambda a: sort_key(a), reverse=True)
    return ranked[:limit]


def _build_dashboard_opportunities(
    site_cards: list[dict],
    *,
    limit: int = 4,
    recent_alerts: list[dict] | None = None,
) -> list[dict]:
    alert_lookup = _build_alert_query_lookup(recent_alerts or [])
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
            alert_key = ((card.get("domain") or "").lower(), str(query.get("query") or "").strip().lower())
            candidates.append(
                {
                    "domain": card.get("domain"),
                    "site_id": card.get("id"),
                    "query": query.get("query"),
                    "title": title,
                    "detail": detail,
                    "action": action,
                    "score": score,
                    "classes": _dashboard_tone_classes(tone),
                    "alert_id": alert_lookup.get(alert_key),
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
    """Dashboard ilk yüklemesi: slim veri ile anında render, site kartları HTMX lazy load."""
    with SessionLocal() as db:
        recent_alerts = get_recent_alerts(db, limit=100, include_external=False)
        external_ids = _external_site_ids(db)
        sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_ids]
        sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        period, _period_days = _resolve_period(request.query_params.get("period"))
        # Batched queries: N+1 yerine ~5 toplam sorgu (SC scope'ları seçilen döneme göre)
        slim_cards = _build_dashboard_slim_cards_batch(db, sites, recent_alerts_cache=recent_alerts, period=period)
        dashboard_platform = _normalize_dashboard_platform(request.query_params.get("platform"))
        sc_segment = _dashboard_period_to_sc_segment(period)
        payload = {
            "site_name": "SEO Agent Dashboard",
            "sites": get_sidebar_sites(),
            "period": period,
            "sc_segment": sc_segment,
            "dashboard_platform": dashboard_platform,
            "dashboard_platform_label": _dashboard_platform_label(dashboard_platform),
            "overview_items": _build_dashboard_overview(slim_cards, recent_alerts),
            "critical_alerts": _build_dashboard_critical_panel(slim_cards, recent_alerts, limit=6),
            "lazy_site_ids": [(s.id, s.display_name, s.domain) for s in sites],
            "top_drop_items": _build_dashboard_top_drops(slim_cards, limit=7, recent_alerts=recent_alerts),
            "opportunity_items": _build_dashboard_opportunities(slim_cards, limit=8, recent_alerts=recent_alerts),
        }
        ctx = {"request": request, **payload}
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "partials/dashboard_content.html", context=ctx)
    return templates.TemplateResponse(request, "dashboard.html", context=ctx)


@app.get("/dashboard/cards/{site_id}", response_class=HTMLResponse)
def dashboard_card_lazy(request: Request, site_id: int):
    """HTMX lazy loading ile tek site kartı render eder."""
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("", status_code=404)
        # Sadece bu site için alert'leri çek (site_id_filter), tüm tabloyu tarama
        recent_alerts = get_recent_alerts(db, limit=30, include_external=False, site_id_filter=site.id)
        dash_pf = request.query_params.get("platform")
        period, _pd = _resolve_period(request.query_params.get("period"))
        card = _build_dashboard_card(
            db, site, recent_alerts_cache=recent_alerts, dashboard_platform=dash_pf, dashboard_period=period
        )
        sc_segment = _dashboard_period_to_sc_segment(period)
    return templates.TemplateResponse(
        request,
        "partials/dashboard_site_card.html",
        context={"request": request, "card": card, "period": period, "sc_segment": sc_segment},
    )


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

        dash_pf = request.query_params.get("platform")
        card = _build_dashboard_card(
            db, site, flash_message=flash_message, dashboard_platform=dash_pf, dashboard_period=period
        )
        sc_segment = _dashboard_period_to_sc_segment(period)
    return templates.TemplateResponse(
        request,
        "partials/dashboard_site_card.html",
        context={"request": request, "card": card, "period": period, "sc_segment": sc_segment},
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
async def public_sites_create_site(request: Request, background_tasks: BackgroundTasks):
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
            existing_job_id = _find_running_external_onboarding_job_id(db, site.id)
            if existing_job_id:
                return {
                    "ok": True,
                    "site": {
                        "id": site.id,
                        "domain": site.domain,
                        "display_name": site.display_name,
                    },
                    "job_id": existing_job_id,
                    "summary": "Bu site icin onboarding zaten devam ediyor.",
                }
            site.display_name = display_name or site.display_name
            site.is_active = is_active

        db.add(site)
        try:
            _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
            db.refresh(site)
        except OperationalError as exc:
            if _is_sqlite_lock_error(exc):
                return JSONResponse(
                    {
                        "ok": False,
                        "error": "Veritabanı meşgul olduğu için işlem kısa süreliğine tamamlanamadı. Lütfen tekrar deneyin.",
                    },
                    status_code=503,
                )
            raise

        if not _is_external_site(db, site.id):
            db.add(ExternalSite(site_id=site.id))
            try:
                _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
            except OperationalError as exc:
                if _is_sqlite_lock_error(exc):
                    return JSONResponse(
                        {
                            "ok": False,
                            "error": "External profil işaretlenirken veritabanı kilidi oluştu. Lütfen tekrar deneyin.",
                        },
                        status_code=503,
                    )
                raise

        job_id, created_new = _create_external_onboarding_job(db, site_id=site.id, domain=site.domain)
        if created_new:
            background_tasks.add_task(_run_external_onboarding_background, site.id, job_id)

        return {
            "ok": True,
            "site": {
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
            },
            "job_id": job_id,
            "summary": "External site eklendi. Ilk olcumler arka planda baslatildi." if created_new else "Bu site icin onboarding zaten devam ediyor.",
        }


@app.get("/external/jobs/{job_id}")
def external_onboarding_job_status(job_id: str):
    state = _get_external_onboarding_job(job_id)
    if state is None:
        return JSONResponse({"ok": False, "error": "Job bulunamadı."}, status_code=404)
    return JSONResponse({"ok": True, **state})


@app.delete("/external/sites/{site_id}")
def public_sites_delete_site(request: Request, site_id: int):
    last_lock_error = False
    for attempt in range(1, 5):
        with SessionLocal() as db:
            site = db.query(Site).filter(Site.id == site_id).first()
            if site is None:
                return JSONResponse({"ok": False, "error": "Site bulunamadı."}, status_code=404)
            marker = db.query(ExternalSite).filter(ExternalSite.site_id == site.id).first()
            if marker is None:
                return JSONResponse({"ok": False, "error": "Site external profilinde değil."}, status_code=404)

            db.delete(site)
            try:
                db.commit()
            except OperationalError as exc:
                db.rollback()
                if _is_sqlite_lock_error(exc):
                    last_lock_error = True
                    time.sleep(0.25 * attempt)
                    continue
                raise

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

    if last_lock_error:
        return JSONResponse(
            {
                "ok": False,
                "error": "Silme işlemi sırasında veritabanı kilidi oluştu. Lütfen tekrar deneyin.",
            },
            status_code=503,
        )
    return JSONResponse({"ok": False, "error": "Silme işlemi tamamlanamadı."}, status_code=500)


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

        results = _refresh_site_detail_measurements(
            db,
            site,
            include_pagespeed=True,
            include_crawler=False,
            include_search_console=False,
            force=True,
        )
        try:
            results["crux_history"] = collect_crux_history(db, site)
        except Exception as exc:  # noqa: BLE001
            results["crux_history"] = {"state": "failed", "error": str(exc)}
        db.commit()
        notify_result_map(
            trigger_source="manual",
            site=site,
            results=results,
            action_label="Data Explorer manuel refresh (PSI + CrUX)",
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
        external_domains = _external_site_domains(db)
        alert_rows = get_recent_alerts(db, limit=100, include_external=True)
        payload = {
            "site_name": "Alerts",
            "sites": get_sidebar_sites(),
            "recent_alerts": alert_rows,
            "selected_alert_id": request.query_params.get("selected_alert", "").strip(),
            "has_external_sites": bool(external_domains),
        }
    template_name = "partials/alerts_content.html" if request.headers.get("HX-Request") == "true" else "alerts.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.post("/alerts/refresh")
def alerts_refresh(request: Request):
    summaries: list[dict[str, object]] = []
    with SessionLocal() as db:
        external_ids = _external_site_ids(db)
        sites = [site for site in _active_sites(db) if site.id not in external_ids]
        alert_batch: list[tuple[Site, dict]] = []
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
                alert_batch.append((site, results["search_console"]))
                summaries.append({"site": site.domain, "results": results})
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                alert_batch.append((site, {"state": "failed", "error": str(exc)}))
                summaries.append({"site": site.domain, "error": str(exc)})

            if index < len(sites) - 1:
                time.sleep(max(0, int(settings.scheduled_refresh_site_spacing_seconds)))

        if alert_batch:
            send_consolidated_system_email(
                system_key="search_console_alerts",
                trigger_source="manual",
                action_label="Uyarıları yenile",
                items=alert_batch,
                db=db,
            )

        return JSONResponse(
            {
                "refreshed": True,
                "sites": summaries,
                "recent_alerts": get_recent_alerts(db, limit=100, include_external=True),
            }
        )


@app.get("/settings")
def settings_page(request: Request):
    # Settings ekranı site yönetimi arayüzünü gösterir.
    with SessionLocal() as db:
        payload = {
            "site_name": "Settings",
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


def _ga4_engagement_rate_pct(raw: float) -> float:
    try:
        v = float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return v * 100.0 if v <= 1.0 else v


def _ga4_period_pct_change(last: float, prev: float) -> float:
    """Önceki döneme göre yüzde değişim; prev=0 iken last>0 ise +100% kabul."""
    try:
        lv = float(last or 0.0)
        pv = float(prev or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if pv > 0.0:
        return (lv - pv) / pv * 100.0
    if lv > 0.0:
        return 100.0
    return 0.0


def _ga4_sw_float(m: dict | None, key: str) -> float:
    if not isinstance(m, dict):
        return 0.0
    try:
        return float(m.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ga4_organic_share_from_channel_maps(
    channels_last: dict[str, float] | None, channels_prev: dict[str, float] | None
) -> tuple[float, float, float] | None:
    """Organic Search oturum payı (last %, prev %, pay değişim %). Snapshot kanal haritalarından."""
    if not isinstance(channels_last, dict) or not isinstance(channels_prev, dict) or not channels_last:
        return None
    tl = sum(float(v or 0) for v in channels_last.values())
    tp = sum(float(v or 0) for v in channels_prev.values())
    org_l = float(channels_last.get("organic_search", 0) or 0)
    org_p = float(channels_prev.get("organic_search", 0) or 0)
    sl = (org_l / tl * 100.0) if tl > 0 else 0.0
    sp = (org_p / tp * 100.0) if tp > 0 else 0.0
    return sl, sp, _ga4_period_pct_change(sl, sp)


def _ga4_organic_from_snapshot_payload(pl: dict | None) -> tuple[float, float] | None:
    """Collector'ın snapshot'a yazdığı organic_share_pct + organic_share_pct_change (varsa)."""
    if not isinstance(pl, dict):
        return None
    if "organic_share_pct" not in pl or "organic_share_pct_change" not in pl:
        return None
    try:
        return float(pl.get("organic_share_pct") or 0.0), float(pl.get("organic_share_pct_change") or 0.0)
    except (TypeError, ValueError):
        return None


def _ga4_top_channels_with_pct_change(
    latest: dict[str, float],
    profile: str,
    pd_days: int,
    snapshot_payload: dict | None = None,
) -> list[dict]:
    """Son N gün kanal oturumları vs önceki N gün. Önce snapshot (channel_summary_rows), sonra channels_*, sonra metrics."""
    pl = snapshot_payload if isinstance(snapshot_payload, dict) else None
    if isinstance(pl, dict):
        pre = pl.get("channel_summary_rows")
        if isinstance(pre, list) and len(pre) > 0:
            out: list[dict] = []
            for r in pre[:4]:
                if not isinstance(r, dict):
                    continue
                try:
                    out.append(
                        {
                            "label": str(r.get("label") or ""),
                            "value": float(r.get("value") or 0),
                            "pct_change": float(r.get("pct_change") or 0),
                        }
                    )
                except (TypeError, ValueError):
                    continue
            if out:
                return out
    cl = pl.get("channels_last") if isinstance(pl, dict) else None
    cp = pl.get("channels_prev") if isinstance(pl, dict) else None
    if isinstance(cl, dict) and isinstance(cp, dict) and cl:
        rows: list[tuple[str, float, float, float]] = []
        for slug, last_val in cl.items():
            last_v = float(last_val or 0.0)
            prev_v = float(cp.get(slug, 0.0) or 0.0)
            pct = _ga4_period_pct_change(last_v, prev_v)
            label = str(slug).replace("_", " ")
            rows.append((label, last_v, prev_v, pct))
        rows.sort(key=lambda x: x[1], reverse=True)
        out: list[dict] = []
        for label, last_v, _pv, pct in rows[:4]:
            out.append({"label": label, "value": last_v, "pct_change": pct})
        return out

    last_prefix = f"ga4_{profile}_sessions_last{pd_days}d_channel__"
    prev_prefix = f"ga4_{profile}_sessions_prev{pd_days}d_channel__"
    rows: list[tuple[str, float, float, float]] = []
    for key, value in latest.items():
        if not key.startswith(last_prefix):
            continue
        slug = key[len(last_prefix) :]
        label = slug.replace("_", " ")
        last_val = float(value or 0.0)
        prev_val = float(latest.get(f"{prev_prefix}{slug}", 0.0) or 0.0)
        pct = _ga4_period_pct_change(last_val, prev_val)
        rows.append((label, last_val, prev_val, pct))
    rows.sort(key=lambda x: x[1], reverse=True)
    out: list[dict] = []
    for label, last_val, _prev_val, pct in rows[:4]:
        out.append(
            {
                "label": label,
                "value": last_val,
                "pct_change": pct,
            }
        )
    return out


def _ga4_profile_payload_for_same_weekday_day(
    db,
    *,
    site_id: int,
    profile: str,
    latest: dict[str, float],
    prop_id: str,
) -> dict:
    """Son tam gün vs bir önceki haftanın aynı günü (same_weekday_kpi); tablolar 7g snapshot ile uyumlu."""
    snap_ref = get_latest_ga4_report_snapshot(db, site_id=site_id, profile=profile, period_days=7) or get_latest_ga4_report_snapshot(
        db, site_id=site_id, profile=profile, period_days=30
    )
    pl = (snap_ref or {}).get("payload") or {}
    swk = pl.get("same_weekday_kpi") if isinstance(pl.get("same_weekday_kpi"), dict) else {}
    la = swk.get("last")
    pr = swk.get("prev")

    def pick(metric_key: str) -> float:
        try:
            return float(latest.get(metric_key, 0.0) or 0.0)
        except Exception:  # noqa: BLE001
            return 0.0

    if not isinstance(la, dict) or not isinstance(pr, dict):
        return {
            "property_id": prop_id,
            "period_days": 1,
            "comparison_mode": "same_weekday",
            "ranges": {"last_start": "", "last_end": "", "prev_start": "", "prev_end": ""},
            "last_total": 0.0,
            "prev_total": 0.0,
            "wow_change_pct": 0.0,
            "sessions_pct_change": 0.0,
            "users_last": 0.0,
            "users_prev": 0.0,
            "users_pct_change": 0.0,
            "new_users_last": 0.0,
            "new_users_prev": 0.0,
            "new_users_pct_change": 0.0,
            "engaged_last": 0.0,
            "engaged_prev": 0.0,
            "engaged_pct_change": 0.0,
            "engagement_rate_last_pct": 0.0,
            "engagement_rate_prev_pct": 0.0,
            "engagement_rate_pct_change": 0.0,
            "avg_session_last_sec": 0.0,
            "avg_session_prev_sec": 0.0,
            "avg_session_pct_change": 0.0,
            "pageviews_last": 0.0,
            "pageviews_prev": 0.0,
            "pageviews_pct_change": 0.0,
            "organic_share_pct": 0.0,
            "organic_share_pct_change": 0.0,
            "top_channels": [],
            "pages_no_news": [],
            "sources": [],
            "daily_trend": (
                pl.get("daily_trend")
                if isinstance(pl.get("daily_trend"), dict) and (pl.get("daily_trend") or {}).get("dates")
                else {"dates": [], "sessions": [], "totalUsers": [], "engagedSessions": [], "engagementRate": []}
            ),
            "same_weekday_kpi": swk,
            "has_snapshot": bool(snap_ref),
            "has_period_data": False,
        }

    last_total = _ga4_sw_float(la, "sessions")
    prev_total = _ga4_sw_float(pr, "sessions")
    users_last = _ga4_sw_float(la, "totalUsers")
    users_prev = _ga4_sw_float(pr, "totalUsers")
    new_users_last = _ga4_sw_float(la, "newUsers")
    new_users_prev = _ga4_sw_float(pr, "newUsers")
    engaged_last = _ga4_sw_float(la, "engagedSessions")
    engaged_prev = _ga4_sw_float(pr, "engagedSessions")
    engagement_rate_last_pct = _ga4_engagement_rate_pct(_ga4_sw_float(la, "engagementRate"))
    engagement_rate_prev_pct = _ga4_engagement_rate_pct(_ga4_sw_float(pr, "engagementRate"))
    avg_session_last_sec = _ga4_sw_float(la, "averageSessionDuration")
    avg_session_prev_sec = _ga4_sw_float(pr, "averageSessionDuration")
    pageviews_last = _ga4_sw_float(la, "screenPageViews")
    pageviews_prev = _ga4_sw_float(pr, "screenPageViews")

    ref_d = str(swk.get("reference_date") or "")
    prev_d = str(swk.get("previous_week_date") or "")
    wow = _ga4_period_pct_change(last_total, prev_total)

    _org_flat_sw = _ga4_organic_from_snapshot_payload(pl)
    if _org_flat_sw is not None:
        organic_share, organic_share_pct_change = _org_flat_sw
    else:
        _org_sw = _ga4_organic_share_from_channel_maps(
            pl.get("channels_last") if isinstance(pl.get("channels_last"), dict) else None,
            pl.get("channels_prev") if isinstance(pl.get("channels_prev"), dict) else None,
        )
        if _org_sw is not None:
            organic_share = _org_sw[0]
            organic_share_pct_change = _org_sw[2]
        else:
            lt7 = float(pick(f"ga4_{profile}_sessions_last7d_total") or 0.0)
            lt7_prev = float(pick(f"ga4_{profile}_sessions_prev7d_total") or 0.0)
            organic = float(pick(f"ga4_{profile}_sessions_last7d_channel__organic_search") or 0.0)
            organic_prev = float(pick(f"ga4_{profile}_sessions_prev7d_channel__organic_search") or 0.0)
            organic_share = (organic / lt7 * 100.0) if lt7 > 0 else 0.0
            organic_share_prev = (organic_prev / lt7_prev * 100.0) if lt7_prev > 0 else 0.0
            organic_share_pct_change = _ga4_period_pct_change(organic_share, organic_share_prev)

    # Grafik: 7g snapshot ile aynı günlük seri (2 noktalı WoW çizgisi yanıltıcı oluyordu)
    _dt = pl.get("daily_trend") if isinstance(pl.get("daily_trend"), dict) else {}
    if _dt.get("dates"):
        daily_trend = _dt
    else:
        daily_trend = {
            "dates": [],
            "sessions": [],
            "totalUsers": [],
            "engagedSessions": [],
            "engagementRate": [],
        }

    return {
        "property_id": prop_id,
        "period_days": 1,
        "comparison_mode": "same_weekday",
        "ranges": {
            "last_start": ref_d,
            "last_end": ref_d,
            "prev_start": prev_d,
            "prev_end": prev_d,
        },
        "last_total": last_total,
        "prev_total": prev_total,
        "wow_change_pct": wow,
        "sessions_pct_change": _ga4_period_pct_change(last_total, prev_total),
        "users_last": users_last,
        "users_prev": users_prev,
        "users_pct_change": _ga4_period_pct_change(users_last, users_prev),
        "new_users_last": new_users_last,
        "new_users_prev": new_users_prev,
        "new_users_pct_change": _ga4_period_pct_change(new_users_last, new_users_prev),
        "engaged_last": engaged_last,
        "engaged_prev": engaged_prev,
        "engaged_pct_change": _ga4_period_pct_change(engaged_last, engaged_prev),
        "engagement_rate_last_pct": engagement_rate_last_pct,
        "engagement_rate_prev_pct": engagement_rate_prev_pct,
        "engagement_rate_pct_change": _ga4_period_pct_change(engagement_rate_last_pct, engagement_rate_prev_pct),
        "avg_session_last_sec": avg_session_last_sec,
        "avg_session_prev_sec": avg_session_prev_sec,
        "avg_session_pct_change": _ga4_period_pct_change(avg_session_last_sec, avg_session_prev_sec),
        "pageviews_last": pageviews_last,
        "pageviews_prev": pageviews_prev,
        "pageviews_pct_change": _ga4_period_pct_change(pageviews_last, pageviews_prev),
        "organic_share_pct": organic_share,
        "organic_share_pct_change": organic_share_pct_change,
        "top_channels": _ga4_top_channels_with_pct_change(latest, profile, 7, pl),
        "pages_no_news": _enrich_ga4_page_rows(pl.get("pages_no_news")),
        "sources": pl.get("sources") or [],
        "daily_trend": daily_trend,
        "same_weekday_kpi": swk,
        "has_snapshot": bool(snap_ref),
        "has_period_data": True,
    }


def _ga4_profile_payload_for_period(
    db,
    *,
    site_id: int,
    profile: str,
    period_days: int,
    latest: dict[str, float],
    prop_id: str,
) -> dict:
    """Tek GA4 profili için son N gün vs önceki N gün kart yükü (snapshot + metrik fallback)."""

    def pick(metric_key: str) -> float:
        try:
            return float(latest.get(metric_key, 0.0) or 0.0)
        except Exception:  # noqa: BLE001
            return 0.0

    pd = int(period_days) if int(period_days) > 0 else 30
    if pd == 1:
        return _ga4_profile_payload_for_same_weekday_day(
            db, site_id=site_id, profile=profile, latest=latest, prop_id=prop_id
        )
    sk = f"_{pd}d"

    def sum_channel_prefix(prefix: str) -> float:
        total = 0.0
        for key, value in latest.items():
            if key.startswith(prefix):
                try:
                    total += float(value or 0.0)
                except (TypeError, ValueError):
                    continue
        return total

    snap = get_latest_ga4_report_snapshot(db, site_id=site_id, profile=profile, period_days=pd)
    pl = (snap or {}).get("payload") or {}
    summary = pl.get("summary") or {}
    last_s = summary.get("last") or {}
    prev_s = summary.get("prev") or {}

    # Snapshot KPI: 0 oturum geçerli olabilir; None/boş ise metrik tablosuna düş.
    _ls_raw = last_s.get("sessions")
    if _ls_raw is not None and _ls_raw != "":
        try:
            last_total = float(_ls_raw)
        except (TypeError, ValueError):
            last_total = 0.0
    else:
        last_total = 0.0
    if last_total <= 0:
        last_total = float(
            pick(f"ga4_{profile}_sessions_last{pd}d_total")
            or (pick(f"ga4_{profile}_sessions_last30d_total") if pd == 30 else 0.0)
            or 0.0
        )
    _ps_raw = prev_s.get("sessions")
    if _ps_raw is not None and _ps_raw != "":
        try:
            prev_total = float(_ps_raw)
        except (TypeError, ValueError):
            prev_total = 0.0
    else:
        prev_total = 0.0
    if prev_total <= 0:
        prev_total = float(
            pick(f"ga4_{profile}_sessions_prev{pd}d_total")
            or (pick(f"ga4_{profile}_sessions_prev30d_total") if pd == 30 else 0.0)
            or 0.0
        )

    # Kanal kırılımı toplamları (skaler prev/last ile tutarsızlık veya eksik metrik düzeltmesi)
    ch_last_sum = sum_channel_prefix(f"ga4_{profile}_sessions_last{pd}d_channel__")
    ch_prev_sum = sum_channel_prefix(f"ga4_{profile}_sessions_prev{pd}d_channel__")
    if ch_last_sum > 0 and (last_total <= 0 or abs(last_total - ch_last_sum) > max(1.0, 0.05 * ch_last_sum)):
        last_total = ch_last_sum
    if ch_prev_sum > 0 and (prev_total <= 0 or abs(prev_total - ch_prev_sum) > max(1.0, 0.05 * ch_prev_sum)):
        prev_total = ch_prev_sum

    # Snapshot'ta sessions=0 iken kanal/metriklerde trafik varsa özet KPI bloğu eski/hatalıdır; skaler metriklere güven.
    if snap and last_total > 0:
        _sv = last_s.get("sessions")
        _snap_sess_bad = False
        if _sv is not None and _sv != "":
            try:
                _snap_sess_bad = float(_sv) <= 0
            except (TypeError, ValueError):
                _snap_sess_bad = False
        if _snap_sess_bad:
            last_s = {}
            prev_s = {}

    wow = _ga4_period_pct_change(last_total, prev_total)

    _org_flat_pd = _ga4_organic_from_snapshot_payload(pl)
    if _org_flat_pd is not None:
        organic_share, organic_share_pct_change = _org_flat_pd
    else:
        _org_pd = _ga4_organic_share_from_channel_maps(
            pl.get("channels_last") if isinstance(pl.get("channels_last"), dict) else None,
            pl.get("channels_prev") if isinstance(pl.get("channels_prev"), dict) else None,
        )
        if _org_pd is not None:
            organic_share = _org_pd[0]
            organic_share_pct_change = _org_pd[2]
        else:
            organic = pick(f"ga4_{profile}_sessions_last{pd}d_channel__organic_search")
            organic_prev = pick(f"ga4_{profile}_sessions_prev{pd}d_channel__organic_search")
            organic_share = (organic / last_total * 100.0) if last_total > 0 else 0.0
            organic_share_prev = (organic_prev / prev_total * 100.0) if prev_total > 0 else 0.0
            organic_share_pct_change = _ga4_period_pct_change(organic_share, organic_share_prev)

    users_last = float(
        last_s.get("totalUsers")
        or pick(f"ga4_{profile}_kpi_last_totalUsers{sk}")
        or (pick(f"ga4_{profile}_kpi_last_totalUsers") if pd == 30 else 0.0)
        or 0.0
    )
    users_prev = float(
        prev_s.get("totalUsers")
        or pick(f"ga4_{profile}_kpi_prev_totalUsers{sk}")
        or (pick(f"ga4_{profile}_kpi_prev_totalUsers") if pd == 30 else 0.0)
        or 0.0
    )
    new_users_last = float(
        last_s.get("newUsers")
        or pick(f"ga4_{profile}_kpi_last_newUsers{sk}")
        or (pick(f"ga4_{profile}_kpi_last_newUsers") if pd == 30 else 0.0)
        or 0.0
    )
    new_users_prev = float(
        prev_s.get("newUsers")
        or pick(f"ga4_{profile}_kpi_prev_newUsers{sk}")
        or (pick(f"ga4_{profile}_kpi_prev_newUsers") if pd == 30 else 0.0)
        or 0.0
    )
    engaged_last = float(
        last_s.get("engagedSessions")
        or pick(f"ga4_{profile}_kpi_last_engagedSessions{sk}")
        or (pick(f"ga4_{profile}_kpi_last_engagedSessions") if pd == 30 else 0.0)
        or 0.0
    )
    engaged_prev = float(
        prev_s.get("engagedSessions")
        or pick(f"ga4_{profile}_kpi_prev_engagedSessions{sk}")
        or (pick(f"ga4_{profile}_kpi_prev_engagedSessions") if pd == 30 else 0.0)
        or 0.0
    )
    _erl = last_s.get("engagementRate")
    if _erl is None or _erl == "":
        _erl = pick(f"ga4_{profile}_kpi_last_engagementRate{sk}") or (
            pick(f"ga4_{profile}_kpi_last_engagementRate") if pd == 30 else 0.0
        )
    engagement_rate_last_pct = _ga4_engagement_rate_pct(_erl)
    _erp = prev_s.get("engagementRate")
    if _erp is None or _erp == "":
        _erp = pick(f"ga4_{profile}_kpi_prev_engagementRate{sk}") or (
            pick(f"ga4_{profile}_kpi_prev_engagementRate") if pd == 30 else 0.0
        )
    engagement_rate_prev_pct = _ga4_engagement_rate_pct(_erp)
    avg_session_last_sec = float(
        last_s.get("averageSessionDuration")
        or pick(f"ga4_{profile}_kpi_last_averageSessionDuration{sk}")
        or (pick(f"ga4_{profile}_kpi_last_averageSessionDuration") if pd == 30 else 0.0)
        or 0.0
    )
    avg_session_prev_sec = float(
        prev_s.get("averageSessionDuration")
        or pick(f"ga4_{profile}_kpi_prev_averageSessionDuration{sk}")
        or (pick(f"ga4_{profile}_kpi_prev_averageSessionDuration") if pd == 30 else 0.0)
        or 0.0
    )
    pageviews_last = float(
        last_s.get("screenPageViews")
        or pick(f"ga4_{profile}_kpi_last_screenPageViews{sk}")
        or (pick(f"ga4_{profile}_kpi_last_screenPageViews") if pd == 30 else 0.0)
        or 0.0
    )
    pageviews_prev = float(
        prev_s.get("screenPageViews")
        or pick(f"ga4_{profile}_kpi_prev_screenPageViews{sk}")
        or (pick(f"ga4_{profile}_kpi_prev_screenPageViews") if pd == 30 else 0.0)
        or 0.0
    )

    return {
        "property_id": prop_id,
        "period_days": pd,
        "ranges": {
            "last_start": (snap or {}).get("last_start") or "",
            "last_end": (snap or {}).get("last_end") or "",
            "prev_start": (snap or {}).get("prev_start") or "",
            "prev_end": (snap or {}).get("prev_end") or "",
        },
        "last_total": last_total,
        "prev_total": prev_total,
        "wow_change_pct": wow,
        "sessions_pct_change": _ga4_period_pct_change(last_total, prev_total),
        "users_last": users_last,
        "users_prev": users_prev,
        "users_pct_change": _ga4_period_pct_change(users_last, users_prev),
        "new_users_last": new_users_last,
        "new_users_prev": new_users_prev,
        "new_users_pct_change": _ga4_period_pct_change(new_users_last, new_users_prev),
        "engaged_last": engaged_last,
        "engaged_prev": engaged_prev,
        "engaged_pct_change": _ga4_period_pct_change(engaged_last, engaged_prev),
        "engagement_rate_last_pct": engagement_rate_last_pct,
        "engagement_rate_prev_pct": engagement_rate_prev_pct,
        "engagement_rate_pct_change": _ga4_period_pct_change(engagement_rate_last_pct, engagement_rate_prev_pct),
        "avg_session_last_sec": avg_session_last_sec,
        "avg_session_prev_sec": avg_session_prev_sec,
        "avg_session_pct_change": _ga4_period_pct_change(avg_session_last_sec, avg_session_prev_sec),
        "pageviews_last": pageviews_last,
        "pageviews_prev": pageviews_prev,
        "pageviews_pct_change": _ga4_period_pct_change(pageviews_last, pageviews_prev),
        "organic_share_pct": organic_share,
        "organic_share_pct_change": organic_share_pct_change,
        "top_channels": _ga4_top_channels_with_pct_change(latest, profile, pd, pl),
        "pages_no_news": _enrich_ga4_page_rows(pl.get("pages_no_news")),
        "sources": pl.get("sources") or [],
        "daily_trend": pl.get("daily_trend")
        or {
            "dates": [],
            "sessions": [],
            "totalUsers": [],
            "engagedSessions": [],
            "engagementRate": [],
        },
        "same_weekday_kpi": pl.get("same_weekday_kpi") if isinstance(pl.get("same_weekday_kpi"), dict) else {},
        "has_snapshot": bool(snap),
        "has_period_data": bool(
            snap
            or last_total > 0
            or prev_total > 0
            or ch_last_sum > 0
            or ch_prev_sum > 0
        ),
    }


def _ga4_sites_payload(db) -> list[dict]:
    external_site_ids = _external_site_ids(db)
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    rows: list[dict] = []
    for site in sites:
        if site.id in external_site_ids:
            continue
        latest = {m.metric_type: m.value for m in get_latest_metrics(db, site.id)}
        ga4_status = get_ga4_connection_status(db, site.id)
        profiles: dict[str, dict] = {}

        for profile in ("web", "mweb", "android", "ios"):
            prop_id = str((ga4_status.get("properties") or {}).get(profile, "") or "").strip()
            if not prop_id:
                continue

            profiles[profile] = {
                "property_id": prop_id,
                "periods": {
                    "1": _ga4_profile_payload_for_period(
                        db,
                        site_id=site.id,
                        profile=profile,
                        period_days=1,
                        latest=latest,
                        prop_id=prop_id,
                    ),
                    "7": _ga4_profile_payload_for_period(
                        db,
                        site_id=site.id,
                        profile=profile,
                        period_days=7,
                        latest=latest,
                        prop_id=prop_id,
                    ),
                    "30": _ga4_profile_payload_for_period(
                        db,
                        site_id=site.id,
                        profile=profile,
                        period_days=30,
                        latest=latest,
                        prop_id=prop_id,
                    ),
                },
            }

        rows.append(
            {
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
                "ga4": ga4_status,
                "profiles": profiles,
                "default_profile": next((k for k in ("web", "mweb", "android", "ios") if k in profiles), "web"),
            }
        )
    rows.sort(key=lambda item: _preferred_site_order_key(item.get("domain"), item.get("display_name")))
    return rows


@app.get("/ga4")
def ga4_page(request: Request):
    payload = {
        "site_name": "GA4",
        "sites": get_sidebar_sites(),
    }
    return templates.TemplateResponse(request, "ga4.html", context={"request": request, **payload})


def _ai_brief_llm_availability() -> dict[str, bool]:
    return {
        "groq": bool((settings.groq_api_key or "").strip()),
        "gemini": bool((settings.gemini_api_key or "").strip()),
    }


@app.get("/ai")
def ai_daily_brief_page(request: Request):
    from backend.services.ai_daily_brief import get_latest_brief_for_ui

    with SessionLocal() as db:
        brief = get_latest_brief_for_ui(db)
        payload = {
            "site_name": "AI",
            "sites": get_sidebar_sites(),
            "ai_brief": brief,
            "ai_brief_llm": _ai_brief_llm_availability(),
        }
    template_name = "partials/ai_content.html" if request.headers.get("HX-Request") == "true" else "ai.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.post("/ai/generate")
def ai_daily_brief_generate(request: Request, llm_provider: str = Form("groq")):
    """Operasyon: aynı gün özeti yeniden üretilir ve operasyon alıcılarına e-posta gider (Groq veya Gemini; yalnızca bu akış LLM kullanır)."""

    from backend.services.ai_daily_brief import get_latest_brief_for_ui, run_ai_daily_brief_job

    raw = (llm_provider or "groq").strip().lower()
    pov = raw if raw in ("groq", "gemini") else "groq"
    avail = _ai_brief_llm_availability()
    if not avail.get(pov):
        msg = (
            "Groq API anahtarı yapılandırılmadı."
            if pov == "groq"
            else "Gemini API anahtarı yapılandırılmadı."
        )
        return PlainTextResponse(msg, status_code=400)

    run_ai_daily_brief_job(force=True, provider_override=pov)
    if request.headers.get("HX-Request") == "true":
        with SessionLocal() as db:
            brief = get_latest_brief_for_ui(db)
            ctx = {
                "request": request,
                "site_name": "AI",
                "sites": get_sidebar_sites(),
                "ai_brief": brief,
                "ai_brief_llm": _ai_brief_llm_availability(),
            }
        return templates.TemplateResponse(request, "partials/ai_content.html", context=ctx)
    return RedirectResponse(url="/ai", status_code=303)


@app.get("/ga4/site-list")
def ga4_site_list(request: Request):
    with SessionLocal() as db:
        return templates.TemplateResponse(
            request,
            "partials/ga4_site_cards.html",
            context={"request": request, "ga4_sites": _ga4_sites_payload(db)},
        )


@app.post("/ga4/refresh/{site_id}")
def ga4_refresh_site(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        if _is_external_site(db, site.id):
            return HTMLResponse("Bu site GA4 listesinde yer almaz (external).", status_code=404)
        if not ga4_is_configured():
            return HTMLResponse("GA4 service account ayarlı değil.", status_code=503)
        conn = get_ga4_connection_status(db, site.id)
        if not conn.get("connected"):
            return HTMLResponse("Bu site için GA4 property tanımlı değil.", status_code=422)
        from backend.collectors.ga4 import collect_ga4_channel_sessions

        try:
            collect_ga4_channel_sessions(db, site, days=30)
            collect_ga4_channel_sessions(db, site, days=7)
            db.commit()
            bucket = ga4_digest_bucket_for_domain(site.domain)
            if bucket:
                send_ga4_weekly_digest_emails(
                    db,
                    trigger_source="manual",
                    action_label="GA4 verisini yenile",
                    only_buckets=frozenset({bucket}),
                )
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            bucket = ga4_digest_bucket_for_domain(site.domain)
            if bucket:
                send_ga4_weekly_digest_emails(
                    db,
                    trigger_source="manual",
                    action_label="GA4 verisini yenile",
                    only_buckets=frozenset({bucket}),
                    collect_failures=[(site.domain, str(exc))],
                )
            else:
                send_consolidated_system_email(
                    system_key="ga4",
                    trigger_source="manual",
                    action_label="GA4 verisini yenile",
                    items=[(site, {"state": "failed", "error": str(exc)})],
                )
            return HTMLResponse(f"GA4 yenileme başarısız: {exc}", status_code=500)
        return templates.TemplateResponse(
            request,
            "partials/ga4_site_cards.html",
            context={"request": request, "ga4_sites": _ga4_sites_payload(db)},
        )


@app.post("/ga4/refresh-all")
def ga4_refresh_all(request: Request):
    if not ga4_is_configured():
        return HTMLResponse("GA4 service account ayarlı değil (.env: GA4_SERVICE_ACCOUNT_FILE / JSON).", status_code=503)
    with SessionLocal() as db:
        from backend.collectors.ga4 import collect_ga4_channel_sessions

        external_site_ids = _external_site_ids(db)
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
        any_ga4_ok = False
        ga4_failures: list[tuple[str, str]] = []
        for site in sites:
            if site.id in external_site_ids:
                continue
            conn = get_ga4_connection_status(db, site.id)
            if not conn.get("connected"):
                continue
            try:
                collect_ga4_channel_sessions(db, site, days=30)
                collect_ga4_channel_sessions(db, site, days=7)
                db.commit()
                any_ga4_ok = True
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                ga4_failures.append((site.domain, str(exc)))
        if any_ga4_ok or ga4_failures:
            try:
                send_ga4_weekly_digest_emails(
                    db,
                    trigger_source="manual",
                    action_label="Tüm GA4 sitelerini yenile",
                    collect_failures=ga4_failures,
                )
            except Exception:
                logging.warning("GA4 refresh-all: bildirim maili gönderilemedi, atlanıyor.")
        return templates.TemplateResponse(
            request,
            "partials/ga4_site_cards.html",
            context={"request": request, "ga4_sites": _ga4_sites_payload(db)},
        )


@app.get("/ga4/pages/{site_id}")
def ga4_pages_partial(request: Request, site_id: int):
    profile = (request.query_params.get("profile") or "").strip().lower()
    raw_days = (request.query_params.get("days") or "").strip()
    try:
        days = int(raw_days) if raw_days else 30
    except ValueError:
        days = 30

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        if _is_external_site(db, site.id):
            return HTMLResponse("Bu site GA4 listesinde yer almaz (external).", status_code=404)

        ga4_status = get_ga4_connection_status(db, site.id)
        properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}
        property_id = str(properties.get(profile) or "").strip()
        if not property_id:
            return HTMLResponse("Bu profil için GA4 property tanımlı değil.", status_code=422)

        from backend.collectors.ga4 import fetch_ga4_landing_pages

        try:
            if days == 1:
                # 1g: grafik 7g snapshot ile aynı kalır; landing listesi ayrı — dün vs geçen haftanın aynı günü (7g snapshot pages_no_news kullanılmaz)
                rows = fetch_ga4_landing_pages(
                    property_id=property_id,
                    days=1,
                    limit=50,
                    exclude_news=True,
                    same_weekday_day=True,
                )
            else:
                snap = get_latest_ga4_report_snapshot(db, site_id=site.id, profile=profile, period_days=days)
                snap_pages = ((snap or {}).get("payload") or {}).get("pages_no_news") or []
                if snap_pages:
                    rows = snap_pages
                else:
                    rows = fetch_ga4_landing_pages(property_id=property_id, days=days, limit=50, exclude_news=True)
            rows = _enrich_ga4_page_rows(rows)
        except Exception as exc:  # noqa: BLE001
            return HTMLResponse(f"GA4 sayfa verisi çekilemedi: {exc}", status_code=500)

        return templates.TemplateResponse(
            request,
            "partials/ga4_pages_table.html",
            context={
                "request": request,
                "rows": rows,
                "days": days,
                "profile": profile,
                "property_id": property_id,
                "site_domain": site.domain,
            },
        )


@app.get("/ga4/sources/{site_id}")
def ga4_sources_partial(request: Request, site_id: int):
    profile = (request.query_params.get("profile") or "").strip().lower()
    raw_days = (request.query_params.get("days") or "").strip()
    try:
        days = int(raw_days) if raw_days else 30
    except ValueError:
        days = 30

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        if _is_external_site(db, site.id):
            return HTMLResponse("Bu site GA4 listesinde yer almaz (external).", status_code=404)

        ga4_status = get_ga4_connection_status(db, site.id)
        properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}
        property_id = str(properties.get(profile) or "").strip()
        if not property_id:
            return HTMLResponse("Bu profil için GA4 property tanımlı değil.", status_code=422)

        from backend.collectors.ga4 import fetch_ga4_session_source_medium

        try:
            if days == 1:
                rows = fetch_ga4_session_source_medium(
                    property_id=property_id,
                    days=1,
                    limit=50,
                    same_weekday_day=True,
                )
            else:
                snap = get_latest_ga4_report_snapshot(db, site_id=site.id, profile=profile, period_days=days)
                snap_sources = ((snap or {}).get("payload") or {}).get("sources") or []
                if snap_sources:
                    rows = snap_sources
                else:
                    rows = fetch_ga4_session_source_medium(
                        property_id=property_id,
                        days=days,
                        limit=50,
                        same_weekday_day=False,
                    )
        except Exception as exc:  # noqa: BLE001
            return HTMLResponse(f"GA4 kaynak/ortam verisi çekilemedi: {exc}", status_code=500)

        return templates.TemplateResponse(
            request,
            "partials/ga4_sources_table.html",
            context={
                "request": request,
                "rows": rows,
                "days": days,
                "profile": profile,
                "property_id": property_id,
                "site_domain": site.domain,
            },
        )


@app.get("/settings/alert-thresholds")
def settings_alert_thresholds(request: Request):
    # HTMX ile alert threshold tablosunu yeniler.
    with SessionLocal() as db:
        alert_rules = get_alert_rules(db)
    return templates.TemplateResponse(request, "partials/alert_thresholds.html", context={"request": request, "alert_rules": alert_rules})


@app.get("/search-console")
def search_console_page(request: Request):
    # Site kartları HTMX ile lazy load edildiğinden burada ağır veri hesabı yapılmaz.
    site_list_mode = "eager" if str(request.query_params.get("refresh_complete") or "").strip() == "1" else "lazy"
    payload = {
        "site_name": "Search Console",
        "sites": get_sidebar_sites(),
        "oauth_ready": oauth_is_configured(),
        "oauth_redirect_uri": settings.google_oauth_redirect_uri,
        "site_list_mode": site_list_mode,
    }
    return templates.TemplateResponse(request, "search_console.html", context={"request": request, **payload})


@app.get("/search-console/site-list")
def search_console_site_list(request: Request):
    """Site listesini anlık render eder; her kart lazy HTMX ile ayrı yüklenir."""
    mode = str(request.query_params.get("mode") or "lazy").strip().lower()
    with SessionLocal() as db:
        external_ids = _external_site_ids(db)
        sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_ids]
        sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        if mode == "eager":
            schedule_label = (
                f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
                f"{int(settings.search_console_scheduled_refresh_minute):02d}"
            )
            search_console_sites = [_search_console_single_site_data(db, site, schedule_label) for site in sites]
            return templates.TemplateResponse(
                request,
                "partials/search_console_site_cards.html",
                context={
                    "request": request,
                    "lazy_mode": False,
                    "search_console_sites": search_console_sites,
                    "oauth_ready": oauth_is_configured(),
                },
            )
        lazy_site_ids = [(s.id, s.display_name) for s in sites]
    return templates.TemplateResponse(
        request,
        "partials/search_console_site_cards.html",
        context={
            "request": request,
            "lazy_mode": True,
            "lazy_site_ids": lazy_site_ids,
            "oauth_ready": oauth_is_configured(),
        },
    )


@app.get("/search-console/site/{site_id}", response_class=HTMLResponse)
def search_console_single_site_card(request: Request, site_id: int):
    """HTMX lazy loading ile tek site kartını tam veriyle render eder."""
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("", status_code=404)
        try:
            # site_count: tek COUNT sorgusu, tüm objeleri çekme
            external_ids = _external_site_ids(db)
            from sqlalchemy import func as sqlfunc
            site_count = db.query(sqlfunc.count(Site.id)).filter(Site.id.notin_(external_ids)).scalar() or 1
            schedule_label = (
                f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
                f"{int(settings.search_console_scheduled_refresh_minute):02d}"
            )
            site_data = _search_console_single_site_data(db, site, schedule_label)
        except Exception as exc:
            logging.exception("search_console_single_site_card site_id=%s hata", site_id)
            import html as _html
            err_msg = _html.escape(f"{type(exc).__name__}: {exc}")
            return HTMLResponse(
                f'<section id="sc-card-{site_id}" class="rounded-3xl border border-red-300 dark:border-red-700 '
                f'bg-red-50 dark:bg-red-900/30 p-5 text-sm text-red-700 dark:text-red-300">'
                f'<p class="font-semibold">Kart yüklenemedi</p>'
                f'<p class="mt-1 text-xs">Site #{site_id} verisi hazırlanırken hata oluştu. '
                f'Sayfayı yenileyerek tekrar deneyin.</p>'
                f'<p class="mt-2 text-xs opacity-70 font-mono break-all">{err_msg}</p></section>',
                status_code=200,
            )
    return templates.TemplateResponse(
        request,
        "partials/sc_single_site_card.html",
        context={
            "request": request,
            "site": site_data,
            "oauth_ready": oauth_is_configured(),
            "site_count": site_count,
        },
    )


@app.get("/search-console/health")
def search_console_health():
    # UI yanlışlıkla /search-console/health çağırırsa, /health ile uyumlu cevap ver.
    return {"status": "ok"}


@app.post("/search-console/cwv-screenshot/upload/{site_id}", response_class=HTMLResponse)
async def search_console_upload_cwv_screenshot(
    request: Request,
    site_id: int,
    variant: str = "full",
    file: UploadFile = File(...),
):
    """Manuel CWV screenshot yükleme (local)."""
    variant = str(variant or "full").strip().lower()
    if variant not in {"full", "mobile", "desktop", "extra"}:
        return HTMLResponse("Geçersiz variant.", status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        domain_slug = _gsc_domain_slug(site.domain)

    name = (file.filename or "").lower()
    if not (name.endswith(".png") or name.endswith(".jpg") or name.endswith(".jpeg") or name.endswith(".webp")):
        return HTMLResponse("Sadece png/jpg/webp kabul edilir.", status_code=400)

    content = await file.read()
    if not content:
        return HTMLResponse("Boş dosya.", status_code=400)
    if len(content) > 10 * 1024 * 1024:
        return HTMLResponse("Dosya çok büyük (max 10MB).", status_code=413)

    # PNG/JPG magic check (basit)
    if not (
        content.startswith(b"\x89PNG\r\n\x1a\n")
        or content.startswith(b"\xff\xd8\xff")
        or content.startswith(b"RIFF")  # webp container
    ):
        return HTMLResponse("Dosya tipi doğrulanamadı.", status_code=400)

    GSC_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-{variant}.png"
    out_path.write_bytes(content)

    return search_console_single_site_card(request, site_id)


@app.post("/search-console/cwv-screenshot/delete/{site_id}", response_class=HTMLResponse)
def search_console_delete_cwv_screenshot(request: Request, site_id: int, variant: str = "full"):
    """Manuel yüklenen CWV screenshot dosyasını diskten siler (local)."""
    variant = str(variant or "full").strip().lower()
    if variant not in {"full", "mobile", "desktop", "extra"}:
        return HTMLResponse("Geçersiz variant.", status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        domain_slug = _gsc_domain_slug(site.domain)

    path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-{variant}.png"
    try:
        if path.exists():
            path.unlink()
    except OSError as exc:
        return HTMLResponse(f"Silinemedi: {exc}", status_code=500)

    return search_console_single_site_card(request, site_id)


@app.post("/search-console/refresh-all")
def search_console_refresh_all(request: Request):
    with SessionLocal() as db:
        external = _external_site_ids(db)
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
        sc_batch: list[tuple[Site, dict]] = []
        for site in sites:
            if site.id in external:
                continue
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
                if isinstance(results.get("search_console"), dict):
                    sc_batch.append((site, results["search_console"]))
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                sc_batch.append((site, {"state": "failed", "error": str(exc)}))
        if sc_batch:
            try:
                send_consolidated_system_email(
                    system_key="search_console",
                    trigger_source="manual",
                    action_label="Tüm Search Console sitelerini yenile",
                    items=sc_batch,
                    db=db,
                )
            except Exception:
                logging.warning("SC refresh-all: bildirim maili gönderilemedi, atlanıyor.")
        # Refresh-all sonrası her kart lazy yeniden yüklenir
        external_ids = _external_site_ids(db)
        sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_ids]
        sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        return templates.TemplateResponse(
            request,
            "partials/search_console_site_cards.html",
            context={
                "request": request,
                "lazy_mode": True,
                "lazy_site_ids": [(s.id, s.display_name) for s in sites],
                "oauth_ready": oauth_is_configured(),
            },
        )


@app.post("/search-console/refresh/{site_id}")
def search_console_manual_refresh(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        if _is_external_site(db, site.id):
            return HTMLResponse("Bu site için Search Console raporu gönderilmez (external).", status_code=404)
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
        # Yalnızca bu site kartını döndür — hx-target="closest section" veya #sc-card-{id}
        schedule_label = (
            f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
            f"{int(settings.search_console_scheduled_refresh_minute):02d}"
        )
        external_ids = _external_site_ids(db)
        site_count = len([s for s in db.query(Site).all() if s.id not in external_ids])
        site_data = _search_console_single_site_data(db, site, schedule_label)
        return templates.TemplateResponse(
            request,
            "partials/sc_single_site_card.html",
            context={
                "request": request,
                "site": site_data,
                "oauth_ready": oauth_is_configured(),
                "site_count": site_count,
            },
        )


@app.post("/search-console/disconnect/{site_id}")
def search_console_disconnect_from_header(request: Request, site_id: int):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)
        delete_oauth_credentials(db, site_id)
        schedule_label = (
            f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
            f"{int(settings.search_console_scheduled_refresh_minute):02d}"
        )
        external_ids = _external_site_ids(db)
        site_count = len([s for s in db.query(Site).all() if s.id not in external_ids])
        site_data = _search_console_single_site_data(db, site, schedule_label)
        return templates.TemplateResponse(
            request,
            "partials/sc_single_site_card.html",
            context={
                "request": request,
                "site": site_data,
                "oauth_ready": oauth_is_configured(),
                "site_count": site_count,
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


def _run_db_retention_cleanup() -> dict:
    """Tüm zaman serisi tablolar için eski verileri temizler.

    Her tablo grubu için strateji:
    - 'keep_latest': Her site (+ varsa ek group key) için sadece son N snapshot kalır
    - 'keep_days': Belirtilen gün sayısından eski satırlar silinir
    """
    from sqlalchemy import and_, func as sqlfunc, text

    stats: dict[str, int] = {}

    # ── keep_latest tabloları: her site+group için sadece son snapshot ──
    keep_latest_tables = [
        # (Model, group_columns, label)
        (SearchConsoleQuerySnapshot, [SearchConsoleQuerySnapshot.data_scope], "search_console_query_snapshots"),
        (UrlAuditRecord, [], "url_audit_records"),
        (LighthouseAuditRecord, [LighthouseAuditRecord.strategy], "lighthouse_audit_records"),
        (PageSpeedPayloadSnapshot, [PageSpeedPayloadSnapshot.strategy], "pagespeed_payload_snapshots"),
        (PageSpeedAuditSnapshot, [PageSpeedAuditSnapshot.strategy], "pagespeed_audit_snapshots"),
        (Ga4ReportSnapshot, [], "ga4_report_snapshots"),
        (CruxHistorySnapshot, [CruxHistorySnapshot.form_factor], "crux_history_snapshots"),
        (UrlInspectionSnapshot, [], "url_inspection_snapshots"),
    ]

    with SessionLocal() as db:
        for Model, group_cols, table_label in keep_latest_tables:
            try:
                group_keys = [Model.site_id] + group_cols
                latest_sub = (
                    db.query(
                        *group_keys,
                        sqlfunc.max(Model.collected_at).label("max_ts"),
                    )
                    .group_by(*group_keys)
                    .subquery()
                )

                join_conds = [Model.site_id == latest_sub.c.site_id]
                for col in group_cols:
                    join_conds.append(col == latest_sub.c[col.key])

                old_rows = (
                    db.query(Model.id)
                    .join(latest_sub, and_(*join_conds))
                    .filter(Model.collected_at < latest_sub.c.max_ts)
                    .all()
                )
                old_ids = [r[0] for r in old_rows]
                if old_ids:
                    # Batch delete
                    batch_size = 500
                    for i in range(0, len(old_ids), batch_size):
                        db.query(Model).filter(Model.id.in_(old_ids[i:i + batch_size])).delete(synchronize_session=False)
                    db.commit()
                stats[table_label] = len(old_ids)
            except Exception:
                logging.exception("Retention cleanup hatası: %s", table_label)
                db.rollback()
                stats[table_label] = -1

        # ── keep_days tabloları: belirli gün sayısından eski satırlar ──
        keep_days_tables = [
            # (Model, time_column, days, label)
            (CollectorRun, CollectorRun.requested_at, settings.db_retention_collector_run_days, "collector_runs"),
            (AlertLog, AlertLog.triggered_at, settings.db_retention_alert_log_days, "alert_logs"),
            (Metric, Metric.collected_at, settings.db_retention_metric_days, "metrics"),
            (
                NotificationDeliveryLog,
                NotificationDeliveryLog.sent_at,
                settings.db_retention_notification_delivery_days,
                "notification_delivery_logs",
            ),
        ]
        cutoff_now = datetime.utcnow()
        for Model, time_col, days, table_label in keep_days_tables:
            try:
                cutoff = cutoff_now - timedelta(days=days)
                count = db.query(Model).filter(time_col < cutoff).delete(synchronize_session=False)
                db.commit()
                stats[table_label] = count
            except Exception:
                logging.exception("Retention cleanup hatası: %s", table_label)
                db.rollback()
                stats[table_label] = -1

        # PostgreSQL'de disk alanını geri al
        if not _IS_SQLITE:
            try:
                # VACUUM autocommit gerektirir, session dışında çalıştır
                from backend.database import engine
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    conn.execute(text("VACUUM ANALYZE"))
                stats["vacuum"] = 0
            except Exception:
                logging.exception("VACUUM ANALYZE hatası")
                stats["vacuum"] = -1

    total_deleted = sum(v for v in stats.values() if v > 0)
    logging.info("DB retention cleanup tamamlandı — toplam %d satır silindi: %s", total_deleted, stats)
    return stats


def _run_scheduled_db_cleanup_job() -> None:
    """APScheduler tarafından çağrılan wrapper."""
    try:
        _run_db_retention_cleanup()
    except Exception:
        logging.exception("Zamanlanmış DB cleanup hatası")


@app.post("/admin/cleanup-sc-snapshots")
def admin_cleanup_sc_snapshots(request: Request):
    """Tüm tablolardaki eski verileri temizler (geriye uyumlu endpoint)."""
    stats = _run_db_retention_cleanup()
    return JSONResponse({"status": "ok", "details": stats})


@app.post("/admin/vacuum")
def admin_vacuum():
    """PostgreSQL VACUUM çalıştırır — disk alanını geri kazanır."""
    if _IS_SQLITE:
        return JSONResponse({"status": "skip", "reason": "sqlite"})
    from sqlalchemy import text
    from backend.database import engine
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("VACUUM ANALYZE"))
        return JSONResponse({"status": "ok"})
    except Exception as exc:
        logging.exception("VACUUM hatası")
        return JSONResponse({"status": "error", "detail": str(exc)})


@app.post("/admin/truncate-sc-snapshots")
def admin_truncate_sc_snapshots():
    """search_console_query_snapshots tablosunu tamamen boşaltır (disk alanını anında geri kazanır)."""
    from sqlalchemy import text
    from backend.database import engine
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("TRUNCATE TABLE search_console_query_snapshots RESTART IDENTITY"))
        return JSONResponse({"status": "ok", "message": "Tablo boşaltıldı. Tüm Siteleri Yenile ile veri tekrar çekilebilir."})
    except Exception as exc:
        logging.exception("TRUNCATE hatası")
        return JSONResponse({"status": "error", "detail": str(exc)})


@app.get("/admin/db-size")
def admin_db_size():
    """PostgreSQL veritabanı boyutunu ve tablo bazlı kullanımı gösterir."""
    from sqlalchemy import text
    if _IS_SQLITE:
        import os
        db_path = settings.database_url.replace("sqlite:///", "")
        size_bytes = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        return JSONResponse({"total_mb": round(size_bytes / 1024 / 1024, 2), "engine": "sqlite"})

    from backend.database import engine
    result = {}
    with engine.connect() as conn:
        # Toplam DB boyutu
        row = conn.execute(text("SELECT pg_database_size(current_database())")).fetchone()
        result["total_mb"] = round(row[0] / 1024 / 1024, 2) if row else 0

        # WAL dizini (pg_wal dolunca recovery yazamaz; volume artır veya retention kısalt)
        try:
            wal_row = conn.execute(text("SELECT COALESCE(SUM(size), 0) FROM pg_ls_waldir()")).fetchone()
            result["wal_size_mb"] = round((wal_row[0] or 0) / 1024 / 1024, 2) if wal_row else 0
        except Exception:
            result["wal_size_mb"] = None

        # Tablo bazlı boyut
        tables = conn.execute(text(
            "SELECT relname, pg_total_relation_size(relid) AS size "
            "FROM pg_catalog.pg_statio_user_tables ORDER BY size DESC"
        )).fetchall()
        result["tables"] = [{"table": t[0], "size_mb": round(t[1] / 1024 / 1024, 2)} for t in tables]

    return JSONResponse(result)


@app.get("/admin/sc-scope-stats")
def admin_sc_scope_stats():
    """SC query snapshot tablosundaki scope/device dağılımını gösterir (debug)."""
    from sqlalchemy import text
    db = SessionLocal()
    try:
        if _IS_SQLITE:
            rows = db.execute(text(
                "SELECT site_id, data_scope, device, COUNT(*) as cnt "
                "FROM search_console_query_snapshots "
                "GROUP BY site_id, data_scope, device "
                "ORDER BY site_id, data_scope, device"
            )).fetchall()
        else:
            rows = db.execute(text(
                "SELECT site_id, data_scope, device, COUNT(*) as cnt "
                "FROM search_console_query_snapshots "
                "GROUP BY site_id, data_scope, device "
                "ORDER BY site_id, data_scope, device"
            )).fetchall()
        return JSONResponse({
            "scopes": [
                {"site_id": r[0], "data_scope": r[1], "device": r[2], "count": r[3]}
                for r in rows
            ]
        })
    finally:
        db.close()


@app.get("/health")
def health_check():
    # Basit sağlık kontrol endpoint'i JSON döner.
    return JSONResponse({"status": "ok", "host": settings.app_host})


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
