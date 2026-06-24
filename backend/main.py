"""FastAPI uygulama giriş noktası."""
import json
import hashlib
import hmac
import logging
import os
import re
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_UP
from ipaddress import ip_address, ip_network
from pathlib import Path
from urllib.parse import parse_qsl, quote, unquote, urlparse
from uuid import uuid4
from typing import Any
from zoneinfo import ZoneInfo
import httpx

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Localhost development için insecure OAuth transport'u allow et
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from fastapi import BackgroundTasks, FastAPI, File, Form, Request, Response, UploadFile, Depends
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
from sqlalchemy.exc import OperationalError, PendingRollbackError
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler
from sqlalchemy.orm import Session

from backend.api.alerts import router as alerts_router
from backend.api.ga4 import router as ga4_router
from backend.api.metrics import router as metrics_router
from backend.api.sites import router as sites_router
from backend.api.inbox import router as inbox_router
from backend.api.backlinks import router as backlinks_router
from backend.api.store_catalog import router as store_catalog_router
from backend.api.notification_analytics import router as notification_analytics_router
from backend.api.ad_analytics import router as ad_analytics_router
from backend.api.market_quotes import router as market_quotes_router
from backend.api.member_auth import router as member_auth_router
from backend.collectors.crawler import collect_crawler_metrics
from backend.collectors.crux_history import collect_crux_history
from backend.collectors.pagespeed import (
    STRATEGY_METRIC_MAP,
    collect_pagespeed_metrics,
    fetch_live_lighthouse_category_scores,
    get_latest_pagespeed_audit_snapshot,
)
from backend.collectors.search_console import (
    _resolve_search_console_targets,
    collect_search_console_alert_metrics,
    collect_search_console_metrics,
    get_top_queries,
)
from backend.collectors.url_inspection import collect_url_inspection
from backend.config import is_railway_runtime, settings
from backend.services.panel_auth import panel_session_granted
from backend.database import SessionLocal, _IS_SQLITE, init_db, get_db
from backend.models import (
    Alert, AlertLog, CollectorRun, CruxHistorySnapshot, ExternalOnboardingJob,
    ExternalSite, Ga4ReportSnapshot, LighthouseAuditRecord, Metric,
    NotificationDeliveryLog, PageSpeedAuditSnapshot, PageSpeedPayloadSnapshot,
    RealtimeAlarmLog, RealtimeNewsSnapshot, RealtimePageSnapshot, RealtimeSnapshot,
    SearchConsoleQuerySnapshot, Site, SiteCredential, SiteErrorLog, UrlAuditRecord, UrlInspectionSnapshot, AdminAuthSetting,
    AppMember,
    AppStoreRankSnapshot, AiDailyBriefReport, AiBriefRunLog, AppIntelRawCache,
    SupportInboxThread, SupportInboxMessage,
)
from backend.rate_limiter import limiter
from backend.services.alert_engine import ensure_site_alerts, get_alert_rules, get_recent_alerts, get_site_alerts
from backend.services.metric_store import get_latest_metrics, get_metric_history, get_metric_day_over_day_score
from backend.services.quota_guard import get_quota_status
from backend.services.search_console_auth import (
    SEARCH_CONSOLE_SCOPES,
    build_oauth_flow,
    decode_oauth_state,
    delete_oauth_credentials,
    encode_oauth_state,
    get_search_console_connection_status,
    oauth_saved_at_for_site,
    search_console_last_run_error_for_ui,
    get_search_console_credentials_record,
    load_google_credentials,
    oauth_is_configured,
    save_oauth_credentials,
)
from backend.services.ga4_auth import ga4_is_configured, get_ga4_connection_status
from backend.services.pagespeed_analyzer import analyze_pagespeed_alerts
from backend.services.pagespeed_detailed import analyze_pagespeed_detailed
from backend.services.lighthouse_analyzer import get_lighthouse_analysis
from backend.services.ga4_digest_email import ga4_digest_bucket_for_domain, send_ga4_weekly_digest_emails
from backend.services.ga4_page_urls import (
    enrich_ga4_page_rows as _enrich_ga4_page_rows,
    ga4_fallback_page_url as _ga4_fallback_page_url,
    ga4_row_news_display_text as _ga4_row_news_display_text,
    ga4_row_page_href as _ga4_row_page_href,
    ga4_row_page_label as _ga4_row_page_label,
    ga4_site_host as _ga4_site_host,
    ga4_url_match_keys as _ga4_url_match_keys,
)
from backend.services.operations_notifier import (
    notify_crawler_audit_emails,
    notify_crawler_audit_emails_batch,
    notify_missed_scheduled_refreshes,
    notify_result_map,
    notify_system_trigger,
    send_consolidated_system_email,
)
from backend.services.search_console_reports import (
    SC_VIEW_SPECS,
    fetch_sc_analytics_report,
    fetch_sc_sitemaps,
    inspect_sc_url,
    sc_extra_card_should_render,
    sc_extra_views_for_nav,
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
_APP_REVISION_CACHE: str | None = None
# Search Console HTML (sayfa + HTMX parçaları): tarayıcı önbelleği eski kart göstermesin; F5 gerçekten DB’den gelsin.
_SC_HTML_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
}
_SC_HTML_OMIT_HEADERS = {
    **_SC_HTML_NO_CACHE_HEADERS,
    "HX-Reswap": "delete",
}
_SC_JSON_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
}

_SC_REFRESH_ALL_LOCK = threading.Lock()
# Toplu GSC yenileme: uzun HTTP isteği tarayıcı/proxy tarafından kesilmesin diye arka planda çalışır.
_SC_REFRESH_ALL_JOB: dict | None = None
_SC_REFRESH_ALL_STALE_SECONDS = 50 * 60

_GA4_REFRESH_ALL_LOCK = threading.Lock()
_GA4_REFRESH_ALL_JOB: dict | None = None
_GA4_REFRESH_ALL_STALE_SECONDS = 50 * 60
_GA4_JSON_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
}


def _search_console_request_wants_json(request: Request) -> bool:
    """Yenileme fetch'leri 401/500 gövdesini JSON okuyabilsin (admin middleware + hata ayrıntısı)."""
    if (request.headers.get("hx-request") or "").lower() == "true":
        return True
    return "application/json" in (request.headers.get("accept") or "").lower()


def get_app_revision() -> str:
    """Çalışan sürecin kod sürümü (Railway env, docker build, yerel git). Arayüz güncellenmiyorsa /health ile karşılaştır."""
    global _APP_REVISION_CACHE
    if _APP_REVISION_CACHE is not None:
        return _APP_REVISION_CACHE
    for env_key in ("RAILWAY_GIT_COMMIT_SHA", "RAILWAY_GIT_COMMIT", "GIT_COMMIT", "APP_GIT_REV"):
        raw = (os.environ.get(env_key) or "").strip()
        if raw:
            _APP_REVISION_CACHE = raw if len(raw) <= 12 else raw[:12]
            return _APP_REVISION_CACHE
    try:
        proc = subprocess.run(
            ["git", "-C", str(BASE_DIR), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
        if proc.returncode == 0 and (proc.stdout or "").strip():
            _APP_REVISION_CACHE = proc.stdout.strip()
            return _APP_REVISION_CACHE
    except Exception:
        pass
    _APP_REVISION_CACHE = "unknown"
    return _APP_REVISION_CACHE


def _is_docker_runtime() -> bool:
    return Path("/.dockerenv").is_file()


def _request_uses_https(request: Request) -> bool:
    """Uvicorn http gösterse bile (Railway edge) TLS: X-Forwarded-Proto ve eşdeğer başlıklar."""
    proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    if proto in ("https", "on", "1"):
        return True
    if (request.headers.get("x-forwarded-ssl") or "").strip() in ("1", "on", "true", "True"):
        return True
    return (request.url.scheme or "").lower() == "https"


def _admin_auth_cookie_secure(request: Request) -> bool:
    # Secure=True iken http://127.0.0.1 gibi HTTP oturumları çerez kaydetmez; sadece gerçekten TLS kırılımında aç.
    return _request_uses_https(request)


DAILY_REFRESH_LOCK = threading.Lock()
SEO_AUDIT_JOB_LOCK = threading.Lock()
APP_INTEL_REFRESH_LOCK = threading.Lock()
INBOX_SYNC_LOCK = threading.Lock()
SCHEDULER: BackgroundScheduler | None = None
EXTERNAL_ONBOARDING_JOB_TTL_SECONDS = 1800
EXTERNAL_ONBOARDING_MAX_RUNNING_SECONDS = 300

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


def _format_signed_pct_tr(value) -> str:
    """Yüzde farklarını TR biçimde gösterir: +41.328,18 / -4,2 / 0."""
    if value is None or value == "":
        return "N/A"
    if isinstance(value, str):
        return value
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return str(value)

    clipped = decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if clipped == 0:
        return "0"

    abs_text = f"{abs(float(clipped)):,.2f}"
    # EN 12,345.60 -> TR 12.345,60
    abs_text = abs_text.replace(",", "_").replace(".", ",").replace("_", ".")
    if abs_text.endswith(",00"):
        abs_text = abs_text[:-3]
    elif abs_text.endswith("0"):
        abs_text = abs_text[:-1]

    return f"+{abs_text}" if clipped > 0 else f"-{abs_text}"


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


def _sc_extra_page_label(url, domain="") -> str:
    """SC extras tablolarinda URL'den kisa path etiketi."""
    from urllib.parse import urlparse

    u = str(url or "").strip()
    if not u:
        return "—"
    try:
        p = urlparse(u)
        path = p.path or "/"
        if p.query:
            path = f"{path}?{p.query}"
        host = (p.netloc or "").lower()
        dom = str(domain or "").lower().replace("www.", "")
        if dom and (host == dom or host == f"www.{dom}" or dom in host):
            return path
        return u
    except Exception:
        return u


jinja_env.filters["exact"] = _format_exact
jinja_env.filters["max_two_decimals"] = _format_max_two_decimals
jinja_env.filters["exact_signed"] = _format_exact_signed
jinja_env.filters["signed_max_two_decimals"] = _format_signed_max_two_decimals
jinja_env.filters["signed_pct_tr"] = _format_signed_pct_tr
jinja_env.filters["seconds_exact"] = _ms_to_exact_seconds
jinja_env.filters["tr_int"] = _format_tr_int
jinja_env.filters["ga4_abs_page_url"] = _filter_ga4_abs_page_url
jinja_env.filters["ga4_site_root"] = _filter_ga4_site_root
jinja_env.filters["ga4_source_href"] = _filter_ga4_source_href
jinja_env.filters["ga4_iso_ddmmyy"] = _filter_ga4_iso_ddmmyy
jinja_env.filters["ga4_iso_ddmmyyyy"] = _filter_ga4_iso_ddmmyyyy
jinja_env.filters["ga4_row_page_href"] = _ga4_row_page_href
jinja_env.filters["ga4_row_page_label"] = _ga4_row_page_label
jinja_env.filters["sc_extra_page_label"] = _sc_extra_page_label
jinja_env.filters["ga4_row_news_display_text"] = _ga4_row_news_display_text


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


def _ai_brief_stacked_p(body: str, *, p_class: str, highlight: bool = False):
    """\\n\\n ile ayrılmış paragrafları <p> olarak döndür; tek paragraf içi \\n → <br />."""
    from markupsafe import Markup

    chunks = [c.strip() for c in body.split("\n\n") if c.strip()]
    if not chunks:
        return Markup("")
    out: list[str] = []
    for chunk in chunks:
        inner = _ai_brief_nl2br(chunk)
        if highlight:
            inner = Markup(_ai_brief_highlight_metrics_html(str(inner)))
        out.append(f'<p class="{p_class}">{inner}</p>')
    return Markup("".join(out))


def _ai_brief_parse_metric_number(num_s: str) -> float | None:
    try:
        return float(num_s.replace(",", ".").strip())
    except ValueError:
        return None


def _ai_brief_metric_direction_class(value: float) -> str:
    if value > 0:
        return "ai-metric ai-metric--up"
    if value < 0:
        return "ai-metric ai-metric--down"
    return "ai-metric ai-metric--flat"


def _ai_brief_highlight_metrics_html(escaped_html: str) -> str:
    """escape + br sonrası güvenli HTML üzerinde değişim yüzdeleri ve trend sözcüklerini renklendir."""
    import re

    s = escaped_html

    def _num_repl(m: re.Match[str]) -> str:
        prefix, num_s = m.group(1), m.group(2)
        val = _ai_brief_parse_metric_number(num_s)
        if val is None:
            return m.group(0)
        cls = _ai_brief_metric_direction_class(val)
        return f'{prefix}<span class="{cls}">{num_s}</span>'

    # GA4 özetinde sık geçen kalıplar (İngilizce / varyasyon toleransı)
    patterns = [
        r"(?i)(değişim\s*%:\s*)(-?[\d.,]+)",
        r"(?i)(change\s*%:\s*)(-?[\d.,]+)",
        r"(?i)(%\s*değişim:\s*)(-?[\d.,]+)",
    ]
    for pat in patterns:
        s = re.sub(pat, _num_repl, s)

    s = re.sub(
        r"(?i)\b(düşüş|azalma|düşen|kayıp|negatif|düşük)\b",
        r'<span class="ai-kw ai-kw--down">\1</span>',
        s,
    )
    s = re.sub(
        r"(?i)\b(artış|yükseliş|yükselen|büyüme|pozitif)\b",
        r'<span class="ai-kw ai-kw--up">\1</span>',
        s,
    )
    return s


def _ai_brief_line_is_kpi_bullet(line: str) -> bool:
    t = line.lstrip()
    if not t:
        return False
    if t[0] in ("•", "·", "▪", "\u2022"):
        return True
    return t.startswith(("- ", "– "))


def _ai_brief_strip_kpi_bullet(line: str) -> str:
    s = line.strip()
    for pref in ("•", "·", "▪", "\u2022"):
        if s.startswith(pref):
            return s[len(pref):].strip()
    if s.startswith("- "):
        return s[2:].strip()
    if s.startswith("– "):
        return s[2:].strip()
    return s


def _ai_brief_rakamlar_body_markup(rest: str, *, body_cls: str):
    """RAKAMLAR: madde işaretli satırları listeye çevir; metrikleri renklendir."""
    from markupsafe import Markup

    lines = [ln.rstrip() for ln in rest.splitlines() if ln.strip()]
    if len(lines) >= 2 and all(_ai_brief_line_is_kpi_bullet(ln) for ln in lines):
        lis: list[str] = []
        for ln in lines:
            raw = _ai_brief_strip_kpi_bullet(ln)
            inner = _ai_brief_highlight_metrics_html(str(_ai_brief_nl2br(raw)))
            lis.append(f'<li class="ai-brief-kpi-item">{inner}</li>')
        return Markup(
            f'<ul class="ai-brief-kpi-list" role="list" aria-label="Rakamlar">{"".join(lis)}</ul>'
        )
    inner = _ai_brief_highlight_metrics_html(str(_ai_brief_nl2br(rest.strip())))
    return Markup(f'<div class="ai-brief-rakamlar-flow {body_cls}">{inner}</div>')


def _ai_brief_html_paragraphs(value: str | None):
    """AI özet metnini çift satır sonundan paragraflara böler; light/dark uyumlu kutular."""
    from markupsafe import Markup, escape

    if not value or not str(value).strip():
        return Markup("")
    normalized = _ai_brief_normalize_breaks(str(value))
    parts = [p.strip() for p in normalized.split("\n\n") if p.strip()]
    blocks: list[str] = []
    label_cls = (
        "mb-1.5 text-[11px] font-bold uppercase tracking-wide text-slate-600 dark:text-zinc-400"
    )
    label_rakamlar_cls = (
        "mb-1.5 text-[11px] font-bold uppercase tracking-wide text-emerald-800 dark:text-emerald-300/95"
    )
    body_cls = "text-[13px] leading-relaxed text-slate-800 dark:text-zinc-100"
    stacked_p_cls = (
        "mb-2.5 last:mb-0 text-[13px] leading-relaxed text-slate-800 dark:text-zinc-100"
    )
    plain_cls = f"mb-3 {body_cls}"
    # Yarı saydam değil: dark modda metin/beyaz karışımı ve düşük kontrast önlenir.
    card_durum = (
        "mb-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2.5 "
        "dark:border-zinc-600 dark:bg-zinc-900/80"
    )
    card_neutral = (
        "mb-3 rounded-xl border border-slate-200 bg-white px-3 py-2.5 shadow-sm shadow-slate-200/40 "
        "dark:border-zinc-600 dark:bg-zinc-900 dark:shadow-none"
    )
    for p in parts:
        head = p.lstrip()
        up = head.upper()
        with_br = _ai_brief_nl2br(p)
        if up.startswith("DURUM:") or up.startswith("RAKAMLAR:"):
            first_ln, _, rest = p.partition("\n")
            if rest.strip():
                if up.startswith("RAKAMLAR:"):
                    lbl = escape(first_ln.strip())
                    body = _ai_brief_rakamlar_body_markup(rest.strip(), body_cls=body_cls)
                    blocks.append(
                        f'<div class="{card_durum} ai-brief-card ai-brief-card--rakamlar">'
                        f'<p class="{label_rakamlar_cls}">{lbl}</p><div class="{body_cls}">{body}</div></div>'
                    )
                else:
                    lbl = escape(first_ln.strip())
                    body = _ai_brief_stacked_p(rest.strip(), p_class=stacked_p_cls, highlight=True)
                    blocks.append(
                        f'<div class="{card_durum} ai-brief-card ai-brief-card--durum">'
                        f'<p class="{label_cls}">{lbl}</p><div class="{body_cls} space-y-0">{body}</div></div>'
                    )
            else:
                blocks.append(f'<div class="{plain_cls}">{with_br}</div>')
        elif up.startswith("NE ANLAMA GELİYOR:") or up.startswith("ÖNCELİK:"):
            first_ln, _, rest = p.partition("\n")
            if rest.strip():
                lbl = escape(first_ln.strip())
                body = _ai_brief_stacked_p(rest.strip(), p_class=stacked_p_cls, highlight=True)
                blocks.append(
                    f'<div class="{card_neutral} ai-brief-card ai-brief-card--explain">'
                    f'<p class="{label_cls}">{lbl}</p><div class="{body_cls} space-y-0">{body}</div></div>'
                )
            else:
                blocks.append(f'<div class="{plain_cls}">{with_br}</div>')
        else:
            inner = _ai_brief_highlight_metrics_html(str(with_br))
            blocks.append(f'<div class="{plain_cls}">{inner}</div>')
    return Markup("".join(blocks))


jinja_env.filters["ai_brief_sites"] = _ai_brief_sites_filter
jinja_env.filters["ai_brief_html_paragraphs"] = _ai_brief_html_paragraphs
templates = Jinja2Templates(env=jinja_env)
app = FastAPI(title="SEO Agent Dashboard")

from starlette.middleware.gzip import GZipMiddleware

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Starlette varsayılan multipart parça limiti 1MB; büyük GSC CSV importları için artır.
try:
    from starlette.formparsers import MultiPartParser

    # Reklam raporu: tek xlsx ~10–40 MB; 12 dosya sırayla veya toplu multipart parça limiti
    _MULTIPART_MAX_BYTES = 128 * 1024 * 1024
    MultiPartParser.max_part_size = _MULTIPART_MAX_BYTES
    if hasattr(MultiPartParser, "max_file_size"):
        MultiPartParser.max_file_size = _MULTIPART_MAX_BYTES
except ImportError:
    pass


@app.get("/api/admin/omdb-test")
def admin_omdb_test():
    """OMDB API key doğrulama + örnek sorgu."""
    from backend.services.omdb import _api_key, _fetch_omdb
    try:
        key = _api_key()
        data = _fetch_omdb("The Godfather", "1972")
        if data:
            return {
                "status": "ok",
                "key_prefix": key[:6] + "***",
                "test_movie": data.get("Title"),
                "imdb_rating": data.get("imdbRating"),
                "rt": next((r["Value"] for r in data.get("Ratings", []) if "Rotten" in r["Source"]), None),
            }
        return {"status": "error", "message": "OMDB boş yanıt döndü — key geçersiz olabilir"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/omdb-enrich-now")
def admin_omdb_enrich_now():
    """OMDB zenginleştirmeyi MANUEL tetikler (max 999 film)."""
    try:
        from backend.services.omdb import run_daily_omdb_enrichment
        with SessionLocal() as db:
            result = run_daily_omdb_enrichment(db)
        return {"status": "ok", **result}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/refresh-tmdb-cache")
def admin_refresh_tmdb_cache():
    """TMDB vizyon takvimi cache'ini MANUEL yeniler."""
    try:
        from backend.services.tmdb import refresh_combined_cache
        result = refresh_combined_cache(months_ahead=5)
        return {
            "status": "ok",
            "theatrical": len(result.get("theatrical", [])),
            "streaming":  len(result.get("streaming", [])),
            "tv_series":  len(result.get("tv_series", [])),
            "turkish_only": len(result.get("turkish_only", [])),
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/run-inbox-summary-now")
def admin_run_inbox_summary_now():
    """Inbox özet mailini MANUEL tetikler (5 sekme: doviz, sinemalar, medya, nstat, firebase)."""
    from backend.services.inbox_summary import run_inbox_summary_email

    try:
        with SessionLocal() as db:
            ok = run_inbox_summary_email(db)
        return {
            "status": "ok" if ok else "skipped",
            "message": "Inbox özet maili gönderildi." if ok else "Mail gönderilmedi (kapalı veya Gmail yok).",
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/run-inbox-sync-now")
def admin_run_inbox_sync_now():
    """Inbox Gmail → DB senkronunu MANUEL tetikler (zamanlanmış job ile aynı yol)."""
    try:
        _run_inbox_scheduled_sync_job()
        with SessionLocal() as db:
            from backend.services import inbox_gmail_auth

            row = inbox_gmail_auth.get_inbox_credential_row(db)
            last = row.scheduled_sync_last_success_at.isoformat() if row and row.scheduled_sync_last_success_at else None
        return {"status": "ok", "message": "Inbox senkron tamamlandı.", "scheduled_sync_last_success_at": last}
    except Exception as exc:
        LOGGER.exception("admin run-inbox-sync-now")
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/scheduler-status")
def admin_scheduler_status():
    """APScheduler job listesi — gece/periyodik işlerin ayakta olduğunu doğrulamak için."""
    jobs: list[dict] = []
    if SCHEDULER is not None:
        for job in SCHEDULER.get_jobs():
            nxt = job.next_run_time
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": nxt.isoformat() if nxt else None,
                    "trigger": str(job.trigger),
                }
            )
        jobs.sort(key=lambda j: (j.get("id") or ""))
    return {
        "status": "ok",
        "scheduler_running": SCHEDULER is not None and bool(getattr(SCHEDULER, "running", False)),
        "job_count": len(jobs),
        "jobs": jobs,
        "data_explorer": _data_explorer_scheduler_health(),
        "seo_audit": _seo_audit_scheduler_health(),
        "inbox_sync_interval_minutes": settings.inbox_scheduled_sync_interval_minutes,
        "ga4_scheduled_kpi_period_days": settings.ga4_scheduled_kpi_period_days,
    }


@app.get("/api/admin/run-seo-audit-now")
def admin_run_seo_audit_now():
    """Günlük SEO audit job'unu MANUEL tetikler (zamanlanmış akış ile aynı)."""
    try:
        _run_seo_audit_job()
        return {"status": "ok", "message": "SEO audit job arka planda başlatıldı."}
    except Exception as exc:
        LOGGER.exception("admin run-seo-audit-now")
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/run-daily-refresh-now")
def admin_run_daily_refresh_now():
    """Günlük PSI + CrUX + crawler yenilemesini MANUEL tetikler (zamanlanmış job ile aynı akış)."""
    try:
        _run_daily_refresh_job()
        return {"status": "ok", "message": "Günlük site yenilemesi tamamlandı."}
    except Exception as exc:
        LOGGER.exception("admin run-daily-refresh-now")
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/run-news-intelligence-now")
def admin_run_news_intelligence_now():
    """Haber istihbarat taramasını MANUEL olarak tetikler (Sıfırlayarak)."""
    from backend.services.news_intelligence import run_news_intelligence_job
    from backend.database import SessionLocal
    from backend.models import NewsIntelligenceItem
    try:
        # Önce mevcut içeriği tamamen sil
        with SessionLocal() as db:
            db.query(NewsIntelligenceItem).delete()
            db.commit()
        # Sonra yeni taramayı başlat
        run_news_intelligence_job()
        return {"status": "ok", "message": "RESET_SUCCESS_9H"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/admin/news-intelligence/sources")
def get_news_intelligence_sources(hours: int | None = None, sort: str = "name", order: str = "asc"):
    """Son N saatte haber içeren kaynak adlarını (ve adet) döner — dropdown için."""
    from backend.menu_excluded import is_menu_excluded_label
    from backend.services.news_intelligence import RETENTION_HOURS

    with SessionLocal() as db:
        from backend.models import NewsIntelligenceItem
        from sqlalchemy import func
        import datetime as _dt

        try:
            hours_int = int(hours) if hours is not None else RETENTION_HOURS
        except (TypeError, ValueError):
            hours_int = RETENTION_HOURS

        query = (
            db.query(
                NewsIntelligenceItem.source_name,
                func.count(NewsIntelligenceItem.id).label("count"),
            )
            .filter(NewsIntelligenceItem.source_name.notin_(["Unknown", "Bilinmiyor", ""]))
            .group_by(NewsIntelligenceItem.source_name)
        )
        if hours_int > 0:
            cutoff = _dt.datetime.utcnow() - _dt.timedelta(hours=hours_int)
            query = query.filter(NewsIntelligenceItem.published_at >= cutoff)

        rows = query.all()
        sources = [
            {"name": name, "count": int(count or 0)}
            for name, count in rows
            if name and not is_menu_excluded_label(name)
        ]

        sort_key = (sort or "name").strip().lower()
        order_key = (order or "asc").strip().lower()
        reverse = order_key == "desc"
        if sort_key == "count":
            sources.sort(key=lambda x: (x["count"], x["name"].casefold()), reverse=reverse)
        else:
            sources.sort(key=lambda x: x["name"].casefold(), reverse=reverse)

        return {"sources": sources}


@app.get("/api/admin/news-intelligence/list")
def get_news_intelligence(
    category: str = None,
    source: str = None,
    limit: int = 24,
    offset: int = 0,
    since: str = None,
    hours: int | None = None,
):
    """Veritabanındaki haber istihbaratı verilerini döner.
    Varsayılan: son `hours` saatlik haberler (zaman bazlı), offset/limit ile lazyload.
    `since` parametresi auto-refresh için kullanılır (sadece bu zamandan sonrasını döner).
    """
    from backend.services.news_intelligence import RETENTION_HOURS
    from backend.menu_excluded import is_menu_excluded_label

    with SessionLocal() as db:
        from backend.models import NewsIntelligenceItem
        from backend.services.news_intelligence import dedupe_news_rows
        from sqlalchemy import desc
        import datetime as _dt
        if source and is_menu_excluded_label(source):
            return {"items": []}
        query = db.query(NewsIntelligenceItem).order_by(desc(NewsIntelligenceItem.published_at))
        if category:
            query = query.filter(NewsIntelligenceItem.category == category)
        if source:
            query = query.filter(NewsIntelligenceItem.source_name == source)
        query = query.filter(
            NewsIntelligenceItem.source_name.notin_(["Unknown", "Bilinmiyor", ""])
        )
        try:
            hours_int = int(hours) if hours is not None else RETENTION_HOURS
        except (TypeError, ValueError):
            hours_int = RETENTION_HOURS
        if hours_int > 0:
            cutoff = _dt.datetime.utcnow() - _dt.timedelta(hours=hours_int)
            query = query.filter(NewsIntelligenceItem.published_at >= cutoff)
        if since:
            try:
                since_dt = _dt.datetime.fromisoformat(since.replace("Z", "+00:00")).replace(tzinfo=None)
                query = query.filter(NewsIntelligenceItem.published_at > since_dt)
            except Exception:
                pass
            items = query.limit(50).all()
            items = dedupe_news_rows(items)
        else:
            safe_limit = min(limit, 50)
            collected: list = []
            cur_offset = offset
            rounds = 0
            while len(collected) < safe_limit and rounds < 6:
                chunk = query.offset(cur_offset).limit(max(safe_limit * 2, 24)).all()
                if not chunk:
                    break
                for row in chunk:
                    collected.append(row)
                collected = dedupe_news_rows(collected)
                cur_offset += len(chunk)
                rounds += 1
                if len(chunk) < max(safe_limit * 2, 24):
                    break
            items = collected[:safe_limit]
        items = [item for item in items if not is_menu_excluded_label(item.source_name)]
        return {
            "items": [
                {
                    "id": item.id,
                    "headline": item.headline,
                    "url": item.url,
                    "content": item.content,
                    "source_name": item.source_name,
                    "source_url": item.source_url,
                    "image_url": item.image_url,
                    "category": item.category,
                    "topic": item.topic,
                    "is_in_our_site": item.is_in_our_site,
                    "ai_note": item.ai_note,
                    "published_at": item.published_at.isoformat() if item.published_at else None
                }
                for item in items
            ]
        }


@app.post("/api/admin/news-intelligence/sync")
def sync_news_intelligence(reset: bool = False):
    """Haberleri manuel olarak tarar ve veritabanına kaydeder."""
    from backend.services.news_intelligence import run_news_intelligence_job
    try:
        run_news_intelligence_job(reset=reset)
        return {"status": "ok", "message": "News intelligence sync completed successfully."}
    except Exception as e:
        logger.exception("Manual news sync failed")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico() -> RedirectResponse:
    return RedirectResponse(url="/static/favicon.png", status_code=307)


@app.get("/apple-touch-icon.png", include_in_schema=False)
def apple_touch_icon() -> RedirectResponse:
    return RedirectResponse(url="/static/apple-touch-icon.png", status_code=307)


# Static mount dosya sonunda (Starlette: Mount genelde en sonda; aksi halde bazı rotalar 404 dönebilir).
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# API routers
app.include_router(alerts_router, prefix="/api")
app.include_router(metrics_router, prefix="/api")
app.include_router(sites_router, prefix="/api")
app.include_router(ga4_router, prefix="/api")
app.include_router(store_catalog_router, prefix="/api")
app.include_router(inbox_router, prefix="/api")
app.include_router(backlinks_router, prefix="/api")
app.include_router(notification_analytics_router, prefix="/api")
app.include_router(ad_analytics_router, prefix="/api")
app.include_router(market_quotes_router, prefix="/api")

from backend.karma.router import router as karma_router

app.include_router(karma_router)
app.include_router(member_auth_router)

PERIOD_DAYS_MAP = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
}


def _resolve_period(raw_period: str | None) -> tuple[str, int]:
    normalized = (raw_period or "weekly").strip().lower()
    aliases = {
        "day": "daily",
        "week": "weekly",
        "month": "monthly",
        "daily": "daily",
        "weekly": "weekly",
        "monthly": "monthly",
    }
    period = aliases.get(normalized, "weekly")
    return period, PERIOD_DAYS_MAP[period]


def _dashboard_sc_scopes_for_url_period(period: str) -> tuple[str, str]:
    """Dashboard `period` (daily|weekly|monthly) → Search Console snapshot scope çifti."""
    if period == "daily":
        return "current_day", "previous_week_same_weekday"
    if period == "weekly":
        return "current_7d", "previous_7d"
    return "current_30d", "previous_30d"


def _dashboard_period_to_sc_segment(period: str) -> str:
    return {"daily": "1", "weekly": "7", "monthly": "30"}.get(period, "7")


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


def _crux_history_latest_collected_at(db, site_id: int) -> datetime | None:
    from sqlalchemy import func

    ts = (
        db.query(func.max(CruxHistorySnapshot.collected_at))
        .filter(CruxHistorySnapshot.site_id == site_id)
        .scalar()
    )
    return ts if isinstance(ts, datetime) else None


def _psi_lighthouse_metrics_latest_collected_at(db, site_id: int) -> datetime | None:
    latest = {m.metric_type: m for m in get_latest_metrics(db, site_id)}
    times: list[datetime] = []
    for key in ("pagespeed_mobile_score", "pagespeed_desktop_score"):
        m = latest.get(key)
        if m is not None and getattr(m, "collected_at", None):
            times.append(m.collected_at)
    return max(times) if times else None


def _extract_client_ip(request: Request) -> str:
    # Proxy arkasında çalışırken gerçek istemci IP'sini alır.
    if settings.trust_proxy_headers:
        forwarded = request.headers.get("x-forwarded-for", "").strip()
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else ""


def _is_loopback_direct_client(request: Request) -> bool:
    """Sadece TCP peer loopback mi (127.0.0.1 / ::1). X-Forwarded-For kullanılmaz; sahte başlıkla bypass riski olmasın."""
    direct = (request.client.host or "").strip() if request.client else ""
    if not direct:
        return False
    try:
        return bool(ip_address(direct).is_loopback)
    except ValueError:
        return False


def _request_target_host_looks_local(request: Request) -> bool:
    """Tarayıcıda 127.0.0.1 / localhost ile açılmış mı (Host başlığı).

    Docker veya reverse proxy arkasında TCP istemcisi 172.x olsa da Host genelde 127.0.0.1:port gelir;
    sadece _is_loopback_direct_client kullanınca ilk kurulum /settings döngüsü kırılmıyordu.
    """
    raw = (request.headers.get("host") or "").strip()
    if not raw:
        return False
    if raw.startswith("["):
        end = raw.find("]")
        if end > 1:
            try:
                return bool(ip_address(raw[1:end]).is_loopback)
            except ValueError:
                return False
        return False
    host = raw
    if ":" in host:
        pre, last = host.rsplit(":", 1)
        if last.isdigit():
            host = pre
    h = host.lower()
    if h in ("localhost", "127.0.0.1", "::1"):
        return True
    try:
        return bool(ip_address(h).is_loopback)
    except ValueError:
        return False


def _is_local_dev_first_password_client(request: Request) -> bool:
    """İlk admin şifresi (henüz DB'de yok) için yerel tarayıcı / loopback TCP."""
    return _is_loopback_direct_client(request) or _request_target_host_looks_local(request)


def _may_set_or_update_admin_password(request: Request) -> bool:
    """Allowlist / oturum VEYA (şifre hiç yokken yalnızca yerel ilk kurulum)."""
    if not _admin_auth_active():
        return True
    if _is_admin_authenticated(request):
        return True
    with SessionLocal() as db:
        if _admin_password_configured(db):
            return False
    return _is_local_dev_first_password_client(request)


_ADMIN_AUTH_COOKIE = "seo_admin_auth"
_SETTINGS_AUTH_COOKIE = "seo_settings_auth"
_INBOX_ACTION_AUTH_COOKIE = "seo_inbox_action_auth"

# ── Aktif oturum takibi ──────────────────────────────────────────────────────
# Anahtar: cookie token'ın SHA-256 hash'i (ilk 16 karakter). Değer: oturum meta.
_active_sessions: dict[str, dict] = {}
_SESSION_IDLE_MINUTES = 30  # Bu süreden uzun süre istek gelmezse oturumu sil


def _session_key(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:16]


def _parse_device(ua: str) -> str:
    from backend.services.admin_access_log import parse_device_label

    return parse_device_label(ua)


def _current_panel_session_key(request: Request | None) -> str:
    if not request:
        return ""
    from backend.services import app_member_auth as ama

    member_tok = str(request.cookies.get(ama.APP_MEMBER_COOKIE) or "")
    if member_tok:
        return "m:" + _session_key(member_tok)
    admin_tok = str(request.cookies.get(_ADMIN_AUTH_COOKIE) or "")
    if admin_tok:
        return "a:" + _session_key(admin_tok)
    return ""


def _record_session(request: Request) -> None:
    from backend.services import app_member_auth as ama

    member = _app_member_from_request(request)
    member_tok = str(request.cookies.get(ama.APP_MEMBER_COOKIE) or "")
    admin_tok = str(request.cookies.get(_ADMIN_AUTH_COOKIE) or "")

    key = ""
    email = ""
    label = ""
    session_kind = ""

    if member and member_tok:
        key = "m:" + _session_key(member_tok)
        email = (member.email or "").strip()
        label = (member.display_name or member.email or "").strip()
        session_kind = "member"
    elif admin_tok and _is_admin_authenticated(request):
        key = "a:" + _session_key(admin_tok)
        label = "Admin şifre"
        session_kind = "admin"
    else:
        return

    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=_SESSION_IDLE_MINUTES)
    dead = [k for k, v in _active_sessions.items() if v.get("last_seen", now) < cutoff]
    for k in dead:
        del _active_sessions[k]
    ip = _extract_client_ip(request)
    ua = request.headers.get("user-agent", "")
    if key in _active_sessions:
        _active_sessions[key]["last_seen"] = now
        _active_sessions[key]["ip"] = ip
        if email:
            _active_sessions[key]["email"] = email
        if label:
            _active_sessions[key]["label"] = label
    else:
        _active_sessions[key] = {
            "ip": ip,
            "device": _parse_device(ua),
            "user_agent": ua[:512],
            "first_seen": now,
            "last_seen": now,
            "email": email,
            "label": label,
            "session_kind": session_kind,
        }
    from backend.services import admin_access_log as aal

    fp = aal.device_fingerprint(ip, ua)
    aal.record_admin_nav(fp, (request.url.path or ""))


def get_online_presence_api_payload(request: Request | None = None) -> dict:
    from backend.services import app_member_auth as ama
    from backend.services.panel_presence import build_online_presence_api_payload

    sessions = _get_active_sessions(request)
    return build_online_presence_api_payload(
        sessions,
        viewer_emails=ama.ONLINE_PRESENCE_VIEWER_EMAILS,
    )


def _get_active_sessions(request: Request | None = None) -> list[dict]:
    from backend.services import admin_access_log as aal

    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=_SESSION_IDLE_MINUTES)
    current_key = _current_panel_session_key(request)
    trusted_fps: set[str] = set()
    try:
        with SessionLocal() as db:
            trusted_fps = aal.trusted_fingerprints(db)
    except Exception:
        trusted_fps = set()
    rows = sorted(
        [
            {**v, "key": k}
            for k, v in _active_sessions.items()
            if v.get("last_seen", now) >= cutoff
        ],
        key=lambda x: x["last_seen"],
        reverse=True,
    )
    return [
        aal.enrich_active_session(
            row,
            trusted_fps=trusted_fps,
            current_key=current_key,
            session_key=str(row.get("key") or ""),
        )
        for row in rows
    ]


def _is_settings_authenticated(request: Request) -> bool:
    """Settings sayfası: allowlist üyeleri veya admin şifre + isteğe bağlı settings şifresi."""
    from backend.services.settings_menu_access import is_settings_menu_allowed_email

    member = _app_member_from_request(request)
    if member is not None:
        if not is_settings_menu_allowed_email(member.email):
            return False
        return True
    if not _is_admin_authenticated(request):
        return False
    raw_pwd = (getattr(settings, "settings_password", "") or "").strip()
    if not raw_pwd:
        return True
    token = str(request.cookies.get(_SETTINGS_AUTH_COOKIE) or "")
    if not token:
        return False
    secret = str(getattr(settings, "secret_key", "") or "").encode("utf-8")
    expected = hmac.new(secret, raw_pwd.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    return hmac.compare_digest(token, expected)


def _member_denied_settings_menu(request: Request) -> bool:
    from backend.services.settings_menu_access import member_denied_settings_access

    member = _app_member_from_request(request)
    if member is None:
        return False
    return member_denied_settings_access(member.email)


def _inbox_action_token(raw_pwd: str) -> str:
    from backend.services.inbox_action_auth import inbox_action_token

    return inbox_action_token(raw_pwd)


def _is_inbox_action_authenticated(request: Request) -> bool:
    from backend.services.inbox_action_auth import is_inbox_action_authenticated

    return is_inbox_action_authenticated(request)


def _admin_auth_row(db) -> AdminAuthSetting | None:
    return db.query(AdminAuthSetting).order_by(AdminAuthSetting.id.asc()).first()


def _admin_password_configured(db) -> bool:
    row = _admin_auth_row(db)
    return bool(row and row.password_hash and row.password_salt)


def _hash_admin_password(raw_password: str, salt_hex: str) -> str:
    key = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        260_000,
    )
    return key.hex()


def _verify_admin_password(db, raw_password: str) -> bool:
    row = _admin_auth_row(db)
    if not row or not row.password_hash or not row.password_salt:
        return False
    actual = _hash_admin_password(raw_password, row.password_salt)
    return hmac.compare_digest(actual, row.password_hash)


def _upsert_admin_password(db, raw_password: str) -> None:
    row = _admin_auth_row(db)
    salt_hex = os.urandom(16).hex()
    pwd_hash = _hash_admin_password(raw_password, salt_hex)
    if not row:
        row = AdminAuthSetting(password_hash=pwd_hash, password_salt=salt_hex, updated_at=datetime.utcnow())
        db.add(row)
    else:
        row.password_hash = pwd_hash
        row.password_salt = salt_hex
        row.updated_at = datetime.utcnow()
    _commit_with_lock_retry(db, attempts=5, base_wait=0.15)


def _build_admin_cookie_token(password_hash: str) -> str:
    secret = str(getattr(settings, "secret_key", "") or "").encode("utf-8")
    return hmac.new(secret, password_hash.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()


def _is_admin_authenticated(request: Request) -> bool:
    token = str(request.cookies.get(_ADMIN_AUTH_COOKIE) or "")
    if not token:
        return False
    with SessionLocal() as db:
        row = _admin_auth_row(db)
        if not row or not row.password_hash:
            return False
        expected = _build_admin_cookie_token(row.password_hash)
    return hmac.compare_digest(token, expected)


def _app_member_from_request(request: Request) -> AppMember | None:
    from backend.services import app_member_auth as ama

    return ama.member_from_request(request)


def _app_member_authenticated(request: Request) -> bool:
    return _app_member_from_request(request) is not None


def _is_app_panel_authenticated(request: Request) -> bool:
    """Uygulama kapısı: admin şifresi veya Google üye oturumu (site SC/GA4 OAuth ayrı)."""
    return _is_admin_authenticated(request) or _app_member_authenticated(request)


def _is_membership_admin(request: Request) -> bool:
    if _is_admin_authenticated(request):
        return True
    from backend.services import app_member_auth as ama

    return ama.is_membership_admin(request)


def _bootstrap_admin_password_from_env() -> None:
    """Veritabanında admin hash yoksa .env ADMIN_PASSWORD (config: admin_bootstrap_password) ile doldurur."""
    raw = (getattr(settings, "admin_bootstrap_password", None) or "").strip()
    if len(raw) < 6:
        return
    try:
        with SessionLocal() as db:
            if _admin_password_configured(db):
                return
            _upsert_admin_password(db, raw)
        LOGGER.info("Admin parolası ADMIN_PASSWORD ile veritabanına bootstrap edildi (ilk kurulum).")
    except Exception as exc:
        LOGGER.warning("ADMIN_PASSWORD veritabanına yazılamadı: %s", exc)



def _admin_auth_active() -> bool:
    """Şablonlar için: Railway'de zorunlu; yerelde ADMIN_AUTH_ENFORCED."""
    if is_railway_runtime():
        return True
    return bool(settings.admin_auth_enforced)


def _request_host(request: Request) -> str:
    from backend.services.panel_auth import request_host

    return request_host(request)


def _auth_gate_enabled(request: Request) -> bool:
    from backend.services.panel_auth import auth_gate_enabled

    return auth_gate_enabled(request)


def _panel_redirect_login() -> RedirectResponse:
    return RedirectResponse(url="/admin/login", status_code=303)


def _ensure_panel_session(request: Request) -> RedirectResponse | None:
    """Middleware dışı savunma: oturum yoksa login."""
    if not _auth_gate_enabled(request):
        return None
    with SessionLocal() as db:
        password_ready = _admin_password_configured(db)
    if panel_session_granted(
        password_ready=password_ready,
        admin_authenticated=_is_admin_authenticated(request),
        member_authenticated=_app_member_authenticated(request),
    ):
        return None
    return _panel_redirect_login()


jinja_env.globals["admin_access_ui"] = _admin_auth_active


def _template_is_tmdb_guest_view(request: Request | None) -> bool:
    if request is None:
        return False
    return bool(getattr(request.state, "tmdb_guest_view", False))


jinja_env.globals["is_tmdb_guest_view"] = _template_is_tmdb_guest_view


def _template_settings_menu_visible(request: Request | None) -> bool:
    if request is None:
        return False
    return bool(getattr(request.state, "settings_menu_visible", False))


jinja_env.globals["settings_menu_visible"] = _template_settings_menu_visible


def _template_online_presence_visible(request: Request | None) -> bool:
    from backend.services import app_member_auth as ama

    return ama.can_view_online_presence(request)


jinja_env.globals["online_presence_visible"] = _template_online_presence_visible


def _tmdb_guest_login_response(request: Request, *, redirect_path: str) -> RedirectResponse:
    from backend.services.tmdb_guest_auth import TMDB_GUEST_COOKIE, guest_cookie_value

    resp = RedirectResponse(url=redirect_path, status_code=303)
    resp.set_cookie(
        key=TMDB_GUEST_COOKIE,
        value=guest_cookie_value(),
        httponly=True,
        secure=_admin_auth_cookie_secure(request),
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
        path="/",
    )
    return resp


@app.middleware("http")
async def ip_allowlist_middleware(request: Request, call_next):
    # Allowlist IP'ler doğrudan geçer; diğerleri admin parolası ile giriş yapar.
    if not _auth_gate_enabled(request):
        return await call_next(request)
    path = (request.url.path or "").strip()
    public_prefixes = (
        "/health",
        "/static/",
        "/favicon",
        "/apple-touch-icon",
        "/admin/login",
        "/admin/auth/login",
        "/auth/google/",
    )
    if any(path.startswith(prefix) for prefix in public_prefixes):
        return await call_next(request)

    with SessionLocal() as db:
        password_ready = _admin_password_configured(db)
    # İlk admin şifresi yokken: yerel tarayıcı (Host: 127.0.0.1 veya loopback TCP) /settings ve şifre formuna gidebilsin.
    if not password_ready and _is_local_dev_first_password_client(request):
        if (path == "/admin/password" and request.method == "POST") or path.startswith("/settings"):
            return await call_next(request)
    panel_authed = panel_session_granted(
        password_ready=password_ready,
        admin_authenticated=_is_admin_authenticated(request),
        member_authenticated=_app_member_authenticated(request),
    )
    if panel_authed:
        _record_session(request)
        member = _app_member_from_request(request)
        if member is not None:
            request.state.app_member = member
        from backend.services.settings_menu_access import resolve_settings_menu_visible

        request.state.settings_menu_visible = resolve_settings_menu_visible(
            member_email=member.email if member else None,
            admin_authenticated=_is_admin_authenticated(request),
        )
        if path.startswith("/settings") and not _is_settings_authenticated(request):
            if _member_denied_settings_menu(request):
                return RedirectResponse(url="/admin/settings-denied", status_code=303)
            return RedirectResponse(url="/admin/settings-login", status_code=303)
        return await call_next(request)

    from backend.services import tmdb_guest_auth as tga

    if tga.guest_access_configured():
        if path.startswith(tga.TMDB_GUEST_PATH) and request.query_params.get("guest_logout") == "1":
            dest = "/admin/login"
            resp = RedirectResponse(url=dest, status_code=303)
            resp.delete_cookie(tga.TMDB_GUEST_COOKIE, path="/")
            return resp

        access_key = request.query_params.get("access", "")
        if path.startswith(tga.TMDB_GUEST_PATH) and access_key:
            if tga.access_query_matches(access_key):
                qs = []
                months = request.query_params.get("months")
                if months:
                    qs.append(f"months={months}")
                redirect_path = tga.TMDB_GUEST_PATH + ("?" + "&".join(qs) if qs else "")
                return _tmdb_guest_login_response(request, redirect_path=redirect_path)
            return RedirectResponse(url="/admin/login?guest=invalid", status_code=303)

        if tga.is_tmdb_guest_authenticated(request):
            if path.startswith("/api/"):
                return JSONResponse(status_code=403, content={"detail": "Misafir erişimi yalnızca vizyon takvimi."})
            if not tga.guest_path_allowed(path):
                return RedirectResponse(url=tga.TMDB_GUEST_PATH, status_code=303)
            request.state.tmdb_guest_view = True
            return await call_next(request)

    wants_json = request.headers.get("hx-request") == "true" or "application/json" in (
        request.headers.get("accept", "").lower()
    )
    if wants_json:
        return JSONResponse(
            status_code=401,
            content={
                "detail": "Admin girişi gerekli.",
                "client_ip": _extract_client_ip(request),
            },
        )
    resp = RedirectResponse(url="/admin/login", status_code=303)
    resp.headers["X-Seo-Panel-Auth"] = "required"
    return resp

# (legacy) Router include blokları yukarı taşındı.


@app.on_event("startup")
def on_startup() -> None:
    """Uygulama başlarken gerekli kontrolleri yapar."""
    # Her migration statement ayrı bağlantıda çalışır — önceki hata sonrakini etkilemez
    try:
        from sqlalchemy import text
        from backend.database import engine
        for stmt in [
            "ALTER TABLE news_intelligence_items ADD COLUMN source_url VARCHAR(512)",
            "ALTER TABLE news_intelligence_items ADD COLUMN image_url VARCHAR(1024)",
            "CREATE INDEX IF NOT EXISTS ix_news_intel_published_at ON news_intelligence_items (published_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_news_intel_cat_pub ON news_intelligence_items (category, published_at DESC)",
            # AdPolicyViolation: CSV import için eklenen kolonlar
            "ALTER TABLE ad_policy_violations ADD COLUMN page_title VARCHAR(500) NOT NULL DEFAULT ''",
            "ALTER TABLE ad_policy_violations ADD COLUMN page_title_fetched_at TIMESTAMP",
            "ALTER TABLE ad_policy_violations ADD COLUMN extra_json TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE ad_policy_violations ADD COLUMN first_seen_at TIMESTAMP",
            # Eski satırlar için first_seen_at boşsa fetched_at'i geriye yaz (geriye dönük olarak hiçbiri "yeni" sayılmasın)
            "UPDATE ad_policy_violations SET first_seen_at = fetched_at WHERE first_seen_at IS NULL",
            # Unique constraint — duplicate engelle (mevcut duplicate varsa hata verir, pas geçilir)
            "ALTER TABLE ad_policy_violations ADD CONSTRAINT uq_adpolicy_url_issue UNIQUE (url, issue_type)",
            # RealtimeAlarmLog.email_sent_at — cooldown sadece mail atılan alarmları saysın
            "ALTER TABLE realtime_alarm_logs ADD COLUMN email_sent_at TIMESTAMP",
            "CREATE INDEX IF NOT EXISTS ix_realtime_alarm_logs_email_sent_at ON realtime_alarm_logs (email_sent_at)",
            "ALTER TABLE inbox_gmail_credentials ADD COLUMN scheduled_sync_last_success_at TIMESTAMP",
            "ALTER TABLE support_inbox_messages ADD COLUMN body_html TEXT DEFAULT ''",
        ]:
            try:
                with engine.connect() as _conn:
                    _conn.execute(text(stmt))
                    _conn.commit()
            except Exception:
                pass
    except Exception as e:
        LOGGER.warning("Startup migration hatası: %s", e)

    # Startup logic continued
    # Uygulama açılışında tablolar create_all ile hazırlanır.
    global SCHEDULER
    init_db()
    _bootstrap_admin_password_from_env()
    if is_railway_runtime():
        LOGGER.info(
            "Panel auth: Railway ortamı — giriş zorunlu (ADMIN_AUTH_ENFORCED=%s yok sayılır).",
            settings.admin_auth_enforced,
        )
    elif not settings.admin_auth_enforced:
        LOGGER.warning(
            "ADMIN_AUTH_ENFORCED=false — panel girişi KAPALI (yalnızca güvenli yerel geliştirme için)."
        )
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
    try:
        from backend.services.app_intel import prewarm_app_intel_cache_background

        prewarm_app_intel_cache_background()
    except Exception:
        LOGGER.exception("app_intel prewarm registration failed")

    # Crashlytics BQ cache'ini arka planda ısıt — /firebase açılışı anlık olsun
    def _prewarm_crashlytics():
        try:
            from backend.services import crashlytics_bq as cbq
            cbq.prewarm_cache("doviz")
        except Exception as exc:
            LOGGER.warning("Crashlytics startup prewarm hatası: %s", exc)

    import threading as _threading
    _threading.Thread(target=_prewarm_crashlytics, daemon=True, name="crashlytics-prewarm-startup").start()

    # TMDB vizyon takvimi cache'ini arka planda ısıt (ilk sayfa açılışı hızlı olsun)
    def _prewarm_tmdb():
        try:
            from backend.services.tmdb import refresh_combined_cache
            refresh_combined_cache()
        except Exception as exc:
            LOGGER.warning("TMDB startup prewarm hatası: %s", exc)

    import threading as _threading
    _threading.Thread(target=_prewarm_tmdb, daemon=True, name="tmdb-prewarm").start()

    if settings.inbox_startup_sync_enabled:

        def _startup_inbox_sync() -> None:
            delay = max(10, int(settings.inbox_startup_sync_delay_seconds))
            time.sleep(delay)
            try:
                _run_inbox_scheduled_sync_job()
            except Exception as exc:
                LOGGER.warning("Startup inbox sync: %s", exc)

        _threading.Thread(target=_startup_inbox_sync, daemon=True, name="inbox-startup-sync").start()

    if settings.market_sheets_startup_sync_enabled:

        def _startup_market_sheets_sync() -> None:
            delay = max(15, int(settings.market_sheets_startup_sync_delay_seconds))
            time.sleep(delay)
            try:
                _run_market_sheets_sync_job(trigger_source="startup")
            except Exception as exc:
                LOGGER.warning("Startup market sheets sync: %s", exc)

        _threading.Thread(target=_startup_market_sheets_sync, daemon=True, name="market-sheets-startup").start()


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


_SITE_DOMAIN_ALIASES: dict[str, tuple[str, ...]] = {
    "doviz.com": ("doviz.com", "www.doviz.com"),
    "www.doviz.com": ("www.doviz.com", "doviz.com"),
    "sinemalar.com": ("sinemalar.com", "www.sinemalar.com"),
    "www.sinemalar.com": ("www.sinemalar.com", "sinemalar.com"),
}


def _site_domain_candidates(domain: str) -> list[str]:
    raw = str(domain or "").strip().lower()
    if not raw:
        return []
    aliases = _SITE_DOMAIN_ALIASES.get(raw)
    if aliases:
        return list(dict.fromkeys(aliases))
    return [raw]


def _resolve_site_by_domain(db, domain: str) -> Site | None:
    from sqlalchemy import func

    for candidate in _site_domain_candidates(domain):
        site = db.query(Site).filter(Site.domain == candidate).first()
        if site is not None:
            return site
    raw = str(domain or "").strip().lower()
    if raw:
        return db.query(Site).filter(func.lower(Site.domain) == raw).first()
    return None


def _collector_run_trigger_source(run: CollectorRun | None) -> str:
    if run is None:
        return ""
    try:
        data = json.loads(run.summary_json or "{}")
    except json.JSONDecodeError:
        return "legacy"
    if not isinstance(data, dict):
        return "legacy"
    ts = str(data.get("trigger_source") or "").strip().lower()
    if ts in ("manual", "system", "onboarding"):
        return ts
    return "legacy"


def _collector_run_counts_as_scheduled(run: CollectorRun | None) -> bool:
    ts = _collector_run_trigger_source(run)
    return ts in ("system", "legacy")


_COLLECTOR_STATUS_LABELS = {
    "success": "Başarılı",
    "failed": "Başarısız",
    "stale": "Eski veri",
    "no_data": "Veri yok",
    "started": "Devam ediyor",
    "skipped": "Atlandı",
}


def _collector_status_label(status: str | None) -> str:
    key = str(status or "").strip().lower()
    return _COLLECTOR_STATUS_LABELS.get(key, key or "—")


def _latest_scheduled_provider_run(
    db,
    *,
    site_id: int,
    provider: str,
    strategy: str | None = None,
    success_only: bool = False,
) -> CollectorRun | None:
    query = db.query(CollectorRun).filter(
        CollectorRun.site_id == site_id,
        CollectorRun.provider == provider,
    )
    if strategy is not None:
        query = query.filter(CollectorRun.strategy == strategy)
    candidates = query.order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc()).limit(80).all()
    for run in candidates:
        if not _collector_run_counts_as_scheduled(run):
            continue
        if success_only and str(run.status or "").lower() != "success":
            continue
        return run
    return None


def _data_explorer_last_auto_refresh_label(db, site_id: int) -> str:
    times: list[datetime] = []
    for provider, strategy in (("pagespeed", None), ("crux_history", None)):
        run = _latest_scheduled_provider_run(
            db,
            site_id=site_id,
            provider=provider,
            strategy=strategy,
            success_only=True,
        )
        finished = getattr(run, "finished_at", None) if run else None
        if isinstance(finished, datetime):
            times.append(finished)
    if not times:
        return "Henüz otomatik yenileme kaydı yok"
    return format_local_datetime(max(times), fallback="Henüz otomatik yenileme kaydı yok")


def _build_data_explorer_auto_refresh_log(db, site_id: int, *, limit: int = 12) -> list[dict]:
    provider_labels = {
        ("pagespeed", "mobile"): "PSI · Mobil",
        ("pagespeed", "desktop"): "PSI · Masaüstü",
        ("crux_history", "mobile"): "CrUX · Mobil",
        ("crux_history", "desktop"): "CrUX · Masaüstü",
    }
    rows: list[dict] = []
    for (provider, strategy), label in provider_labels.items():
        runs = (
            db.query(CollectorRun)
            .filter(
                CollectorRun.site_id == site_id,
                CollectorRun.provider == provider,
                CollectorRun.strategy == strategy,
            )
            .order_by(CollectorRun.requested_at.desc(), CollectorRun.id.desc())
            .limit(40)
            .all()
        )
        per_label = 0
        for run in runs:
            if not _collector_run_counts_as_scheduled(run):
                continue
            finished = run.finished_at or run.requested_at
            rows.append(
                {
                    "label": label,
                    "provider": provider,
                    "strategy": strategy,
                    "status": str(run.status or ""),
                    "status_label": _collector_status_label(run.status),
                    "status_ok": str(run.status or "").lower() == "success",
                    "finished_at": format_local_datetime(finished, fallback="—"),
                    "trigger_label": "Zamanlanmış" if _collector_run_trigger_source(run) == "system" else "Zamanlanmış (eski kayıt)",
                    "error": (run.error_message or "").strip()[:160],
                    "_sort": finished,
                }
            )
            per_label += 1
            if per_label >= 3:
                break
    rows.sort(key=lambda row: row.get("_sort") or datetime.min, reverse=True)
    for row in rows:
        row.pop("_sort", None)
    return rows[:limit]


def _data_explorer_scheduler_health() -> dict:
    next_run_label = "—"
    job_registered = False
    if SCHEDULER is not None:
        job = SCHEDULER.get_job("daily-site-refresh")
        if job is not None:
            job_registered = True
            nxt = job.next_run_time
            if nxt is not None:
                next_run_label = format_local_datetime(
                    nxt if nxt.tzinfo else nxt.replace(tzinfo=ZoneInfo("UTC")),
                    fallback="—",
                )
    scheduler_ok = (
        SCHEDULER is not None
        and bool(getattr(SCHEDULER, "running", False))
        and settings.scheduled_refresh_enabled
        and job_registered
    )
    return {
        "enabled": bool(settings.scheduled_refresh_enabled),
        "schedule": _data_explorer_nightly_schedule(),
        "scheduler_running": scheduler_ok,
        "job_registered": job_registered,
        "next_run": next_run_label,
        "monitor_enabled": bool(settings.scheduled_refresh_monitor_enabled),
    }


def _seo_audit_scheduler_health() -> dict:
    next_run_label = "—"
    job_registered = False
    if SCHEDULER is not None:
        job = SCHEDULER.get_job("daily-seo-audit")
        if job is not None:
            job_registered = True
            nxt = job.next_run_time
            if nxt is not None:
                next_run_label = format_local_datetime(
                    nxt if nxt.tzinfo else nxt.replace(tzinfo=ZoneInfo("UTC")),
                    fallback="—",
                )
    hour = max(0, min(23, int(settings.seo_audit_scheduled_hour)))
    minute = max(0, min(59, int(settings.seo_audit_scheduled_minute)))
    enabled = bool(settings.seo_audit_scheduled_enabled)
    scheduler_ok = (
        SCHEDULER is not None
        and bool(getattr(SCHEDULER, "running", False))
        and enabled
        and job_registered
    )
    return {
        "enabled": enabled,
        "schedule": f"{hour:02d}:{minute:02d}",
        "scheduler_running": scheduler_ok,
        "job_registered": job_registered,
        "next_run": next_run_label,
    }


def _seo_audit_last_auto_run_label(db, site_id: int) -> str:
    run = (
        db.query(CollectorRun)
        .filter(
            CollectorRun.site_id == site_id,
            CollectorRun.provider == "seo_audit",
            CollectorRun.strategy == "scheduled",
            CollectorRun.status == "success",
        )
        .order_by(CollectorRun.finished_at.desc(), CollectorRun.id.desc())
        .first()
    )
    finished = getattr(run, "finished_at", None) if run else None
    if not isinstance(finished, datetime):
        return "Henüz otomatik tarama kaydı yok"
    return format_local_datetime(finished, fallback="Henüz otomatik tarama kaydı yok")


def _dashboard_spotlight_card_limit(domain: str | None) -> int:
    """Öne çıkan sorgu kartı sayısı: döviz daha kompakt; sinemalar vb. tam liste."""
    d = str(domain or "").strip().lower()
    if d in ("doviz.com", "www.doviz.com"):
        return 20
    return 24


_sidebar_cache: dict = {"data": None, "ts": 0.0}
_SIDEBAR_CACHE_TTL = 30  # saniye


def get_sidebar_sites() -> list[dict]:
    # Sidebar için aktif siteler veritabanından okunur (30sn TTL cache).
    now = time.monotonic()
    if _sidebar_cache["data"] is not None and (now - _sidebar_cache["ts"]) < _SIDEBAR_CACHE_TTL:
        return _sidebar_cache["data"]
    with SessionLocal() as db:
        from backend.services.search_console_auth import OAUTH_CREDENTIAL_TYPE, SERVICE_ACCOUNT_CREDENTIAL_TYPE

        external_site_ids = _external_site_ids(db)
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.desc()).all()

        # Tek sorguda tüm SC credential'ları yükle (N+1 yerine 1 sorgu)
        sc_creds = (
            db.query(SiteCredential.site_id, SiteCredential.credential_type)
            .filter(SiteCredential.credential_type.in_([OAUTH_CREDENTIAL_TYPE, SERVICE_ACCOUNT_CREDENTIAL_TYPE]))
            .all()
        )
        sc_connected: set[int] = {row.site_id for row in sc_creds}

        rows = []
        for site in sites:
            if site.id in external_site_ids:
                continue
            is_public = site.id not in sc_connected
            rows.append({
                "domain": site.domain,
                "label": site.display_name,
                "profile": "public" if is_public else "verified",
                "href": f"/external-explorer/{site.domain}" if is_public else f"/data-explorer/{site.domain}",
            })
        rows.sort(key=lambda site: _preferred_site_order_key(site.get("domain"), site.get("label")))
        _sidebar_cache["data"] = rows
        _sidebar_cache["ts"] = now
        return rows


def invalidate_sidebar_cache():
    """Site eklendiğinde/silindiğinde sidebar cache'ini geçersiz kıl."""
    _sidebar_cache["data"] = None
    _sidebar_cache["ts"] = 0.0


from backend.menu_excluded import is_menu_excluded_label


def _is_menu_excluded_domain(domain: str | None) -> bool:
    """Harici / menüden gizlenecek domainler (ör. canlidoviz.com)."""
    return is_menu_excluded_label(domain)


def _external_site_ids(db) -> set[int]:
    ids = {int(row.site_id) for row in db.query(ExternalSite.site_id).all()}
    for site in db.query(Site.id, Site.domain).all():
        if _is_menu_excluded_domain(site.domain):
            ids.add(int(site.id))
    return ids


def _external_site_domains(db) -> set[str]:
    rows = (
        db.query(Site.domain)
        .join(ExternalSite, ExternalSite.site_id == Site.id)
        .all()
    )
    domains = {str(row[0] or "").lower() for row in rows if row and row[0]}
    for site in db.query(Site.domain).all():
        if _is_menu_excluded_domain(site.domain):
            domains.add(str(site.domain or "").lower())
    return domains


def _internal_active_sites(db, *, active_only: bool = True) -> list[Site]:
    excluded = _external_site_ids(db)
    q = db.query(Site)
    if active_only:
        q = q.filter(Site.is_active.is_(True))
    return [s for s in q.order_by(Site.id.asc()).all() if s.id not in excluded]


def _internal_site_selector_rows(db) -> list[dict]:
    sites = _internal_active_sites(db)
    sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
    return [
        {"id": s.id, "domain": s.domain, "display_name": s.display_name or s.domain}
        for s in sites
    ]


def _default_internal_site_id(rows: list[dict]) -> int:
    preferred = {"doviz.com", "www.doviz.com"}
    for row in rows:
        if (row.get("domain") or "").lower() in preferred:
            return int(row["id"])
    return int(rows[0]["id"]) if rows else 1


def _default_active_site_id(db: Session) -> int | None:
    external_ids = _external_site_ids(db)
    sites = [
        s
        for s in db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.display_name.asc()).all()
        if s.id not in external_ids
    ]
    if not sites:
        return None
    rows = [
        {"id": s.id, "domain": s.domain, "display_name": s.display_name or s.domain}
        for s in sites
    ]
    return _default_internal_site_id(rows)


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
                    "position_diff": _sc_position_delta(current_position, previous_position),
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


def _build_search_console_top_entities(
    current_rows: list[dict],
    previous_rows: list[dict],
    *,
    label_key: str = "query",
    limit: int = 50,
) -> list[dict]:
    current_map: dict[str, dict] = {}
    previous_map: dict[str, dict] = {}
    for row in current_rows:
        key = str(row.get(label_key) or row.get("query") or "").strip()
        if not key:
            continue
        bucket = current_map.setdefault(
            key,
            {"clicks": 0.0, "impressions": 0.0, "position_weighted_sum": 0.0, "position_weight": 0.0},
        )
        clicks = float(row.get("clicks") or 0.0)
        impressions = float(row.get("impressions") or 0.0)
        position = float(row.get("position") or 0.0)
        bucket["clicks"] += clicks
        bucket["impressions"] += impressions
        if impressions > 0:
            bucket["position_weighted_sum"] += position * impressions
            bucket["position_weight"] += impressions
    for row in previous_rows:
        key = str(row.get(label_key) or row.get("query") or "").strip()
        if not key:
            continue
        bucket = previous_map.setdefault(
            key,
            {"clicks": 0.0, "impressions": 0.0, "position_weighted_sum": 0.0, "position_weight": 0.0},
        )
        clicks = float(row.get("clicks") or 0.0)
        impressions = float(row.get("impressions") or 0.0)
        position = float(row.get("position") or 0.0)
        bucket["clicks"] += clicks
        bucket["impressions"] += impressions
        if impressions > 0:
            bucket["position_weighted_sum"] += position * impressions
            bucket["position_weight"] += impressions

    rows: list[dict] = []
    for key in set(current_map.keys()) | set(previous_map.keys()):
        current = current_map.get(key) or {}
        previous = previous_map.get(key) or {}
        current_clicks = float(current.get("clicks") or 0.0)
        previous_clicks = float(previous.get("clicks") or 0.0)
        current_impressions = float(current.get("impressions") or 0.0)
        previous_impressions = float(previous.get("impressions") or 0.0)
        current_weight = float(current.get("position_weight") or 0.0)
        previous_weight = float(previous.get("position_weight") or 0.0)
        current_position = (
            float(current.get("position_weighted_sum") or 0.0) / current_weight if current_weight > 0 else 0.0
        )
        previous_position = (
            float(previous.get("position_weighted_sum") or 0.0) / previous_weight if previous_weight > 0 else 0.0
        )
        rows.append(
            {
                "label": key,
                "clicks_current": current_clicks,
                "clicks_previous": previous_clicks,
                "clicks_diff": current_clicks - previous_clicks,
                "impressions_current": current_impressions,
                "impressions_previous": previous_impressions,
                "impressions_diff": current_impressions - previous_impressions,
                "position_current": current_position,
                "position_previous": previous_position,
                "position_diff": _sc_position_delta(current_position, previous_position),
            }
        )
    rows.sort(key=lambda item: float(item.get("clicks_current") or 0.0), reverse=True)
    return rows[:limit]


def _search_console_trend_has_signal(trend: dict) -> bool:
    """Günlük trend serisinde en az bir gün gerçek trafik var mı."""
    for key in ("clicks", "impressions"):
        for value in trend.get(key) or []:
            if value is None:
                continue
            try:
                if float(value) > 0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def _sanitize_search_console_trend(trend: dict) -> dict:
    sanitized = dict(trend or {})
    if str(sanitized.get("mode") or "") in ("last_28d", "last_12m"):
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
    starts: list[str] = []
    ends: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        s = str(row.get("start_date") or "").strip()
        e = str(row.get("end_date") or "").strip()
        if s:
            starts.append(s)
        if e:
            ends.append(e)
    if not starts and not ends:
        return ("", "")
    if not starts:
        starts = ends[:]
    if not ends:
        ends = starts[:]
    return (min(starts), max(ends))


def _slice_search_console_trend_last_days(trend: dict, last_n: int) -> dict:
    t = dict(trend or {})
    if str(t.get("mode") or "") not in ("last_28d", "last_12m"):
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
        "mode": str(t.get("mode") or "last_28d"),
        "dates": dates[-take:],
        "labels": labels[-take:] if len(labels) >= take else labels,
        "clicks": clicks[-take:],
        "impressions": impressions[-take:] if len(impressions) >= take else impressions,
        "ctr": ctr[-take:] if len(ctr) >= take else ctr,
        "position": position[-take:],
    }


def _align_search_console_trend_to_dates(trend: dict, date_keys: list[str]) -> dict:
    """SC position serisini GA4 günlük eksen tarihleriyle hizalar (eksik günler null)."""
    t = dict(trend or {})
    src_dates = list(t.get("dates") or [])
    src_pos = list(t.get("position") or [])
    pos_map: dict[str, float | None] = {}
    for i, d in enumerate(src_dates):
        if i >= len(src_pos):
            break
        key = str(d)[:10]
        if not key:
            continue
        raw = src_pos[i]
        if raw is None:
            pos_map[key] = None
            continue
        try:
            fv = float(raw)
            pos_map[key] = fv if fv > 0 else None
        except (TypeError, ValueError):
            pos_map[key] = None
    out_dates: list[str] = []
    out_pos: list[float | None] = []
    for dk in date_keys or []:
        key = str(dk)[:10]
        if not key:
            continue
        out_dates.append(key)
        out_pos.append(pos_map.get(key))
    return {**t, "dates": out_dates, "position": out_pos}


def _pick_ga4_sc_position_trend_base(
    dev_trends: dict[str, dict],
    *,
    period_key: str,
    period_days: int,
) -> dict | None:
    pd12 = int(settings.ga4_trend_12m_period_days)
    slice_days = int(period_days) if int(period_days) > 1 else 7
    if period_key == "12m" or slice_days >= pd12:
        return dev_trends.get("12m")
    if slice_days > 28:
        return dev_trends.get("12m") or dev_trends.get("28d")
    return dev_trends.get("28d")


def _search_console_report_payload(
    db,
    *,
    site_id: int,
    compare_opts: dict | None = None,
) -> dict:
    _sc_batch = get_latest_search_console_rows_batch(
        db,
        site_id=site_id,
        scopes=[
            "current_7d",
            "previous_7d",
            "current_30d",
            "previous_30d",
            "current_60d",
            "previous_60d",
            "current_90d",
            "previous_90d",
            "current_day",
            "previous_week_same_weekday",
            "current_1d_pages",
            "previous_1d_pages",
            "current_7d_pages",
            "previous_7d_pages",
            "current_30d_pages",
            "previous_30d_pages",
            "current_60d_pages",
            "previous_60d_pages",
        ],
    )
    current_rows_7 = _sc_batch.get("current_7d", [])
    previous_rows_7 = _sc_batch.get("previous_7d", [])
    current_rows_30 = _sc_batch.get("current_30d", [])
    previous_rows_30 = _sc_batch.get("previous_30d", [])
    current_rows_60 = _sc_batch.get("current_60d", [])
    previous_rows_60 = _sc_batch.get("previous_60d", [])
    current_rows_90 = _sc_batch.get("current_90d", [])
    previous_rows_90 = _sc_batch.get("previous_90d", [])
    current_rows_1 = _sc_batch.get("current_day", [])
    previous_rows_wow = _sc_batch.get("previous_week_same_weekday", [])
    current_pages_1 = _sc_batch.get("current_1d_pages", [])
    previous_pages_1 = _sc_batch.get("previous_1d_pages", [])
    current_pages_7 = _sc_batch.get("current_7d_pages", [])
    previous_pages_7 = _sc_batch.get("previous_7d_pages", [])
    current_pages_30 = _sc_batch.get("current_30d_pages", [])
    previous_pages_30 = _sc_batch.get("previous_30d_pages", [])
    current_pages_60 = _sc_batch.get("current_60d_pages", [])
    previous_pages_60 = _sc_batch.get("previous_60d_pages", [])
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
    current_60d_by_device = summary_payload.get("current_60d_summary_by_device") or {}
    previous_60d_by_device = summary_payload.get("previous_60d_summary_by_device") or {}
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
    content_trending_down = summary_payload.get("content_trending_down") or []
    trend_summary_by_device = _raw_trend_by_device

    _raw_12m_trend_by_device = summary_payload.get("trend_12m_summary_by_device") or {}
    _stored_12m_trend_rows = summary_payload.get("trend_12m_rows") or []
    if _stored_12m_trend_rows and not _raw_12m_trend_by_device:
        from backend.collectors.search_console import _build_recent_trend_summary_by_device

        try:
            from datetime import date as _date_cls_12m_early

            _12s = str(summary_payload.get("trend_12m_start_date") or "").strip()
            _12e = str(summary_payload.get("trend_12m_end_date") or "").strip()
            if _12s and _12e:
                _s12e = _date_cls_12m_early.fromisoformat(_12s[:10])
                _e12e = _date_cls_12m_early.fromisoformat(_12e[:10])
            else:
                _d12e = [r.get("date") for r in _stored_12m_trend_rows if r.get("date")]
                if not _d12e:
                    raise ValueError("no 12m trend dates")
                _s12e = _date_cls_12m_early.fromisoformat(min(_d12e)[:10])
                _e12e = _date_cls_12m_early.fromisoformat(_12e[:10]) if _12e else _date_cls_12m_early.fromisoformat(max(_d12e)[:10])
            _raw_12m_trend_by_device = _build_recent_trend_summary_by_device(
                _stored_12m_trend_rows, start_date=_s12e, end_date=_e12e
            )
            for _dev_s in _raw_12m_trend_by_device.values():
                if isinstance(_dev_s, dict):
                    _dev_s["mode"] = "last_12m"
        except Exception:
            _raw_12m_trend_by_device = {}
    trend_12m_by_device = _raw_12m_trend_by_device

    range_7_last = _scope_range_from_rows(current_rows_7)
    range_7_prev = _scope_range_from_rows(previous_rows_7)
    range_30_last = _scope_range_from_rows(current_rows_30)
    range_30_prev = _scope_range_from_rows(previous_rows_30)
    range_60_last = _scope_range_from_rows(current_rows_60)
    range_60_prev = _scope_range_from_rows(previous_rows_60)
    range_90_last = _scope_range_from_rows(current_rows_90)
    range_90_prev = _scope_range_from_rows(previous_rows_90)
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
        ("60", 60, "Son 60 gün", "Önceki 60 gün", 60),
        ("90", 90, "Son 90 gün", "Önceki 90 gün", 90),
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
                pages_current = _filter_search_console_rows_by_device(current_pages_1, device_code)
                pages_previous = _filter_search_console_rows_by_device(previous_pages_1, device_code)
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
                pages_current = _filter_search_console_rows_by_device(current_pages_7, device_code)
                pages_previous = _filter_search_console_rows_by_device(previous_pages_7, device_code)
                chart_trend = _slice_search_console_trend_last_days(base_trend, trend_days)
                range_last = _format_sc_tr_date_range(*range_7_last)
                range_prev = _format_sc_tr_date_range(*range_7_prev)
            elif period_key == "30":
                fc = _filter_search_console_rows_by_device(current_rows_30, device_code)
                fp = _filter_search_console_rows_by_device(previous_rows_30, device_code)
                summary_current = current_30d_by_device.get(device_code) or _summarize_search_console_rows(fc)
                summary_previous = previous_30d_by_device.get(device_code) or _summarize_search_console_rows(fp)
                device_top = _build_search_console_top_queries(fc, fp, limit=50)
                pages_current = _filter_search_console_rows_by_device(current_pages_30, device_code)
                pages_previous = _filter_search_console_rows_by_device(previous_pages_30, device_code)
                chart_trend = _slice_search_console_trend_last_days(base_trend, trend_days)
                range_last = _format_sc_tr_date_range(*range_30_last)
                range_prev = _format_sc_tr_date_range(*range_30_prev)
            elif period_key == "60":
                fc = _filter_search_console_rows_by_device(current_rows_60, device_code)
                fp = _filter_search_console_rows_by_device(previous_rows_60, device_code)
                summary_current = current_60d_by_device.get(device_code) or _summarize_search_console_rows(fc)
                summary_previous = previous_60d_by_device.get(device_code) or _summarize_search_console_rows(fp)
                device_top = _build_search_console_top_queries(fc, fp, limit=50)
                pages_current = _filter_search_console_rows_by_device(current_pages_60, device_code)
                pages_previous = _filter_search_console_rows_by_device(previous_pages_60, device_code)
                base_60 = _sanitize_search_console_trend(
                    trend_12m_by_device.get(device_code)
                    or {**empty_trend, "mode": "last_12m"}
                )
                chart_trend = _slice_search_console_trend_last_days(base_60, trend_days)
                if not _search_console_trend_has_signal(chart_trend):
                    chart_trend = _slice_search_console_trend_last_days(base_trend, min(30, len(base_trend.get("dates") or []) or 30))
                range_last = _format_sc_tr_date_range(*range_60_last)
                range_prev = _format_sc_tr_date_range(*range_60_prev)
            else:
                fc = _filter_search_console_rows_by_device(current_rows_90, device_code)
                fp = _filter_search_console_rows_by_device(previous_rows_90, device_code)
                summary_current = _summarize_search_console_rows(fc)
                summary_previous = _summarize_search_console_rows(fp)
                device_top = _build_search_console_top_queries(fc, fp, limit=50)
                pages_current = []
                pages_previous = []
                base_90 = _sanitize_search_console_trend(
                    trend_12m_by_device.get(device_code)
                    or {**empty_trend, "mode": "last_12m"}
                )
                chart_trend = _slice_search_console_trend_last_days(base_90, trend_days)
                if not _search_console_trend_has_signal(chart_trend):
                    chart_trend = _slice_search_console_trend_last_days(base_trend, min(30, len(base_trend.get("dates") or []) or 30))
                range_last = _format_sc_tr_date_range(*range_90_last)
                range_prev = _format_sc_tr_date_range(*range_90_prev)

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
                "top_pages": _build_search_console_top_entities(
                    pages_current, pages_previous, label_key="query", limit=50
                ),
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
                "position_delta": _sc_position_delta(
                    float(_cur.get("position") or 0),
                    float(_prev.get("position") or 0),
                ),
                "content_trending_down": content_trending_down if period_key in ("30", "60", "90") else [],
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
            else ("Son 90 günün günlük trendi" if pd_days == 90 else ("Son 60 günün günlük trendi" if pd_days == 60 else "Son 30 günün günlük trendi")),
            "views": views,
            "trend_only": False,
        }

    # 12 ay: yalnızca günlük trend (karşılaştırma / sorgu tabloları yok)
    _stored_12m_rows = summary_payload.get("trend_12m_rows") or []
    _raw_12m_by_device = summary_payload.get("trend_12m_summary_by_device") or {}
    _12m_start_iso = str(summary_payload.get("trend_12m_start_date") or "").strip()
    _12m_end_iso = str(summary_payload.get("trend_12m_end_date") or "").strip()
    if _stored_12m_rows and not _raw_12m_by_device:
        from backend.collectors.search_console import _build_recent_trend_summary_by_device

        try:
            from datetime import date as _date_cls_12m

            if _12m_start_iso and _12m_end_iso:
                _s12 = _date_cls_12m.fromisoformat(_12m_start_iso[:10])
                _e12 = _date_cls_12m.fromisoformat(_12m_end_iso[:10])
            else:
                _d12 = [r.get("date") for r in _stored_12m_rows if r.get("date")]
                if not _d12:
                    raise ValueError("no dates")
                _s12 = _date_cls_12m.fromisoformat(min(_d12)[:10])
                _e12 = _date_cls_12m.fromisoformat(max(_d12)[:10])
            _raw_12m_by_device = _build_recent_trend_summary_by_device(
                _stored_12m_rows, start_date=_s12, end_date=_e12
            )
            for _dev_summary in _raw_12m_by_device.values():
                _dev_summary["mode"] = "last_12m"
        except Exception:
            _raw_12m_by_device = {}
    if not _12m_start_iso and _stored_12m_rows:
        try:
            _d12 = [r.get("date") for r in _stored_12m_rows if r.get("date")]
            if _d12:
                _12m_start_iso = min(_d12)
                _12m_end_iso = max(_d12)
        except Exception:
            pass
    _12m_range_label = _format_sc_tr_date_range(_12m_start_iso, _12m_end_iso)
    views_12m: dict[str, dict] = {}
    for device_key, device_label in (("mobile", "Mobile"), ("desktop", "Desktop")):
        device_code = device_key.upper()
        empty_12m = {"mode": "last_12m", "labels": [], "dates": [], "clicks": [], "position": []}
        base_12m = _sanitize_search_console_trend(_raw_12m_by_device.get(device_code) or empty_12m)
        if str(base_12m.get("mode") or "") != "last_12m":
            base_12m["mode"] = "last_12m"
        views_12m[device_key] = {
            "device_code": device_code,
            "device_label": device_label,
            "has_data": _search_console_trend_has_signal(base_12m),
            "trend": base_12m,
            "range_label": _12m_range_label,
        }
    if any(v.get("has_data") for v in views_12m.values()):
        periods["12m"] = {
            "period_days": int(settings.search_console_trend_12m_days),
            "heading": "Son 12 ay — günlük trend",
            "subtitle": f"Tarih aralığı: {_12m_range_label or '—'} · Karşılaştırma yok",
            "label_current": _12m_range_label,
            "label_previous": "",
            "trend_caption": "Son 12 ayın günlük metrik seyri (tıklama, gösterim, CTR, pozisyon)",
            "views": views_12m,
            "trend_only": True,
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

    report = {
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
        "range_current_60d": _format_sc_tr_date_range(*range_60_last),
        "range_previous_60d": _format_sc_tr_date_range(*range_60_prev),
        "range_current_90d": _format_sc_tr_date_range(*range_90_last),
        "range_previous_90d": _format_sc_tr_date_range(*range_90_prev),
    }
    if compare_opts:
        from backend.services.analytics_compare import (
            apply_search_console_report_compare,
            resolve_sc_summary_period_range,
        )
        from backend.services.sc_compare_daily import supplement_summary_for_compare

        period_primary_ranges = {
            "7": resolve_sc_summary_period_range(summary_payload, "7", range_7_last),
            "30": resolve_sc_summary_period_range(summary_payload, "30", range_30_last),
            "60": resolve_sc_summary_period_range(summary_payload, "60", range_60_last),
            "90": resolve_sc_summary_period_range(summary_payload, "90", range_90_last),
        }
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is not None:
            try:
                summary_payload = supplement_summary_for_compare(
                    db,
                    site,
                    summary_payload,
                    compare_opts,
                    period_primary_ranges,
                )
            except Exception as exc:
                from backend.services.search_console_auth import (
                    SearchConsoleOAuthError,
                    format_search_console_error_for_ui,
                    record_search_console_oauth_revoked,
                )

                if isinstance(exc, SearchConsoleOAuthError):
                    record_search_console_oauth_revoked(db, site.id, str(exc))
                    logging.warning(
                        "SC compare supplement skipped site_id=%s: %s",
                        site.id,
                        format_search_console_error_for_ui(str(exc)),
                    )
                else:
                    logging.warning("SC compare supplement failed site_id=%s: %s", site.id, exc)

        report = apply_search_console_report_compare(
            report,
            compare=compare_opts,
            summary_payload=summary_payload,
            period_primary_ranges=period_primary_ranges,
            format_prev_label=lambda a, b: _format_sc_tr_date_range(a, b),
        )
        if compare_opts.get("enabled") and compare_opts.get("mode") not in (None, "previous_period"):
            mv = ((report.get("periods") or {}).get("7") or {}).get("views") or {}
            mob = mv.get("mobile") or {}
            if mob.get("range_prev"):
                report["range_previous_7d"] = mob["range_prev"]
    return report


def _search_console_single_site_data(
    db, site, schedule_label: str, *, compare_opts: dict | None = None
) -> dict:
    """Tek bir site için tam Search Console kart verisi üretir."""
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    connection = get_search_console_connection_status(db, site.id)
    has_rows_28d = bool(get_latest_search_console_rows(db, site_id=site.id, data_scope="current_28d"))
    status = _search_console_status_from_cache(latest, connection, has_rows_28d)
    last_run = _latest_provider_run(db, site_id=site.id, provider="search_console", strategy="all")
    cooldown_active = _latest_collector_run_recent(
        db,
        site_id=site.id,
        provider="search_console",
        cooldown_seconds=settings.search_console_refresh_cooldown_seconds,
    )
    # GSC CWV: Postgres (Railway) + isteğe bağlı disk yedek
    import re as _re

    from backend.services import gsc_cwv_storage

    raw_domain = str(site.domain or "").strip()
    domain_for_property = _re.sub(r"^https?://", "", raw_domain, flags=_re.I).strip().strip("/")
    domain_slug = _gsc_domain_slug(site.domain)

    gsc_cwv = gsc_cwv_storage.build_gsc_cwv_urls(
        db, site_id=site.id, domain_for_property=domain_for_property
    )

    def _static_url_if_exists(path: Path) -> str:
        if not path.exists():
            return ""
        try:
            v = int(path.stat().st_mtime)
        except OSError:
            v = int(time.time())
        return f"/static/gsc/{path.name}?v={v}"

    for variant, key in (
        ("mobile", "mobile_url"),
        ("desktop", "desktop_url"),
        ("full", "full_url"),
        ("extra", "extra_url"),
    ):
        if not gsc_cwv.get(key):
            gsc_cwv[key] = _static_url_if_exists(GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-{variant}.png")

    return {
        "id": site.id,
        "domain": site.domain,
        "display_name": site.display_name,
        "is_active": site.is_active,
        "connection": connection,
        "status": status,
        "last_run_status": str(last_run.status or "").upper() if last_run and last_run.status else "NEVER",
        "last_run_at": _format_optional_datetime(last_run.requested_at if last_run else None),
        "last_run_error": search_console_last_run_error_for_ui(
            error_message=last_run.error_message if last_run else "",
            requires_reauth=bool(connection.get("requires_reauth")),
            oauth_saved_at=oauth_saved_at_for_site(db, site.id),
            run_requested_at=last_run.requested_at if last_run else None,
        ),
        "cooldown_active": cooldown_active,
        "manual_mode_label": f"{schedule_label} otomatik + manuel",
        "report": _search_console_report_payload(db, site_id=site.id, compare_opts=compare_opts),
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
        trigger_source="onboarding",
    )


def _collect_crux_external_fast(db, site: Site) -> dict:
    return collect_crux_history(
        db,
        site,
        request_timeout=8,
        max_identifier_attempts=2,
        form_factors=("mobile", "desktop"),
        include_current=True,
        trigger_source="onboarding",
    )


def _collect_crawler_external_deep(db, site: Site) -> dict:
    return collect_crawler_metrics(
        db,
        site,
        source_page_limit=24,
        target_url_limit=48,
        links_per_page_limit=8,
        issue_sample_limit=12,
        sitemap_url_limit=24,
        request_timeout_seconds=10,
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


def _refresh_public_site_measurements(db, site: Site, *, force: bool = True, deep: bool = False) -> dict[str, dict]:
    # Search Console yetkisi gerektirmeyen collector akisi.
    results: dict[str, dict] = {}

    try:
        if deep:
            results["pagespeed"] = collect_pagespeed_metrics(
                db,
                site,
                bypass_quota=settings.pagespeed_manual_refresh_bypass_quota,
                trigger_source="manual",
            )
        else:
            results["pagespeed"] = _collect_pagespeed_external_fast(db, site)
    except Exception as exc:  # noqa: BLE001
        results["pagespeed"] = {"errors": {"exception": str(exc)}}

    try:
        if deep:
            results["crawler"] = _collect_crawler_external_deep(db, site)
        else:
            results["crawler"] = _collect_crawler_external_fast(db, site)
    except Exception as exc:  # noqa: BLE001
        results["crawler"] = {"errors": {"exception": str(exc)}}

    try:
        if deep:
            results["crux_history"] = collect_crux_history(db, site, trigger_source="manual")
        else:
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
        except PendingRollbackError as exc:
            db.rollback()
            if not _is_sqlite_lock_error(exc) or attempt >= attempts:
                raise
            time.sleep(base_wait * attempt)
        except OperationalError as exc:
            db.rollback()
            if not _is_sqlite_lock_error(exc) or attempt >= attempts:
                raise
            time.sleep(base_wait * attempt)


def _friendly_measure_error_message(exc: Exception) -> str:
    if _is_sqlite_lock_error(exc):
        return "Ölçüm şu anda tamamlanamadı (veritabanı meşgul). Mevcut kayıtlı veri gösteriliyor; lütfen birkaç saniye sonra tekrar deneyin."
    return "Ölçüm şu anda tamamlanamadı. Mevcut kayıtlı veri gösteriliyor; lütfen tekrar deneyin."


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


def _run_external_deep_refresh_background(site_id: int, job_id: str) -> None:
    """Derin external yenileme — HTTP isteğini bloklamamak için arka planda çalışır."""
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            _set_external_onboarding_job(
                job_id,
                status="failed",
                percent=100,
                title="Yenileme başarısız",
                detail="Site kaydı bulunamadı.",
                finished_at=datetime.utcnow(),
            )
            return

        results: dict[str, dict] = {}
        warnings: list[str] = []
        try:
            _set_external_onboarding_job(
                job_id,
                percent=12,
                title="PageSpeed ölçümü",
                detail=f"{site.domain} için PSI/Lighthouse verileri güncelleniyor.",
            )
            try:
                results["pagespeed"] = collect_pagespeed_metrics(
                    db,
                    site,
                    bypass_quota=settings.pagespeed_manual_refresh_bypass_quota,
                    trigger_source="manual",
                )
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"PageSpeed: {exc}")
                results["pagespeed"] = {"errors": {"exception": str(exc)}}

            _set_external_onboarding_job(
                job_id,
                percent=48,
                title="Crawler analizi",
                detail="Site haritası, bağlantılar ve teknik denetim yazılıyor.",
            )
            try:
                results["crawler"] = _collect_crawler_external_deep(db, site)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"Crawler: {exc}")
                results["crawler"] = {"errors": {"exception": str(exc)}}

            _set_external_onboarding_job(
                job_id,
                percent=76,
                title="CrUX geçmişi",
                detail="Chrome UX Report verileri güncelleniyor.",
            )
            try:
                results["crux_history"] = collect_crux_history(db, site, trigger_source="manual")
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"CrUX: {exc}")
                results["crux_history"] = {"state": "failed", "error": str(exc)}

            results["url_inspection"] = {
                "state": "skipped",
                "reason": "URL Inspection için Search Console property yetkisi gerekiyor.",
            }

            try:
                _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
            except OperationalError as exc:
                db.rollback()
                if _is_sqlite_lock_error(exc):
                    _set_external_onboarding_job(
                        job_id,
                        status="failed",
                        percent=100,
                        title="Yenileme tamamlanamadı",
                        detail="Veritabanı meşgul. Birkaç saniye sonra tekrar deneyin.",
                        finished_at=datetime.utcnow(),
                    )
                    return
                raise

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

            if warnings:
                _set_external_onboarding_job(
                    job_id,
                    status="completed",
                    percent=100,
                    title="Derin yenileme tamamlandı (kısmi uyarı)",
                    detail="; ".join(warnings),
                    finished_at=datetime.utcnow(),
                )
            else:
                _set_external_onboarding_job(
                    job_id,
                    status="completed",
                    percent=100,
                    title="Derin yenileme tamamlandı",
                    detail=f"{site.domain} ölçümleri güncellendi.",
                    finished_at=datetime.utcnow(),
                )
        except Exception as exc:  # noqa: BLE001
            db.rollback()
            LOGGER.warning("External deep refresh failed for site_id=%s: %s", site_id, exc)
            _set_external_onboarding_job(
                job_id,
                status="failed",
                percent=100,
                title="Derin yenileme başarısız",
                detail=str(exc),
                finished_at=datetime.utcnow(),
            )


def _external_lazy_site_card_context(db) -> dict:
    ext_sites = (
        db.query(Site)
        .join(ExternalSite, ExternalSite.site_id == Site.id)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.desc())
        .all()
    )
    ext_sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
    return {
        "lazy_mode": True,
        "lazy_site_ids": [(s.id, s.display_name, s.domain) for s in ext_sites],
    }


def _request_wants_json(request: Request) -> bool:
    if str(request.query_params.get("format") or "").strip().lower() == "json":
        return True
    accept = str(request.headers.get("Accept") or "").lower()
    return "application/json" in accept and "text/html" not in accept


def _collect_sc_for_site_in_own_session(site_id: int) -> tuple[int, dict]:
    """Nightly refresh için tek site — kendi DB session'ında collect çağrısı."""
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return (site_id, {"state": "failed", "error": "Site bulunamadı."})
        try:
            result = collect_search_console_metrics(db, site)
            db.commit()
            return (site_id, result)
        except Exception as exc:  # noqa: BLE001
            try:
                db.rollback()
            except Exception:
                pass
            LOGGER.warning("Daily Search Console refresh failed for site_id=%s: %s", site_id, exc)
            return (site_id, {"state": "failed", "error": str(exc)})


def _run_daily_search_console_refresh_job() -> None:
    if not DAILY_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Daily Search Console refresh skipped because another scheduled job is still in progress.")
        return

    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        LOGGER.info("Daily Search Console refresh started.")
        with SessionLocal() as db:
            external = _external_site_ids(db)
            connected_site_ids = [
                site.id
                for site in _active_sites(db)
                if site.id not in external and get_search_console_connection_status(db, site.id).get("connected")
            ]

        results_by_site: dict[int, dict] = {}
        if connected_site_ids:
            max_workers = min(4, len(connected_site_ids))
            LOGGER.info(
                "Daily Search Console refresh: %s site paralel çekiliyor (max_workers=%s)",
                len(connected_site_ids), max_workers,
            )
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="sc-nightly") as pool:
                futures = {pool.submit(_collect_sc_for_site_in_own_session, sid): sid for sid in connected_site_ids}
                for fut in as_completed(futures):
                    sid = futures[fut]
                    try:
                        sid, result = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        result = {"state": "failed", "error": str(exc)}
                    results_by_site[sid] = result

        if results_by_site:
            with SessionLocal() as db:
                sites_by_id = {
                    s.id: s
                    for s in db.query(Site).filter(Site.id.in_(list(results_by_site.keys()))).all()
                }
                sc_batch: list[tuple[Site, dict]] = [
                    (sites_by_id[sid], result) for sid, result in results_by_site.items() if sid in sites_by_id
                ]
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
                    result = collect_search_console_alert_metrics(db, site, send_notifications=False)
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
                    trigger_source="system",
                )
                db.commit()
                if isinstance(results.get("pagespeed"), dict):
                    pagespeed_batch.append((site, results["pagespeed"]))
                if isinstance(results.get("crawler"), dict):
                    crawler_batch.append((site, results["crawler"]))

                try:
                    crux_result = collect_crux_history(db, site, trigger_source="system")
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
        from backend.collectors.ga4 import collect_ga4_scheduled_site_metrics

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
                    collect_ga4_scheduled_site_metrics(db, site)
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


def _run_oauth_connection_monitor_job() -> None:
    if not settings.oauth_connection_alert_enabled:
        return
    try:
        from backend.services.connection_alerts import notify_oauth_connection_broken_scan

        with SessionLocal() as db:
            sent = notify_oauth_connection_broken_scan(db)
        for subject in sent:
            LOGGER.warning("OAuth connection monitor sent alert: %s", subject)
    except Exception:
        LOGGER.exception("OAuth connection monitor job failed")


def _run_ai_daily_brief_scheduled() -> None:
    try:
        from backend.services.ai_daily_brief import run_ai_daily_brief_job

        run_ai_daily_brief_job()
    except Exception:  # noqa: BLE001
        LOGGER.exception("Scheduled AI daily brief job failed.")


def _run_app_intel_digest_job(*, trigger_source: str, action_label: str) -> None:
    if not APP_INTEL_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("App Intel digest is already running; skipping duplicate trigger (%s).", trigger_source)
        return
    try:
        from backend.services.app_intel import APP_PRODUCTS, build_intel_payload, invalidate_raw_cache

        invalidate_raw_cache()
        summary: dict[str, object] = {
            "urun_sayisi": len(APP_PRODUCTS),
            "period_days": 30,
        }
        errors: list[str] = []
        ok_count = 0
        for pid, spec in APP_PRODUCTS.items():
            label = str(spec.get("label") or pid)
            try:
                payload = build_intel_payload(pid, 30, force_refresh=True)
                aw = payload.get("active_window") if isinstance(payload, dict) else {}
                android = aw.get("android") if isinstance(aw, dict) else {}
                ios = aw.get("ios") if isinstance(aw, dict) else {}
                per_errors = payload.get("errors") if isinstance(payload, dict) else {}
                a_err = (per_errors or {}).get("android")
                i_err = (per_errors or {}).get("ios")
                if a_err or i_err:
                    errors.append(f"{label}: Play={a_err or '-'} | App Store={i_err or '-'}")
                summary[f"{pid}_play_donem_yorumu"] = int((android or {}).get("review_count_period") or 0)
                summary[f"{pid}_ios_donem_yorumu"] = int((ios or {}).get("review_count_period") or 0)
                ok_count += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{label}: {exc}")
                LOGGER.exception("App Intel digest failed for %s", pid)

        summary["basarili_urun"] = ok_count
        summary["hatali_urun"] = len(errors)
        state = "warning" if errors else "success"
        result: dict[str, object] = {"state": state, "summary": summary}
        if errors:
            result["errors"] = " | ".join(errors)

        send_consolidated_system_email(
            system_key="app_intel",
            trigger_source=trigger_source,
            action_label=action_label,
            items=[(None, result)],
        )
    finally:
        APP_INTEL_REFRESH_LOCK.release()


def _run_app_intel_scheduled() -> None:
    try:
        _run_app_intel_digest_job(trigger_source="system", action_label="Günlük App mağaza özeti")
    except Exception:  # noqa: BLE001
        LOGGER.exception("Scheduled App Intel digest job failed.")


_RANK_REFRESH_LOCK = threading.Lock()


def _run_rank_refresh_job() -> None:
    """Sadece kategori sırasını çekip kaydeder. 3 saatte bir çalışır."""
    if not _RANK_REFRESH_LOCK.acquire(blocking=False):
        LOGGER.info("Rank refresh zaten çalışıyor, atlandı.")
        return
    try:
        from backend.services.app_intel import refresh_category_ranks
        results = refresh_category_ranks()
        LOGGER.info("Periyodik rank refresh tamamlandı: %s", results)
    except Exception:  # noqa: BLE001
        LOGGER.exception("Periyodik rank refresh başarısız.")
    finally:
        _RANK_REFRESH_LOCK.release()


def _run_market_sheets_sync_job(*, trigger_source: str = "scheduler") -> None:
    from backend.services.market_sheets_sync import sync_all_market_sheets

    if not settings.market_sheets_sync_enabled:
        return
    try:
        out = sync_all_market_sheets()
        LOGGER.info(
            "Market sheets sync (%s): ok=%s series=%s/%s rows=%s",
            trigger_source,
            out.get("ok"),
            out.get("ok_count"),
            out.get("series_count"),
            out.get("rows_upserted"),
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Market sheets sync failed (%s): %s", trigger_source, exc)


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

    if settings.app_intel_scheduled_refresh_enabled:
        scheduler.add_job(
            _run_app_intel_scheduled,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.app_intel_scheduled_refresh_hour))),
                minute=max(0, min(59, int(settings.app_intel_scheduled_refresh_minute))),
                timezone=timezone,
            ),
            id="daily-app-intel-digest",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

        # Her 3 saatte bir sadece kategori sırasını güncelle
        from apscheduler.triggers.interval import IntervalTrigger
        scheduler.add_job(
            _run_rank_refresh_job,
            trigger=IntervalTrigger(hours=3, timezone=timezone),
            id="rank-refresh-3h",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=1800,
        )
        job_count += 1

    if settings.market_sheets_sync_enabled:
        scheduler.add_job(
            _run_market_sheets_sync_job,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.market_sheets_sync_hour))),
                minute=max(0, min(59, int(settings.market_sheets_sync_minute))),
                timezone=timezone,
            ),
            id="daily-market-sheets-sync",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if settings.ai_daily_brief_enabled and settings.ai_daily_brief_scheduler_enabled:
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

    if settings.oauth_connection_alert_enabled:
        scheduler.add_job(
            _run_oauth_connection_monitor_job,
            trigger="interval",
            minutes=max(5, int(settings.scheduled_refresh_monitor_interval_minutes)),
            id="oauth-connection-monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    if settings.ga4_realtime_enabled:
        from apscheduler.triggers.interval import IntervalTrigger as _IntTrigger
        scheduler.add_job(
            _run_ga4_realtime_check_job,
            trigger=_IntTrigger(
                # Tüm realtime alarmları tek mailde; aralık + batch_interval ile sıklık sınırlanır.
                minutes=max(5, int(settings.ga4_realtime_interval_minutes)),
                timezone=timezone,
            ),
            id="ga4-realtime-check",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        LOGGER.info(
            "GA4 Realtime monitoring aktif: her %d dk, pencere %d dk.",
            settings.ga4_realtime_interval_minutes,
            settings.ga4_realtime_window_minutes,
        )
        job_count += 1

    # Günlük hata tespiti — her gece 01:30'da, tüm siteler için 4 periyot (1/7/14/30g)
    scheduler.add_job(
        _run_error_detection_job,
        trigger=CronTrigger(hour=1, minute=30, timezone=timezone),
        id="daily-error-detection",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    job_count += 1

    # Günlük meta tag snapshot + kritik regresyon alarmı — 02:15
    scheduler.add_job(
        _run_meta_audit_snapshot_job,
        trigger=CronTrigger(hour=2, minute=15, timezone=timezone),
        id="daily-meta-audit-snapshot",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    job_count += 1

    # Günlük SEO meta tag taraması — GA4 top 250 web + 250 mweb
    if settings.seo_audit_scheduled_enabled:
        scheduler.add_job(
            _run_seo_audit_job,
            trigger=CronTrigger(
                hour=max(0, min(23, int(settings.seo_audit_scheduled_hour))),
                minute=max(0, min(59, int(settings.seo_audit_scheduled_minute))),
                timezone=timezone,
            ),
            id="daily-seo-audit",
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

    # Inbox senkron — varsayılan 5 dk (Gmail → DB; e-posta göndermez).
    from apscheduler.triggers.interval import IntervalTrigger as _InboxIntervalTrigger
    if settings.inbox_scheduled_sync_enabled:
        inbox_iv = max(2, int(settings.inbox_scheduled_sync_interval_minutes))
        scheduler.add_job(
            _run_inbox_scheduled_sync_job,
            trigger=_InboxIntervalTrigger(minutes=inbox_iv, timezone=timezone),
            id="inbox-scheduled-sync",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(600, inbox_iv * 60),
        )
        job_count += 1
        LOGGER.info("Inbox scheduled sync aktif: her %d dk.", inbox_iv)

    if settings.inbox_firebase_sync_enabled:
        fb_iv = max(2, int(settings.inbox_firebase_sync_interval_minutes))
        scheduler.add_job(
            _run_inbox_firebase_sync_job,
            trigger=_InboxIntervalTrigger(minutes=fb_iv, timezone=timezone),
            id="inbox-firebase-sync",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(600, fb_iv * 60),
        )
        job_count += 1
        LOGGER.info("Inbox Firebase sync aktif: her %d dk.", fb_iv)

    # Inbox 4 sekmeli özet maili — 2 saatte bir, çeyrek geçe (:15).
    scheduler.add_job(
        _run_inbox_summary_email_job,
        trigger=CronTrigger(
            minute=15,
            hour="0,2,4,6,8,10,12,14,16,18,20,22",
            timezone=timezone,
        ),
        id="inbox-summary-email-2h",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )
    job_count += 1

    # Eski inbox job id'lerini kaldır
    for _legacy_inbox_job in (
        "inbox-summary-30min",
        "inbox-scheduled-sync-10min",
        "inbox-firebase-sync-3min",
        "inbox-summary-on-hour",
        "inbox-summary-on-half",
    ):
        try:
            scheduler.remove_job(_legacy_inbox_job)
        except Exception:
            pass

    # TMDB vizyon takvimi cache — her gece 02:30'da
    scheduler.add_job(
        _run_tmdb_cache_refresh_job,
        trigger=CronTrigger(hour=2, minute=30, timezone=timezone),
        id="daily-tmdb-cache-refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    job_count += 1

    # Günlük OMDB zenginleştirme — her gece 02:30'da, max 999 film
    if (settings.omdb_api_key or "").strip():
        scheduler.add_job(
            _run_omdb_enrichment_job,
            trigger=CronTrigger(hour=1, minute=0, timezone=timezone),
            id="daily-omdb-enrichment",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        job_count += 1

    # NEWS (07:00 - 23:55 arası her 5 dakikada bir)
    scheduler.add_job(
        _run_news_intelligence_job,
        trigger=CronTrigger(hour='7-23', minute='*/5', timezone=timezone),
        id="news-intelligence-sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    job_count += 1

    if job_count == 0:
        LOGGER.info("All scheduled refresh jobs are disabled via settings.")
        return None

    # Crashlytics günlük çekim — her sabah 06:15 (startup'ta çalışmaz)
    def _run_crashlytics_daily() -> None:
        from backend.services import crashlytics_bq as cbq
        LOGGER.info("Crashlytics günlük çekim başladı.")
        for prod in cbq.list_crashlytics_products():
            pid = prod["id"]
            if cbq.any_platform_ready():
                try:
                    cbq.run_daily_refresh(pid)
                    LOGGER.info("Crashlytics %s yenileme tetiklendi.", pid)
                except Exception as exc:
                    LOGGER.warning("Crashlytics %s yenileme hatası: %s", pid, exc)

    scheduler.add_job(
        _run_crashlytics_daily,
        trigger=CronTrigger(hour=6, minute=15, timezone=timezone),
        id="crashlytics-daily-refresh",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )

    # Crashlytics cache re-warm — her 2 saatte bir (cache TTL 4 saat; soğumasın)
    def _run_crashlytics_prewarm() -> None:
        from backend.services import crashlytics_bq as cbq
        for prod in cbq.list_crashlytics_products():
            pid = prod["id"]
            if cbq.any_platform_ready():
                try:
                    cbq.prewarm_cache(pid)
                except Exception as exc:
                    LOGGER.warning("Crashlytics prewarm hatası (%s): %s", pid, exc)

    from apscheduler.triggers.interval import IntervalTrigger as _CrashIntervalTrigger
    scheduler.add_job(
        _run_crashlytics_prewarm,
        trigger=_CrashIntervalTrigger(hours=1, timezone=timezone),
        id="crashlytics-cache-prewarm-1h",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )

    if settings.doviz_asset_monitor_enabled:
        from apscheduler.triggers.interval import IntervalTrigger as _DovizAssetTrigger

        def _run_doviz_asset_monitor_job():
            try:
                from backend.services.doviz_asset_monitor import cleanup_old_runs, run_doviz_asset_monitor

                with SessionLocal() as db:
                    run_doviz_asset_monitor(db)
                    cleanup_old_runs(db, keep_days=30)
            except Exception as _dz_exc:
                logging.getLogger(__name__).warning("Döviz varlık izleme hatası: %s", _dz_exc)

        dz_iv = max(5, int(settings.doviz_asset_monitor_interval_minutes))
        scheduler.add_job(
            _run_doviz_asset_monitor_job,
            trigger=_DovizAssetTrigger(minutes=dz_iv, timezone=timezone),
            id="doviz-asset-monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(600, dz_iv * 60),
        )
        job_count += 1
        LOGGER.info("Döviz varlık izleme aktif: her %d dk.", dz_iv)

    if settings.doviz_asset_csv_manifest_enabled:
        from apscheduler.triggers.interval import IntervalTrigger as _DovizCsvTrigger

        def _run_doviz_csv_manifest_job():
            try:
                from backend.services.doviz_asset_csv_manifest import (
                    cleanup_old_csv_runs,
                    manifest_url_count,
                    run_doviz_asset_csv_manifest,
                    set_csv_scan_progress,
                )

                with SessionLocal() as db:
                    if manifest_url_count(db) < 1:
                        return
                    set_csv_scan_progress(running=True, done=0, total=0, started_at=None, error=None)
                    run_doviz_asset_csv_manifest(
                        db,
                        on_progress=lambda done, total: set_csv_scan_progress(done=done, total=total),
                    )
                    cleanup_old_csv_runs(db, keep_days=14)
                    set_csv_scan_progress(running=False)
            except Exception as _csv_exc:
                logging.getLogger(__name__).warning("Döviz CSV manifest tarama hatası: %s", _csv_exc)

        csv_iv = max(15, int(settings.doviz_asset_csv_manifest_interval_minutes))
        scheduler.add_job(
            _run_doviz_csv_manifest_job,
            trigger=_DovizCsvTrigger(minutes=csv_iv, timezone=timezone),
            id="doviz-asset-csv-manifest",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(900, csv_iv * 60),
        )
        job_count += 1
        LOGGER.info("Döviz CSV manifest tarama aktif: her %d dk.", csv_iv)

    # AI Talk proaktif izleme — her 30 dakikada bir
    from apscheduler.triggers.interval import IntervalTrigger as _AiMonitorTrigger
    def _run_proactive_monitor():
        try:
            from backend.services.proactive_monitor import run_proactive_checks
            run_proactive_checks()
        except Exception as _e:
            logging.getLogger(__name__).warning("Proaktif izleme hatası: %s", _e)

    scheduler.add_job(
        _run_proactive_monitor,
        trigger=_AiMonitorTrigger(minutes=30),
        id="ai-talk-proactive-monitor",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )

    def _run_notification_analytics_alerts() -> None:
        try:
            from backend.services.notification_analytics_alerts import evaluate_notification_analytics_alerts

            with SessionLocal() as db:
                evaluate_notification_analytics_alerts(db, send_email=True)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning("Notification analytics alert job: %s", exc)

    from apscheduler.triggers.cron import CronTrigger as _NtAlertCron

    scheduler.add_job(
        _run_notification_analytics_alerts,
        trigger=_NtAlertCron(hour=8, minute=15),
        id="notification-analytics-alerts",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )

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
    send_notifications: bool = False,
    bypass_pagespeed_quota: bool = False,
    trigger_source: str = "system",
) -> dict[str, dict]:
    # LIVE_REFRESH yalnızca otomatik tetikleri kapatır; manuel/API `force=True` ile PSI vb. yine çalışır (Railway).
    if not settings.live_refresh_enabled and not force:
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
                results["pagespeed"] = collect_pagespeed_metrics(
                    db,
                    site,
                    send_notifications=send_notifications,
                    bypass_quota=bypass_pagespeed_quota,
                    trigger_source=trigger_source,
                )
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
                results["crawler"] = collect_crawler_metrics(db, site, send_notifications=send_notifications)
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
                results["search_console"] = collect_search_console_metrics(
                    db,
                    site,
                    send_notifications=send_notifications,
                )
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


def _pagespeed_category_score_trend(db, site_id: int, strategy: str, limit: int = 12) -> list[dict]:
    """Son N adet PSI snapshot için 4 kategori skor serisini döner (haftalık trend için).

    Format: [{"date": "2026-04-12", "performance": 78, "accessibility": 85, "best_practices": 60, "seo": 92}, ...]
    En eskiden yeniye doğru sıralı.
    """
    rows = (
        db.query(PageSpeedPayloadSnapshot)
        .filter(PageSpeedPayloadSnapshot.site_id == site_id, PageSpeedPayloadSnapshot.strategy == strategy)
        .order_by(PageSpeedPayloadSnapshot.collected_at.desc())
        .limit(limit * 3)  # gün başı dedup için biraz fazla çek
        .all()
    )
    by_date: dict[str, dict] = {}
    for row in rows:
        try:
            payload = json.loads(row.payload_json or "{}")
        except json.JSONDecodeError:
            continue
        categories = ((payload.get("lighthouseResult") or {}).get("categories") or {})
        date_key = (row.collected_at or datetime.utcnow()).strftime("%Y-%m-%d")
        entry = {
            "date": date_key,
            "performance": None,
            "accessibility": None,
            "best_practices": None,
            "seo": None,
        }
        for src, dst in (("performance", "performance"), ("accessibility", "accessibility"),
                         ("best-practices", "best_practices"), ("seo", "seo")):
            sc = (categories.get(src) or {}).get("score")
            if sc is not None:
                entry[dst] = round(float(sc) * 100.0)
        # Aynı tarih için en yenisi tutulur (DB desc sıralı, ilk gelen en yeni)
        if date_key not in by_date:
            by_date[date_key] = entry
    series = sorted(by_date.values(), key=lambda e: e["date"])[-limit:]
    return series


def _pagespeed_audit_priority_trend(db, site_id: int, strategy: str, limit: int = 12) -> list[dict]:
    """Lighthouse analysis_json içindeki audit önem sayılarını tarih bazlı seri olarak döner.

    Format: [{"date": "2026-04-12", "CRITICAL": 2, "HIGH": 5, "MEDIUM": 12, "LOW": 8}, ...]
    """
    rows = (
        db.query(PageSpeedAuditSnapshot)
        .filter(PageSpeedAuditSnapshot.site_id == site_id, PageSpeedAuditSnapshot.strategy == strategy)
        .order_by(PageSpeedAuditSnapshot.collected_at.desc())
        .limit(limit * 3)
        .all()
    )
    by_date: dict[str, dict] = {}
    for row in rows:
        try:
            analysis = json.loads(row.analysis_json or "{}")
        except json.JSONDecodeError:
            continue
        date_key = (row.collected_at or datetime.utcnow()).strftime("%Y-%m-%d")
        # Önce pre-computed priority_counts varsa kullan, yoksa issues'ı say
        counts = analysis.get("priority_counts")
        if not isinstance(counts, dict):
            counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
            for iss in (analysis.get("issues") or []):
                p = str(iss.get("priority") or "").upper()
                if p in counts:
                    counts[p] += 1
        if date_key not in by_date:
            by_date[date_key] = {
                "date": date_key,
                "CRITICAL": int(counts.get("CRITICAL", 0) or 0),
                "HIGH": int(counts.get("HIGH", 0) or 0),
                "MEDIUM": int(counts.get("MEDIUM", 0) or 0),
                "LOW": int(counts.get("LOW", 0) or 0),
            }
    return sorted(by_date.values(), key=lambda e: e["date"])[-limit:]


def _pagespeed_field_lab_comparison(db, site_id: int, strategy: str) -> list[dict]:
    """Mevcut snapshot'tan field vs lab değerlerini çıkarır.

    Format: [{"key": "lcp", "label": "LCP", "field": 2400, "lab": 3200, "delta_pct": 33.3, "unit": "ms"}, ...]
    """
    payload, _ = _latest_pagespeed_payload_snapshot(db, site_id, strategy)
    if not payload:
        return []
    from backend.collectors.pagespeed import _extract_lighthouse_metrics
    try:
        m = _extract_lighthouse_metrics(payload)
    except Exception:
        return []
    rows: list[dict] = []
    metric_defs = [
        ("lcp", "LCP", "ms"),
        ("fcp", "FCP", "ms"),
        ("ttfb", "TTFB", "ms"),
        ("inp", "INP", "ms"),
        ("cls", "CLS", ""),
    ]
    for key, label, unit in metric_defs:
        field_v = float(m.get(f"{key}_field", 0) or 0)
        lab_v = float(m.get(f"{key}_lab", 0) or 0)
        if field_v == 0 and lab_v == 0:
            continue
        # CLS hariç delta_pct hesapla (CLS oran, 0'a yakın)
        if field_v > 0 and lab_v > 0 and key != "cls":
            delta_pct = round(((lab_v - field_v) / field_v) * 100, 1)
        else:
            delta_pct = None
        rows.append({
            "key": key,
            "label": label,
            "field": field_v if field_v > 0 else None,
            "lab": lab_v if lab_v > 0 else None,
            "delta_pct": delta_pct,
            "unit": unit,
        })
    return rows


def _pagespeed_mobile_desktop_delta(mobile_panel: dict, desktop_panel: dict) -> list[dict]:
    """Mobil ve masaüstü metrik kartlarını karşılaştırıp delta üretir.

    Format: [{"label": "LCP", "mobile": "2.4s", "desktop": "1.6s", "delta_text": "+0.8s mobil daha yavaş", "tone": "negative"}, ...]
    """
    if not mobile_panel or not desktop_panel:
        return []
    mobile_tiles = {t.get("key"): t for t in (mobile_panel.get("metric_tiles") or [])}
    desktop_tiles = {t.get("key"): t for t in (desktop_panel.get("metric_tiles") or [])}
    rows: list[dict] = []
    for key in ("fcp", "lcp", "tbt", "cls", "speed_index"):
        m = mobile_tiles.get(key) or {}
        d = desktop_tiles.get(key) or {}
        m_num = _pagespeed_extract_numeric_from_display(str(m.get("value", "")))
        d_num = _pagespeed_extract_numeric_from_display(str(d.get("value", "")))
        if m_num is None or d_num is None:
            continue
        diff = m_num - d_num
        if abs(diff) < 1e-9:
            continue
        # Pozitif diff = mobil daha kötü (yavaş veya yüksek skor değil, ms cinsinden büyük)
        is_negative = diff > 0  # mobil kötü
        rows.append({
            "key": key,
            "label": m.get("label") or key.upper(),
            "mobile": m.get("value"),
            "desktop": d.get("value"),
            "delta_pct": round((diff / d_num * 100), 1) if d_num > 0 else None,
            "delta_text": _format_pagespeed_delta_text(diff, key),
            "tone": "negative" if is_negative else "positive",
        })
    return rows


def _pagespeed_extract_numeric_from_display(text: str) -> float | None:
    """'2.4 s', '300 ms', '0.05' → numerik ms değeri (s → ms çevrilir)."""
    if not text:
        return None
    m = re.search(r"([-+]?\d*\.?\d+)\s*(ms|s)?", text.strip())
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    unit = (m.group(2) or "").lower()
    if unit == "s":
        return v * 1000.0
    return v


def _format_pagespeed_delta_text(diff_ms: float, metric_key: str) -> str:
    """Delta için kullanıcı dostu açıklama. Pozitif = mobil daha kötü."""
    sign = "+" if diff_ms > 0 else "-"
    abs_v = abs(diff_ms)
    if metric_key == "cls":
        # CLS unitsiz; diff_ms aslında oran farkı
        return f"{sign}{abs_v / 1000:.3f} mobil {'kötü' if diff_ms > 0 else 'iyi'}"
    if abs_v >= 1000:
        return f"{sign}{abs_v / 1000:.2f}s mobil {'yavaş' if diff_ms > 0 else 'hızlı'}"
    return f"{sign}{abs_v:.0f}ms mobil {'yavaş' if diff_ms > 0 else 'hızlı'}"


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


def _pagespeed_metric_benchmark_tr(metric_key: str) -> str:
    """Google CWV (lab) ve Lighthouse ile uyumlu eşikler; arayüzde 'hedef' satırı için."""
    mapping = {
        "fcp": "ideal: ≤1,8sn · hedef ≤3sn",
        "lcp": "ideal: ≤2,5sn · hedef ≤4sn",
        "tbt": "ideal: ≤200ms · hedef ≤600ms",
        "cls": "ideal: ≤0,10 · hedef ≤0,25",
        "speed_index": "ideal: ≤3,4sn · hedef ≤5,8sn",
    }
    return mapping.get(metric_key, "")


PSI_LIGHTHOUSE_CATEGORY_BENCHMARK_TR = "ideal ≥90 · hedef 50–89 · zayıf <50"


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
            "benchmark_tr": _pagespeed_metric_benchmark_tr("fcp"),
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
            "benchmark_tr": _pagespeed_metric_benchmark_tr("lcp"),
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
            "benchmark_tr": _pagespeed_metric_benchmark_tr("tbt"),
            "value": _format_pagespeed_metric_display(audits.get("total-blocking-time") or {}),
            "tone": _pagespeed_metric_tone(
                "tbt",
                _pagespeed_metric_numeric_value(audits.get("total-blocking-time") or {}),
            ),
        },
        {
            "key": "cls",
            "label": "Cumulative Layout Shift",
            "benchmark_tr": _pagespeed_metric_benchmark_tr("cls"),
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
            "benchmark_tr": _pagespeed_metric_benchmark_tr("speed_index"),
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
                "benchmark_tr": PSI_LIGHTHOUSE_CATEGORY_BENCHMARK_TR,
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

    # Yeni zenginleştirmeler — analysis json'unda yeni alanlar mevcutsa al, yoksa boş
    third_party = (analysis or {}).get("third_party") or {}
    priority_counts = (analysis or {}).get("priority_counts") or {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    roi_top = (analysis or {}).get("roi_top") or []
    category_trend = _pagespeed_category_score_trend(db, site_id, strategy, limit=12)
    severity_trend = _pagespeed_audit_priority_trend(db, site_id, strategy, limit=12)
    field_lab = _pagespeed_field_lab_comparison(db, site_id, strategy)

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
        "third_party": third_party,
        "priority_counts": priority_counts,
        "roi_top": roi_top,
        "category_trend": category_trend,
        "severity_trend": severity_trend,
        "field_lab": field_lab,
        "sections": sections,
    }


def _format_crux_series(snapshot: dict | None, current_override: dict[str, dict] | None = None) -> dict[str, dict]:
    summary = (snapshot or {}).get("summary") or {}
    series = summary.get("series") or {}
    # Eski snapshot'larda summary.series, null p75 haftalarını atlayarak üretilmiş olabilir.
    # Ham API record'undan yeniden çıkarınca eksen düzelir (yeniden çekmeye gerek kalmayabilir).
    payload = (snapshot or {}).get("payload") or {}
    hist = payload.get("history") if isinstance(payload.get("history"), dict) else {}
    raw_record = hist.get("record")
    if isinstance(raw_record, dict) and (
        raw_record.get("collectionPeriods") or raw_record.get("metrics") is not None
    ):
        from backend.collectors.crux_history import _extract_crux_points

        series = _extract_crux_points(raw_record) or series
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
        # Son dönem dolu bir noktanın metadata'sı (collection period bilgisi için)
        latest_period_first = ""
        latest_period_last = ""
        for p in reversed(points):
            if p.get("value") is not None:
                latest_period_first = p.get("period_first", "")
                latest_period_last = p.get("period_last", "")
                break
        formatted[metric_key] = {
            "label": current_item.get("label") or item.get("label") or metric_key.upper(),
            "latest": latest_value,
            "good_share": good_share,
            "latest_period_first": latest_period_first,
            "latest_period_last": latest_period_last,
            "chart": {
                "x": [point.get("label") for point in points],
                "y": [point.get("value") for point in points],
                "p25": [point.get("p25") for point in points],
                "p50": [point.get("p50") for point in points],
                "p90": [point.get("p90") for point in points],
                "good_share": [point.get("good_share") for point in points],
                "ni_share": [point.get("ni_share") for point in points],
                "poor_share": [point.get("poor_share") for point in points],
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
        if metric_key == "interaction_to_next_paint":
            alt_mp = metrics_data.get("experimental_interaction_to_next_paint") or {}
            if (not mp or len(mp.get("histogramTimeseries") or []) < 3) and isinstance(alt_mp, dict):
                mp = alt_mp or mp
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


def _build_performance_budget(
    crux_mobile_series: dict | None = None,
    crux_desktop_series: dict | None = None,
) -> dict:
    """Web Vitals "iyi" eşikleri için bütçe paneli — Google önerilen eşikleri kullanır.

    Format: {"items": [{"metric": "LCP", "form_factor": "mobile", "threshold": 2500, "actual": 3200, "exceeded": True, "delta_pct": 28.0, "unit": "ms"}, ...]}
    """
    # Google CWV "iyi" eşikleri — endüstri standardı, kullanıcı override gerekirse ayarlardan eklenebilir
    budgets = {
        "largest_contentful_paint": {"label": "LCP", "threshold": 2500, "unit": "ms"},
        "interaction_to_next_paint": {"label": "INP", "threshold": 200, "unit": "ms"},
        "cumulative_layout_shift": {"label": "CLS", "threshold": 0.1, "unit": ""},
        "first_contentful_paint": {"label": "FCP", "threshold": 1800, "unit": "ms"},
        "experimental_time_to_first_byte": {"label": "TTFB", "threshold": 800, "unit": "ms"},
    }
    entries: list[dict] = []
    summary = {"total": 0, "exceeded": 0, "warning": 0, "ok": 0}
    for form_factor, series in (("mobile", crux_mobile_series or {}), ("desktop", crux_desktop_series or {})):
        for metric_key, budget in budgets.items():
            metric = series.get(metric_key) or {}
            actual = metric.get("latest")
            if actual is None:
                continue
            threshold = budget["threshold"]
            try:
                actual_f = float(actual)
            except (TypeError, ValueError):
                continue
            exceeded = actual_f > threshold
            # %20'den az aşma = uyarı, %20+ aşma = ihlal
            if exceeded:
                delta_pct = ((actual_f - threshold) / threshold * 100) if threshold > 0 else 0
                state = "warning" if delta_pct < 20 else "exceeded"
            else:
                delta_pct = ((actual_f - threshold) / threshold * 100) if threshold > 0 else 0
                state = "ok"
            summary["total"] += 1
            summary[state] += 1
            entries.append({
                "metric": budget["label"],
                "metric_key": metric_key,
                "form_factor": form_factor,
                "threshold": threshold,
                "actual": round(actual_f, 3) if metric_key == "cumulative_layout_shift" else round(actual_f),
                "exceeded": exceeded,
                "state": state,
                "delta_pct": round(delta_pct, 1),
                "unit": budget["unit"],
            })
    # NOT: "items" anahtar adı Jinja'da dict.items() method'u ile çakışır — "entries" kullanılıyor.
    return {
        "entries": entries,
        "summary": summary,
        "has_data": bool(entries),
    }


def _data_explorer_nightly_schedule() -> str:
    """Data Explorer'ı (PSI + CrUX) otomatik yenileyen cron'un HH:MM string'i."""
    hour = max(0, min(23, int(settings.scheduled_refresh_hour)))
    minute = max(0, min(59, int(settings.scheduled_refresh_minute)))
    return f"{hour:02d}:{minute:02d}"


def _build_dashboard_data_explorer_summary(db, only_site_ids: set[int] | None = None) -> dict:
    """Ana sayfa için Data Explorer özet kartı — her site için CrUX p75 + verdict + son güncelleme."""
    from backend.models import CruxHistorySnapshot
    from sqlalchemy import func as sqlfunc

    external_ids = _external_site_ids(db)
    sites = (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .order_by(Site.created_at.desc())
        .all()
    )
    sites = [s for s in sites if s.id not in external_ids]
    if only_site_ids is not None:
        sites = [s for s in sites if s.id in only_site_ids]
    sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))

    # CWV "iyi" eşikleri — Google standardı
    cwv_thresholds = {
        "largest_contentful_paint": (2500, 4000),  # (good, ni)
        "interaction_to_next_paint": (200, 500),
        "cumulative_layout_shift": (0.1, 0.25),
        "first_contentful_paint": (1800, 3000),
        "experimental_time_to_first_byte": (800, 1800),
    }
    metric_short = {
        "largest_contentful_paint": "LCP",
        "interaction_to_next_paint": "INP",
        "cumulative_layout_shift": "CLS",
        "first_contentful_paint": "FCP",
        "experimental_time_to_first_byte": "TTFB",
    }

    def _verdict(metric_key: str, value: float) -> str:
        good, ni = cwv_thresholds.get(metric_key, (0, 0))
        if value <= good:
            return "good"
        if value <= ni:
            return "ni"
        return "poor"

    def _fmt(metric_key: str, value: float) -> str:
        if metric_key == "cumulative_layout_shift":
            return f"{value:.2f}"
        if value >= 1000:
            return f"{value / 1000:.1f}s"
        return f"{int(round(value))}ms"

    rows: list[dict] = []
    for site in sites:
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")
        latest_collected = None
        for snap in (mobile_crux, desktop_crux):
            if snap and snap.get("collected_at"):
                ts = snap.get("collected_at")
                if latest_collected is None or ts > latest_collected:
                    latest_collected = ts

        # Sadece "saha" verisi olan 5 metrik için verdict üret (mobile öncelikli)
        ff_metrics: dict[str, list[dict]] = {"mobile": [], "desktop": []}
        ff_periods: dict[str, tuple[str, str]] = {"mobile": ("", ""), "desktop": ("", "")}
        for form_factor, snap in (("mobile", mobile_crux), ("desktop", desktop_crux)):
            series = _format_crux_series(snap) if snap else {}
            for metric_key, item in series.items():
                latest = item.get("latest")
                if latest is None:
                    continue
                try:
                    v = float(latest)
                except (TypeError, ValueError):
                    continue
                good_share_raw = item.get("good_share")
                good_pct = _safe_pct(good_share_raw) if good_share_raw is not None else None
                p_first = item.get("latest_period_first") or ""
                p_last = item.get("latest_period_last") or ""
                if p_first or p_last:
                    ff_periods[form_factor] = (p_first, p_last)
                ff_metrics[form_factor].append({
                    "key": metric_key,
                    "label": metric_short.get(metric_key, item.get("label", "")),
                    "value": v,
                    "formatted": _fmt(metric_key, v),
                    "verdict": _verdict(metric_key, v),
                    "good_pct": good_pct,
                    "period_first": p_first,
                    "period_last": p_last,
                })

        # Form-factor'ın hangisi varsa öncelikli — varsayılan mobile
        primary_metrics = ff_metrics["mobile"] or ff_metrics["desktop"]
        primary_form_factor = "mobile" if ff_metrics["mobile"] else ("desktop" if ff_metrics["desktop"] else "")
        primary_period = ff_periods.get(primary_form_factor or "mobile", ("", ""))

        def _short_date(s: str) -> str:
            if not s:
                return ""
            try:
                return s[5:10] if len(s) >= 10 else s
            except Exception:
                return s

        period_label = ""
        if primary_period[0] or primary_period[1]:
            period_label = f"{_short_date(primary_period[0])}→{_short_date(primary_period[1])}"

        # Genel skor — verdict sayımı
        good_count = sum(1 for m in primary_metrics if m["verdict"] == "good")
        ni_count = sum(1 for m in primary_metrics if m["verdict"] == "ni")
        poor_count = sum(1 for m in primary_metrics if m["verdict"] == "poor")
        if poor_count > 0:
            overall = "poor"
        elif ni_count > 0:
            overall = "ni"
        elif good_count > 0:
            overall = "good"
        else:
            overall = "no_data"

        rows.append({
            "domain": site.domain,
            "display_name": site.display_name,
            "form_factor": primary_form_factor,
            "metrics": primary_metrics,
            "overall": overall,
            "good_count": good_count,
            "ni_count": ni_count,
            "poor_count": poor_count,
            "last_updated": format_datetime_like(latest_collected, fallback="—"),
            "has_data": bool(primary_metrics),
            "href": f"/data-explorer/{site.domain}",
            "period_label": period_label,
        })
    return {
        "rows": rows,
        "schedule": _data_explorer_nightly_schedule(),
    }


def _build_error_widget(db, site_id: int) -> dict:
    """Son 7 günde sitedeki hata kayıtlarının özetini döner (404, 5xx).

    Format: {"total_404": 12, "total_5xx": 3, "top_404_urls": [{"url": "...", "hits": 42}, ...]}
    """
    from sqlalchemy import func as sqlfunc
    cutoff = datetime.utcnow() - timedelta(days=7)
    rows = (
        db.query(SiteErrorLog)
        .filter(SiteErrorLog.site_id == site_id, SiteErrorLog.last_seen >= cutoff)
        .all()
    )
    total_404 = 0
    total_5xx = 0
    by_url_404: dict[str, int] = {}
    for r in rows:
        sc = int(r.status_code or 0)
        hits = int(r.hit_count or 0)
        if sc == 404:
            total_404 += hits
            by_url_404[r.url] = by_url_404.get(r.url, 0) + hits
        elif 500 <= sc < 600:
            total_5xx += hits
    top_404 = sorted(by_url_404.items(), key=lambda kv: -kv[1])[:5]
    return {
        "total_404": total_404,
        "total_5xx": total_5xx,
        "top_404_urls": [{"url": u, "hits": h} for u, h in top_404],
        "window_days": 7,
    }


def _data_explorer_context(domain: str) -> dict:
    with SessionLocal() as db:
        site = _resolve_site_by_domain(db, domain)
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
        psi_collected = _psi_lighthouse_metrics_latest_collected_at(db, site.id)
        crux_collected = _crux_history_latest_collected_at(db, site.id)
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
        mobile_panel = _build_pagespeed_report_panel(db, site.id, "mobile", mobile_lighthouse_analysis)
        desktop_panel = _build_pagespeed_report_panel(db, site.id, "desktop", desktop_lighthouse_analysis)
        mobile_desktop_delta = _pagespeed_mobile_desktop_delta(mobile_panel, desktop_panel)
        error_widget = _build_error_widget(db, site.id)
        # Gece otomatik yenileme saati — `_run_daily_refresh_job` cron'u
        schedule_str = _data_explorer_nightly_schedule()

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
            "pagespeed_report_mobile": mobile_panel,
            "pagespeed_report_desktop": desktop_panel,
            "mobile_desktop_delta": mobile_desktop_delta,
            "error_widget": error_widget,
            "data_explorer_schedule": schedule_str,
            "data_explorer_last_auto_refresh": _data_explorer_last_auto_refresh_label(db, site.id),
            "data_explorer_auto_refresh_log": _build_data_explorer_auto_refresh_log(db, site.id),
            "data_explorer_scheduler_health": _data_explorer_scheduler_health(),
            "psi_lighthouse_last_updated": format_local_datetime(
                psi_collected,
                fallback="Henüz PSI/Lighthouse ölçümü yok",
            ),
            "crux_history_last_updated": format_local_datetime(
                crux_collected,
                fallback="Henüz CrUX geçmişi yok",
            ),
            "crux_mobile_last_updated": format_datetime_like(
                (mobile_crux or {}).get("collected_at"),
                fallback="Henüz mobil CrUX kaydı yok",
            ),
            "crux_desktop_last_updated": format_datetime_like(
                (desktop_crux or {}).get("collected_at"),
                fallback="Henüz masaüstü CrUX kaydı yok",
            ),
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
        site = _resolve_site_by_domain(db, domain)
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
        psi_collected = _psi_lighthouse_metrics_latest_collected_at(db, site.id)
        crux_collected = _crux_history_latest_collected_at(db, site.id)

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
            "site_id": site.id,
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
            "psi_lighthouse_last_updated": format_local_datetime(
                psi_collected,
                fallback="Henüz PSI/Lighthouse ölçümü yok",
            ),
            "crux_history_last_updated": format_local_datetime(
                crux_collected,
                fallback="Henüz CrUX geçmişi yok",
            ),
            "warehouse_summary": warehouse,
            "site_alerts": get_site_alerts(db, site_id=site.id, limit=200),
            "backlinks_href": f"/backlinks?site_id={site.id}",
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
    """Search Console kartı için tek dönem (1g/7g/30g) etiketler; tıklama/CTR %, pozisyon mutlak fark."""
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
        "position_change": _sc_position_delta(npc, npp),
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
        "sessions_last": float(pl.get("last_total") or 0.0) if pl.get("has_period_data") else None,
        "organic_share_pct": float(pl.get("organic_share_pct") or 0.0) if pl.get("has_period_data") else None,
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
            if clicks_diff >= 0 and position_diff >= -0.15:
                continue

            reason = "Tıklama düşüşü"
            tone = "rose"
            metric = f"{_format_compact_number(clicks_previous)} -> {_format_compact_number(clicks_current)} tıklama"
            secondary = ""
            impact = abs(clicks_diff)

            if clicks_diff < 0 and position_diff < -0.15:
                reason = "Tıklama + pozisyon kaybı"
                metric = f"{_format_compact_number(clicks_previous)} -> {_format_compact_number(clicks_current)} tıklama"
                secondary = f"Pozisyon {_format_max_two_decimals(position_previous)} -> {_format_max_two_decimals(position_current)}"
                impact = abs(clicks_diff) + abs(position_diff) * 1000
            elif position_diff < -0.15:
                reason = "Pozisyon düşüşü"
                tone = "rose"
                metric = f"Pozisyon {_format_max_two_decimals(position_previous)} -> {_format_max_two_decimals(position_current)}"
                secondary = f"{_format_compact_number(clicks_current)} tıklama"
                impact = max(clicks_current, 1.0) + abs(position_diff) * 1000

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

            if clicks_current >= 2000 and 3.0 <= position_current <= 8.0 and position_diff >= -0.25:
                title = "İlk sayfa fırsatı"
                detail = f"Pozisyon {_format_max_two_decimals(position_current)} · {_format_compact_number(clicks_current)} tıklama"
                action = "İçerik, başlık ve iç link güçlendirmesiyle daha yukarı taşınabilir."
                tone = "sky"
                score = clicks_current / max(position_current, 1.0)
            elif clicks_current >= 800 and position_diff > 0.15 and clicks_current >= clicks_previous:
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
    denied = _ensure_panel_session(request)
    if denied is not None:
        return denied
    with SessionLocal() as db:
        recent_alerts = get_recent_alerts(db, limit=100, include_external=False)
        external_ids = _external_site_ids(db)
        all_sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_ids]
        all_sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        sites = list(all_sites)
        selected_site_raw = (request.query_params.get("site") or "").strip()
        selected_site_id = int(selected_site_raw) if selected_site_raw.isdigit() else None
        if selected_site_id is not None:
            sites = [s for s in sites if s.id == selected_site_id]
            if not sites:
                selected_site_id = None
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
            "site_filters": [(s.id, s.display_name) for s in all_sites],
            "selected_site_id": selected_site_id,
            "top_drop_items": _build_dashboard_top_drops(slim_cards, limit=7, recent_alerts=recent_alerts),
            "opportunity_items": _build_dashboard_opportunities(slim_cards, limit=8, recent_alerts=recent_alerts),
            "ga4_realtime_ui_poll_seconds": settings.ga4_realtime_ui_poll_seconds,
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
                send_notifications=True,
                bypass_pagespeed_quota=settings.pagespeed_manual_refresh_bypass_quota,
                trigger_source="manual",
            )
            if not search_console_connected:
                try:
                    results["crux_history"] = collect_crux_history(db, site, trigger_source="manual")
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
            flash_message = _friendly_measure_error_message(exc)

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


# ─────────────────────────────────────────────────────────────────────────────
# Yeni Ana Sayfa API'leri — HTMX lazy-load partial endpoint'leri.
# Tüm endpoint'ler HTMLResponse döner; hata durumunda boş/uyarı state'i gösterilir.
# Tab routing veya ana navigation'a dokunmaz; sadece dashboard_content.html içine doldurur.
# ─────────────────────────────────────────────────────────────────────────────

_HOME_SITE_DOMAINS = {1: ("doviz.com", "Döviz"), 2: ("www.sinemalar.com", "Sinemalar")}
_HOME_DOVIZ_PROFILES = [("web", "Web"), ("mweb", "MWeb"), ("ios", "iOS"), ("android", "Android")]
_HOME_SINEMA_PROFILES = [("web", "Web"), ("mweb", "MWeb")]
_HOME_REALTIME_TREND_LIMIT = 120


def _home_format_int(n: float) -> str:
    try:
        n = int(round(float(n)))
    except (TypeError, ValueError):
        return "—"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{n/1_000:.1f}K".replace(".0K", "K")
    return str(n)


def _home_format_int_exact(n: float) -> str:
    try:
        n = int(round(float(n)))
    except (TypeError, ValueError):
        return "—"
    return f"{n:,}".replace(",", ".")


def _home_cf_fmt(pct: float | None) -> str:
    if pct is None:
        return "—"
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return "—"
    if v >= 99.995:
        return f"{v:.4f}%"
    return f"{v:.2f}%"


def _home_crashlytics_card(product_id: str) -> dict:
    """Ana sayfa Firebase/Crashlytics mini kart — cache-only, soğuksa arka planda ısıtır."""
    from backend.services import crashlytics_bq as cbq
    from backend.services.app_intel import APP_PRODUCTS

    pid = (product_id or "doviz").strip().lower()
    label = APP_PRODUCTS.get(pid, {}).get("label") or pid
    out: dict = {
        "product_id": pid,
        "product_label": label,
        "ok": False,
        "warming": False,
        "days": 7,
    }
    if pid not in APP_PRODUCTS:
        out["message"] = "Ürün tanımlı değil"
        return out

    payload = cbq.peek_cached_payload(pid, days=7, platform_filter="all")
    if not payload:
        cbq.prewarm_cache(pid)
        out["warming"] = True
        out["message"] = "Crashlytics verisi arka planda yükleniyor…"
        return out

    if not payload or payload.get("ok") is False:
        out["message"] = (payload or {}).get("message") or "BigQuery verisi yok"
        out["configured"] = (payload or {}).get("configured", True)
        return out

    totals = payload.get("totals") or {}
    summary_by = payload.get("summary_by_platform") or {}
    cf_by = payload.get("crash_free_by_platform") or {}
    issues_by = payload.get("issues_by_platform") or {}

    platforms: list[dict] = []
    for plat, plat_label in (("ios", "iOS"), ("android", "Android")):
        summ = summary_by.get(plat) or {}
        cf = cf_by.get(plat) or {}
        cf_pct = cf.get("crash_free_sessions_pct")
        if cf_pct is None:
            cf_pct = cf.get("crash_free_pct")
        issues = issues_by.get(plat) or []
        top = issues[0] if issues else None
        platforms.append(
            {
                "key": plat,
                "label": plat_label,
                "crash_free_fmt": _home_cf_fmt(cf_pct),
                "fatal_fmt": _home_format_int(summ.get("fatal") or 0),
                "anr_fmt": _home_format_int(summ.get("anr") or 0),
                "top_issue_title": ((top.get("title") or top.get("issue_title") or "")[:72] if top else None),
                "top_issue_events_fmt": _home_format_int(top.get("event_count") or 0) if top else None,
                "has_data": bool(summ.get("fatal") or summ.get("anr") or issues),
            }
        )

    top_all = (payload.get("issues") or [])[:1]
    top_global = top_all[0] if top_all else None

    out.update(
        {
            "ok": True,
            "days": payload.get("days") or 7,
            "crash_free_fmt": _home_cf_fmt(
                payload.get("crash_free_sessions_pct") or payload.get("crash_free_pct")
            ),
            "fatal_fmt": _home_format_int(totals.get("fatal") or 0),
            "anr_fmt": _home_format_int(totals.get("anr") or 0),
            "non_fatal_fmt": _home_format_int(totals.get("non_fatal") or 0),
            "platforms": platforms,
            "top_issue_title": (
                (top_global.get("title") or top_global.get("issue_title") or "")[:72] if top_global else None
            ),
            "top_issue_events_fmt": (
                _home_format_int(top_global.get("event_count") or 0) if top_global else None
            ),
        }
    )
    return out


def _home_pct_delta(cur: float, prev: float) -> tuple[str, str, float]:
    try:
        c = float(cur); p = float(prev)
    except (TypeError, ValueError):
        return ("—", "flat", 0.0)
    if p <= 0:
        if c > 0:
            return ("+∞", "up", 999.0)
        return ("0%", "flat", 0.0)
    pct = (c - p) / p * 100.0
    tone = "up" if pct > 0.5 else ("down" if pct < -0.5 else "flat")
    sign = "+" if pct > 0 else ""
    return (f"{sign}{pct:.1f}%", tone, round(pct, 2))


def _home_spark_paths(values: list[float], *, width: int = 128, height: int = 38, pad: int = 3) -> dict:
    clean: list[float] = []
    for value in values:
        try:
            clean.append(float(value or 0))
        except (TypeError, ValueError):
            continue

    if not clean:
        return {"has_points": False}
    if len(clean) == 1:
        clean = [clean[0], clean[0]]

    min_v = min(clean)
    max_v = max(clean)
    span = max_v - min_v
    if span <= 0:
        span = max(abs(max_v) * 0.12, 1.0)
        min_v = max(0.0, min_v - span / 2)
        max_v = max_v + span / 2
    else:
        min_v = max(0.0, min_v - span * 0.12)
        max_v = max_v + span * 0.12
        span = max_v - min_v

    inner_w = width - pad * 2
    inner_h = height - pad * 2
    denom = max(len(clean) - 1, 1)
    points: list[tuple[float, float]] = []
    for idx, value in enumerate(clean):
        x = pad + (inner_w * idx / denom)
        y = pad + inner_h - ((value - min_v) / span * inner_h)
        points.append((round(x, 2), round(y, 2)))

    def _polyline_path_d(pts: list[tuple[float, float]]) -> str:
        if not pts:
            return ""
        if len(pts) == 1:
            x, y = pts[0]
            return "M %.2f %.2f" % (x, y)
        segs = ["M %.2f %.2f" % pts[0]]
        for x, y in pts[1:]:
            segs.append("L %.2f %.2f" % (x, y))
        return " ".join(segs)

    path_d = _polyline_path_d(points)
    mean_v = sum(clean) / len(clean)

    line_segments: list[dict] = []
    seg_pts: list[tuple[float, float]] = [points[0]]
    seg_above = clean[0] >= mean_v
    for idx in range(1, len(clean)):
        v0, v1 = clean[idx - 1], clean[idx]
        p0, p1 = points[idx - 1], points[idx]
        above0 = v0 >= mean_v
        above1 = v1 >= mean_v
        if above0 != above1 and v1 != v0:
            t = (mean_v - v0) / (v1 - v0)
            t = max(0.0, min(1.0, t))
            cx = p0[0] + t * (p1[0] - p0[0])
            cy = p0[1] + t * (p1[1] - p0[1])
            seg_pts.append((round(cx, 2), round(cy, 2)))
            line_segments.append({"above": seg_above, "path_d": _polyline_path_d(seg_pts)})
            seg_pts = [(round(cx, 2), round(cy, 2)), p1]
            seg_above = above1
        else:
            seg_pts.append(p1)
            seg_above = above1
    if len(seg_pts) >= 2:
        line_segments.append({"above": seg_above, "path_d": _polyline_path_d(seg_pts)})

    first_x, _first_y = points[0]
    last_x, _last_y = points[-1]
    baseline = height - pad
    area_path = f"{path_d} L {last_x:.2f} {baseline:.2f} L {first_x:.2f} {baseline:.2f} Z"
    last_value = clean[-1]

    return {
        "has_points": True,
        "path_d": path_d,
        "area_path": area_path,
        "line_segments": line_segments,
        "mean_value": mean_v,
        "end_x": points[-1][0],
        "end_y": points[-1][1],
        "last_value_fmt": _home_format_int(last_value),
        "point_count": len(clean),
    }


def _home_build_realtime_profile(site_id: int, prof_key: str, prof_label: str, bundle: dict) -> dict:
    from backend.services.ga4_realtime import active_users_kpi_from_realtime_result

    empty = {
        "key": prof_key,
        "label": prof_label,
        "value_fmt": "—",
        "value_raw": 0.0,
        "pageviews_fmt": "—",
        "pageviews_raw": 0.0,
        "delta_fmt": None,
        "delta_tone": "flat",
        "delta_pct": 0.0,
        "spark": {"has_points": False},
    }
    trend = bundle.get("trend") or []
    if bundle.get("error") and not trend:
        return empty

    cur, delta_fmt, tone, delta_pct = active_users_kpi_from_realtime_result(bundle)
    pv_raw = _home_realtime_main_metric(bundle, "screenPageViews")
    spark = _home_spark_paths([float(t.get("active_users") or 0) for t in trend])
    if not trend and cur <= 0 and bundle.get("error"):
        return empty

    return {
        "key": prof_key,
        "label": prof_label,
        "value_fmt": _home_format_int(cur),
        "value_raw": float(cur or 0),
        "pageviews_fmt": _home_format_int(pv_raw),
        "pageviews_raw": float(pv_raw or 0),
        "delta_fmt": delta_fmt,
        "delta_tone": tone,
        "delta_pct": delta_pct,
        "spark": spark,
    }


def _home_realtime_main_metric(bundle: dict, key: str) -> float:
    total = bundle.get("total") or {}
    if total.get(key) is not None:
        return float(total.get(key) or 0)
    comp = (bundle.get("comparison") or {}).get(key)
    if comp and comp.get("current") is not None:
        return float(comp.get("current") or 0)
    cur = (bundle.get("current") or {}).get(key)
    if cur is not None:
        return float(cur or 0)
    trend = bundle.get("trend") or []
    if trend:
        last = trend[-1]
        if key == "activeUsers":
            return float(last.get("active_users") or 0)
        if key == "screenPageViews":
            return float(last.get("pageviews") or 0)
    return 0.0


def _home_get_site(db, site_id: int):
    return db.query(Site).filter(Site.id == site_id).first()


def _home_site_filter_ids(site: str | None) -> set[int] | None:
    """`site=doviz|sinemalar` query param'ı site_id setine çevirir. None = filtre yok."""
    if not site:
        return None
    s = str(site).strip().lower()
    if s in ("doviz", "doviz.com", "1"):
        return {1}
    if s in ("sinemalar", "sinemalar.com", "2"):
        return {2}
    return None


def _home_load_realtime_for_site(db, site_id: int, profiles: list[tuple[str, str]]) -> list[dict]:
    """Tek site — profiller paralel (her iş parçacığı kendi DB oturumu)."""
    from backend.services.ga4_realtime import fetch_home_realtime_profile_bundle

    site_obj = _home_get_site(db, site_id)
    if site_obj is None:
        return []
    window = settings.ga4_realtime_window_minutes

    def _one(prof_key: str, prof_label: str) -> dict:
        from backend.database import SessionLocal

        with SessionLocal() as worker_db:
            worker_site = _home_get_site(worker_db, site_id)
            if worker_site is None:
                return _home_build_realtime_profile(site_id, prof_key, prof_label, {"error": "site", "trend": []})
            bundle = fetch_home_realtime_profile_bundle(
                worker_db,
                worker_site,
                profile=prof_key,
                window_minutes=window,
                trend_limit=_HOME_REALTIME_TREND_LIMIT,
            )
            return _home_build_realtime_profile(site_id, prof_key, prof_label, bundle)

    if len(profiles) <= 1:
        return [_one(pk, pl) for pk, pl in profiles]

    from concurrent.futures import ThreadPoolExecutor, as_completed

    order = {pk: i for i, (pk, _) in enumerate(profiles)}
    by_key: dict[str, dict] = {}
    max_workers = min(8, len(profiles))
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="home-rt") as pool:
        futs = {pool.submit(_one, pk, pl): pk for pk, pl in profiles}
        for fut in as_completed(futs):
            pk = futs[fut]
            try:
                by_key[pk] = fut.result()
            except Exception:
                pl = next(pl for p, pl in profiles if p == pk)
                by_key[pk] = _home_build_realtime_profile(site_id, pk, pl, {"error": "worker", "trend": []})

    return [by_key[pk] for pk, _ in sorted(profiles, key=lambda t: order[t[0]])]


@app.get("/api/home/realtime", response_class=HTMLResponse)
def api_home_realtime(request: Request, site: str | None = None):
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        sites_out = []
        for site_id, profs in [(1, _HOME_DOVIZ_PROFILES), (2, _HOME_SINEMA_PROFILES)]:
            if _site_filter is not None and site_id not in _site_filter:
                continue
            site_obj = _home_get_site(db, site_id)
            if site_obj is None:
                continue
            profiles = _home_load_realtime_for_site(db, site_id, profs)
            total_au = sum(float(p.get("value_raw") or 0) for p in profiles)
            total_pv = sum(float(p.get("pageviews_raw") or 0) for p in profiles)
            sites_out.append({
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "profiles": profiles,
                "total_active_fmt": _home_format_int_exact(total_au),
                "total_pageviews_fmt": _home_format_int_exact(total_pv),
            })
    now_label = datetime.now(ZoneInfo("Europe/Istanbul")).strftime("%H:%M")
    return templates.TemplateResponse(
        request, "partials/home/realtime.html",
        context={
            "request": request,
            "sites": sites_out,
            "now_label": now_label,
            "poll_seconds": settings.ga4_realtime_ui_poll_seconds,
        },
    )


def _home_ga4_sessions_from_snap(db, site_id: int, prof_key: str, period_days: int) -> tuple[float, float]:
    """Snapshot'tan (last, prev) session değerlerini çek. Sıfırsa None döner."""
    snap = (
        db.query(Ga4ReportSnapshot)
        .filter(
            Ga4ReportSnapshot.site_id == site_id,
            Ga4ReportSnapshot.profile == prof_key,
            Ga4ReportSnapshot.period_days == period_days,
        )
        .order_by(Ga4ReportSnapshot.collected_at.desc())
        .first()
    )
    if snap is None:
        return (0.0, 0.0)
    try:
        payload = json.loads(snap.payload_json or "{}")
    except Exception:
        return (0.0, 0.0)
    summary = payload.get("summary") or {}
    last_v = float(summary.get("last", {}).get("sessions") or 0.0)
    prev_v = float(summary.get("prev", {}).get("sessions") or 0.0)
    return (last_v, prev_v)


def _home_load_ga4_sessions_for_site(db, site_id: int, profiles: list[tuple[str, str]]) -> list[dict]:
    # Metrik tablosundan fallback için
    try:
        latest_metrics = get_latest_metrics(db, site_id=site_id)
    except Exception:
        latest_metrics = {}

    out = []
    for prof_key, prof_label in profiles:
        # 1. period_days=7 snapshot dene
        last_v, prev_v = _home_ga4_sessions_from_snap(db, site_id, prof_key, 7)
        # 2. Sıfırsa period_days=30 dene
        if last_v <= 0 and prev_v <= 0:
            last_v, prev_v = _home_ga4_sessions_from_snap(db, site_id, prof_key, 30)
        # 3. Hâlâ sıfırsa metrik tablosu fallback (collector'ın yazdığı flat key)
        if last_v <= 0:
            for pd in (7, 30):
                v = float(latest_metrics.get(f"ga4_{prof_key}_sessions_last{pd}d_total") or 0.0)
                if v > 0:
                    last_v = v
                    prev_v = float(latest_metrics.get(f"ga4_{prof_key}_sessions_prev{pd}d_total") or 0.0)
                    break
        delta_fmt, tone, delta_pct = _home_pct_delta(last_v, prev_v)
        out.append({
            "label": prof_label,
            "last_fmt": _home_format_int(last_v),
            "prev_fmt": _home_format_int(prev_v),
            "delta_fmt": delta_fmt,
            "tone": tone,
            "delta_pct": delta_pct,
        })
    return out


@app.get("/api/home/ga4-sessions", response_class=HTMLResponse)
def api_home_ga4_sessions(request: Request, site: str | None = None):
    sites_out = []
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        for site_id, profs in [(1, _HOME_DOVIZ_PROFILES), (2, _HOME_SINEMA_PROFILES)]:
            if _site_filter is not None and site_id not in _site_filter:
                continue
            site_obj = _home_get_site(db, site_id)
            if site_obj is None:
                continue
            sites_out.append({
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "profiles": _home_load_ga4_sessions_for_site(db, site_id, profs),
            })
    return templates.TemplateResponse(
        request, "partials/home/ga4_sessions.html",
        context={"request": request, "sites": sites_out},
    )


def _home_sc_device_aggregate(db, site_id: int, device: str) -> dict:
    """Tek site & device için current_7d ve previous_7d toplamları."""
    from sqlalchemy import func as sa_func
    latest_ts = db.query(sa_func.max(SearchConsoleQuerySnapshot.collected_at)).filter(
        SearchConsoleQuerySnapshot.site_id == site_id
    ).scalar()

    def _sum(scope: str) -> tuple[float, float]:
        if not latest_ts:
            return (0.0, 0.0)
        row = (
            db.query(
                sa_func.coalesce(sa_func.sum(SearchConsoleQuerySnapshot.clicks), 0.0),
                sa_func.coalesce(sa_func.sum(SearchConsoleQuerySnapshot.impressions), 0.0),
            )
            .filter(
                SearchConsoleQuerySnapshot.site_id == site_id,
                SearchConsoleQuerySnapshot.data_scope == scope,
                SearchConsoleQuerySnapshot.device == device,
                SearchConsoleQuerySnapshot.collected_at == latest_ts,
            )
            .first()
        )
        return (float(row[0] or 0.0), float(row[1] or 0.0)) if row else (0.0, 0.0)

    c_clicks, c_impr = _sum("current_7d")
    p_clicks, p_impr = _sum("previous_7d")
    clicks_delta, clicks_tone, clicks_delta_pct = _home_pct_delta(c_clicks, p_clicks)
    impr_delta, impr_tone, impr_delta_pct = _home_pct_delta(c_impr, p_impr)
    return {
        "clicks_last_fmt": _home_format_int(c_clicks),
        "clicks_prev_fmt": _home_format_int(p_clicks),
        "clicks_delta_fmt": clicks_delta,
        "clicks_tone": clicks_tone,
        "clicks_delta_pct": clicks_delta_pct,
        "impr_last_fmt": _home_format_int(c_impr),
        "impr_prev_fmt": _home_format_int(p_impr),
        "impr_delta_fmt": impr_delta,
        "impr_tone": impr_tone,
        "impr_delta_pct": impr_delta_pct,
    }


@app.get("/api/home/sc-summary", response_class=HTMLResponse)
def api_home_sc_summary(request: Request, site: str | None = None):
    sites_out = []
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        for site_id in (1, 2):
            if _site_filter is not None and site_id not in _site_filter:
                continue
            site_obj = _home_get_site(db, site_id)
            if site_obj is None:
                continue
            devices = []
            for dev_code, dev_label in (("MOBILE", "Mobil Web"), ("DESKTOP", "Web")):
                agg = _home_sc_device_aggregate(db, site_id, dev_code)
                agg["label"] = dev_label
                devices.append(agg)
            sites_out.append({
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "devices": devices,
            })
    return templates.TemplateResponse(
        request, "partials/home/sc_summary.html",
        context={"request": request, "sites": sites_out},
    )


def _home_position_drops_for_site(db, site_id: int, limit: int | None = None) -> dict:
    """SC M+Web ağırlıklı pozisyon — alert motoru ile aynı kaynak."""
    from backend.services.alert_engine import HOME_POSITION_DROPS_ROW_LIMIT, list_sc_position_drops_7d

    site_obj = _home_get_site(db, site_id)
    if site_obj is None:
        return {"drops": []}
    lim = HOME_POSITION_DROPS_ROW_LIMIT if limit is None else limit
    return list_sc_position_drops_7d(db, site_obj, limit=lim)


def _live_position_drop_sites(db, domain: str | None = None) -> list[dict]:
    """Ana sayfa ile aynı canlı pozisyon listesi — /alerts üst bölümü."""
    dom = (domain or "").strip()
    out: list[dict] = []
    for site_id in (1, 2):
        site_obj = _home_get_site(db, site_id)
        if site_obj is None:
            continue
        if dom and site_obj.domain != dom:
            continue
        drop_payload = _home_position_drops_for_site(db, site_id)
        out.append(
            {
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name or site_obj.domain,
                "drops": drop_payload.get("drops") or [],
                "rises": drop_payload.get("rises") or [],
                "as_of_label": drop_payload.get("as_of_label"),
                "as_of_iso": drop_payload.get("as_of_iso"),
                "scope_label": drop_payload.get("scope_label"),
                "period_label": drop_payload.get("period_label"),
                "sort_label": drop_payload.get("sort_label"),
                "alerts_href": f"/alerts?domain={site_obj.domain}&focus=position",
            }
        )
    return out


def _sc_alert_scan_note(db) -> str:
    """Üst canlı liste — tek satır güncelleme tarihi (TSİ)."""
    from backend.services.alert_engine import _sc_position_data_as_of, get_latest_search_console_alert_run
    from backend.services.timezone_utils import format_local_datetime

    best_raw = None
    for site_id in (1, 2):
        site_obj = _home_get_site(db, site_id)
        if site_obj is None:
            continue
        raw, _, _ = _sc_position_data_as_of(db, site_obj)
        if raw is not None:
            if best_raw is None or raw > best_raw:
                best_raw = raw
            continue
        run = get_latest_search_console_alert_run(db, site_id)
        if run is not None and run.finished_at is not None:
            rt = run.finished_at
            if best_raw is None or rt > best_raw:
                best_raw = rt

    if best_raw is None:
        return ""
    label = format_local_datetime(best_raw, fmt="%d.%m.%Y %H:%M", include_suffix=True)
    return label or ""


def home_summary_payload(db) -> dict:
    """Ana sayfa (Günün Özeti) — AI Talk için yapılandırılmış metrik özeti."""
    sites_out: list[dict] = []
    for site_id, rt_profs in [(1, _HOME_DOVIZ_PROFILES), (2, _HOME_SINEMA_PROFILES)]:
        site_obj = _home_get_site(db, site_id)
        if site_obj is None:
            continue
        ga4_profs = _HOME_DOVIZ_PROFILES if site_id == 1 else _HOME_SINEMA_PROFILES
        sc_devices = []
        for dev_code, dev_label in (("MOBILE", "Mobil Web"), ("DESKTOP", "Web")):
            agg = _home_sc_device_aggregate(db, site_id, dev_code)
            sc_devices.append({"label": dev_label, **agg})
        sites_out.append(
            {
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "ga4_sessions_7d": _home_load_ga4_sessions_for_site(db, site_id, ga4_profs),
                "search_console_7d": sc_devices,
                "position_drops_7d": (_home_position_drops_for_site(db, site_id, limit=8).get("drops") or []),
            }
        )
    return {
        "page": "home",
        "title": "Günün Özeti",
        "updated_at": datetime.now(ZoneInfo("Europe/Istanbul")).isoformat(),
        "sites": sites_out,
    }


@app.get("/api/home/summary")
def api_home_summary():
    """Ana sayfa metrikleri — JSON (AI Talk + frontend context)."""
    with SessionLocal() as db:
        return JSONResponse(home_summary_payload(db))


@app.get("/api/home/position-drops", response_class=HTMLResponse)
def api_home_position_drops(request: Request, site: str | None = None):
    sites_out = []
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        for site_id in (1, 2):
            if _site_filter is not None and site_id not in _site_filter:
                continue
            site_obj = _home_get_site(db, site_id)
            if site_obj is None:
                continue
            drop_payload = _home_position_drops_for_site(db, site_id)
            sites_out.append({
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "drops": drop_payload.get("drops") or [],
                "rises": drop_payload.get("rises") or [],
                "as_of_label": drop_payload.get("as_of_label"),
                "scope_label": drop_payload.get("scope_label"),
                "period_label": drop_payload.get("period_label"),
                "sort_label": drop_payload.get("sort_label"),
                "alerts_href": f"/alerts?domain={site_obj.domain}&focus=position",
            })
    return templates.TemplateResponse(
        request, "partials/home/position_drops.html",
        context={"request": request, "sites": sites_out},
    )


def _home_parse_iso_date(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def _home_app_raw_from_db(db, product_id: str) -> dict | None:
    """AppIntelRawCache tablosundan ham payload'ı direkt oku — cache_only fallback."""
    try:
        row = db.query(AppIntelRawCache).filter(AppIntelRawCache.product_id == product_id).first()
        if row and row.payload_json:
            raw = json.loads(row.payload_json)
            if isinstance(raw, dict) and raw.get("product_id") == product_id:
                return raw
    except Exception:
        pass
    return None


def _home_build_app_platform(raw: dict, key: str, label: str, version_key: str, date_key: str) -> dict:
    meta = (raw.get(key) or {}).get("meta") or {}
    tz_utc = ZoneInfo("UTC")
    tz_ist = ZoneInfo("Europe/Istanbul")
    now = datetime.now(tz_utc)
    ver = meta.get(version_key)
    updated_raw = meta.get(date_key)
    updated_dt = _home_parse_iso_date(updated_raw)
    updated_label = None
    is_recent = False
    if updated_dt is not None:
        try:
            if updated_dt.tzinfo is None:
                updated_dt = updated_dt.replace(tzinfo=tz_utc)
            updated_label = updated_dt.astimezone(tz_ist).strftime("%d %b %Y")
            is_recent = (now - updated_dt).days <= 7
        except Exception:
            updated_label = str(updated_raw)
    score = meta.get("score")
    score_fmt = f"{float(score):.2f}" if isinstance(score, (int, float)) else "—"
    ratings_val = meta.get("ratings") if key == "android" else meta.get("ratings_count")
    ratings_fmt = _home_format_int(ratings_val) if ratings_val else "—"
    rank = (meta.get("category_rank") or {}).get("rank") if isinstance(meta.get("category_rank"), dict) else None
    rank_fmt = f"#{rank}" if rank else "—"
    return {
        "key": key,
        "label": label,
        "subtitle": meta.get("genre") or meta.get("primary_genre_name") or "—",
        "version": ver or None,
        "updated_label": updated_label,
        "is_recent": is_recent,
        "score_fmt": score_fmt,
        "ratings_fmt": ratings_fmt,
        "rank_fmt": rank_fmt,
    }


@app.get("/api/home/crashlytics", response_class=HTMLResponse)
def api_home_crashlytics(request: Request, product: str | None = None):
    """Ana sayfa Firebase Crashlytics özeti — yalnızca doviz (Sinemalar BQ yok)."""
    pid = (product or "doviz").strip().lower()
    if pid != "doviz":
        pid = "doviz"
    card = _home_crashlytics_card(pid)
    return templates.TemplateResponse(
        request,
        "partials/home/crashlytics.html",
        context={
            "request": request,
            "card": card,
            "firebase_url": f"/firebase?product={pid}",
        },
    )


@app.get("/api/home/app-release", response_class=HTMLResponse)
def api_home_app_release(request: Request):
    product_id = "doviz"
    platforms = []
    raw = None

    # 1. In-memory / disk / DB cache zinciri
    try:
        from backend.services.app_intel import get_raw_product_data
        result = get_raw_product_data(product_id, force_refresh=False, cache_only=True)
        if not result.get("error"):
            raw = result
    except Exception:
        pass

    # 2. Cache yoksa doğrudan AppIntelRawCache tablosundan oku
    if raw is None:
        with SessionLocal() as db:
            raw = _home_app_raw_from_db(db, product_id)

    if raw and not raw.get("error"):
        try:
            from backend.services.app_intel import ensure_android_category_rank_on_raw

            raw = ensure_android_category_rank_on_raw(product_id, raw, allow_live_fetch=True)
        except Exception:
            LOGGER.debug("Home app-release Android sıra zenginleştirmesi atlandı", exc_info=True)
        for key, label, version_key, date_key in [
            ("android", "Android · Play", "play_version", "play_last_updated_at"),
            ("ios", "iOS · App Store", "version", "currentVersionReleaseDate"),
        ]:
            platforms.append(_home_build_app_platform(raw, key, label, version_key, date_key))
    else:
        for key, label in [("android", "Android · Play"), ("ios", "iOS · App Store")]:
            platforms.append({
                "key": key, "label": label, "subtitle": "Veri henüz toplanmadı",
                "version": None, "updated_label": None, "is_recent": False,
                "score_fmt": "—", "ratings_fmt": "—", "rank_fmt": "—",
            })

    return templates.TemplateResponse(
        request, "partials/home/app_release.html",
        context={"request": request, "platforms": platforms},
    )


def _home_shorten_url(u: str, max_len: int = 60) -> str:
    if not u:
        return ""
    try:
        from urllib.parse import urlparse as _up
        p = _up(u)
        s = (p.path or "/") + (("?" + p.query) if p.query else "")
        if len(s) > max_len:
            s = s[: max_len - 1] + "…"
        return s
    except Exception:
        return u[:max_len]


def _home_top_404s_for_site(db, site_id: int, limit: int = 5) -> list[dict]:
    """Dün için en kritik 404 URL'leri. SiteErrorLog tablosu yoksa UrlAuditRecord'a düşer."""
    yday_start = datetime.now() - timedelta(days=1)
    yday_start = yday_start.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start = yday_start + timedelta(days=1)

    def _short_key(u: str) -> str:
        """Dedupe key: url_short kullanılır; aynı kısaltma birden fazla URL'yi temsil edebilir."""
        return _home_shorten_url(u or "").strip().lower()

    try:
        from backend.models import SiteErrorLog
        rows = (
            db.query(SiteErrorLog)
            .filter(
                SiteErrorLog.site_id == site_id,
                SiteErrorLog.status_code == 404,
                SiteErrorLog.last_seen >= yday_start,
                SiteErrorLog.last_seen < today_start,
            )
            .order_by(SiteErrorLog.hit_count.desc())
            .limit(limit * 5)
            .all()
        )
        if rows:
            out: list[dict] = []
            seen: set[str] = set()
            for r in rows:
                k = _short_key(r.url)
                if k in seen:
                    continue
                seen.add(k)
                out.append({
                    "url": r.url,
                    "url_short": _home_shorten_url(r.url),
                    "hit_label": f"{_home_format_int(r.hit_count)} hit",
                })
                if len(out) >= limit:
                    break
            return out
    except Exception:
        pass

    try:
        rows = (
            db.query(UrlAuditRecord.url, UrlAuditRecord.status_code)
            .filter(
                UrlAuditRecord.site_id == site_id,
                UrlAuditRecord.status_code == 404,
                UrlAuditRecord.collected_at >= yday_start,
            )
            .order_by(UrlAuditRecord.collected_at.desc())
            .limit(limit * 5)
            .all()
        )
        out: list[dict] = []
        seen: set[str] = set()
        for r in rows:
            k = _short_key(r[0])
            if k in seen:
                continue
            seen.add(k)
            out.append({
                "url": r[0],
                "url_short": _home_shorten_url(r[0]),
                "hit_label": "404",
            })
            if len(out) >= limit:
                break
        return out
    except Exception:
        return []


@app.get("/api/home/top-404s", response_class=HTMLResponse)
def api_home_top_404s(request: Request, site: str | None = None):
    sites_out = []
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        for site_id in (1, 2):
            if _site_filter is not None and site_id not in _site_filter:
                continue
            site_obj = _home_get_site(db, site_id)
            if site_obj is None:
                continue
            sites_out.append({
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "rows": _home_top_404s_for_site(db, site_id, limit=7),
            })
    return templates.TemplateResponse(
        request, "partials/home/top_404s.html",
        context={"request": request, "sites": sites_out},
    )


def _home_seo_problem_summary(rec: UrlAuditRecord) -> str:
    """Hangi alanlar eksik — kısa Türkçe."""
    parts = []
    if not rec.has_title:
        parts.append("title yok")
    elif rec.title_length and (rec.title_length < 30 or rec.title_length > 65):
        parts.append("title uzunluk")
    if not rec.has_meta_description:
        parts.append("meta yok")
    elif rec.meta_description_length and rec.meta_description_length < 80:
        parts.append("kısa meta")
    if not rec.has_h1:
        parts.append("h1 yok")
    elif rec.h1_count and rec.h1_count > 1:
        parts.append("çoklu h1")
    if not rec.has_canonical:
        parts.append("canonical yok")
    elif not rec.canonical_matches_final:
        parts.append("canonical uyumsuz")
    if rec.is_noindex:
        parts.append("noindex")
    if not rec.has_schema:
        parts.append("schema yok")
    return ", ".join(parts[:4])


def _home_seo_errors_for_site(db, site_id: int, limit: int = 5) -> list[dict]:
    # Daha fazla kayıt çekip uygulama katmanında akıllı deduplikasyon yapıyoruz
    rows = (
        db.query(UrlAuditRecord)
        .filter(
            UrlAuditRecord.site_id == site_id,
            UrlAuditRecord.status_code < 400,
        )
        .filter((UrlAuditRecord.seo_score == "poor") | (UrlAuditRecord.issue_count >= 3))
        .order_by(UrlAuditRecord.issue_count.desc(), UrlAuditRecord.collected_at.desc())
        .limit(limit * 5) 
        .all()
    )

    def _is_web(u: str) -> bool:
        u_low = u.lower()
        return "www." in u_low or "://m." not in u_low

    # Path bazlı grupla ve en "kaliteli" (web + yüksek hata) olanı seç
    grouped: dict[str, UrlAuditRecord] = {}
    for r in rows:
        path = _home_shorten_url(r.url)
        existing = grouped.get(path)
        if not existing:
            grouped[path] = r
            continue
        
        # Eğer mevcut olan m. ise ve yeni gelen web ise, web'i tercih et (hata sayısı yakınsa)
        # Veya yeni gelenin hata sayısı çok daha fazlaysa onu al (isteğe göre ayarlanabilir)
        curr_is_web = _is_web(existing.url)
        new_is_web = _is_web(r.url)
        
        if not curr_is_web and new_is_web:
            grouped[path] = r
        elif curr_is_web == new_is_web:
            if (r.issue_count or 0) > (existing.issue_count or 0):
                grouped[path] = r

    # Sonuçları tekrar hata sayısına göre diz ve limit uygula
    final_rows = sorted(grouped.values(), key=lambda x: (x.issue_count or 0), reverse=True)
    
    out: list[dict] = []
    for r in final_rows[:limit]:
        out.append({
            "url": r.url,
            "url_short": _home_shorten_url(r.url),
            "issue_count": r.issue_count or 0,
            "problems": _home_seo_problem_summary(r),
        })
    return out


@app.get("/api/home/data-explorer", response_class=HTMLResponse)
def api_home_data_explorer(request: Request, site: str | None = None):
    """Ana sayfa Data Explorer özet kartı — site bazlı CWV (CrUX) snapshot + verdict."""
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        payload = _build_dashboard_data_explorer_summary(db, only_site_ids=_site_filter)
    return templates.TemplateResponse(
        request, "partials/home/data_explorer.html",
        context={"request": request, **payload},
    )


@app.get("/api/home/seo-errors", response_class=HTMLResponse)
def api_home_seo_errors(request: Request, site: str | None = None):
    sites_out = []
    _site_filter = _home_site_filter_ids(site)
    with SessionLocal() as db:
        for site_id in (1, 2):
            if _site_filter is not None and site_id not in _site_filter:
                continue
            site_obj = _home_get_site(db, site_id)
            if site_obj is None:
                continue
            sites_out.append({
                "site_id": site_id,
                "domain": site_obj.domain,
                "display_name": site_obj.display_name,
                "rows": _home_seo_errors_for_site(db, site_id, limit=5),
            })
    return templates.TemplateResponse(
        request, "partials/home/seo_errors.html",
        context={"request": request, "sites": sites_out},
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

    canonical_domain = str(payload.get("domain") or "").strip()
    if canonical_domain and canonical_domain.lower() != str(domain or "").strip().lower():
        redirect_url = f"/data-explorer/{canonical_domain}"
        if request.url.query:
            redirect_url = f"{redirect_url}?{request.url.query}"
        return RedirectResponse(url=redirect_url, status_code=307)

    de_headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
    }
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(
            request,
            "partials/data_explorer_content.html",
            context={"request": request, **payload},
            headers=de_headers,
        )
    return templates.TemplateResponse(
        request,
        "data_explorer.html",
        context={"request": request, **payload},
        headers=de_headers,
    )


@app.get("/external")
@app.get("/public-sites")
def public_sites_page(request: Request):
    with SessionLocal() as db:
        ext_sites = (
            db.query(Site)
            .join(ExternalSite, ExternalSite.site_id == Site.id)
            .filter(Site.is_active.is_(True))
            .order_by(Site.created_at.desc())
            .all()
        )
        ext_sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        lazy_site_ids = [(s.id, s.display_name, s.domain) for s in ext_sites]
        payload = {
            "site_name": "External",
            "sites": get_sidebar_sites(),
            "lazy_mode": True,
            "lazy_site_ids": lazy_site_ids,
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
    """External site listesi: varsayılan lazy, refresh sonrası eager."""
    mode = str(request.query_params.get("mode") or "lazy").strip().lower()
    with SessionLocal() as db:
        if mode == "eager":
            return templates.TemplateResponse(
                request,
                "partials/public_site_cards.html",
                context={"request": request, "lazy_mode": False, "public_sites": _public_sites_payload(db)},
            )
        ext_sites = (
            db.query(Site)
            .join(ExternalSite, ExternalSite.site_id == Site.id)
            .filter(Site.is_active.is_(True))
            .order_by(Site.created_at.desc())
            .all()
        )
        ext_sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        lazy_site_ids = [(s.id, s.display_name, s.domain) for s in ext_sites]
    return templates.TemplateResponse(
        request,
        "partials/public_site_cards.html",
        context={"request": request, "lazy_mode": True, "lazy_site_ids": lazy_site_ids},
    )


@app.get("/external/site/{site_id}", response_class=HTMLResponse)
def external_single_site_card(request: Request, site_id: int):
    """HTMX lazy loading ile tek external site kartını tam veriyle render eder."""
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("", status_code=404)
        if not _is_external_site(db, site.id):
            return HTMLResponse("", status_code=404)
        try:
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
            site_data = {
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
        except Exception as exc:
            logging.exception("external_single_site_card site_id=%s hata", site_id)
            import html as _html
            err_msg = _html.escape(f"{type(exc).__name__}: {exc}")
            return HTMLResponse(
                f'<article id="ext-card-{site_id}" class="rounded-[1.7rem] border border-red-300 dark:border-red-700 '
                f'bg-red-50 dark:bg-red-900/30 p-5 text-sm text-red-700 dark:text-red-300">'
                f'<p class="font-semibold">External kart yüklenemedi</p>'
                f'<p class="mt-1 text-xs">Site #{site_id} verisi hazırlanırken hata oluştu.</p>'
                f'<p class="mt-2 text-xs opacity-70 font-mono break-all">{err_msg}</p></article>',
                status_code=200,
            )
        return templates.TemplateResponse(
            request,
            "partials/public_single_site_card.html",
            context={"request": request, "site": site_data},
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
            invalidate_sidebar_cache()
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
                invalidate_sidebar_cache()
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
                        "lazy_mode": False,
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
def public_sites_refresh_site(request: Request, site_id: int, background_tasks: BackgroundTasks):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            if _request_wants_json(request):
                return JSONResponse({"ok": False, "error": "Site bulunamadı."}, status_code=404)
            return HTMLResponse("Site bulunamadı.", status_code=404)
        if not _is_external_site(db, site.id):
            if _request_wants_json(request):
                return JSONResponse({"ok": False, "error": "Site external profilinde değil."}, status_code=404)
            return HTMLResponse("Site external profilinde değil.", status_code=404)

        job_id, created_new = _create_external_onboarding_job(db, site_id=site.id, domain=site.domain)
        if created_new:
            _set_external_onboarding_job(
                job_id,
                percent=5,
                title="Derin yenileme başlatıldı",
                detail=f"{site.domain} için ölçümler arka planda güncelleniyor.",
            )
            background_tasks.add_task(_run_external_deep_refresh_background, site.id, job_id)

        summary = (
            f"{site.domain} için derin yenileme kuyruğa alındı."
            if created_new
            else "Bu site için yenileme zaten devam ediyor; mevcut iş izleniyor."
        )

        if _request_wants_json(request):
            return JSONResponse(
                {
                    "ok": True,
                    "job_id": job_id,
                    "created_new": created_new,
                    "summary": summary,
                }
            )

        card_context = _external_lazy_site_card_context(db)
        card_context["refresh_job_id"] = job_id
        return templates.TemplateResponse(
            request,
            "partials/public_site_cards.html",
            context={"request": request, **card_context},
        )


@app.post("/external/refresh-all")
@app.post("/public-sites/refresh-all")
def public_sites_refresh_all(request: Request, background_tasks: BackgroundTasks):
    with SessionLocal() as db:
        sites = (
            db.query(Site)
            .join(ExternalSite, ExternalSite.site_id == Site.id)
            .filter(Site.is_active.is_(True))
            .order_by(Site.created_at.asc(), Site.id.asc())
            .all()
        )
        job_ids: list[str] = []
        for site in sites:
            job_id, created_new = _create_external_onboarding_job(db, site_id=site.id, domain=site.domain)
            if created_new:
                _set_external_onboarding_job(
                    job_id,
                    percent=5,
                    title="Derin yenileme başlatıldı",
                    detail=f"{site.domain} için ölçümler arka planda güncelleniyor.",
                )
                background_tasks.add_task(_run_external_deep_refresh_background, site.id, job_id)
                job_ids.append(job_id)

        queued = len(job_ids)
        summary = (
            f"{queued} external site için derin yenileme kuyruğa alındı."
            if queued
            else "Tüm siteler için yenileme zaten devam ediyor veya aktif site yok."
        )

        if _request_wants_json(request):
            return JSONResponse(
                {
                    "ok": True,
                    "job_ids": job_ids,
                    "queued": queued,
                    "summary": summary,
                }
            )

        card_context = _external_lazy_site_card_context(db)
        if job_ids:
            card_context["refresh_job_id"] = job_ids[0]
            card_context["refresh_job_summary"] = summary
        return templates.TemplateResponse(
            request,
            "partials/public_site_cards.html",
            context={"request": request, **card_context},
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
        site = _resolve_site_by_domain(db, domain)
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        results = _refresh_site_detail_measurements(
            db,
            site,
            include_pagespeed=True,
            include_crawler=False,
            include_search_console=False,
            force=True,
            send_notifications=True,
            bypass_pagespeed_quota=settings.pagespeed_manual_refresh_bypass_quota,
            trigger_source="manual",
        )
        ps = results.get("pagespeed")
        if isinstance(ps, dict) and ps.get("blocked"):
            return JSONResponse(
                {
                    "site": site.domain,
                    "refreshed": False,
                    "error": ps.get("reason") or "PageSpeed kota sınırı; yenileme yapılmadı.",
                    "results": results,
                },
                status_code=429,
            )
        try:
            results["crux_history"] = collect_crux_history(db, site, trigger_source="manual")
        except Exception as exc:  # noqa: BLE001
            results["crux_history"] = {"state": "failed", "error": str(exc)}
        try:
            _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
        except OperationalError as exc:
            db.rollback()
            if _is_sqlite_lock_error(exc):
                return JSONResponse({"error": "Veritabanı meşgul, lütfen tekrar deneyin."}, status_code=503)
            raise
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
            send_notifications=True,
            bypass_pagespeed_quota=settings.pagespeed_manual_refresh_bypass_quota,
            trigger_source="manual",
        )
        try:
            results["crux_history"] = collect_crux_history(db, site, trigger_source="manual")
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
        try:
            _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
        except OperationalError as exc:
            db.rollback()
            if _is_sqlite_lock_error(exc):
                return JSONResponse({"error": "Veritabanı meşgul, lütfen tekrar deneyin."}, status_code=503)
            raise
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


def _build_threshold_alerts_payload(db, *, days: int = 7) -> dict:
    """Threshold sekmesi için: GA4 Realtime alarmları + 404 hata logları (mail edilen eşik bazlı uyarılar)."""
    from backend.models import RealtimeAlarmLog, SiteErrorLog, Site
    from backend.services.ga4_realtime import _alarm_row_public_url
    from datetime import datetime as _dt, timedelta as _td

    cutoff = _dt.utcnow() - _td(days=max(1, int(days)))
    site_map = {s.id: s for s in db.query(Site).all()}

    # GA4 Realtime alarmları
    rt_rows = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.triggered_at >= cutoff)
        .order_by(RealtimeAlarmLog.triggered_at.desc())
        .limit(200)
        .all()
    )
    realtime_alerts: list[dict] = []
    for r in rt_rows:
        site = site_map.get(r.site_id)
        if site is None:
            continue
        try:
            from backend.services.timezone_utils import format_local_datetime as _fld
            triggered_label = _fld(r.triggered_at)
        except Exception:
            triggered_label = r.triggered_at.strftime("%d.%m %H:%M") if r.triggered_at else ""
        cur_v = float(r.current_value or 0.0)
        prev_v = float(r.previous_value or 0.0)
        # 0 → XX geçişleri anlamsız görünür, gizle
        if prev_v == 0:
            continue
        pct = float(r.change_pct or 0.0)
        if cur_v == int(cur_v):
            cur_fmt = f"{int(cur_v)}"
        else:
            cur_fmt = f"{cur_v:.1f}"
        if prev_v == int(prev_v):
            prev_fmt = f"{int(prev_v)}"
        else:
            prev_fmt = f"{prev_v:.1f}"
        # Tıklanabilir URL — metric "news:/..." veya "page:/..." formatındaysa
        metric_raw = r.metric or ""
        public_url = ""
        try:
            public_url = _alarm_row_public_url(site.domain, metric_raw)
        except Exception:
            public_url = ""
        # Mesajdan başlığı çıkar (ör: "Delikanlı Oyuncuları — zirveden düştü: 56 → 12 (−79%)")
        # Önce em dash, sonra normal dash ile böl
        message_raw = r.message or ""
        title_text = ""
        if message_raw:
            for sep in (" — ", " - ", " · "):
                if sep in message_raw:
                    title_text = message_raw.split(sep, 1)[0].strip()
                    break
            if not title_text:
                title_text = message_raw.strip()
        # Title 80 karakterden uzunsa kes
        if len(title_text) > 80:
            title_text = title_text[:80] + "…"
        # Profil bilgisini mesajdan çıkar: "doviz.com Desktop — ..." → "web"
        _pmap = {"Desktop": "web", "Mobile Web": "mweb", "Android": "android", "iOS": "ios"}
        profile_key = ""
        if " — " in message_raw:
            _prefix = message_raw.split(" — ", 1)[0].strip()
            for _lbl, _key in _pmap.items():
                if _prefix.endswith(" " + _lbl):
                    profile_key = _key
                    break
        realtime_alerts.append({
            "id": r.id,
            "domain": site.domain,
            "display_name": site.display_name,
            "profile": profile_key,
            "rule_id": r.rule_id or "",
            "metric": r.metric or "",
            "severity": (r.severity or "warning").lower(),
            "current_value": cur_v,
            "previous_value": prev_v,
            "current_fmt": cur_fmt,
            "previous_fmt": prev_fmt,
            "change_pct": pct,
            "change_fmt": f"{pct:+.1f}%" if pct else "—",
            "message": r.message or "",
            "title_text": title_text or (r.metric or r.rule_id or "GA4 alarm"),
            "public_url": public_url,
            "triggered_at": triggered_label,
            "triggered_at_iso": r.triggered_at.isoformat() if r.triggered_at else "",
        })

    # 404 hata logları (mail edilen 404 raporlarının kaynak verisi)
    err_rows = (
        db.query(SiteErrorLog)
        .filter(
            SiteErrorLog.status_code == 404,
            SiteErrorLog.last_seen >= cutoff,
        )
        .order_by(SiteErrorLog.hit_count.desc(), SiteErrorLog.last_seen.desc())
        .limit(200)
        .all()
    )
    error_alerts: list[dict] = []
    seen_keys: set[tuple[int, str]] = set()
    for e in err_rows:
        site = site_map.get(e.site_id)
        if site is None:
            continue
        key = (e.site_id, (e.url or "").lower())
        if key in seen_keys:
            continue
        seen_keys.add(key)
        try:
            from backend.services.timezone_utils import format_local_datetime as _fld
            last_seen_label = _fld(e.last_seen)
        except Exception:
            last_seen_label = e.last_seen.strftime("%d.%m %H:%M") if e.last_seen else ""
        url_short = e.url or ""
        if len(url_short) > 70:
            url_short = url_short[:70] + "…"
        # 404 URL'leri için tam URL inşa et
        raw_path = (e.url or "").strip()
        if raw_path.startswith(("http://", "https://")):
            full_url = raw_path
        elif raw_path.startswith("/"):
            full_url = f"https://{site.domain}{raw_path}"
        else:
            full_url = ""
        error_alerts.append({
            "id": e.id,
            "domain": site.domain,
            "display_name": site.display_name,
            "url": e.url or "",
            "url_short": url_short,
            "public_url": full_url,
            "status_code": int(e.status_code or 404),
            "hit_count": int(e.hit_count or 0),
            "source": e.source or "",
            "error_type": e.error_type or "not_found",
            "last_seen": last_seen_label,
            "last_seen_iso": e.last_seen.isoformat() if e.last_seen else "",
        })

    return {
        "threshold_realtime_alerts": realtime_alerts,
        "threshold_error_alerts": error_alerts,
        "threshold_window_days": days,
    }


@app.get("/api/alerts/live-position-drops", response_class=HTMLResponse)
def api_alerts_live_position_drops(request: Request, domain: str | None = None):
    """Canlı SC pozisyon listesi (ana sayfa ile aynı) — site filtresine göre fragment."""
    from backend.services.alert_engine import list_live_position_alert_rows

    dom = (domain or "").strip() or None
    with SessionLocal() as db:
        live_sites = _live_position_drop_sites(db, dom)
        live_position_alert_rows = list_live_position_alert_rows(db, domain=dom)
    return templates.TemplateResponse(
        request,
        "partials/alerts/live_position_refresh_bundle.html",
        context={
            "request": request,
            "live_position_sites": live_sites,
            "live_position_alert_rows": live_position_alert_rows,
        },
    )


@app.get("/alerts")
def alerts_page(request: Request):
    # Search Console: üstte canlı snapshot listesi; altta AlertLog olay kayıtları.
    with SessionLocal() as db:
        external_domains = _external_site_domains(db)
        domain_q = (request.query_params.get("domain") or "").strip() or None
        from backend.services.alert_engine import list_live_position_alert_rows

        alert_rows = get_recent_alerts(db, limit=100, include_external=True, only_latest_sc_scan=False)
        alert_rows = [a for a in alert_rows if a.get("metric_type") != "Pozisyon"]
        live_position_alert_rows = list_live_position_alert_rows(db, domain=None)
        threshold_payload = _build_threshold_alerts_payload(db, days=7)
        payload = {
            "site_name": "Alerts",
            "sites": get_sidebar_sites(),
            "recent_alerts": alert_rows,
            "live_position_alert_rows": live_position_alert_rows,
            "live_position_sites": _live_position_drop_sites(db, None),
            "sc_scan_note": _sc_alert_scan_note(db),
            "selected_alert_id": request.query_params.get("selected_alert", "").strip(),
            "has_external_sites": bool(external_domains),
            **threshold_payload,
        }
    template_name = "partials/alerts_content.html" if request.headers.get("HX-Request") == "true" else "alerts.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.get("/alerts/threshold-panel")
def alerts_threshold_panel(request: Request):
    with SessionLocal() as db:
        threshold_payload = _build_threshold_alerts_payload(db, days=7)
    return templates.TemplateResponse(
        request,
        "partials/alerts_threshold_body.html",
        context={"request": request, **threshold_payload},
    )


_alerts_refresh_status: dict = {"running": False, "done": False}


def _run_alerts_refresh_bg():
    """Alert yenilemeyi arka planda çalıştırır (timeout'u önlemek için)."""
    _alerts_refresh_status["running"] = True
    _alerts_refresh_status["done"] = False
    try:
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
                            send_notifications=True,
                        )
                    }
                    _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
                    alert_batch.append((site, results["search_console"]))
                except Exception as exc:  # noqa: BLE001
                    db.rollback()
                    alert_batch.append((site, {"state": "failed", "error": str(exc)}))

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
    finally:
        _alerts_refresh_status["running"] = False
        _alerts_refresh_status["done"] = True


@app.post("/alerts/refresh")
def alerts_refresh(request: Request):
    if not _alerts_refresh_status.get("running"):
        t = threading.Thread(target=_run_alerts_refresh_bg, daemon=True)
        t.start()
    return JSONResponse({"refreshed": True, "background": True})


@app.get("/alerts/refresh/status")
def alerts_refresh_status(request: Request):
    return JSONResponse(_alerts_refresh_status)


@app.get("/settings")
def settings_page(request: Request):
    # Settings ekranı site yönetimi arayüzünü gösterir.
    from backend.services import admin_access_log as aal
    from backend.services import app_member_auth as ama

    with SessionLocal() as db:
        admin_password_configured = _admin_password_configured(db)
        membership_admin = _is_membership_admin(request)
        payload = {
            "site_name": "Settings",
            "sites": get_sidebar_sites(),
            "alert_rules": get_alert_rules(db),
            "quota_status": get_quota_status(db),
            "oauth_ready": oauth_is_configured(),
            "oauth_redirect_uri": settings.google_oauth_redirect_uri,
            "admin_password_configured": admin_password_configured,
            "active_sessions": _get_active_sessions(request),
            "login_history": aal.recent_login_history(db) if admin_password_configured else [],
            "membership_admin": membership_admin,
            "app_members": ama.member_list_payload(db) if membership_admin else [],
            "current_app_member": _app_member_from_request(request),
            "google_member_oauth_redirect": ama.get_member_oauth_redirect_uri(request=request),
        }
    flash_key = (request.query_params.get("admin_pw") or "").strip()
    _admin_pw_flash_messages = {
        "short": "Şifre en az 6 karakter olmalı.",
        "mismatch": "Şifreler eşleşmiyor.",
        "save_error": "Şifre kaydedilemedi (veritabanı veya sunucu hatası). Railway deploy loglarına bakın; bir süre sonra tekrar deneyin.",
        "forbidden": "Bu işlem için yetki yok.",
        "saved": "Şifre güncellendi.",
        "first_setup": "Henüz veritabanında admin şifresi yok. Aşağıda en az 6 karakter olacak şekilde belirleyip kaydedin; sonra /admin/login ile giriş yapın.",
    }
    if flash_key in _admin_pw_flash_messages:
        payload["admin_password_flash"] = _admin_pw_flash_messages[flash_key]
        payload["admin_password_flash_ok"] = flash_key == "saved"
        payload["admin_password_flash_info"] = flash_key == "first_setup"
    if (request.query_params.get("device_trusted") or "").strip() == "1":
        payload["admin_password_flash"] = "Cihaz tanıdık olarak kaydedildi."
        payload["admin_password_flash_ok"] = True
    return templates.TemplateResponse(request, "settings.html", context={"request": request, **payload})


def _admin_password_form_wants_json(request: Request) -> bool:
    return request.headers.get("hx-request") == "true" or "application/json" in (
        request.headers.get("accept", "").lower()
    )


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


@app.get("/admin/login")
def admin_login_page(request: Request):
    if not _auth_gate_enabled(request):
        return RedirectResponse(url="/", status_code=303)
    if _is_admin_authenticated(request) or _app_member_authenticated(request):
        return RedirectResponse(url="/", status_code=303)
    from backend.services import app_member_auth as ama

    with SessionLocal() as db:
        configured = _admin_password_configured(db)
    # Şifre yokken giriş formu yine 503; yerelde önce /settings’te belirle.
    if not configured and _is_local_dev_first_password_client(request):
        return RedirectResponse(url="/settings?admin_pw=first_setup", status_code=303)
    from urllib.parse import unquote

    oauth_err = unquote((request.query_params.get("oauth_error") or "").strip())
    if oauth_err.lower() in ("redirect_uri_mismatch", "access_denied"):
        oauth_display = ama.format_member_oauth_login_error(oauth_err, request=request)
    else:
        oauth_display = oauth_err[:400]
    return templates.TemplateResponse(
        request,
        "admin_login.html",
        context={
            "request": request,
            "site_name": "Giriş — SEO Agent",
            "password_configured": configured,
            "client_ip": _extract_client_ip(request),
            "local_first_setup": (not configured) and _is_local_dev_first_password_client(request),
            "google_member_oauth_ready": ama.member_oauth_configured(),
            "oauth_error": oauth_display,
        },
        headers={"Cache-Control": "no-store"},
    )


@app.get("/admin/auth/login")
def admin_auth_login_get():
    """Eski/yanlış URL; giriş formu `/admin/login` üzerinde."""
    return RedirectResponse(url="/admin/login", status_code=303)


def _admin_password_login_submit(request: Request, password: str):
    from backend.services import admin_access_log as aal

    if not _admin_auth_active():
        return RedirectResponse(url="/", status_code=303)
    raw_password = str(password or "").strip()
    client_ip = _extract_client_ip(request)
    client_ua = request.headers.get("user-agent", "")
    with SessionLocal() as db:
        if not _admin_password_configured(db):
            return JSONResponse(status_code=503, content={"ok": False, "detail": "Admin şifresi henüz ayarlanmadı."})
        if not raw_password or not _verify_admin_password(db, raw_password):
            aal.record_access_event(
                db,
                event_type="login_fail",
                ip=client_ip,
                user_agent=client_ua,
                referer=(request.headers.get("referer") or "")[:512],
                accept_language=(request.headers.get("accept-language") or "")[:120],
            )
            return JSONResponse(status_code=401, content={"ok": False, "detail": "Şifre hatalı."})
        row = _admin_auth_row(db)
        token = _build_admin_cookie_token(row.password_hash if row else "")
        aal.record_access_event(
            db,
            event_type="login_ok",
            ip=client_ip,
            user_agent=client_ua,
            referer=(request.headers.get("referer") or "")[:512],
            accept_language=(request.headers.get("accept-language") or "")[:120],
        )
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(
        key=_ADMIN_AUTH_COOKIE,
        value=token,
        httponly=True,
        secure=_admin_auth_cookie_secure(request),
        samesite="lax",
        max_age=60 * 60 * 12,
        path="/",
    )
    return response


@app.post("/admin/login")
def admin_login_submit(request: Request, password: str = Form(default="")):
    """Form doğrudan `/admin/login` adresine POST edebilsin (tek sayfa, 405 yok)."""
    return _admin_password_login_submit(request, password)


@app.get("/admin/settings-login")
def settings_login_page(request: Request):
    if _member_denied_settings_menu(request):
        return RedirectResponse(url="/admin/settings-denied", status_code=303)
    member = _app_member_from_request(request)
    if member is not None and _is_settings_authenticated(request):
        return RedirectResponse(url="/settings", status_code=303)
    if not _is_app_panel_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    if _is_settings_authenticated(request):
        return RedirectResponse(url="/settings", status_code=303)
    
    html_content = """
    <!DOCTYPE html>
    <html lang="tr" class="dark">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Settings Login</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body { background: radial-gradient(circle at top, #09090b, #111113 55%, #18181b); color: #d4d4d8; }
        </style>
    </head>
    <body class="min-h-screen flex items-center justify-center p-4">
        <div class="w-full max-w-md p-8 rounded-3xl border border-zinc-800 bg-zinc-900/50 shadow-2xl backdrop-blur-xl">
            <div class="text-center mb-8">
                <div class="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-indigo-500/10 text-indigo-400 mb-4 border border-indigo-500/20">
                    <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/></svg>
                </div>
                <h1 class="text-2xl font-bold text-white">Güvenli Ayar Erişimi</h1>
                <p class="text-zinc-500 text-sm mt-2">Hassas ayarlar için ikinci şifrenizi girin.</p>
            </div>
            <form action="/admin/settings-login" method="POST" class="space-y-4">
                <div>
                    <input type="password" name="password" required autofocus
                        placeholder="Settings şifresi"
                        class="w-full px-4 py-3 rounded-xl bg-zinc-950 border border-zinc-800 text-white focus:outline-none focus:ring-2 focus:ring-indigo-500/50 transition">
                </div>
                <button type="submit" class="w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-xl shadow-lg shadow-indigo-500/20 transition-all active:scale-95">
                    Giriş Yap
                </button>
            </form>
            <div class="mt-6 text-center">
                <a href="/" class="text-xs text-zinc-600 hover:text-zinc-400">Ana Sayfaya Dön</a>
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/admin/settings-denied")
def settings_denied_page(request: Request):
    from backend.services.settings_menu_access import render_settings_denied_html

    if not _is_app_panel_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    member = _app_member_from_request(request)
    if member is None or not _member_denied_settings_menu(request):
        if _is_settings_authenticated(request):
            return RedirectResponse(url="/settings", status_code=303)
        return RedirectResponse(url="/admin/settings-login", status_code=303)
    requested = (request.query_params.get("requested") or "").strip() == "1"
    err = (request.query_params.get("error") or "").strip()
    err_msg = ""
    if err == "mail":
        err_msg = "E-posta gönderilemedi. SMTP yapılandırmasını kontrol edin veya doğrudan yazın."
    elif err == "rate":
        err_msg = "Kısa süre önce istek gönderildi. Lütfen bir saat sonra tekrar deneyin."
    html = render_settings_denied_html(
        member_email=member.email,
        member_name=member.display_name or "",
        requested=requested,
        request_error=err_msg,
    )
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@app.post("/admin/settings-access-request")
def settings_access_request_submit(request: Request):
    from backend.services import admin_access_log as aal
    from backend.services.settings_menu_access import (
        SETTINGS_ACCESS_REQUEST_COOLDOWN_SEC,
        _access_request_cooldown_ok,
        send_settings_access_request_email,
    )

    if not _is_app_panel_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    member = _app_member_from_request(request)
    if member is None or not _member_denied_settings_menu(request):
        if _is_settings_authenticated(request):
            return RedirectResponse(url="/settings", status_code=303)
        return RedirectResponse(url="/admin/settings-login", status_code=303)
    if not _access_request_cooldown_ok(request):
        return RedirectResponse(url="/admin/settings-denied?error=rate", status_code=303)

    client_ip = _extract_client_ip(request)
    client_ua = request.headers.get("user-agent", "")
    ok = send_settings_access_request_email(
        requester_email=member.email,
        requester_name=member.display_name or "",
        client_ip=client_ip,
        user_agent=client_ua,
    )
    with SessionLocal() as db:
        aal.record_access_event(
            db,
            event_type="settings_access_request" if ok else "settings_access_request_fail",
            ip=client_ip,
            user_agent=client_ua,
            actor_email=member.email,
        )
    if not ok:
        return RedirectResponse(url="/admin/settings-denied?error=mail", status_code=303)
    import time as _time

    resp = RedirectResponse(url="/admin/settings-denied?requested=1", status_code=303)
    resp.set_cookie(
        key="seo_settings_req_at",
        value=str(int(_time.time())),
        httponly=True,
        secure=_admin_auth_cookie_secure(request),
        samesite="lax",
        max_age=SETTINGS_ACCESS_REQUEST_COOLDOWN_SEC,
        path="/",
    )
    return resp


@app.post("/admin/settings-login")
def settings_login_submit(request: Request, password: str = Form(default="")):
    from backend.services import admin_access_log as aal

    if _member_denied_settings_menu(request):
        return RedirectResponse(url="/admin/settings-denied", status_code=303)
    if _is_settings_authenticated(request):
        return RedirectResponse(url="/settings", status_code=303)
    if not _is_admin_authenticated(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    raw_pwd = (getattr(settings, "settings_password", "") or "").strip()
    input_pwd = str(password or "").strip()
    client_ip = _extract_client_ip(request)
    client_ua = request.headers.get("user-agent", "")

    if not raw_pwd or hmac.compare_digest(input_pwd, raw_pwd):
        with SessionLocal() as db:
            aal.record_access_event(
                db,
                event_type="settings_ok",
                ip=client_ip,
                user_agent=client_ua,
                referer=(request.headers.get("referer") or "")[:512],
                accept_language=(request.headers.get("accept-language") or "")[:120],
            )
        # Başarılı: çerezi set et
        secret = str(getattr(settings, "secret_key", "") or "").encode("utf-8")
        token = hmac.new(secret, raw_pwd.encode("utf-8"), digestmod=hashlib.sha256).hexdigest()

        response = RedirectResponse(url="/settings", status_code=303)
        response.set_cookie(
            key=_SETTINGS_AUTH_COOKIE,
            value=token,
            httponly=True,
            secure=_admin_auth_cookie_secure(request),
            samesite="lax",
            max_age=60 * 60 * 2, # 2 saat yeterli
            path="/",
        )
        return response

    with SessionLocal() as db:
        aal.record_access_event(
            db,
            event_type="settings_fail",
            ip=client_ip,
            user_agent=client_ua,
        )
    return RedirectResponse(url="/admin/settings-login?error=1", status_code=303)


@app.post("/admin/trust-device")
def admin_trust_device(request: Request, fingerprint: str = Form(default="")):
    """Settings'ten «Bu cihaz benim» — parmak izini tanıdık olarak kaydet."""
    from backend.services import admin_access_log as aal

    if not _is_settings_authenticated(request):
        return RedirectResponse(url="/admin/settings-login", status_code=303)
    fp = (fingerprint or "").strip()
    ip = _extract_client_ip(request)
    ua = request.headers.get("user-agent", "")
    if not fp:
        fp = aal.device_fingerprint(ip, ua)
    with SessionLocal() as db:
        aal.trust_fingerprint(
            db,
            fp,
            label=aal.parse_device_label(ua),
            ip_hint=ip,
        )
    return RedirectResponse(url="/settings?device_trusted=1", status_code=303)


@app.post("/api/inbox/action-auth")
def inbox_action_auth(request: Request, password: str = Form(default="")):
    """Inbox aksiyon şifresi doğrulama — başarılıysa cookie set eder."""
    raw_pwd = (getattr(settings, "inbox_action_password", "") or "").strip()
    if not raw_pwd:
        return JSONResponse({"ok": True})
    input_pwd = str(password or "").strip()
    if hmac.compare_digest(input_pwd, raw_pwd):
        token = _inbox_action_token(raw_pwd)
        resp = JSONResponse({"ok": True})
        resp.set_cookie(
            key=_INBOX_ACTION_AUTH_COOKIE,
            value=token,
            httponly=True,
            secure=_admin_auth_cookie_secure(request),
            samesite="lax",
            max_age=60 * 60 * 8,  # 8 saat
            path="/",
        )
        return resp
    return JSONResponse({"ok": False, "error": "Yanlış şifre"}, status_code=401)


@app.get("/api/inbox/action-auth/status")
def inbox_action_auth_status(request: Request):
    """Inbox aksiyon yetkisi var mı?"""
    return JSONResponse({"authenticated": _is_inbox_action_authenticated(request)})


@app.post("/admin/auth/login")
def admin_auth_login(request: Request, password: str = Form(default="")):
    return _admin_password_login_submit(request, password)


@app.post("/admin/auth/logout")
def admin_auth_logout(request: Request):
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie(
        key=_ADMIN_AUTH_COOKIE,
        path="/",
        secure=_admin_auth_cookie_secure(request),
    )
    return response


@app.post("/admin/password")
def admin_password_set(request: Request, password: str = Form(default=""), password_confirm: str = Form(default="")):
    # Şifre belirleme/güncelleme: allowlist / oturum; ilk kurumda ayrıca yerel Host/loopback (yukarıdaki yardımcı).
    wants_json = _admin_password_form_wants_json(request)
    if not _may_set_or_update_admin_password(request):
        if wants_json:
            return JSONResponse(status_code=403, content={"ok": False, "detail": "Bu işlem için yetki yok."})
        return RedirectResponse(url="/settings?admin_pw=forbidden", status_code=303)
    raw_password = str(password or "")
    confirm = str(password_confirm or "")
    if len(raw_password) < 6:
        if wants_json:
            return JSONResponse(status_code=400, content={"ok": False, "detail": "Şifre en az 6 karakter olmalı."})
        return RedirectResponse(url="/settings?admin_pw=short", status_code=303)
    if raw_password != confirm:
        if wants_json:
            return JSONResponse(status_code=400, content={"ok": False, "detail": "Şifreler eşleşmiyor."})
        return RedirectResponse(url="/settings?admin_pw=mismatch", status_code=303)
    try:
        with SessionLocal() as db:
            _upsert_admin_password(db, raw_password)
            row = _admin_auth_row(db)
            token = _build_admin_cookie_token(row.password_hash if row else "")
    except Exception:  # noqa: BLE001
        LOGGER.exception("Admin şifre kaydı başarısız (POST /admin/password)")
        if wants_json:
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "detail": "Şifre kaydedilemedi. Veritabanı veya sunucu hatası; deploy loglarına bakın.",
                },
            )
        return RedirectResponse(url="/settings?admin_pw=save_error", status_code=303)
    response = RedirectResponse(url="/settings?admin_pw=saved", status_code=303)
    response.set_cookie(
        key=_ADMIN_AUTH_COOKIE,
        value=token,
        httponly=True,
        secure=_admin_auth_cookie_secure(request),
        samesite="lax",
        max_age=60 * 60 * 12,
        path="/",
    )
    return response


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


def _sc_position_delta(current: float, previous: float) -> float:
    """Search Console ort. pozisyon: önceki − güncel (sıra birimi, yüzde değil).
    Pozitif = sıra sayısı düştü (iyileşme), negatif = yükseldi (kötüleşme)."""
    try:
        c = float(current or 0.0)
        p = float(previous or 0.0)
    except (TypeError, ValueError):
        return 0.0
    d = p - c
    # UI `{:+.2f}` ile gösterim; iki ondalıkta 0.00 olan farkları tam sıfır yap (yeşil/kırmızı sınıfları)
    if round(d, 2) == 0.0:
        return 0.0
    return d


def _ga4_sw_float(m: dict | None, key: str) -> float:
    if not isinstance(m, dict):
        return 0.0
    try:
        return float(m.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ga4_users_from_kpi_slice(m: dict | None) -> float | None:
    """GA4 arayüzündeki 'Users' (Traffic acquisition vb.) ile uyum: activeUsers, yoksa totalUsers."""
    if not isinstance(m, dict):
        return None
    for key in ("activeUsers", "totalUsers"):
        if key not in m:
            continue
        raw = m.get(key)
        if raw is None or raw == "":
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return None


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
    """Son N gün kanal oturumları vs önceki N gün.

    Not: Eski snapshot'larda channel_summary_rows yüzde hesabı hatalı kalmış olabilir.
    Bu yüzden öncelik her zaman channels_last/channels_prev -> runtime hesaplamadır.
    """
    pl = snapshot_payload if isinstance(snapshot_payload, dict) else None
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
            "pages_news": [],
            "sources": [],
            "daily_trend": (
                pl.get("daily_trend")
                if isinstance(pl.get("daily_trend"), dict) and (pl.get("daily_trend") or {}).get("dates")
                else {"dates": [], "sessions": [], "activeUsers": [], "engagedSessions": [], "engagementRate": []}
            ),
            "same_weekday_kpi": swk,
            "has_snapshot": bool(snap_ref),
            "has_period_data": False,
        }

    last_total = _ga4_sw_float(la, "sessions")
    prev_total = _ga4_sw_float(pr, "sessions")
    users_last = _ga4_users_from_kpi_slice(la)
    if users_last is None:
        users_last = _ga4_sw_float(la, "activeUsers") or _ga4_sw_float(la, "totalUsers")
    users_prev = _ga4_users_from_kpi_slice(pr)
    if users_prev is None:
        users_prev = _ga4_sw_float(pr, "activeUsers") or _ga4_sw_float(pr, "totalUsers")
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

    sw_channels_last = swk.get("channels_last") if isinstance(swk.get("channels_last"), dict) else None
    sw_channels_prev = swk.get("channels_prev") if isinstance(swk.get("channels_prev"), dict) else None
    if not (isinstance(sw_channels_last, dict) and sw_channels_last and isinstance(sw_channels_prev, dict)):
        try:
            from backend.collectors.ga4 import fetch_ga4_same_weekday_channel_maps

            _sw_live = fetch_ga4_same_weekday_channel_maps(property_id=prop_id)
            _swl = _sw_live.get("channels_last")
            _swp = _sw_live.get("channels_prev")
            if isinstance(_swl, dict) and _swl:
                sw_channels_last = _swl
            if isinstance(_swp, dict):
                sw_channels_prev = _swp
        except Exception:
            pass
    _org_sw_day = _ga4_organic_share_from_channel_maps(sw_channels_last, sw_channels_prev)
    if _org_sw_day is not None:
        organic_share = _org_sw_day[0]
        organic_share_pct_change = _org_sw_day[2]
    else:
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
            "activeUsers": [],
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
        "top_channels": (
            _ga4_top_channels_with_pct_change(
                latest,
                profile,
                1,
                {"channels_last": sw_channels_last or {}, "channels_prev": sw_channels_prev or {}},
            )
            if (isinstance(sw_channels_last, dict) and sw_channels_last)
            else _ga4_top_channels_with_pct_change(latest, profile, 7, pl)
        ),
        "pages_no_news": _enrich_ga4_page_rows(pl.get("pages_no_news")),
        "pages_news": _enrich_ga4_page_rows(pl.get("pages_news"), keep_news_articles=True),
        "sources": pl.get("sources") or [],
        "daily_trend": daily_trend,
        "same_weekday_kpi": swk,
        "has_snapshot": bool(snap_ref),
        "has_period_data": True,
    }


def _ga4_trend_has_signal(daily_trend: dict) -> bool:
    for key in ("sessions", "activeUsers", "engagedSessions"):
        for value in daily_trend.get(key) or []:
            try:
                if float(value or 0) > 0:
                    return True
            except (TypeError, ValueError):
                continue
    return False


_GA4_PROFILE_SC_DEVICE = {"web": "DESKTOP", "mweb": "MOBILE"}


def _sc_position_trend_has_values(trend: dict) -> bool:
    for value in (trend or {}).get("position") or []:
        if value is None:
            continue
        try:
            if float(value) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def _load_ga4_sc_position_trends_by_device(db, site_id: int) -> dict[str, dict[str, dict]]:
    """Site için SC günlük position serileri (DESKTOP/MOBILE, 28g + 12ay)."""
    summary_payload = _latest_successful_provider_summary(
        db, site_id=site_id, provider="search_console", strategy="all"
    )
    if not summary_payload:
        return {}

    _raw_trend_by_device = (
        summary_payload.get("trend_28d_summary_by_device")
        or summary_payload.get("trend_7d_summary_by_device")
        or {}
    )
    _stored_trend_rows = summary_payload.get("trend_28d_rows") or []
    if _stored_trend_rows and not _raw_trend_by_device:
        from backend.collectors.search_console import _build_recent_trend_summary_by_device

        try:
            from datetime import date as _date_cls

            _dates = [r.get("date") for r in _stored_trend_rows if r.get("date")]
            if _dates:
                _start = _date_cls.fromisoformat(min(_dates)[:10])
                _end = _date_cls.fromisoformat(max(_dates)[:10])
                _raw_trend_by_device = _build_recent_trend_summary_by_device(
                    _stored_trend_rows, start_date=_start, end_date=_end
                )
        except Exception:
            _raw_trend_by_device = {}

    _raw_12m_by_device = summary_payload.get("trend_12m_summary_by_device") or {}
    _stored_12m_trend_rows = summary_payload.get("trend_12m_rows") or []
    if _stored_12m_trend_rows and not _raw_12m_by_device:
        from backend.collectors.search_console import _build_recent_trend_summary_by_device

        try:
            from datetime import date as _date_cls_12m

            _12s = str(summary_payload.get("trend_12m_start_date") or "").strip()
            _12e = str(summary_payload.get("trend_12m_end_date") or "").strip()
            if _12s and _12e:
                _s12e = _date_cls_12m.fromisoformat(_12s[:10])
                _e12e = _date_cls_12m.fromisoformat(_12e[:10])
            else:
                _d12e = [r.get("date") for r in _stored_12m_trend_rows if r.get("date")]
                if not _d12e:
                    raise ValueError("no 12m trend dates")
                _s12e = _date_cls_12m.fromisoformat(min(_d12e)[:10])
                _e12e = _date_cls_12m.fromisoformat(max(_d12e)[:10])
            _raw_12m_by_device = _build_recent_trend_summary_by_device(
                _stored_12m_trend_rows, start_date=_s12e, end_date=_e12e
            )
            for _dev_s in _raw_12m_by_device.values():
                if isinstance(_dev_s, dict):
                    _dev_s["mode"] = "last_12m"
        except Exception:
            _raw_12m_by_device = {}

    out: dict[str, dict[str, dict]] = {}
    for device in ("DESKTOP", "MOBILE"):
        t28 = _sanitize_search_console_trend(_raw_trend_by_device.get(device) or {})
        t12 = _sanitize_search_console_trend(_raw_12m_by_device.get(device) or {})
        if t12:
            t12 = {**t12, "mode": "last_12m"}
        dev_out: dict[str, dict] = {}
        if _sc_position_trend_has_values(t28):
            dev_out["28d"] = t28
        if _sc_position_trend_has_values(t12):
            dev_out["12m"] = t12
        if dev_out:
            out[device] = dev_out
    return out


def _ga4_sc_position_trend_for_period(
    sc_by_device: dict[str, dict[str, dict]],
    *,
    profile: str,
    period_key: str,
    period_days: int,
    target_dates: list[str] | None = None,
) -> dict | None:
    device = _GA4_PROFILE_SC_DEVICE.get(profile)
    if not device:
        return None
    dev_trends = sc_by_device.get(device) or {}
    base = _pick_ga4_sc_position_trend_base(
        dev_trends, period_key=str(period_key), period_days=int(period_days)
    )
    if not base:
        return None
    slice_days = int(period_days) if int(period_days) > 1 else 7
    if target_dates:
        sliced = _align_search_console_trend_to_dates(base, target_dates)
    else:
        sliced = _slice_search_console_trend_last_days(base, slice_days)
    dates = list(sliced.get("dates") or [])
    position = list(sliced.get("position") or [])
    if not dates or not _sc_position_trend_has_values(sliced):
        return None
    return {"dates": dates, "position": position}


def _attach_ga4_sc_position_trends(profiles: dict[str, dict], sc_by_device: dict[str, dict[str, dict]]) -> None:
    if not sc_by_device:
        return
    for profile in ("web", "mweb"):
        pdata = profiles.get(profile)
        if not isinstance(pdata, dict):
            continue
        periods = pdata.get("periods")
        if not isinstance(periods, dict):
            continue
        for period_key, period_payload in periods.items():
            if not isinstance(period_payload, dict):
                continue
            pd = int(period_payload.get("period_days") or 0)
            if period_key == "12m":
                pd = int(settings.ga4_trend_12m_period_days)
            daily = period_payload.get("daily_trend")
            target_dates = (
                list(daily.get("dates") or [])
                if isinstance(daily, dict) and daily.get("dates")
                else None
            )
            trend = _ga4_sc_position_trend_for_period(
                sc_by_device,
                profile=profile,
                period_key=str(period_key),
                period_days=pd,
                target_dates=target_dates,
            )
            if trend:
                period_payload["sc_position_trend"] = trend


def _ga4_profile_payload_for_12m_trend(
    db,
    *,
    site_id: int,
    profile: str,
    prop_id: str,
) -> dict:
    """12 ay: yalnızca günlük trend (karşılaştırma yok)."""
    pd = int(settings.ga4_trend_12m_period_days)
    snap = get_latest_ga4_report_snapshot(db, site_id=site_id, profile=profile, period_days=pd)
    pl = (snap or {}).get("payload") or {}
    daily = pl.get("daily_trend") if isinstance(pl.get("daily_trend"), dict) else {}
    if daily and not daily.get("mode"):
        daily = {**daily, "mode": "last_12m"}
    return {
        "property_id": prop_id,
        "period_days": pd,
        "trend_only": True,
        "ranges": {
            "last_start": (snap or {}).get("last_start") or "",
            "last_end": (snap or {}).get("last_end") or "",
            "prev_start": "",
            "prev_end": "",
        },
        "daily_trend": daily
        or {
            "mode": "last_12m",
            "dates": [],
            "sessions": [],
            "activeUsers": [],
            "engagedSessions": [],
            "engagementRate": [],
        },
        "has_snapshot": bool(snap),
        "has_period_data": bool(snap) and _ga4_trend_has_signal(daily),
        "last_total": 0.0,
        "prev_total": 0.0,
        "top_channels": [],
        "pages_no_news": [],
        "sources": [],
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
    if pd == int(settings.ga4_trend_12m_period_days):
        return _ga4_profile_payload_for_12m_trend(
            db,
            site_id=site_id,
            profile=profile,
            prop_id=prop_id,
        )
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

    # Sessions toplamı GA4 KPI'dan gelir; kanal kırılımı sadece breakdown görseli içindir.
    ch_last_sum = sum_channel_prefix(f"ga4_{profile}_sessions_last{pd}d_channel__")
    ch_prev_sum = sum_channel_prefix(f"ga4_{profile}_sessions_prev{pd}d_channel__")

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

    _ul = _ga4_users_from_kpi_slice(last_s)
    if _ul is not None:
        users_last = _ul
    else:
        users_last = float(
            pick(f"ga4_{profile}_kpi_last_activeUsers{sk}")
            or pick(f"ga4_{profile}_kpi_last_totalUsers{sk}")
            or (pick(f"ga4_{profile}_kpi_last_activeUsers") if pd == 30 else 0.0)
            or (pick(f"ga4_{profile}_kpi_last_totalUsers") if pd == 30 else 0.0)
            or 0.0
        )
    _up = _ga4_users_from_kpi_slice(prev_s)
    if _up is not None:
        users_prev = _up
    else:
        users_prev = float(
            pick(f"ga4_{profile}_kpi_prev_activeUsers{sk}")
            or pick(f"ga4_{profile}_kpi_prev_totalUsers{sk}")
            or (pick(f"ga4_{profile}_kpi_prev_activeUsers") if pd == 30 else 0.0)
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
        "pages_news": _enrich_ga4_page_rows(pl.get("pages_news"), keep_news_articles=True),
        "sources": pl.get("sources") or [],
        "daily_trend": pl.get("daily_trend")
        or {
            "dates": [],
            "sessions": [],
            "activeUsers": [],
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
                    "60": _ga4_profile_payload_for_period(
                        db,
                        site_id=site.id,
                        profile=profile,
                        period_days=60,
                        latest=latest,
                        prop_id=prop_id,
                    ),
                    "90": _ga4_profile_payload_for_period(
                        db,
                        site_id=site.id,
                        profile=profile,
                        period_days=90,
                        latest=latest,
                        prop_id=prop_id,
                    ),
                    "12m": _ga4_profile_payload_for_period(
                        db,
                        site_id=site.id,
                        profile=profile,
                        period_days=int(settings.ga4_trend_12m_period_days),
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


@app.get("/intelligence", response_class=HTMLResponse)
def get_intelligence_page(request: Request, db: Session = Depends(get_db)):
    """Market İstihbaratı sayfası."""
    sites = _internal_active_sites(db, active_only=False)
    return templates.TemplateResponse(
        "intelligence.html",
        {
            "request": request,
            "sites": sites,
            "site_name": "NEWS",
            "domain": "intelligence"
        }
    )


@app.get("/ga4")
def ga4_page(request: Request):
    with SessionLocal() as db:
        default_site_id = _default_active_site_id(db)
    payload = {
        "site_name": "GA4",
        "sites": get_sidebar_sites(),
        "default_site_id": default_site_id,
    }
    return templates.TemplateResponse(request, "ga4.html", context={"request": request, **payload})


def _ai_brief_llm_availability() -> dict[str, bool]:
    return {
        "groq": bool((settings.groq_api_key or "").strip()),
        "gemini": bool((settings.gemini_api_key or "").strip()),
        "openai": bool((settings.openai_api_key or "").strip()),
    }


@app.get("/ai")
def ai_daily_brief_page(request: Request):
    from backend.services.ai_daily_brief import (
        build_ai_brief_visual_context,
        get_ai_brief_run_stats,
        get_last_ai_brief_run_label_tr,
        get_latest_brief_for_ui,
    )
    from backend.services.alert_engine import get_recent_alerts

    with SessionLocal() as db:
        brief = get_latest_brief_for_ui(db)
        visual = build_ai_brief_visual_context(db)

        # Policy özet
        try:
            from backend.services import policy_csv as _pcsv
            policy_stats = _pcsv.get_stats(db)
        except Exception:
            policy_stats = {}

        # Site bazında hata özeti (7g)
        try:
            all_sites = db.query(Site).filter(Site.is_active.is_(True)).all()
            error_by_site: dict[int, dict] = {}
            for _s in all_sites:
                error_by_site[_s.id] = _build_error_widget(db, _s.id)
        except Exception:
            error_by_site = {}

        # Son uyarılar
        try:
            recent_alerts_raw = get_recent_alerts(db, limit=30, include_external=False)
        except Exception:
            recent_alerts_raw = []

        # Site listesi + toplam 404/5xx
        total_404 = sum(v.get("total_404", 0) for v in error_by_site.values())
        total_5xx = sum(v.get("total_5xx", 0) for v in error_by_site.values())

        payload = {
            "site_name": "AI",
            "sites": get_sidebar_sites(),
            "ai_brief": brief,
            "ai_brief_llm": _ai_brief_llm_availability(),
            "ai_brief_run_stats": get_ai_brief_run_stats(db),
            "ai_brief_last_run_at": get_last_ai_brief_run_label_tr(db),
            "ai_brief_visual": visual,
            "policy_stats": policy_stats,
            "error_by_site": error_by_site,
            "total_404": total_404,
            "total_5xx": total_5xx,
            "recent_alerts_data": recent_alerts_raw[:10],
            "recent_alerts_total": len(recent_alerts_raw),
        }
    template_name = "partials/ai_content.html" if request.headers.get("HX-Request") == "true" else "ai.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.post("/ai/generate")
def ai_daily_brief_generate(request: Request, llm_provider: str = Form("gemini")):
    """Operasyon: aynı gün özeti yeniden üretir (Groq/Gemini/OpenAI). E-posta `AI_DAILY_BRIEF_SEND_EMAIL=true` iken gönderilir. AI_DAILY_BRIEF_ENABLED=false olsa da bu uç force ile çalışır."""

    from backend.services.ai_daily_brief import (
        build_ai_brief_visual_context,
        get_ai_brief_run_stats,
        get_last_ai_brief_run_label_tr,
        get_latest_brief_for_ui,
        run_ai_daily_brief_job,
    )

    raw = (llm_provider or "gemini").strip().lower()
    pov = raw if raw in ("groq", "gemini", "openai") else "gemini"
    avail = _ai_brief_llm_availability()
    if not avail.get(pov):
        msg = (
            "Groq API anahtarı yapılandırılmadı."
            if pov == "groq"
            else ("OpenAI API anahtarı yapılandırılmadı." if pov == "openai" else "Gemini API anahtarı yapılandırılmadı.")
        )
        return PlainTextResponse(msg, status_code=400)

    outcome = run_ai_daily_brief_job(force=True, provider_override=pov)
    if not outcome.ok:
        return PlainTextResponse(
            outcome.message_tr
            or "AI yorumu üretilemedi. Lütfen birkaç saniye sonra tekrar deneyin.",
            status_code=500,
        )
    if request.headers.get("HX-Request") == "true":
        with SessionLocal() as db:
            brief = get_latest_brief_for_ui(db)
            ctx = {
                "request": request,
                "site_name": "AI",
                "sites": get_sidebar_sites(),
                "ai_brief": brief,
                "ai_brief_llm": _ai_brief_llm_availability(),
                "ai_brief_run_stats": get_ai_brief_run_stats(db),
                "ai_brief_last_run_at": get_last_ai_brief_run_label_tr(db),
                "ai_brief_visual": build_ai_brief_visual_context(db),
            }
        return templates.TemplateResponse(request, "partials/ai_content.html", context=ctx)
    return RedirectResponse(url="/ai", status_code=303)


@app.get("/seo-audit")
def seo_audit_page(request: Request, site_id: int | None = None, filter: str = "all", page: int = 1):
    """SEO meta tag denetim sayfası."""
    from backend.services.meta_audit import (
        get_audit_summary,
        get_audit_issues,
        get_audit_issues_count,
        purge_invalid_m_doviz_audit_urls,
    )
    from backend.models import Site

    sidebar_sites = get_sidebar_sites()

    limit = 100
    offset = (page - 1) * limit
    summary: dict = {"total_pages": 0, "score_counts": {}, "issue_counts": {}, "last_crawled": None}
    issues: list = []
    total_count = 0

    seo_audit_last_auto_run = "—"
    with SessionLocal() as db:
        external_ids = _external_site_ids(db)
        all_sites = [
            {"id": s.id, "domain": s.domain, "display_name": s.display_name}
            for s in db.query(Site).order_by(Site.domain).all()
            if s.id not in external_ids
        ]
        all_sites = sorted(
            all_sites,
            key=lambda s: _preferred_site_order_key(s.get("domain"), s.get("display_name")),
        )

        if not site_id and all_sites:
            site_id = _default_internal_site_id(all_sites)

        if site_id:
            site_row = db.query(Site).filter(Site.id == site_id).first()
            if site_row and (site_row.domain or "").lower() in ("doviz.com", "www.doviz.com"):
                purge_invalid_m_doviz_audit_urls(db, site_id)
            summary = get_audit_summary(db, site_id)
            issues = get_audit_issues(db, site_id, filter_key=filter, limit=limit, offset=offset)
            total_count = get_audit_issues_count(db, site_id, filter_key=filter)
            seo_audit_last_auto_run = _seo_audit_last_auto_run_label(db, site_id)

    selected_site_domain = next((s["domain"] for s in all_sites if s["id"] == site_id), "")

    return templates.TemplateResponse(request, "seo_audit.html", {
        "request": request,
        "sites": sidebar_sites,
        "all_sites": all_sites,
        "selected_site_id": site_id,
        "selected_site_domain": selected_site_domain,
        "summary": summary,
        "issues": issues,
        "filter": filter,
        "page": page,
        "total_count": total_count,
        "per_page": limit,
        "total_pages": max(1, (total_count + limit - 1) // limit),
        "seo_audit_scheduler_health": _seo_audit_scheduler_health(),
        "seo_audit_last_auto_run": seo_audit_last_auto_run,
    })


_seo_audit_progress: dict[int, dict] = {}  # site_id → progress dict



@app.post("/api/seo-audit/{site_id}/run")
def api_seo_audit_run(site_id: int):
    """Site audit — URL'leri tek tek işler, anında kaydeder, progress döner."""
    import threading

    from backend.models import Site
    from backend.services.seo_audit_runner import execute_seo_audit_for_site

    if site_id in _seo_audit_progress and _seo_audit_progress[site_id].get("running"):
        return {"status": "running", "message": "Tarama zaten devam ediyor"}

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if not site:
            return {"status": "error", "message": "site not found"}

    def _run():
        prog = {
            "running": True,
            "total": 0,
            "done": 0,
            "ok": 0,
            "error": 0,
            "current": "Başlıyor…",
        }
        _seo_audit_progress[site_id] = prog
        try:
            with SessionLocal() as db:
                site_row = db.query(Site).filter(Site.id == site_id).first()
                if site_row:
                    execute_seo_audit_for_site(
                        db,
                        site_row,
                        trigger_source="manual",
                        progress=prog,
                        sitemap_source="ga4",
                    )
        except Exception:
            LOGGER.exception("SEO audit manual run hatası site_id=%s", site_id)
            prog["running"] = False
            prog["current"] = "Hata oluştu"

    threading.Thread(target=_run, daemon=True, name=f"seo-audit-{site_id}").start()
    return {"status": "started", "message": "Tarama başladı"}


@app.get("/api/seo-audit/{site_id}/status")
def api_seo_audit_status(site_id: int):
    """Anlık tarama progress'i."""
    prog = _seo_audit_progress.get(site_id, {})
    with SessionLocal() as db:
        from backend.models import UrlAuditRecord
        count = db.query(UrlAuditRecord).filter(UrlAuditRecord.site_id == site_id).count()
    return {
        "running": bool(prog.get("running")),
        "total": prog.get("total", 0),
        "done": prog.get("done", 0),
        "ok": prog.get("ok", 0),
        "error": prog.get("error", 0),
        "current": prog.get("current", ""),
        "url_count": count,
    }


@app.get("/api/seo-audit/{site_id}/issues")
def api_seo_audit_issues(site_id: int, filter: str = "all", limit: int = 100, offset: int = 0):
    from backend.services.meta_audit import get_audit_issues, get_audit_issues_count
    with SessionLocal() as db:
        issues = get_audit_issues(db, site_id, filter_key=filter, limit=limit, offset=offset)
        total = get_audit_issues_count(db, site_id, filter_key=filter)
    return {"issues": issues, "total": total}


@app.get("/api/seo-audit/{site_id}/export.xlsx")
def api_seo_audit_export_xlsx(site_id: int, filter: str = "all"):
    from backend.services.meta_audit import build_audit_xlsx

    safe_filter = filter if filter != "" else "all"
    with SessionLocal() as db:
        blob = build_audit_xlsx(db, site_id, filter_key=safe_filter)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return Response(
        content=blob,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="seo_audit_site{site_id}_{safe_filter}_{today}.xlsx"',
        },
    )


@app.get("/api/seo-audit/{site_id}/export.csv")
def api_seo_audit_export_csv(site_id: int, filter: str = "all"):
    from backend.services.meta_audit import build_audit_csv

    safe_filter = filter if filter != "" else "all"
    with SessionLocal() as db:
        blob = build_audit_csv(db, site_id, filter_key=safe_filter)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return Response(
        content=blob,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="seo_audit_site{site_id}_{safe_filter}_{today}.csv"',
        },
    )


@app.get("/api/seo-audit/{site_id}/duplicates")
def api_seo_audit_duplicates(site_id: int):
    from backend.services.meta_audit import get_duplicates
    with SessionLocal() as db:
        return get_duplicates(db, site_id)


@app.get("/api/seo-audit/{site_id}/changes")
def api_seo_audit_changes(site_id: int, days: int = 7):
    from backend.services.meta_audit import get_changes
    with SessionLocal() as db:
        return {"changes": get_changes(db, site_id, days=days)}


@app.get("/doviz-varliklar")
def doviz_assets_page(request: Request):
    """Döviz banka altını katalog / fiyat kaybı izleme paneli."""
    from backend.services.doviz_asset_page_context import build_doviz_asset_monitor_context

    with SessionLocal() as db:
        ctx = build_doviz_asset_monitor_context(db)
    return templates.TemplateResponse(
        request,
        "doviz_assets.html",
        context={"request": request, "sites": get_sidebar_sites(), **ctx},
    )


@app.post("/api/doviz-asset-monitor/upload-csv")
async def api_doviz_asset_monitor_upload_csv(file: UploadFile = File(...)):
    from backend.services.doviz_asset_csv_manifest import parse_urls_from_csv_text, replace_manifest_urls

    raw = await file.read()
    if len(raw) > 5_000_000:
        return JSONResponse({"ok": False, "error": "Dosya 5 MB sınırını aşıyor."}, status_code=400)
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1", errors="replace")
    urls = parse_urls_from_csv_text(text)
    if not urls:
        return JSONResponse({"ok": False, "error": "Geçerli doviz.com URL bulunamadı."}, status_code=400)
    with SessionLocal() as db:
        meta = replace_manifest_urls(db, urls, source_label=file.filename or "upload.csv")
    return JSONResponse({"ok": True, **meta})


@app.post("/api/doviz-asset-monitor/run-csv")
def api_doviz_asset_monitor_run_csv():
    from backend.services.doviz_asset_csv_manifest import start_csv_manifest_scan_background

    out = start_csv_manifest_scan_background()
    code = 200 if out.get("started") else 409
    return JSONResponse(out, status_code=code)


@app.get("/api/doviz-asset-monitor/csv-scan-status")
def api_doviz_asset_monitor_csv_scan_status():
    from backend.services.doviz_asset_csv_manifest import (
        csv_run_summary,
        get_csv_scan_progress,
        get_latest_csv_run,
        manifest_upload_info,
    )

    with SessionLocal() as db:
        latest = get_latest_csv_run(db)
        manifest = manifest_upload_info(db)
    progress = get_csv_scan_progress()
    summary = (latest or {}).get("summary") if latest else None
    if not summary and latest:
        summary = csv_run_summary((latest or {}).get("payload"))
    return JSONResponse(
        {
            "progress": progress,
            "manifest": manifest,
            "latest": latest,
            "summary": summary,
        }
    )


@app.get("/api/doviz-asset-monitor/manifest")
def api_doviz_asset_monitor_manifest():
    from backend.services.doviz_asset_csv_manifest import get_latest_csv_run, manifest_upload_info

    with SessionLocal() as db:
        return JSONResponse(
            {
                "manifest": manifest_upload_info(db),
                "latest_csv_run": get_latest_csv_run(db),
            }
        )


@app.post("/api/doviz-asset-monitor/run")
def api_doviz_asset_monitor_run():
    from backend.services.doviz_asset_monitor import cleanup_old_runs, run_doviz_asset_monitor

    with SessionLocal() as db:
        out = run_doviz_asset_monitor(db)
        cleanup_old_runs(db, keep_days=30)
    return JSONResponse(out)


@app.get("/api/doviz-asset-monitor/latest")
def api_doviz_asset_monitor_latest():
    from backend.services.doviz_asset_monitor import get_latest_run

    with SessionLocal() as db:
        return JSONResponse(get_latest_run(db) or {})


@app.get("/errors")
def errors_page(request: Request, site_id: int | None = None, days: int = 7):
    """Site hata izleme — iskelet anında render, tablo HTMX lazy load."""
    sidebar_sites = get_sidebar_sites()
    days = max(1, min(int(days), 30))

    with SessionLocal() as db:
        from backend.models import Site
        from backend.services.doviz_asset_page_context import build_doviz_asset_monitor_context

        external_ids = _external_site_ids(db)
        _raw_sites = [s for s in db.query(Site).order_by(Site.domain).all() if s.id not in external_ids]
        all_sites = sorted(_raw_sites, key=lambda s: (0 if "doviz" in (s.domain or "").lower() else 1, s.domain or ""))
        all_sites_list = [{"id": s.id, "domain": s.domain, "display_name": s.display_name} for s in all_sites]
        doviz_ctx = build_doviz_asset_monitor_context(db)

    if not site_id and all_sites_list:
        site_id = all_sites_list[0]["id"]

    selected_site_domain = next((s["domain"] for s in all_sites_list if s["id"] == site_id), "")

    return templates.TemplateResponse(
        request,
        "errors.html",
        context={
            "request": request,
            "sites": sidebar_sites,
            "all_sites": all_sites_list,
            "selected_site_id": site_id,
            "selected_site_domain": selected_site_domain,
            "days": days,
            "show_doviz_full_page_link": True,
            **doviz_ctx,
        },
    )


@app.get("/api/errors/{site_id}/summary")
def api_errors_summary(site_id: int, days: int = 7):
    """HTMX lazy-load: belirtilen site için hata özeti (DB'den, GA4 çağrısı yok)."""
    from backend.services.error_monitor import get_error_summary
    days = max(1, min(int(days), 30))
    with SessionLocal() as db:
        summary = get_error_summary(db, site_id, days=days)
    return JSONResponse(summary)


@app.get("/api/errors/{site_id}/refresh")
def api_errors_refresh(site_id: int, days: int = 1):
    """Manuel hata tespiti tetikle — site için GA4'ten 404 çek."""
    try:
        from backend.services.error_monitor import run_error_detection_for_site
        with SessionLocal() as db:
            result = run_error_detection_for_site(db, site_id, days=days)
        return {"status": "ok", **result}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@app.get("/api/errors/refresh-all")
def api_errors_refresh_all():
    """Tüm siteler için tüm periyotları GA4'ten çek (manuel tetikleme)."""
    try:
        from backend.services.error_monitor import run_error_detection_all_sites
        with SessionLocal() as db:
            results = run_error_detection_all_sites(db)
        total_found = sum(r.get("found", 0) for r in results if isinstance(r, dict))
        return {"status": "ok", "sites": len(results), "total_found": total_found}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# --- Hata izleme: arka plan job + progress ----------------------------------

_err_refresh_job: dict = {"running": False, "steps": [], "current": "", "total": 0, "done": 0, "total_found": 0, "error": ""}
_err_refresh_lock = threading.Lock()


@app.post("/api/errors/refresh-all/start")
def api_errors_refresh_start():
    """Arka planda tüm siteler için GA4 hata çekimini başlatır."""
    with _err_refresh_lock:
        if _err_refresh_job.get("running"):
            return {"status": "already_running"}
        _err_refresh_job.update({"running": True, "steps": [], "current": "", "total": 0, "done": 0, "total_found": 0, "error": ""})

    def _worker():
        try:
            from backend.services.error_monitor import run_error_detection_for_site, _GA4_PERIODS
            from backend.services.ga4_auth import get_ga4_credentials_record, ga4_is_configured

            with SessionLocal() as db:
                if not ga4_is_configured():
                    _err_refresh_job["error"] = "GA4 yapılandırılmamış"
                    return
                sites = [s for s in db.query(Site).all() if get_ga4_credentials_record(db, s.id)]

            total_steps = len(sites) * len(_GA4_PERIODS)
            _err_refresh_job["total"] = total_steps

            done = 0
            total_found = 0
            for site in sites:
                for days in _GA4_PERIODS:
                    _err_refresh_job["current"] = f"{site.display_name or site.domain} · {days}g"
                    try:
                        with SessionLocal() as db:
                            result = run_error_detection_for_site(db, site.id, days=days)
                        found = result.get("found", 0)
                        total_found += found
                        _err_refresh_job["steps"].append({
                            "domain": site.display_name or site.domain,
                            "days": days,
                            "found": found,
                            "status": result.get("status", "ok"),
                        })
                    except Exception as exc:
                        _err_refresh_job["steps"].append({
                            "domain": site.display_name or site.domain,
                            "days": days,
                            "found": 0,
                            "status": "error",
                            "msg": str(exc)[:80],
                        })
                    done += 1
                    _err_refresh_job["done"] = done
                    _err_refresh_job["total_found"] = total_found
        except Exception as exc:
            _err_refresh_job["error"] = str(exc)
        finally:
            _err_refresh_job["running"] = False
            _err_refresh_job["current"] = ""

    threading.Thread(target=_worker, daemon=True, name="err-refresh-all").start()
    return {"status": "started"}


@app.get("/api/errors/refresh-all/progress")
def api_errors_refresh_progress():
    """Arka plan hata çekimi progress durumu."""
    j = _err_refresh_job
    steps = list(j.get("steps") or [])
    total = j.get("total") or 0
    done = j.get("done", 0)
    pct = int(done * 100 / total) if total > 0 else (100 if not j.get("running") else 0)
    return {
        "running": bool(j.get("running")),
        "current": j.get("current", ""),
        "total": total,
        "done": done,
        "pct": pct,
        "total_found": j.get("total_found", 0),
        "error": j.get("error", ""),
        "recent_steps": steps[-8:],
    }


@app.get("/api/errors/{site_id}/debug-pages")
def api_errors_debug(site_id: int, days: int = 7, limit: int = 50):
    """Debug: GA4'teki düşük trafikli sayfaları filtre olmadan listeler.
    Amacı: 404 sayfasının gerçek başlığını bulmak."""
    try:
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, OrderBy, RunReportRequest,
        )
        from backend.services.error_monitor import _build_ga4_client
        from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
        from backend.models import Site

        with SessionLocal() as db:
            site = db.query(Site).filter(Site.id == site_id).first()
            if not site:
                return {"error": "site not found"}
            record = get_ga4_credentials_record(db, site.id)
            properties = load_ga4_properties(record)

        prop_id = properties.get("web") or next(iter(properties.values()), "")
        if not prop_id:
            return {"error": "property bulunamadı"}

        pid = prop_id if prop_id.startswith("properties/") else f"properties/{prop_id}"
        client = _build_ga4_client()
        req = RunReportRequest(
            property=pid,
            date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="today")],
            dimensions=[Dimension(name="pagePath"), Dimension(name="pageTitle")],
            metrics=[Metric(name="screenPageViews"), Metric(name="totalUsers")],
            order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="totalUsers"), desc=False)],
            limit=limit,
        )
        resp = client.run_report(req)
        rows = []
        for row in resp.rows:
            rows.append({
                "path":   row.dimension_values[0].value,
                "title":  row.dimension_values[1].value,
                "views":  row.metric_values[0].value,
                "users":  row.metric_values[1].value,
            })
        return {"rows": rows, "total": len(rows), "property": prop_id, "days": days}
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/errors/{site_id}/widget")
def api_errors_widget(site_id: int, days: int = 7):
    """GA4 kart widget'ı için hata özeti — top 5 hatalı URL."""
    try:
        from backend.services.error_monitor import get_error_summary
        from backend.models import Site
        with SessionLocal() as db:
            summary = get_error_summary(db, site_id, days=days)
            site = db.query(Site).filter(Site.id == site_id).first()
            summary["domain"] = site.domain if site else ""
        return summary
    except Exception as exc:
        return {"errors": [], "total_404": 0, "total_5xx": 0, "total_users": 0, "domain": "", "message": str(exc)}


@app.get("/tmdb-upcoming")
def tmdb_upcoming_page(request: Request, months: int = 5):
    """TMDB vizyon takvimi — sinemalar.com içerik planlama."""
    from backend.services.tmdb import get_combined_upcoming
    months = max(1, min(int(months), 12))
    error = None
    data: dict = {
        "theatrical": [], "theatrical_by_month": {},
        "streaming":  [], "streaming_by_month":  {},
        "turkish_only": [], "turkish_by_month": {},
        "tv_series":  [], "tv_by_month": {},
        "high_potential": [],
        "months_ahead": months,
        "total_theatrical": 0, "total_streaming": 0,
        "total_turkish": 0,    "total_tv": 0,
    }
    try:
        data = get_combined_upcoming(months_ahead=months)
    except Exception as exc:
        LOGGER.exception("TMDB upcoming hatası")
        error = str(exc)

    try:
        from datetime import date as _date
        from backend.services.sinemalar_match import attach_to_upcoming_data

        attach_to_upcoming_data(
            data,
            max_lookups=40,
            current_month=_date.today().strftime("%Y-%m"),
        )
    except Exception:
        LOGGER.exception("Sinemalar eşleştirme (sayfa) atlandı")

    # OMDB zenginleştirme verisini DB'den çek ve filmlere merge et
    if (settings.omdb_api_key or "").strip():
        try:
            from backend.services.omdb import get_enrichment_map
            all_lists = (
                data.get("theatrical", []) +
                data.get("streaming", []) +
                data.get("turkish_only", [])
            )
            tmdb_ids = [m["id"] for m in all_lists if m.get("id")]
            with SessionLocal() as db:
                omdb_map = get_enrichment_map(db, tmdb_ids)
            for lst_key in ("theatrical", "streaming", "turkish_only",
                            "theatrical_by_month", "streaming_by_month", "turkish_by_month"):
                lst = data.get(lst_key, [])
                items = lst.values() if isinstance(lst, dict) else []
                if isinstance(lst, list):
                    items = lst
                else:
                    items = [m for month in lst.values() for m in month]
                for m in items:
                    mid = m.get("id")
                    if mid and mid in omdb_map:
                        row = omdb_map[mid]
                        m["imdb_rating"]  = row.imdb_rating
                        m["imdb_votes"]   = row.imdb_votes
                        m["rt_score"]     = row.rt_score
                        m["metacritic"]   = row.metacritic
                        m["age_rating"]   = row.age_rating
                        m["box_office"]   = row.box_office
                        m["awards"]       = row.awards
        except Exception:
            LOGGER.exception("OMDB merge hatası (sayfa yüklenmesini engellemez)")

    from backend.services.tmdb import streaming_provider_filters

    payload = {
        "site_name": "Movie",
        "sites": get_sidebar_sites(),
        "data": data,
        "error": error,
        "current_month": date.today().strftime("%Y-%m"),
        "streaming_provider_filters": streaming_provider_filters(),
    }
    return templates.TemplateResponse(request, "tmdb_upcoming.html",
                                      context={"request": request, **payload})


@app.post("/api/tmdb-upcoming/sinemalar-lookup")
async def api_tmdb_sinemalar_lookup(request: Request):
    """Kartlar için toplu Sinemalar eşleştirme (önbellekli)."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"results": {}}, status_code=400)
    items = body.get("items") if isinstance(body, dict) else None
    if not isinstance(items, list):
        return JSONResponse({"results": {}}, status_code=400)
    from backend.services.sinemalar_match import lookup_items_batch

    return {"results": lookup_items_batch(items, max_items=16)}


@app.get("/app")
def app_intel_page(request: Request):
    from backend.services.app_intel import list_products

    payload = {
        "site_name": "App",
        "sites": get_sidebar_sites(),
        "app_products": list_products(),
    }
    template_name = "partials/app_content.html" if request.headers.get("HX-Request") == "true" else "app.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.get("/firebase")
def firebase_page(request: Request):
    """Firebase Crashlytics izleme sayfası."""
    from backend.services import crashlytics_bq as cbq

    payload = {
        "site_name": "Firebase",
        "sites": get_sidebar_sites(),
        "crash_products": cbq.list_crashlytics_products(),
    }
    template_name = "partials/firebase_content.html" if request.headers.get("HX-Request") == "true" else "firebase.html"
    return templates.TemplateResponse(request, template_name, context={"request": request, **payload})


@app.get("/api/app/intel")
def api_app_intel(product: str = "doviz", period: int = 30, cache_only: int = 0):
    from backend.services.app_intel import APP_PRODUCTS, build_intel_payload, intel_json_safe

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    try:
        p = int(period)
    except (TypeError, ValueError):
        p = 30
    if p not in (0, 7, 30, 90, 180, 365, 730):
        p = 30
    payload = build_intel_payload(pid, p, cache_only=bool(cache_only))
    if payload.get("error"):
        sc = 404 if payload["error"] == "no_cached_data" else 400
        return JSONResponse(intel_json_safe(payload), status_code=sc)
    return JSONResponse(intel_json_safe(payload))


@app.get("/api/app/version-releases")
def api_app_version_releases(product: str = "doviz", since: str = "2025-01-01"):
    from datetime import date as date_cls

    from backend.services.app_intel import APP_PRODUCTS, intel_json_safe
    from backend.services.store_version_releases import fetch_version_releases_for_product

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    try:
        since_d = date_cls.fromisoformat((since or "2025-01-01").strip()[:10])
    except ValueError:
        return JSONResponse({"error": "invalid_since"}, status_code=400)
    return JSONResponse(intel_json_safe(fetch_version_releases_for_product(pid, since=since_d)))


@app.get("/api/app/asc-preview")
def api_app_asc_preview(
    product: str = "doviz",
    period: int = 30,
    country: str = "all",
    source: str = "all",
    device: str = "all",
):
    """App Store Connect benzeri kazanım / satış / abonelik / etkileşim özeti."""
    from backend.services.app_asc_preview import build_asc_connect_preview_payload
    from backend.services.app_intel import APP_PRODUCTS, intel_json_safe

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    try:
        p = int(period)
    except (TypeError, ValueError):
        p = 30
    if p not in (0, 1, 7, 14, 30, 90, 365):
        p = 30
    payload = build_asc_connect_preview_payload(pid, p, country=country, source=source, device=device)
    if payload.get("error"):
        return JSONResponse(intel_json_safe(payload), status_code=400)
    return JSONResponse(intel_json_safe(payload))


@app.get("/api/app/store-rollout")
def api_app_store_rollout(product: str = "doviz"):
    """iOS phased release + Android production staged rollout yüzdesi (canlı API)."""
    from backend.services.app_intel import APP_PRODUCTS, intel_json_safe
    from backend.services.store_rollout import fetch_store_rollout

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    return JSONResponse(intel_json_safe(fetch_store_rollout(pid)))


@app.get("/api/app/asc-stream")
async def api_app_asc_stream(
    product: str = "doviz",
    period: int = 30,
    country: str = "all",
    source: str = "all",
    device: str = "all",
):
    """SSE endpoint: gün bazlı progress + final payload."""
    import asyncio
    import json
    import threading
    from starlette.responses import StreamingResponse as StarletteStreamingResponse
    from backend.services.app_asc_preview import build_asc_connect_preview_payload
    from backend.services.app_intel import APP_PRODUCTS, intel_json_safe

    pid = (product or "doviz").strip().lower()
    try:
        p = int(period)
    except (TypeError, ValueError):
        p = 30
    if p not in (0, 1, 7, 14, 30, 90, 365):
        p = 30
    if pid not in APP_PRODUCTS:
        async def _err():
            yield 'data: {"type":"error","message":"unknown_product"}\n\n'
        return StarletteStreamingResponse(_err(), media_type="text/event-stream")

    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()

    def _worker():
        def _cb(done: int, total: int):
            asyncio.run_coroutine_threadsafe(
                queue.put({"type": "progress", "done": done, "total": total}), loop
            )

        try:
            payload = build_asc_connect_preview_payload(
                pid, p, country=country, source=source, device=device,
                progress_cb=_cb,
            )
            result = intel_json_safe(payload)
            asyncio.run_coroutine_threadsafe(
                queue.put({"type": "done", "payload": result}), loop
            )
        except Exception as exc:
            asyncio.run_coroutine_threadsafe(
                queue.put({"type": "error", "message": str(exc)[:200]}), loop
            )

    threading.Thread(target=_worker, daemon=True).start()

    async def _generate():
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=90)
            except asyncio.TimeoutError:
                yield 'data: {"type":"error","message":"timeout"}\n\n'
                break
            yield f"data: {json.dumps(event)}\n\n"
            if event.get("type") in ("done", "error"):
                break

    return StarletteStreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/app/gp-preview")
def api_app_gp_preview(
    product: str = "doviz",
    period: int = 30,
    country: str = "all",
    device: str = "all",
):
    """Google Play Store Analytics özeti (indirmeler, vitals, puanlama)."""
    from backend.services.gp_preview import build_gp_preview_payload
    from backend.services.app_intel import APP_PRODUCTS, intel_json_safe

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    try:
        p = int(period)
    except (TypeError, ValueError):
        p = 30
    if p not in (1, 7, 14, 30, 90, 365):
        p = 30
    payload = build_gp_preview_payload(pid, p, country=country, device=device)
    if payload.get("error"):
        return JSONResponse(intel_json_safe(payload), status_code=400)
    return JSONResponse(intel_json_safe(payload))


@app.get("/api/app/crashlytics")
def api_app_crashlytics(product: str = "doviz", days: int = 7):
    """Eski endpoint — geriye uyumluluk için korundu."""
    from backend.services.app_intel import APP_PRODUCTS, intel_json_safe
    from backend.services import crashlytics_bq as cbq

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    try:
        d = int(days)
    except (TypeError, ValueError):
        d = 7
    if d not in (1, 7, 14, 30, 90):
        d = 7
    payload = cbq.build_full_payload(pid, days=d, platform_filter="all")
    return JSONResponse(intel_json_safe(payload))


# ─────────────────────────────────────────────────────────────────────────────
# Crashlytics HTMX Partial Endpoint'leri
# Her endpoint HTML döner; app_content.html içindeki tab'lar hx-get ile çağırır.
# ─────────────────────────────────────────────────────────────────────────────

def _crash_params(request: Request) -> dict:
    """Ortak query param'ları ayrıştır."""
    q = request.query_params
    try:
        days = int(q.get("days", 7))
    except (ValueError, TypeError):
        days = 7
    if days not in (1, 7, 30, 90):
        days = 7
    platform = (q.get("platform") or "all").strip().lower()
    if platform not in ("all", "android", "ios"):
        platform = "all"
    product = (q.get("product") or "doviz").strip().lower()
    error_type = (q.get("type") or "").strip().upper() or None
    versions_raw = (q.get("versions") or q.get("version") or "").strip()
    versions = [v.strip() for v in versions_raw.split(",") if v.strip()]
    version = versions[0] if len(versions) == 1 else None
    return {
        "product": product,
        "days": days,
        "platform": platform,
        "error_type": error_type,
        "version": version,
        "versions": versions,
    }


def _version_list_from_params(params: dict) -> list[str]:
    versions = [str(v).strip() for v in (params.get("versions") or []) if str(v).strip()]
    single = (params.get("version") or "").strip()
    if single and single not in versions:
        versions.append(single)
    return versions


# Filtreli (sürüm/tür) partial istekleri aynı BQ setini tekrar çalıştırmasın.
_CRASH_FETCH_FILTER_CACHE_TTL_S = 10 * 60
_CRASH_FETCH_FILTER_CACHE: dict[str, tuple[float, dict]] = {}
_CRASH_FETCH_FILTER_CACHE_LOCK = threading.Lock()
_CRASH_FETCH_FILTER_LOCKS: dict[str, threading.Lock] = {}
_CRASH_FETCH_FILTER_LOCKS_GUARD = threading.Lock()


def _crash_fetch_filter_cache_key(params: dict) -> str | None:
    ver_list = sorted(_version_list_from_params(params))
    error_type = (params.get("error_type") or "").strip().upper() or ""
    if not ver_list and not error_type:
        return None
    plat = (params.get("platform") or "all").strip().lower()
    return f"{params['product']}:{params['days']}:{plat}:{','.join(ver_list)}:{error_type}"


def _crash_fetch_filter_cache_get(key: str) -> dict | None:
    with _CRASH_FETCH_FILTER_CACHE_LOCK:
        entry = _CRASH_FETCH_FILTER_CACHE.get(key)
        if entry and time.time() - entry[0] < _CRASH_FETCH_FILTER_CACHE_TTL_S:
            return entry[1]
    return None


def _crash_fetch_filter_cache_set(key: str, data: dict) -> None:
    with _CRASH_FETCH_FILTER_CACHE_LOCK:
        _CRASH_FETCH_FILTER_CACHE[key] = (time.time(), data)
        if len(_CRASH_FETCH_FILTER_CACHE) > 80:
            cutoff = time.time() - _CRASH_FETCH_FILTER_CACHE_TTL_S
            stale = [k for k, (ts, _) in _CRASH_FETCH_FILTER_CACHE.items() if ts < cutoff]
            for k in stale:
                _CRASH_FETCH_FILTER_CACHE.pop(k, None)


def clear_crash_fetch_filter_cache(product: str | None = None) -> None:
    """Manuel yenileme sonrası filtreli partial önbelleğini temizler."""
    prefix = f"{(product or '').strip().lower()}:" if product else None
    with _CRASH_FETCH_FILTER_CACHE_LOCK:
        if prefix:
            for k in list(_CRASH_FETCH_FILTER_CACHE):
                if k.startswith(prefix):
                    _CRASH_FETCH_FILTER_CACHE.pop(k, None)
        else:
            _CRASH_FETCH_FILTER_CACHE.clear()


def _crash_fetch_filter_lock(key: str) -> threading.Lock:
    with _CRASH_FETCH_FILTER_LOCKS_GUARD:
        if key not in _CRASH_FETCH_FILTER_LOCKS:
            _CRASH_FETCH_FILTER_LOCKS[key] = threading.Lock()
        return _CRASH_FETCH_FILTER_LOCKS[key]


def _refetch_filtered_payload(data: dict, params: dict) -> dict:
    """Sürüm/tür seçiliyken BQ'da filtreli sorgu — tüm görünüm alanları tutarlı olsun."""
    from backend.services import crashlytics_bq as cbq
    from backend.services.crashlytics_detail import enrich_issue_row

    ver_list = _version_list_from_params(params)
    error_type = (params.get("error_type") or "").strip().upper() or None
    if not ver_list and not error_type:
        return data

    pid = params["product"]
    days = int(data.get("days") or params.get("days") or 7)
    plat_filter = (params.get("platform") or "all").strip().lower()
    scope = plat_filter if plat_filter in ("ios", "android") else "all"
    platforms = cbq._platforms_for(pid, scope)
    if not platforms:
        return data

    meta = cbq.APP_PRODUCTS.get(pid, {})
    filt_kw = {"error_type": error_type, "versions": ver_list or None}

    data = dict(data)
    issues_all: list[tuple[str, list[dict]]] = []
    anr_all: list[tuple[str, list[dict]]] = []
    summary_by_plat: dict[str, dict] = {}
    trend_all: list[tuple[str, list[dict]]] = []
    device_all: list[tuple[str, list[dict]]] = []
    os_all: list[tuple[str, list[dict]]] = []
    process_all: list[tuple[str, list[dict]]] = []
    ver_all: list[tuple[str, list[dict]]] = []
    version_trend_all: list[tuple[str, list[dict]]] = []

    def _fetch_filtered_platform(plat_key: str, tbl: str) -> dict[str, Any]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        bundle = (meta.get("android_package") if plat_key == "android" else meta.get("ios_bundle_id")) or ""
        batch_ref = cbq._batch_table_ref(plat_key, bundle) if bundle else None
        out: dict[str, Any] = {"platform": plat_key}
        sub_tasks = {
            "summary": lambda: ("summary", cbq.query_summary(plat_key, tbl, days, **filt_kw), None),
            "issues": lambda: ("issues", *cbq.query_top_issues(
                plat_key, tbl, days, error_type, None, versions=ver_list or None
            )),
            "anr": lambda: ("anr", *cbq.query_anr_list(plat_key, tbl, days, versions=ver_list or None)),
            "trend": lambda: ("trend", *cbq.query_daily_trend(plat_key, tbl, days, **filt_kw)),
            "devices": lambda: ("devices", *cbq.query_device_breakdown(plat_key, tbl, days, **filt_kw)),
            "os": lambda: ("os", *cbq.query_os_breakdown(plat_key, tbl, days, **filt_kw)),
            "process_state": lambda: (
                "process_state",
                *cbq.query_process_state_breakdown(plat_key, batch_ref, days, **filt_kw),
            ),
            "versions": lambda: ("versions", *cbq.query_version_breakdown(plat_key, tbl, days, **filt_kw)),
            "version_trend": lambda: (
                "version_trend",
                *cbq.query_version_time_series(plat_key, tbl, days, **filt_kw),
            ),
        }
        if error_type and error_type != "ANR":
            sub_tasks.pop("anr", None)
        with ThreadPoolExecutor(max_workers=min(8, len(sub_tasks)), thread_name_prefix="crash-filt") as pool:
            futs = {pool.submit(fn): name for name, fn in sub_tasks.items()}
            for fut in as_completed(futs):
                try:
                    key, payload, err = fut.result()
                    if key == "summary":
                        out["summary"] = payload or {}
                    elif key == "issues":
                        out["issues"] = payload or []
                        out["issues_err"] = err
                    elif key == "anr":
                        out["anr"] = payload or []
                        out["anr_err"] = err
                    else:
                        out[key] = payload or []
                        out[f"{key}_err"] = err
                except Exception as exc:  # noqa: BLE001
                    name = futs[fut]
                    out[name] = [] if name != "summary" else {}
                    if name != "summary":
                        out[f"{name}_err"] = str(exc)[:200]
        return out

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=min(2, len(platforms)), thread_name_prefix="crash-filt-plat") as pool:
        plat_futs = {pool.submit(_fetch_filtered_platform, plat_key, tbl): plat_key for plat_key, tbl in platforms}
        for fut in as_completed(plat_futs):
            try:
                res = fut.result()
                plat_key = res["platform"]
                summary_by_plat[plat_key] = res.get("summary") or {}
                if res.get("issues"):
                    issues_all.append((plat_key, res["issues"]))
                if res.get("anr"):
                    anr_all.append((plat_key, res["anr"]))
                if res.get("trend"):
                    trend_all.append((plat_key, res["trend"]))
                if res.get("devices"):
                    device_all.append((plat_key, res["devices"]))
                if res.get("os"):
                    os_all.append((plat_key, res["os"]))
                if res.get("process_state"):
                    process_all.append((plat_key, res["process_state"]))
                if res.get("versions"):
                    ver_all.append((plat_key, res["versions"]))
                if res.get("version_trend"):
                    version_trend_all.append((plat_key, res["version_trend"]))
            except Exception:  # noqa: BLE001
                continue

    data["summary_by_platform"] = summary_by_plat
    totals = {"fatal": 0, "anr": 0, "non_fatal": 0, "affected_users": 0}
    for s in summary_by_plat.values():
        totals["fatal"] += int(s.get("fatal") or 0)
        totals["anr"] += int(s.get("anr") or 0)
        totals["non_fatal"] += int(s.get("non_fatal") or 0)
        totals["affected_users"] += int(s.get("affected_users") or 0)
    data["totals"] = totals

    data["issues"] = cbq._merge_issues(issues_all, days=days) if issues_all else []
    data["issues_by_platform"] = {
        plat: sorted(
            [enrich_issue_row({**r, "platform": plat}, days=days) for r in rows],
            key=lambda x: -x["event_count"],
        )
        for plat, rows in issues_all
    }
    data["anr"] = cbq._merge_issues(anr_all, days=days) if anr_all else []
    data["anr_by_platform"] = {
        plat: sorted(
            [enrich_issue_row({**r, "platform": plat}, days=days) for r in rows],
            key=lambda x: -x["event_count"],
        )
        for plat, rows in anr_all
    }

    data.update(
        cbq.assemble_breakdown_payload(
            trend_all=trend_all,
            device_all=device_all,
            os_all=os_all,
            process_all=process_all,
            ver_all=ver_all,
            version_trend_all=version_trend_all,
        )
    )

    if ver_list:
        data["crash_free_pct"] = None
        data["crash_free_sessions_pct"] = None
        data["crash_free_users_pct"] = None
        hints = list(data.get("crash_free_hints") or [])
        hint = (
            "Crash-free, seçili app sürümüne göre hesaplanmaz "
            "(oturum tablosu sürüm kırılımı içermez)."
        )
        if hint not in hints:
            hints.append(hint)
        data["crash_free_hints"] = hints

    data["active_filters"] = {
        "versions": ver_list,
        "error_type": error_type,
    }
    return data


def _crash_fetch(params: dict) -> dict:
    """BQ cache + seçili sürüm/tür için filtreli yeniden sorgu (doğru olay sayıları)."""
    filter_key = _crash_fetch_filter_cache_key(params)
    if filter_key:
        cached = _crash_fetch_filter_cache_get(filter_key)
        if cached:
            return cached
        with _crash_fetch_filter_lock(filter_key):
            cached = _crash_fetch_filter_cache_get(filter_key)
            if cached:
                return cached
            data = _crash_fetch_impl(params)
            if data and data.get("ok"):
                _crash_fetch_filter_cache_set(filter_key, data)
            return data
    return _crash_fetch_impl(params)


def _crash_fetch_impl(params: dict) -> dict:
    from backend.services import crashlytics_bq as cbq
    data = cbq.build_full_payload(
        params["product"],
        days=params["days"],
        platform_filter="all",
    )
    if not data or not data.get("ok"):
        return data

    if _version_list_from_params(params) or (params.get("error_type") or "").strip():
        data = _refetch_filtered_payload(data, params)

    plat = (params.get("platform") or "all").strip().lower()
    if plat in ("ios", "android"):
        data = cbq.slice_payload_for_platform(data, plat)

    from backend.services.android_device_names import apply_device_friendly_labels
    return apply_device_friendly_labels(data, plat)


@app.get("/api/app/crashlytics/summary", response_class=HTMLResponse)
def api_crash_summary(request: Request):
    params = _crash_params(request)
    data = _crash_fetch(params)
    return templates.TemplateResponse(
        request, "partials/crashlytics/summary.html",
        {"request": request, "data": data, "params": params},
    )


@app.get("/api/app/crashlytics/crashes", response_class=HTMLResponse)
def api_crash_crashes(request: Request):
    params = _crash_params(request)
    data = _crash_fetch(params)
    return templates.TemplateResponse(
        request, "partials/crashlytics/crashes.html",
        {"request": request, "data": data, "params": params},
    )


@app.get("/api/app/crashlytics/anr", response_class=HTMLResponse)
def api_crash_anr(request: Request):
    params = _crash_params(request)
    data = _crash_fetch(params)
    return templates.TemplateResponse(
        request, "partials/crashlytics/anr.html",
        {"request": request, "data": data, "params": params},
    )


@app.get("/api/app/crashlytics/versions", response_class=HTMLResponse)
def api_crash_versions(request: Request):
    params = _crash_params(request)
    data = _crash_fetch(params)
    # "Tür" filtresine göre yeniden sırala — seçili türün sayısı yüksek olan üstte
    et = params.get("error_type")
    sort_key = {"FATAL": "fatal_count", "ANR": "anr_count", "NON_FATAL": "non_fatal_count"}.get(et or "")
    if sort_key and isinstance(data, dict) and data.get("versions"):
        data = {**data, "versions": sorted(data["versions"], key=lambda v: -int(v.get(sort_key, 0) or 0))}
    return templates.TemplateResponse(
        request, "partials/crashlytics/versions.html",
        {"request": request, "data": data, "params": params},
    )


@app.get("/api/app/crashlytics/breakdown", response_class=HTMLResponse)
def api_crash_breakdown(request: Request):
    params = _crash_params(request)
    data = _crash_fetch(params)
    return templates.TemplateResponse(
        request, "partials/crashlytics/breakdown.html",
        {"request": request, "data": data, "params": params},
    )


@app.get("/api/app/crashlytics/version-chart", response_class=HTMLResponse)
def api_crash_version_chart(request: Request):
    params = _crash_params(request)
    data = _crash_fetch(params)
    return templates.TemplateResponse(
        request, "partials/crashlytics/version_chart.html",
        {"request": request, "data": data, "params": params},
    )


@app.get("/api/app/crashlytics/diagnose")
def api_crash_diagnose(product: str = "doviz"):
    """Firebase Crashlytics BigQuery bağlantısını teşhis et.
    Her adımda gerçek API hatasını yüzeye çıkarır."""
    from backend.services import crashlytics_bq as cbq
    from backend.services.app_intel import APP_PRODUCTS

    pid = (product or "doviz").strip().lower()
    meta = APP_PRODUCTS.get(pid, {})
    out: dict = {"product": pid, "dataset": cbq._DATASET, "platforms": {}}
    for plat in ("android", "ios"):
        bundle = (
            meta.get("android_package") if plat == "android" else meta.get("ios_bundle_id")
        ) or ""
        plat_block = cbq.diagnose_platform(plat)
        plat_block["bundle_id"] = bundle
        plat_block["expected_table_standard"] = (
            f"{bundle.replace('.', '_')}_{plat.upper()}" if bundle else None
        )
        if bundle:
            try:
                plat_block["discovered_table"] = cbq._discover_table_id(plat, bundle)
            except Exception as exc:  # noqa: BLE001
                plat_block["discovery_error"] = str(exc)[:300]
        out["platforms"][plat] = plat_block
    try:
        out["platform_analysis"] = cbq.analyze_platform_parity(pid, days=7)
    except Exception as exc:  # noqa: BLE001
        out["platform_analysis_error"] = str(exc)[:500]
    return JSONResponse(out)


@app.get("/api/app/crashlytics/platform-analysis")
def api_crash_platform_analysis(product: str = "doviz", days: int = 7):
    """iOS vs Android veri farkı ve crash-free teşhisi."""
    from backend.services import crashlytics_bq as cbq

    pid = (product or "doviz").strip().lower()
    try:
        d = int(days)
    except (TypeError, ValueError):
        d = 7
    if d not in (1, 7, 14, 30, 90):
        d = 7
    return JSONResponse(cbq.analyze_platform_parity(pid, days=d))


@app.get("/api/app/crashlytics/progress")
def api_crash_progress(product: str = "doviz"):
    from backend.services import crashlytics_bq as cbq
    pid = product.strip().lower()
    warm = cbq.is_cache_warm(pid)
    state = cbq.get_job_state(pid)
    if state is None:
        return JSONResponse({"running": False, "pct": 0, "step": "", "done": True, "error": None, "cache_warm": warm})
    return JSONResponse({
        "running": not state["done"],
        "pct": state.get("pct", 0),
        "step": state.get("step", ""),
        "done": state.get("done", False),
        "error": state.get("error"),
        "cache_warm": warm,
    })


@app.get("/api/app/crashlytics/issue-detail")
def api_crash_issue_detail(
    request: Request,
    product: str = "doviz",
    platform: str = "android",
    issue_id: str = "",
    days: int = 7,
):
    """Tek bir issue için drill-down: trend, versiyon, OS, cihaz, stack frame.

    Frontend modal'ı bu endpoint'i çağırır. Tablolar yokken/credential eksikken
    JSON ok=false döner; frontend hata mesajını gösterir.
    """
    from backend.services import crashlytics_bq as cbq

    plat = (platform or "").strip().lower()
    if plat not in ("ios", "android"):
        return JSONResponse({"ok": False, "error": "invalid_platform"}, status_code=400)
    try:
        d = int(days)
    except (TypeError, ValueError):
        d = 7
    if d not in (1, 7, 14, 30, 90):
        d = 7

    iid = (issue_id or "").strip()
    if not iid:
        return JSONResponse({"ok": False, "error": "missing_issue_id"}, status_code=400)

    fp = _crash_params(request)
    ver_list = _version_list_from_params(fp)
    data = cbq.get_issue_detail_for_product(
        product,
        plat,
        iid,
        d,
        versions=ver_list or None,
        version=fp.get("version"),
    )
    if ver_list and data.get("ok"):
        total = int((data.get("summary") or {}).get("total_events") or 0)
        if total == 0:
            data["filter_empty"] = True
            data["filter_versions"] = ver_list
    return JSONResponse(data)


@app.get("/api/app/crashlytics/issue-event")
def api_crash_issue_event(
    product: str = "doviz",
    platform: str = "android",
    issue_id: str = "",
    event_timestamp: str = "",
    days: int = 7,
):
    """Tek crash olayı — stack, breadcrumbs, keys."""
    from backend.services import crashlytics_bq as cbq
    from backend.services.app_intel import APP_PRODUCTS

    plat = (platform or "").strip().lower()
    if plat not in ("ios", "android"):
        return JSONResponse({"ok": False, "error": "invalid_platform"}, status_code=400)
    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"ok": False, "error": "unknown_product"}, status_code=400)
    try:
        d = int(days)
    except (TypeError, ValueError):
        d = 7
    if d not in (1, 7, 14, 30, 90):
        d = 7
    iid = (issue_id or "").strip()
    ts = (event_timestamp or "").strip()
    if not iid or not ts:
        return JSONResponse({"ok": False, "error": "missing_params"}, status_code=400)

    meta = APP_PRODUCTS[pid]
    bundle = (meta.get("android_package") if plat == "android" else meta.get("ios_bundle_id")) or ""
    batch_ref = cbq._batch_table_ref(plat, bundle)
    data = cbq.query_issue_event_raw(plat, batch_ref, iid, ts, d)
    return JSONResponse(data)


@app.post("/api/app/crashlytics/issue-ai-summary")
async def api_crash_issue_ai_summary(request: Request):
    """Issue için AI özet."""
    from backend.services.crashlytics_detail import summarize_issue_tr
    from backend.services import crashlytics_bq as cbq

    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    product = str(body.get("product") or "doviz")
    platform = str(body.get("platform") or "android")
    issue_id = str(body.get("issue_id") or "")
    days = int(body.get("days") or 7)

    detail = cbq.get_issue_detail_for_product(product, platform, issue_id, days)
    if not detail.get("ok"):
        return JSONResponse({"ok": False, "error": detail.get("error", "detail_failed")}, status_code=400)

    s = detail.get("summary") or {}
    try:
        text = summarize_issue_tr(
            issue_title=s.get("issue_title") or "",
            error_type=s.get("error_type") or "",
            total_events=int(s.get("total_events") or 0),
            affected_users=int(s.get("affected_users") or 0),
            blame_frames=detail.get("blame_frames") or [],
            trend=detail.get("trend") or [],
            process_states=detail.get("process_states"),
        )
        return JSONResponse({"ok": True, "summary": text})
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)[:200]}, status_code=500)


@app.post("/api/app/crashlytics/refresh")
def api_crash_refresh(product: str = "doviz"):
    from backend.services import crashlytics_bq as cbq
    from backend.services.app_intel import APP_PRODUCTS
    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"ok": False, "error": "unknown_product"}, status_code=400)
    if not cbq.any_platform_ready():
        return JSONResponse({"ok": False, "error": "credential_missing"}, status_code=400)
    jid = cbq.run_daily_refresh(pid)
    clear_crash_fetch_filter_cache(pid)
    if jid == "already_running":
        return JSONResponse({"ok": True, "already_running": True})
    return JSONResponse({"ok": True, "job_id": jid})


@app.post("/app/intel/refresh")
def app_intel_manual_refresh():
    try:
        if APP_INTEL_REFRESH_LOCK.locked():
            return JSONResponse(
                {
                    "ok": True,
                    "started": False,
                    "message": "App mağaza yenilemesi zaten çalışıyor. Birkaç dakika sonra veriler güncellenecek.",
                }
            )

        def _run_in_background() -> None:
            try:
                _run_app_intel_digest_job(
                    trigger_source="manual",
                    action_label="Manuel App mağaza yenilemesi",
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception("Manual app intel refresh background worker failed.")

        threading.Thread(target=_run_in_background, daemon=True).start()
        return JSONResponse(
            {
                "ok": True,
                "started": True,
                "message": "Manuel yenileme başlatıldı. İşlem arka planda devam ediyor; kartlar kısa süre içinde güncellenecek.",
            }
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Manual app intel refresh failed: %s", exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@app.post("/app/intel/rank-refresh")
def app_intel_rank_refresh():
    """Sadece kategori sırasını günceller (hızlı, yorum verisi çekmez)."""
    try:
        from backend.services.app_intel import refresh_category_ranks
        results = refresh_category_ranks()
        return JSONResponse({"ok": True, "results": results})
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Manual rank refresh failed: %s", exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)


@app.get("/api/app/aso")
def api_app_aso(
    product: str = "doviz",
    period: int = 30,
    compare_product: str | None = None,
    compare_label: str | None = None,
    compare_android_package: str | None = None,
    compare_ios_app_id: str | None = None,
):
    from backend.services.aso_intel import aso_json_safe, build_aso_payload
    from backend.services.app_intel import APP_PRODUCTS

    pid = (product or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return JSONResponse({"error": "unknown_product"}, status_code=400)
    try:
        p = int(period)
    except (TypeError, ValueError):
        p = 30
    if p not in (0, 7, 30, 90, 180, 365, 730):
        p = 30
    payload = build_aso_payload(
        pid,
        p,
        compare_product=compare_product,
        compare_label=compare_label,
        compare_android_package=compare_android_package,
        compare_ios_app_id=compare_ios_app_id,
    )
    return JSONResponse(aso_json_safe(payload))


@app.get("/api/app/aso/benchmark")
def api_app_aso_benchmark(
    period: int = 30,
    android_packages: str | None = None,
    ios_app_ids: str | None = None,
    labels: str | None = None,
):
    from backend.services.aso_intel import aso_json_safe, build_competitor_pair_payload

    try:
        p = int(period)
    except (TypeError, ValueError):
        p = 30
    if p not in (0, 7, 30, 90, 180, 365, 730):
        p = 30
    payload = build_competitor_pair_payload(
        period_days=p,
        android_packages=android_packages,
        ios_app_ids=ios_app_ids,
        labels=labels,
    )
    if payload.get("error"):
        return JSONResponse(payload, status_code=400)
    return JSONResponse(aso_json_safe(payload))


@app.get("/ga4/site-list")
def ga4_site_list(request: Request):
    """GA4 site listesi: varsayılan lazy mode ile iskelet kartlar döner, her kart HTMX ile ayrı yüklenir."""
    mode = str(request.query_params.get("mode") or "lazy").strip().lower()
    with SessionLocal() as db:
        if mode == "eager":
            return templates.TemplateResponse(
                request,
                "partials/ga4_site_cards.html",
                context={"request": request, "lazy_mode": False, "ga4_sites": _ga4_sites_payload(db)},
            )
        external_site_ids = _external_site_ids(db)
        sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_site_ids]
        sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        lazy_site_ids = [(s.id, s.display_name, s.domain) for s in sites]
    return templates.TemplateResponse(
        request,
        "partials/ga4_site_cards.html",
        context={"request": request, "lazy_mode": True, "lazy_site_ids": lazy_site_ids},
    )


@app.get("/ga4/site/{site_id}", response_class=HTMLResponse)
def ga4_single_site_card(request: Request, site_id: int):
    """HTMX lazy loading ile tek GA4 site kartını tam veriyle render eder."""
    from backend.services.analytics_compare import apply_ga4_period_compare, parse_compare_options

    q = request.query_params
    compare_enabled = str(q.get("compare") or "").lower() in ("1", "true", "yes", "on")
    compare_opts = parse_compare_options(
        enabled=compare_enabled,
        mode=q.get("compare_mode"),
        custom_start=q.get("compare_start"),
        custom_end=q.get("compare_end"),
    )
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("", status_code=404)
        if _is_external_site(db, site.id):
            return HTMLResponse("", status_code=404)
        try:
            external_site_ids = _external_site_ids(db)
            from sqlalchemy import func as sqlfunc
            site_count = db.query(sqlfunc.count(Site.id)).filter(Site.id.notin_(external_site_ids)).scalar() or 1

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
                        "1":  _ga4_profile_payload_for_period(db, site_id=site.id, profile=profile, period_days=1,  latest=latest, prop_id=prop_id),
                        "7":  _ga4_profile_payload_for_period(db, site_id=site.id, profile=profile, period_days=7,  latest=latest, prop_id=prop_id),
                        "30": _ga4_profile_payload_for_period(db, site_id=site.id, profile=profile, period_days=30, latest=latest, prop_id=prop_id),
                        "60": _ga4_profile_payload_for_period(db, site_id=site.id, profile=profile, period_days=60, latest=latest, prop_id=prop_id),
                        "90": _ga4_profile_payload_for_period(db, site_id=site.id, profile=profile, period_days=90, latest=latest, prop_id=prop_id),
                        "12m": _ga4_profile_payload_for_period(
                            db,
                            site_id=site.id,
                            profile=profile,
                            period_days=int(settings.ga4_trend_12m_period_days),
                            latest=latest,
                            prop_id=prop_id,
                        ),
                    },
                }
            if compare_opts.get("enabled") and compare_opts.get("mode") not in (
                None,
                "previous_period",
            ):
                from backend.services.ga4_compare_daily import apply_compare_daily_to_profiles

                apply_compare_daily_to_profiles(db, site.id, profiles, compare_opts)
            for profile in list(profiles.keys()):
                pdata = profiles[profile]
                daily_long = pdata.get("compare_daily_long")
                if not isinstance(daily_long, dict):
                    daily_long = (pdata["periods"].get("12m") or {}).get("daily_trend")
                for _pk in ("7", "30", "60", "90"):
                    profiles[profile]["periods"][_pk] = apply_ga4_period_compare(
                        profiles[profile]["periods"][_pk],
                        compare=compare_opts,
                        daily_long=daily_long if isinstance(daily_long, dict) else None,
                    )
                pdata.pop("compare_daily_long", None)
            sc_by_device = _load_ga4_sc_position_trends_by_device(db, site.id)
            _attach_ga4_sc_position_trends(profiles, sc_by_device)
            site_data = {
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
                "ga4": ga4_status,
                "profiles": profiles,
                "default_profile": next((k for k in ("web", "mweb", "android", "ios") if k in profiles), "web"),
            }
        except Exception as exc:
            logging.exception("ga4_single_site_card site_id=%s hata", site_id)
            import html as _html
            err_msg = _html.escape(f"{type(exc).__name__}: {exc}")
            return HTMLResponse(
                f'<section id="ga4-card-{site_id}" class="rounded-3xl border border-red-300 dark:border-red-700 '
                f'bg-red-50 dark:bg-red-900/30 p-5 text-sm text-red-700 dark:text-red-300">'
                f'<p class="font-semibold">GA4 kart yüklenemedi</p>'
                f'<p class="mt-1 text-xs">Site #{site_id} verisi hazırlanırken hata oluştu.</p>'
                f'<p class="mt-2 text-xs opacity-70 font-mono break-all">{err_msg}</p></section>',
                status_code=200,
            )
        response = templates.TemplateResponse(
            request,
            "partials/ga4_single_site_card.html",
            context={
                "request": request,
                "site": site_data,
                "site_count": site_count,
                "error_summary": _get_error_summary_for_card(db, site.id),
                "analytics_compare": compare_opts,
            },
        )
        response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        return response


def _ga4_refresh_all_set_progress(*, done: int, total: int, label: str = "") -> None:
    with _GA4_REFRESH_ALL_LOCK:
        job = _GA4_REFRESH_ALL_JOB
        if not job or job.get("status") != "running":
            return
        job["progress"] = {"done": int(done), "total": int(total), "label": str(label or "")}


def _refresh_one_site_for_ga4_batch(site_id: int) -> tuple[int, dict]:
    """Tek site GA4 toplama (kendi DB session'ı). 12 ay trend hata verse bile KPI kayıtları korunur."""
    from backend.collectors.ga4 import collect_ga4_scheduled_site_metrics

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return site_id, {"state": "failed", "error": "Site bulunamadı"}
        if _is_external_site(db, site.id):
            return site_id, {"state": "skipped", "error": "external"}
        conn = get_ga4_connection_status(db, site.id)
        if not conn.get("connected"):
            return site_id, {"state": "skipped", "error": "not_connected"}
        try:
            collect_ga4_scheduled_site_metrics(db, site)
            _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
            return site_id, {"state": "success"}
        except Exception as exc:  # noqa: BLE001
            try:
                db.rollback()
            except Exception:
                pass
            return site_id, {"state": "failed", "error": str(exc)}


def _compute_ga4_refresh_all_payload() -> dict:
    with SessionLocal() as db:
        external_site_ids = _external_site_ids(db)
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
        eligible: list[int] = []
        not_connected = 0
        for site in sites:
            if site.id in external_site_ids:
                continue
            conn = get_ga4_connection_status(db, site.id)
            if not conn.get("connected"):
                not_connected += 1
                continue
            eligible.append(site.id)
        n_active_non_external = len([s for s in sites if s.id not in external_site_ids])

    _ga4_refresh_all_set_progress(done=0, total=len(eligible), label="Başlatılıyor")

    site_results: dict[int, dict] = {}
    ga4_failures: list[tuple[str, str]] = []
    for index, sid in enumerate(eligible):
        sid_out, result = _refresh_one_site_for_ga4_batch(sid)
        site_results[sid_out] = result
        if str(result.get("state") or "").lower() == "failed":
            with SessionLocal() as db:
                site = db.query(Site).filter(Site.id == sid_out).first()
            ga4_failures.append(
                ((site.display_name or site.domain) if site else f"site #{sid_out}", str(result.get("error") or ""))
            )
        label = ""
        with SessionLocal() as db:
            site = db.query(Site).filter(Site.id == sid_out).first()
            if site:
                label = site.display_name or site.domain
        _ga4_refresh_all_set_progress(done=index + 1, total=len(eligible), label=label)

    refreshed_ok = sum(1 for r in site_results.values() if str(r.get("state") or "").lower() == "success")

    with SessionLocal() as db:
        try:
            send_ga4_weekly_digest_emails(
                db,
                trigger_source="manual",
                action_label="Tüm GA4 sitelerini yenile",
                collect_failures=ga4_failures,
            )
        except Exception:
            logging.warning("GA4 refresh-all: bildirim maili gönderilemedi, atlanıyor.")

    failed = len(ga4_failures)
    if refreshed_ok == 0 and failed == 0:
        if n_active_non_external == 0:
            title, detail = "Yenilenecek site yok", "Aktif, internal site bulunamadı."
        elif not_connected > 0:
            title, detail = (
                "API çağrılmadı",
                f"GA4 property tanımlı site yok: {not_connected} aktif site atlandı.",
            )
        else:
            title, detail = "API sonucu yok", "GA4 ölçümü dönmedi; loglara bakın."
    elif failed and refreshed_ok:
        title = "Kısmi yenileme"
        detail = f"{refreshed_ok} site güncellendi, {failed} sitede hata."
    elif failed and not refreshed_ok:
        title = "Yenileme başarısız"
        detail = f"{failed} site için GA4 verisi alınamadı."
    else:
        title = "Tüm siteler güncellendi"
        detail = f"{refreshed_ok} site GA4 verisiyle güncellendi."
        if not_connected:
            detail += f" {not_connected} site GA4 bağlı olmadığı için atlandı."
    return {
        "ok": refreshed_ok > 0 or failed == 0,
        "refreshed": refreshed_ok,
        "failed": failed,
        "not_connected": not_connected,
        "title": title,
        "detail": detail,
    }


def _ga4_refresh_all_job_finish(job_id: str) -> None:
    global _GA4_REFRESH_ALL_JOB
    try:
        payload = _compute_ga4_refresh_all_payload()
        with _GA4_REFRESH_ALL_LOCK:
            job = _GA4_REFRESH_ALL_JOB
            if job and job.get("id") == job_id:
                job["status"] = "done"
                job["result"] = payload
                job["progress"] = None
    except Exception as exc:
        LOGGER.exception("GA4 refresh-all background job failed")
        with _GA4_REFRESH_ALL_LOCK:
            job = _GA4_REFRESH_ALL_JOB
            if job and job.get("id") == job_id:
                job["status"] = "error"
                job["error"] = str(exc).strip() or "Arka plan işinde beklenmeyen hata; loglara bakın."
                job["progress"] = None


def _start_ga4_refresh_all_job(job_id: str) -> None:
    threading.Thread(
        target=_ga4_refresh_all_job_finish,
        args=(job_id,),
        daemon=True,
        name=f"ga4-refresh-all-{job_id[:8]}",
    ).start()


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
        from backend.collectors.ga4 import collect_ga4_scheduled_site_metrics

        try:
            collect_ga4_scheduled_site_metrics(db, site)
            _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
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
            context={"request": request, "lazy_mode": False, "ga4_sites": _ga4_sites_payload(db)},
        )


@app.post("/ga4/refresh-all")
def ga4_refresh_all(request: Request):
    """Toplu GA4 çekimi — uzun sürdüğü için hemen JSON + /status ile izlenir."""
    global _GA4_REFRESH_ALL_JOB
    if not ga4_is_configured():
        return JSONResponse(
            {
                "ok": False,
                "detail": "GA4 service account ayarlı değil (.env: GA4_SERVICE_ACCOUNT_FILE / JSON).",
            },
            status_code=503,
            headers=_GA4_JSON_NO_CACHE_HEADERS,
        )
    with _GA4_REFRESH_ALL_LOCK:
        cur = _GA4_REFRESH_ALL_JOB
        if cur and cur.get("status") == "running":
            started = float(cur.get("started") or 0.0)
            age = time.time() - started if started > 0 else 0.0
            if age < _GA4_REFRESH_ALL_STALE_SECONDS:
                return JSONResponse(
                    {
                        "ok": False,
                        "detail": "Başka bir toplu GA4 yenilemesi hâlâ çalışıyor; bitene kadar bekleyin.",
                    },
                    status_code=409,
                    headers=_GA4_JSON_NO_CACHE_HEADERS,
                )
            cur["status"] = "error"
            cur["error"] = "Önceki toplu yenileme zaman aşımına uğradı; yeni işlem başlatıldı."
        job_id = str(uuid4())
        _GA4_REFRESH_ALL_JOB = {
            "id": job_id,
            "status": "running",
            "result": None,
            "error": None,
            "started": time.time(),
            "progress": {"done": 0, "total": 0, "label": ""},
        }
    _start_ga4_refresh_all_job(job_id)
    return JSONResponse(
        {
            "ok": True,
            "async": True,
            "job_id": job_id,
            "title": "Toplu GA4 yenileme başladı",
            "detail": "İşlem arka planda sürüyor; birkaç dakika sürebilir — sayfayı kapatmayın.",
        },
        headers=_GA4_JSON_NO_CACHE_HEADERS,
    )


@app.get("/ga4/refresh-all/status/{job_id}")
def ga4_refresh_all_status(job_id: str):
    with _GA4_REFRESH_ALL_LOCK:
        job = _GA4_REFRESH_ALL_JOB
        if not job or job.get("id") != job_id:
            return JSONResponse(
                {
                    "ok": False,
                    "done": True,
                    "detail": "İşlem bulunamadı (sunucu yeniden başladıysa yeniden deneyin).",
                },
                status_code=404,
                headers=_GA4_JSON_NO_CACHE_HEADERS,
            )
        if job.get("status") == "running":
            body: dict = {"ok": True, "done": False, "job_id": job_id}
            progress = job.get("progress")
            if isinstance(progress, dict):
                body["progress"] = progress
            return JSONResponse(body, headers=_GA4_JSON_NO_CACHE_HEADERS)
        if job.get("status") == "error":
            return JSONResponse(
                {
                    "ok": False,
                    "done": True,
                    "detail": job.get("error") or "Arka plan işi başarısız.",
                },
                headers=_GA4_JSON_NO_CACHE_HEADERS,
            )
        payload = job.get("result") or {}
        return JSONResponse({"ok": True, "done": True, **payload}, headers=_GA4_JSON_NO_CACHE_HEADERS)


def _ga4_profile_to_sc_device(profile: str) -> str:
    p = (profile or "web").strip().lower()
    return "MOBILE" if p == "mweb" else "DESKTOP"


def _ga4_days_to_sc_page_scopes(days: int) -> tuple[str, str] | None:
    if days == 1:
        return "current_1d_pages", "previous_1d_pages"
    if days == 7:
        return "current_7d_pages", "previous_7d_pages"
    if days == 30:
        return "current_30d_pages", "previous_30d_pages"
    if days == 60:
        return "current_60d_pages", "previous_60d_pages"
    if days == 90:
        return "current_90d_pages", "previous_90d_pages"
    return None


def _sc_page_position_lookups_for_ga4(
    db: Session,
    *,
    site_id: int,
    days: int,
    profile: str,
    site_domain: str | None,
) -> tuple[dict[str, float], dict[str, float]]:
    """Search Console page kırılımı: URL → (position_diff, position_current)."""
    scopes_pair = _ga4_days_to_sc_page_scopes(days)
    if not scopes_pair:
        return {}, {}
    cur_scope, prev_scope = scopes_pair
    batch = get_latest_search_console_rows_batch(db, site_id=site_id, scopes=[cur_scope, prev_scope])
    device = _ga4_profile_to_sc_device(profile)
    cur = _filter_search_console_rows_by_device(batch.get(cur_scope) or [], device)
    prev = _filter_search_console_rows_by_device(batch.get(prev_scope) or [], device)
    if not cur and not prev:
        return {}, {}
    entities = _build_search_console_top_entities(cur, prev, label_key="query", limit=2500)
    diff_lookup: dict[str, float] = {}
    current_lookup: dict[str, float] = {}
    for ent in entities:
        diff = float(ent.get("position_diff") or 0.0)
        pos_cur = float(ent.get("position_current") or 0.0)
        label = str(ent.get("label") or "").strip()
        if not label:
            continue
        for key in _ga4_url_match_keys(label, site_domain):
            diff_lookup.setdefault(key, diff)
            current_lookup.setdefault(key, pos_cur)
    return diff_lookup, current_lookup


def _lookup_sc_page_metric(
    row: dict,
    lookup: dict[str, float],
    site_domain: str | None,
) -> float | None:
    if not lookup or not isinstance(row, dict):
        return None
    href = _ga4_row_page_href(row, site_domain)
    for key in _ga4_url_match_keys(href, site_domain):
        if key in lookup:
            return lookup[key]
    label = _ga4_row_page_label(row, site_domain)
    for key in _ga4_url_match_keys(label, site_domain):
        if key in lookup:
            return lookup[key]
    page = str(row.get("page") or "").strip()
    host = str(row.get("page_host") or "").strip()
    if host and page:
        for key in _ga4_url_match_keys(f"{host}{page if page.startswith('/') else '/' + page}", site_domain):
            if key in lookup:
                return lookup[key]
    return None


def _attach_sc_position_to_ga4_rows(
    rows: list,
    diff_lookup: dict[str, float],
    current_lookup: dict[str, float],
    site_domain: str | None,
) -> list:
    out: list = []
    for row in rows:
        if not isinstance(row, dict):
            out.append(row)
            continue
        item = dict(row)
        item["sc_position_diff"] = _lookup_sc_page_metric(item, diff_lookup, site_domain)
        item["sc_position_current"] = _lookup_sc_page_metric(item, current_lookup, site_domain)
        out.append(item)
    return out


@app.get("/ga4/pages/{site_id}")
def ga4_pages_partial(request: Request, site_id: int):
    profile = (request.query_params.get("profile") or "").strip().lower()
    raw_days = (request.query_params.get("days") or "").strip()
    news_only = (request.query_params.get("news") or "").strip().lower() in ("1", "true", "yes")
    try:
        days = int(raw_days) if raw_days else 30
    except ValueError:
        days = 30

    # Haberler tablosu: yalnızca son 1 veya 7 gün (diğer değerler 7'ye düşer).
    if news_only:
        if days not in (1, 7):
            days = 7

    # Haberler: GA4 çağrısından önce DB oturumu kapanır (eski SQLite/gRPC etkileşimi notu).
    if news_only:
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
            site_domain = site.domain
            profile_candidates: list[tuple[str, str]] = []
            for pf in ("web", "mweb"):
                prop = str(properties.get(pf) or "").strip()
                if prop:
                    profile_candidates.append((pf, prop))
            if not profile_candidates:
                profile_candidates.append((profile, property_id))

        from backend.collectors.ga4 import fetch_ga4_news_landing_pages_total

        def _news_merge_key(row: dict) -> str:
            raw = str(row.get("page") or row.get("page_url") or "").strip()
            m = re.search(r"/(\d+)(?:/amp)?(?:[/?#].*)?$", raw)
            if m:
                return f"id:{m.group(1)}"
            return f"url:{raw.lower()}"

        def _fetch_news_enriched(pair: tuple[str, str]) -> tuple[str, list]:
            pf, prop = pair
            raw_rows = fetch_ga4_news_landing_pages_total(property_id=prop, days=days, limit=250)
            return pf, _enrich_ga4_page_rows(raw_rows, keep_news_articles=True)

        try:
            merged: dict[str, dict] = {}
            # Sıralı çekim: gRPC istemcisi + ortam farklarında paralel çağrılar takılma/500 üretebiliyor.
            profile_rows = [_fetch_news_enriched(p) for p in profile_candidates]

            for pf, rows_pf in profile_rows:
                for row in rows_pf:
                    key = _news_merge_key(row)
                    if not key:
                        continue
                    bucket = merged.setdefault(
                        key,
                        {
                            "page": row.get("page", ""),
                            "page_host": row.get("page_host", ""),
                            "page_url": row.get("page_url", ""),
                            "web_views": 0.0,
                            "mweb_views": 0.0,
                            "total_views": 0.0,
                        },
                    )
                    if "/amp" in str(bucket.get("page") or "").lower() and "/amp" not in str(row.get("page") or "").lower():
                        bucket["page"] = row.get("page", "")
                        bucket["page_host"] = row.get("page_host", "")
                        bucket["page_url"] = row.get("page_url", "")
                    v = float(row.get("views") or 0.0)
                    if pf == "mweb":
                        bucket["mweb_views"] += v
                    else:
                        bucket["web_views"] += v
                    bucket["total_views"] = float(bucket["web_views"]) + float(bucket["mweb_views"])

            rows = sorted(merged.values(), key=lambda item: float(item.get("total_views") or 0.0), reverse=True)[:30]
            for item in rows:
                item.setdefault("web_views", 0.0)
                item.setdefault("mweb_views", 0.0)
                item.setdefault("total_views", float(item.get("web_views") or 0.0) + float(item.get("mweb_views") or 0.0))
        except Exception as exc:  # noqa: BLE001
            logging.exception("GA4 haber tablosu (site_id=%s) başarısız", site_id)
            return HTMLResponse(f"GA4 sayfa verisi çekilemedi: {exc}", status_code=500)

        try:
            news_resp = templates.TemplateResponse(
                request,
                "partials/ga4_news_pages_table.html",
                context={
                    "request": request,
                    "rows": rows,
                    "days": days,
                    "profile": profile,
                    "property_id": property_id,
                    "site_domain": site_domain,
                },
            )
            news_resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
            return news_resp
        except Exception as exc:  # noqa: BLE001
            logging.exception("GA4 haber şablonu (site_id=%s) başarısız", site_id)
            return HTMLResponse(f"GA4 haber tablosu oluşturulamadı: {exc}", status_code=500)

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

        try:
            from backend.collectors.ga4 import fetch_ga4_landing_pages

            api_limit = 50
            if days == 1:
                rows = fetch_ga4_landing_pages(
                    property_id=property_id,
                    days=1,
                    limit=api_limit,
                    exclude_news=True,
                    same_weekday_day=True,
                )
            else:
                snap = get_latest_ga4_report_snapshot(db, site_id=site.id, profile=profile, period_days=days)
                snap_pages = ((snap or {}).get("payload") or {}).get("pages_no_news") or []
                if snap_pages:
                    rows = snap_pages
                else:
                    rows = fetch_ga4_landing_pages(property_id=property_id, days=days, limit=api_limit, exclude_news=True)
            rows = _enrich_ga4_page_rows(rows, keep_news_articles=False)
            sc_diff_lookup, sc_current_lookup = _sc_page_position_lookups_for_ga4(
                db,
                site_id=site.id,
                days=days,
                profile=profile,
                site_domain=site.domain,
            )
            rows = _attach_sc_position_to_ga4_rows(rows, sc_diff_lookup, sc_current_lookup, site.domain)
        except Exception as exc:  # noqa: BLE001
            return HTMLResponse(f"GA4 sayfa verisi çekilemedi: {exc}", status_code=500)

        pages_resp = templates.TemplateResponse(
            request,
            "partials/ga4_pages_table.html",
            context={
                "request": request,
                "rows": rows,
                "days": days,
                "profile": profile,
                "property_id": property_id,
                "site_domain": site.domain,
                "table_heading": "Pages (excl. news)",
                "sc_position_period_supported": _ga4_days_to_sc_page_scopes(days) is not None,
            },
        )
        pages_resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        return pages_resp


@app.get("/ga4/app-detail/{site_id}")
def ga4_app_detail_partial(request: Request, site_id: int):
    """iOS / Android için ekran veya etkinlik kırılımı tablosu (HTMX partial)."""
    profile = (request.query_params.get("profile") or "android").strip().lower()
    kind = (request.query_params.get("kind") or "screens").strip().lower()  # screens | events
    raw_days = (request.query_params.get("days") or "30").strip()
    try:
        days = max(1, int(raw_days))
    except ValueError:
        days = 30

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        ga4_status = get_ga4_connection_status(db, site.id)
        properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}
        property_id = str(properties.get(profile) or "").strip()
        if not property_id:
            return HTMLResponse("Bu profil için GA4 property tanımlı değil.", status_code=422)

    try:
        from backend.collectors.ga4 import fetch_ga4_app_screens, fetch_ga4_app_events
        if kind == "events":
            rows = fetch_ga4_app_events(property_id=property_id, days=days, limit=50)
        else:
            rows = fetch_ga4_app_screens(property_id=property_id, days=days, limit=50)
    except Exception as exc:
        LOGGER.exception("GA4 app detail hatası [site=%s, profile=%s, kind=%s]", site_id, profile, kind)
        label = "Eventler" if kind == "events" else "Ekranlar"
        return HTMLResponse(
            f'<div class="rounded-2xl border border-slate-200 dark:border-slate-700 px-4 py-6 text-sm text-slate-500 dark:text-slate-400">'
            f'{label} verisi çekilemedi: {__import__("html").escape(str(exc))}'
            f'</div>',
            status_code=200,
        )

    resp = templates.TemplateResponse(
        request,
        "partials/ga4_app_table.html",
        context={
            "request": request,
            "rows": rows,
            "kind": kind,
            "days": days,
            "profile": profile,
        },
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
    return resp


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

        src_resp = templates.TemplateResponse(
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
        src_resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        return src_resp


# ── GA4 Realtime ──────────────────────────────────────────────────────────────

@app.get("/realtime")
def realtime_page(request: Request):
    """Bağımsız Realtime sayfası — her site için profil bazlı kutular."""
    with SessionLocal() as db:
        from backend.services.ga4_auth import GA4_CREDENTIAL_TYPE, load_ga4_properties, ga4_is_configured

        external_site_ids = _external_site_ids(db)
        sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_site_ids]
        sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))

        # N+1 yerine tek sorguda tüm GA4 credential'ları yükle
        ga4_creds = (
            db.query(SiteCredential)
            .filter(SiteCredential.credential_type == GA4_CREDENTIAL_TYPE)
            .order_by(SiteCredential.site_id, SiteCredential.id.desc())
            .all()
        )
        creds_by_site: dict[int, SiteCredential] = {}
        for cred in ga4_creds:
            creds_by_site.setdefault(cred.site_id, cred)

        is_ga4_ok = ga4_is_configured()
        site_list = []
        for site in sites:
            cred = creds_by_site.get(site.id)
            if cred is None or not is_ga4_ok:
                continue
            props = load_ga4_properties(cred)
            profiles = [p for p in ("web", "mweb", "ios", "android") if str(props.get(p, "")).strip()]
            if not profiles:
                continue
            site_list.append({
                "id": site.id,
                "domain": site.domain,
                "display_name": site.display_name,
                "profiles": profiles,
            })
    return templates.TemplateResponse(
        request,
        "realtime.html",
        context={
            "request": request,
            "sites": site_list,
            "window_minutes": settings.ga4_realtime_window_minutes,
            "interval_minutes": settings.ga4_realtime_interval_minutes,
            "ui_poll_seconds": settings.ga4_realtime_ui_poll_seconds,
        },
    )


@app.get("/inbox")
def inbox_page(request: Request):
    """Gmail (info@ / feedback@) gelen kutusu — OAuth + senkron + OpenAI özet/taslak."""
    return templates.TemplateResponse(
        request,
        "inbox.html",
        context={"request": request},
    )


@app.get("/notification")
def notification_page(request: Request):
    """Manuel yapıştırılan bildirim performansını birikimli analiz et."""
    return templates.TemplateResponse(
        request,
        "notification.html",
        context={"request": request},
    )


@app.get("/ad")
def ad_analytics_page(request: Request):
    """Reklam raporları — Excel/CSV yükleme, filtre ve grafikler."""
    return templates.TemplateResponse(
        request,
        "ad.html",
        context={"request": request},
        headers=_SC_HTML_NO_CACHE_HEADERS,
    )


@app.get("/ad/app-banner")
def ad_app_banner_ga4_page(request: Request):
    """GA4 mobil banner / first user campaign — geniş panel (Exploration android banner)."""
    return templates.TemplateResponse(
        request,
        "ad_app_banner.html",
        context={"request": request},
        headers=_SC_HTML_NO_CACHE_HEADERS,
    )


@app.get("/api/ga4/realtime/{site_id}")
def api_ga4_realtime(site_id: int, window: int | None = None, profile: str = "web"):
    """Tek site için GA4 Realtime karşılaştırma — frontend polling bu endpoint'i çağırır.

    GA4 Realtime token kotasını korumak için sonuç kısa süreli (TTL) cache'lenir;
    429/hata anında son başarılı CANLI sonuç (stale) döndürülür."""
    from backend.services.ga4_realtime import (
        REALTIME_TREND_HOURS_DEFAULT,
        fetch_realtime_profile_bundle,
        get_recent_alarms,
    )

    w = window if window is not None else settings.ga4_realtime_window_minutes
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)

        result = fetch_realtime_profile_bundle(
            db,
            site,
            profile=profile,
            window_minutes=w,
            trend_hours=REALTIME_TREND_HOURS_DEFAULT,
            skip_alarms=True,
        )
        result["recent_alarms"] = get_recent_alarms(db, site_id, limit=10)
    return JSONResponse(result)


@app.get("/api/ga4/realtime/{site_id}/trend")
def api_ga4_realtime_trend(
    site_id: int,
    profile: str = "web",
    limit: int | None = None,
    hours: float | None = None,
):
    """Snapshot trendi — mini grafik (yalnızca DB; GA4 kotası harcanmaz)."""
    from backend.services.ga4_realtime import (
        REALTIME_TREND_HOURS_DEFAULT,
        REALTIME_TREND_HOURS_MAX,
        REALTIME_TREND_LIMIT_DEFAULT,
        REALTIME_TREND_LIMIT_MAX,
        get_recent_snapshots,
    )

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        if hours is not None and hours > 0:
            h = min(float(hours), REALTIME_TREND_HOURS_MAX)
            snapshots = get_recent_snapshots(db, site_id, profile=profile, hours=h)
        else:
            req_limit = REALTIME_TREND_LIMIT_DEFAULT if limit is None else limit
            snapshots = get_recent_snapshots(
                db, site_id, profile=profile, limit=min(req_limit, REALTIME_TREND_LIMIT_MAX)
            )
    return JSONResponse({"site_id": site_id, "profile": profile, "trend": snapshots})


@app.get("/api/ga4/realtime/{site_id}/trend-combined")
def api_ga4_realtime_trend_combined(site_id: int, hours: float = 24, bucket_pages: bool = True):
    """Site geneli toplam trend — profillerin 15 dk dilim toplamı (yalnızca DB)."""
    from backend.services.ga4_realtime import (
        REALTIME_BUCKET_TOP_PAGES_N,
        REALTIME_TREND_HOURS_DEFAULT,
        get_combined_bucket_top_pages,
        get_combined_site_snapshots,
    )

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        h = hours if hours > 0 else REALTIME_TREND_HOURS_DEFAULT
        trend = get_combined_site_snapshots(db, site_id, hours=h)
        bucket_top_pages = (
            get_combined_bucket_top_pages(
                db, site_id, hours=h, top_n=REALTIME_BUCKET_TOP_PAGES_N
            )
            if bucket_pages
            else {}
        )
    return JSONResponse(
        {"site_id": site_id, "hours": hours, "trend": trend, "bucket_top_pages": bucket_top_pages}
    )


@app.get("/api/ga4/realtime/{site_id}/alarms")
def api_ga4_realtime_alarms(site_id: int, limit: int = 20):
    """Son N alarm kaydı."""
    from backend.services.ga4_realtime import get_recent_alarms

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        alarms = get_recent_alarms(db, site_id, limit=min(limit, 100))
    return JSONResponse({"site_id": site_id, "alarms": alarms})


@app.get("/api/ga4/realtime/{site_id}/404-spike")
def api_ga4_realtime_404_spike(site_id: int, profile: str = "web", window: int = 15):
    """Anlık 404 spike verisi — realtime sayfası için."""
    from backend.services.ga4_realtime import fetch_realtime_404_users, _evaluate_404_spike_severity
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.ga4_realtime_quota import Ga4RealtimeQuotaPausedError, note_realtime_quota_error
    from backend.services.realtime_cache import get_or_call
    from backend.config import settings

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if not site:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        property_id = properties.get(profile) or properties.get("web")
        if not property_id:
            return JSONResponse({"error": "no_property"}, status_code=404)

    w = min(window, 30)
    cache_ttl = max(60, int(settings.ga4_realtime_list_cache_seconds))

    def _fetch():
        return fetch_realtime_404_users(property_id, window_minutes=w)

    try:
        data = get_or_call(
            f"rt:404:{site_id}:{profile}:{w}",
            float(cache_ttl),
            _fetch,
            is_error=lambda r: False,
            last_good_ttl=float(settings.ga4_realtime_last_good_seconds),
        )
        if not isinstance(data, dict):
            data = {}
        warn = int(getattr(settings, "ga4_realtime_404_warning_threshold", 10))
        crit = int(getattr(settings, "ga4_realtime_404_critical_threshold", 25))
        total = data.get("total_404_users", 0)
        previous = int(data.get("previous_404_users") or 0)
        delta = int(data.get("delta_404_users") or (total - previous))
        severity = _evaluate_404_spike_severity(
            total, previous, warn_threshold=warn, crit_threshold=crit
        )
        return JSONResponse({
            "site_id": site_id,
            "profile": profile,
            "total_404_users": total,
            "previous_404_users": previous,
            "delta_404_users": delta,
            "pages": data.get("pages", []),
            "severity": severity,
            "warn_threshold": warn,
            "crit_threshold": crit,
        })
    except Ga4RealtimeQuotaPausedError as exc:
        return JSONResponse(
            {
                "site_id": site_id,
                "profile": profile,
                "quota_paused": True,
                "error": str(exc),
                "total_404_users": 0,
                "pages": [],
                "severity": None,
            }
        )
    except Exception as exc:
        if note_realtime_quota_error(property_id, exc):
            return JSONResponse(
                {
                    "site_id": site_id,
                    "profile": profile,
                    "quota_paused": True,
                    "error": str(exc)[:200],
                    "total_404_users": 0,
                    "pages": [],
                    "severity": None,
                }
            )
        LOGGER.warning("404 spike API hatası [%s]: %s", site_id, exc)
        return JSONResponse({"error": str(exc), "total_404_users": 0, "pages": [], "severity": None})


@app.get("/api/ga4/realtime/{site_id}/drivers")
def api_ga4_realtime_drivers(site_id: int, profile: str = "web"):
    """Realtime trafik değişim analizi — hangi sayfalar site genelindeki değişime katkıda bulunuyor."""
    from backend.services.ga4_realtime import fetch_traffic_drivers
    from backend.services.realtime_cache import get_or_call
    from backend.config import settings

    cache_ttl = max(90, int(settings.ga4_realtime_list_cache_seconds))

    def _produce():
        with SessionLocal() as db:
            return fetch_traffic_drivers(db, site_id, profile)

    try:
        result = get_or_call(
            f"rt:drivers:{site_id}:{profile}",
            float(cache_ttl),
            _produce,
            is_error=lambda r: bool(r.get("error")),
            last_good_ttl=float(settings.ga4_realtime_last_good_seconds),
        )
    except Exception as exc:
        LOGGER.debug("drivers cache miss error site_id=%s: %s", site_id, exc)
        with SessionLocal() as db:
            result = fetch_traffic_drivers(db, site_id, profile)

    if isinstance(result, dict) and result.get("error") == "site_not_found":
        return JSONResponse({"error": "site_not_found"}, status_code=404)
    return JSONResponse(result if isinstance(result, dict) else {})


@app.get("/api/ga4/realtime/{site_id}/insights")
def api_ga4_realtime_insights(site_id: int, profile: str = "web", limit: int = 48):
    """Trend'deki gerçek anomalileri tespit et (ardışık snapshot karşılaştırması)."""
    from backend.models import RealtimeSnapshot, RealtimePageSnapshot
    from sqlalchemy import desc as _desc_ins, func as _sqlfunc
    from zoneinfo import ZoneInfo
    import statistics
    import datetime as _dt

    tz = ZoneInfo(getattr(settings, "report_calendar_timezone", "Europe/Istanbul"))

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)

        rows = (
            db.query(RealtimeSnapshot)
            .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == profile)
            .order_by(_desc_ins(RealtimeSnapshot.collected_at))
            .limit(min(limit, 96))
            .all()
        )
        rows = list(reversed(rows))  # eskiden yeniye

        if len(rows) < 2:
            return JSONResponse({"insights": [], "has_data": False})

        # Ardışık snapshot farkları
        vals = [r.active_users_current or 0 for r in rows]
        diffs = []
        for i in range(1, len(rows)):
            delta = vals[i] - vals[i - 1]
            baseline_vals = vals[max(0, i - 6):i]
            baseline = sum(baseline_vals) / len(baseline_vals) if baseline_vals else vals[i - 1]
            diffs.append({"idx": i, "delta": delta, "baseline": baseline, "row": rows[i], "prev_row": rows[i - 1]})

        # Anomali eşiği: delta'ların standart sapması × 1.5  VEYA  baseline'ın %5'i
        # (hangisi küçükse) — yüksek trafikli sitelerde %25 çok büyük kalıyordu
        all_deltas = [abs(d["delta"]) for d in diffs]
        try:
            stdev = statistics.stdev(all_deltas) if len(all_deltas) >= 3 else 0
        except Exception:
            stdev = 0

        anomalies = []
        for d in diffs:
            b = max(d["baseline"], 10)
            # Stdev varsa: stdev × 1.5 (volatiliteye adapte)
            # Stdev yoksa (az veri): baseline × %3
            # Her halükarda minimum 20 kullanıcı
            if stdev > 0:
                dynamic_thresh = max(stdev * 1.5, 20)
            else:
                dynamic_thresh = max(b * 0.03, 20)
            if abs(d["delta"]) >= dynamic_thresh:
                anomalies.append(d)

        if not anomalies:
            return JSONResponse({"insights": [], "has_data": True})

        # Deduplication: aynı yöndeki ardışık anomalileri grupla, sadece zirvesini tut
        groups: list[list] = []
        for a in anomalies:
            if groups and a["row"].collected_at - groups[-1][-1]["row"].collected_at <= _dt.timedelta(minutes=25) \
                    and (a["delta"] > 0) == (groups[-1][-1]["delta"] > 0):
                groups[-1].append(a)
            else:
                groups.append([a])

        # Her gruptan en yüksek mutlak delta'yı al
        peak_anomalies = [max(g, key=lambda x: abs(x["delta"])) for g in groups]

        # Sayfa katkısı hesapla
        def _page_map(ts_filter, limit=20):
            distinct = (
                db.query(RealtimePageSnapshot.collected_at)
                .filter(RealtimePageSnapshot.site_id == site_id,
                        RealtimePageSnapshot.profile == profile,
                        ts_filter)
                .order_by(_desc_ins(RealtimePageSnapshot.collected_at))
                .distinct().limit(1).scalar()
            )
            if not distinct:
                return {}
            rows_p = db.query(RealtimePageSnapshot).filter(
                RealtimePageSnapshot.site_id == site_id,
                RealtimePageSnapshot.profile == profile,
                RealtimePageSnapshot.collected_at == distinct,
            ).all()
            return {r.page_path: r.active_users for r in rows_p}

        insights = []
        for a in peak_anomalies[-10:]:  # en güncel 10
            r = a["row"]
            prev_r = a["prev_row"]
            site_delta = a["delta"]

            ts_curr = r.collected_at
            ts_prev = prev_r.collected_at

            curr_map = _page_map(RealtimePageSnapshot.collected_at <= ts_curr)
            prev_map = _page_map(RealtimePageSnapshot.collected_at <= ts_prev)

            page_contribs = []
            for path, cu in curr_map.items():
                pu = prev_map.get(path, 0)
                pd = cu - pu
                if abs(pd) < 2:
                    continue
                page_contribs.append({
                    "page": path,
                    "delta": round(pd),
                    "current": round(cu),
                    "contribution_pct": round(pd / site_delta * 100) if site_delta else 0,
                })
            # Sadece site_delta ile AYNI yönde hareket eden sayfalar
            # (karşı yönde gidenler "neden düştü" sorusuna cevap değil)
            if site_delta < 0:
                same_dir = [p for p in page_contribs if p["delta"] < 0]
            else:
                same_dir = [p for p in page_contribs if p["delta"] > 0]
            # Aynı yönde page yoksa tüm listeyi göster (veri az olabilir)
            pool = same_dir if same_dir else page_contribs
            pool.sort(key=lambda x: x["delta"], reverse=site_delta > 0)

            start_local = ts_prev.replace(tzinfo=_dt.timezone.utc).astimezone(tz)
            end_local = ts_curr.replace(tzinfo=_dt.timezone.utc).astimezone(tz)

            insights.append({
                "period_start": start_local.strftime("%H:%M"),
                "period_end": end_local.strftime("%H:%M"),
                "direction": "drop" if site_delta < 0 else "spike",
                "site_delta": round(site_delta),
                "change_pct": round(site_delta / max(a["baseline"], 1) * 100, 1),
                "current_total": round(r.active_users_current or 0),
                "previous_total": round(prev_r.active_users_current or 0),
                "top_pages": pool[:3],
                "collected_at": ts_curr.isoformat(),
            })

        # Kronolojik sıra (en eski → en yeni)
        insights.sort(key=lambda x: x["collected_at"])
        return JSONResponse({"insights": insights, "has_data": True})


@app.get("/api/ga4/realtime/{site_id}/top-pages")
def api_ga4_realtime_top_pages(
    site_id: int,
    profile: str = "web",
    window: int = 30,
    limit: int = 10,
    type: str = "pages",
    range: str | None = None,
):
    """Realtime top sayfalar/linkler — sayfa bazlı aktif kullanıcı ve sayfa görüntüleme."""
    from backend.services.ga4_realtime import (
        aggregate_page_snapshots_over_window,
        fetch_realtime_top_pages_with_app_fallback,
        parse_realtime_list_range,
        REALTIME_LIST_RANGE_LABELS,
    )
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.realtime_cache import get_or_call

    sort_by = "screenPageViews" if type == "views" else "activeUsers"
    mode, list_minutes, range_key = parse_realtime_list_range(range, window=window)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        property_id = properties.get(profile) or properties.get("web")
        if not property_id:
            return JSONResponse({"error": "no_property", "message": f"{profile} profili tanımlı değil"}, status_code=404)

    cap = min(limit, 25)

    def _produce():
        if mode == "snapshots":
            with SessionLocal() as db:
                result = aggregate_page_snapshots_over_window(
                    db,
                    site_id=site_id,
                    profile=profile,
                    window_minutes=list_minutes,
                    limit=cap,
                    sort_by=sort_by,
                )
                result["site_id"] = site_id
                result["profile"] = profile
                result["type"] = type
                result["range"] = range_key
                return result

        # GA4 Realtime «Pages» ile aynı: önce 30 dk toplam (compare kapalı), gerekirse karşılaştırmalı dene.
        for compare in (False, True):
            try:
                result = fetch_realtime_top_pages_with_app_fallback(
                    property_id,
                    profile=profile,
                    window_minutes=min(list_minutes, 30),
                    limit=cap,
                    sort_by=sort_by,
                    compare_previous=compare,
                )
                pages = result.get("pages") or []
                if pages:
                    result["site_id"] = site_id
                    result["profile"] = profile
                    result["type"] = type
                    result["range"] = range_key
                    result["range_label"] = REALTIME_LIST_RANGE_LABELS.get(range_key, f"{list_minutes} dk")
                    result["data_source"] = "ga4"
                    return result
            except Exception as exc:
                LOGGER.warning(
                    "Top pages canlı API başarısız [compare=%s, site=%s, profile=%s]: %s",
                    compare, site_id, profile, exc,
                )
                if not compare:
                    break  # her iki deneme başarısız → DB fallback

        # 2) DB snapshot fallback
        from backend.models import RealtimePageSnapshot
        from sqlalchemy import desc as _desc

        with SessionLocal() as db:
            distinct_times = (
                db.query(RealtimePageSnapshot.collected_at)
                .filter(RealtimePageSnapshot.site_id == site_id, RealtimePageSnapshot.profile == profile)
                .order_by(_desc(RealtimePageSnapshot.collected_at))
                .distinct()
                .limit(2)
                .all()
            )
            if not distinct_times:
                return {"error": "no_snapshot", "pages": [], "site_id": site_id, "profile": profile}

            curr_time = distinct_times[0][0]
            prev_time = distinct_times[1][0] if len(distinct_times) > 1 else None

            curr_rows = (
                db.query(RealtimePageSnapshot)
                .filter(RealtimePageSnapshot.site_id == site_id,
                        RealtimePageSnapshot.profile == profile,
                        RealtimePageSnapshot.collected_at == curr_time)
                .order_by(RealtimePageSnapshot.rank)
                .all()
            )
            prev_map: dict[str, RealtimePageSnapshot] = {}
            if prev_time:
                prev_map = {r.page_path: r for r in db.query(RealtimePageSnapshot)
                            .filter(RealtimePageSnapshot.site_id == site_id,
                                    RealtimePageSnapshot.profile == profile,
                                    RealtimePageSnapshot.collected_at == prev_time)
                            .all()}

            pages = []
            for row in curr_rows:
                prev = prev_map.get(row.page_path)
                path_str = str(row.page_path or "")
                # page_path URL path'iyse (/ ile başlıyorsa) page_paths listesi olarak döndür
                page_paths = [path_str] if path_str.startswith("/") else []
                pages.append({
                    "page":                   path_str,
                    "page_paths":             page_paths,
                    "activeUsers":            row.active_users,
                    "screenPageViews":        row.pageviews,
                    "activeUsers_previous":   prev.active_users if prev else None,
                    "screenPageViews_previous": prev.pageviews if prev else None,
                    "rank":                   row.rank,
                })

            pages.sort(key=lambda p: p.get(sort_by) or 0, reverse=True)
            return {
                "site_id": site_id,
                "profile": profile,
                "type": type,
                "pages": pages[:min(limit, 25)],
                "source": "db_snapshot",
                "fetched_at": curr_time.isoformat() if curr_time else None,
            }

    def _top_pages_is_error(r: dict) -> bool:
        if bool(r.get("error")) or r.get("source") == "db_snapshot":
            return True
        # App profillerinde boş canlı liste cache'lenmesin; son-iyi veya DB yedeğine düşsün.
        if (r.get("profile") or "").lower() in ("android", "ios") and r.get("data_source") == "ga4":
            return not (r.get("pages") or [])
        return False

    result = get_or_call(
        f"rt:pages:v2:{site_id}:{profile}:{range_key}:{type}:{cap}",
        settings.ga4_realtime_list_cache_seconds,
        _produce,
        # db_snapshot da "canlı değil" sayılır: canlı son-iyi varsa o tercih edilir,
        # ayrıca son-iyi (live) DB snapshot ile ezilmez.
        is_error=_top_pages_is_error,
        last_good_ttl=settings.ga4_realtime_last_good_seconds,
    )
    return JSONResponse(result)


def _parse_query_bool(value: object, *, default: bool = True) -> bool:
    raw = str(value).strip().lower() if value is not None else ""
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return default


@app.get("/api/ga4/realtime/{site_id}/wow-day-pages")
def api_ga4_realtime_wow_day_pages(
    site_id: int,
    profile: str = "web",
    limit: int = 12,
    exclude_news: str = "1",
    news_only: str = "0",
):
    """Geçen haftanın aynı günü (tam gün GA4) en çok görüntülenen sayfalar — web/mweb."""
    from backend.collectors.ga4 import (
        _aggregate_landing_rows_by_path,
        fetch_ga4_landing_pages,
        fetch_ga4_wow_day_news_pages,
        same_weekday_day_meta,
    )
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.realtime_cache import get_or_call
    from backend.config import settings

    profile_key = (profile or "web").strip().lower()
    if profile_key not in ("web", "mweb"):
        return JSONResponse(
            {"error": "unsupported_profile", "profile": profile_key, "pages": []},
            status_code=400,
        )

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        property_id = properties.get(profile_key) or properties.get("web")
        if not property_id:
            return JSONResponse(
                {"error": "no_property", "message": f"{profile_key} profili tanımlı değil"},
                status_code=404,
            )

    cap = max(1, min(int(limit), 25))
    news = _parse_query_bool(news_only, default=False)
    excl = _parse_query_bool(exclude_news, default=True) if not news else False
    mode_key = "news" if news else ("excl" if excl else "all")
    cache_ttl = max(300, int(settings.ga4_realtime_list_cache_seconds) * 4)

    def _produce():
        if news:
            rows = fetch_ga4_wow_day_news_pages(
                property_id=str(property_id),
                limit=max(cap, 40),
            )
        else:
            rows = fetch_ga4_landing_pages(
                property_id=str(property_id),
                days=1,
                limit=max(cap, 40),
                exclude_news=excl,
                news_only=False,
                same_weekday_day=True,
            )
            rows = _aggregate_landing_rows_by_path(rows)
        rows.sort(key=lambda r: float(r.get("prev_total") or 0), reverse=True)
        pages = []
        for row in rows[:cap]:
            prev_v = float(row.get("prev_total") or 0)
            last_v = float(row.get("last_total") or 0)
            if prev_v <= 0 and last_v <= 0:
                continue
            delta_pct = row.get("delta_pct")
            pages.append(
                {
                    "page": row.get("page") or "",
                    "page_title": row.get("page_title") or "",
                    "page_url": row.get("page_url") or "",
                    "page_host": row.get("page_host") or "",
                    "screenPageViews": prev_v,
                    "screenPageViews_current": last_v,
                    "delta": float(row.get("delta") or 0),
                    "delta_pct": delta_pct,
                }
            )
        meta = same_weekday_day_meta()
        return {
            "site_id": site_id,
            "profile": profile_key,
            "pages": pages,
            "meta": meta,
            "mode": mode_key,
            "exclude_news": excl,
            "news_only": news,
            "data_source": "ga4_daily",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    result = get_or_call(
        f"rt:wowpages:v2:{site_id}:{profile_key}:{mode_key}:{cap}",
        float(cache_ttl),
        _produce,
        is_error=lambda r: bool(r.get("error")),
        last_good_ttl=float(settings.ga4_realtime_last_good_seconds),
    )
    return JSONResponse(result)


@app.get("/api/ga4/realtime/{site_id}/top-news")
def api_ga4_realtime_top_news(
    site_id: int,
    profile: str = "web",
    window: int = 30,
    limit: int = 12,
    type: str = "pages",
    range: str | None = None,
):
    """Realtime haber sayfaları — web/mweb URL; ios/android ekran başlığı (unifiedScreenName)."""
    from backend.services.ga4_realtime import (
        aggregate_news_snapshots_over_window,
        fetch_realtime_top_news_pages,
        parse_realtime_list_range,
        REALTIME_LIST_RANGE_LABELS,
    )
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.realtime_cache import get_or_call

    if profile not in ("web", "mweb", "android", "ios"):
        return JSONResponse(
            {"site_id": site_id, "profile": profile, "pages": [], "unsupported_profile": True},
        )

    sort_by = "screenPageViews" if type == "views" else "activeUsers"
    mode, list_minutes, range_key = parse_realtime_list_range(range, window=window)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        property_id = properties.get(profile) or properties.get("web")
        if not property_id:
            return JSONResponse({"error": "no_property", "message": f"{profile} profili tanımlı değil"}, status_code=404)
        site_domain_str = (site.domain or "").strip()

    def _produce():
        if mode == "snapshots":
            with SessionLocal() as db:
                result = aggregate_news_snapshots_over_window(
                    db,
                    site_id=site_id,
                    profile=profile,
                    site_domain=site_domain_str,
                    window_minutes=list_minutes,
                    limit=min(limit, 25),
                    sort_by=sort_by,
                )
                result["site_id"] = site_id
                result["profile"] = profile
                result["type"] = type
                result["range"] = range_key
                return result

        # 1) Canlı GA4
        try:
            result = fetch_realtime_top_news_pages(
                property_id,
                site_domain=site_domain_str,
                profile=profile,
                window_minutes=min(list_minutes, 30),
                limit=min(limit, 25),
                sort_by=sort_by,
            )
            result["site_id"] = site_id
            result["profile"] = profile
            result["type"] = type
            result["range"] = range_key
            result["range_label"] = REALTIME_LIST_RANGE_LABELS.get(range_key, f"{list_minutes} dk")
            result["data_source"] = "ga4"
            return result
        except Exception as exc:
            LOGGER.warning("Top news canlı API başarısız, DB snapshot'a düşülüyor [site=%s, profile=%s]: %s", site_id, profile, exc)

        # 2) DB snapshot fallback
        from backend.models import RealtimeNewsSnapshot
        from backend.services.ga4_realtime import _news_row_link
        from sqlalchemy import desc as _desc2

        with SessionLocal() as db:
            distinct_times = (
                db.query(RealtimeNewsSnapshot.collected_at)
                .filter(RealtimeNewsSnapshot.site_id == site_id, RealtimeNewsSnapshot.profile == profile)
                .order_by(_desc2(RealtimeNewsSnapshot.collected_at))
                .distinct()
                .limit(2)
                .all()
            )
            if not distinct_times:
                return {"error": "no_snapshot", "pages": [], "site_id": site_id, "profile": profile}

            curr_time = distinct_times[0][0]
            prev_time = distinct_times[1][0] if len(distinct_times) > 1 else None

            curr_rows = (
                db.query(RealtimeNewsSnapshot)
                .filter(RealtimeNewsSnapshot.site_id == site_id,
                        RealtimeNewsSnapshot.profile == profile,
                        RealtimeNewsSnapshot.collected_at == curr_time)
                .order_by(RealtimeNewsSnapshot.rank)
                .all()
            )
            prev_map_news: dict[str, RealtimeNewsSnapshot] = {}
            if prev_time:
                prev_map_news = {r.screen_title: r for r in db.query(RealtimeNewsSnapshot)
                                 .filter(RealtimeNewsSnapshot.site_id == site_id,
                                         RealtimeNewsSnapshot.profile == profile,
                                         RealtimeNewsSnapshot.collected_at == prev_time)
                                 .all()}

            pages = []
            from backend.services.realtime_news_paths import (
                is_realtime_news_path,
                realtime_news_page_link,
                unified_screen_news_article_title,
            )

            for row in curr_rows:
                title = (row.screen_title or "").strip()
                if title.startswith("/"):
                    if not is_realtime_news_path(title, site_domain=site_domain_str):
                        continue
                elif not unified_screen_news_article_title(title, site_domain=site_domain_str):
                    continue
                prev = prev_map_news.get(row.screen_title)
                pages.append({
                    "page": row.screen_title,
                    "page_path": row.screen_title if row.screen_title.startswith("/") else "",
                    "activeUsers": row.active_users,
                    "screenPageViews": row.pageviews,
                    "activeUsers_previous": prev.active_users if prev else None,
                    "screenPageViews_previous": prev.pageviews if prev else None,
                    "link_url": realtime_news_page_link(row.screen_title, site_domain=site_domain_str)
                    or _news_row_link(site_domain_str, row.screen_title),
                    "rank": row.rank,
                })

            pages.sort(key=lambda p: p.get(sort_by) or 0, reverse=True)
            return {
                "site_id": site_id,
                "profile": profile,
                "type": type,
                "pages": pages[:min(limit, 25)],
                "source": "db_snapshot",
                "fetched_at": curr_time.isoformat() if curr_time else None,
            }

    result = get_or_call(
        f"rt:news:{site_id}:{profile}:{range_key}:{type}",
        settings.ga4_realtime_list_cache_seconds,
        _produce,
        is_error=lambda r: bool(r.get("error")) or r.get("source") == "db_snapshot",
        last_good_ttl=settings.ga4_realtime_last_good_seconds,
    )
    return JSONResponse(result)


@app.get("/api/ga4/realtime/{site_id}/top-events")
def api_ga4_realtime_top_events(
    site_id: int,
    profile: str = "android",
    window: int = 30,
    limit: int = 200,
):
    """Realtime etkinlik adı + eventCount — Android/iOS kartı."""
    from backend.services.ga4_realtime import fetch_realtime_top_events
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.realtime_cache import get_or_call

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return JSONResponse({"error": "site_not_found"}, status_code=404)
        record = get_ga4_credentials_record(db, site.id)
        properties = load_ga4_properties(record)
        property_id = properties.get(profile) or properties.get("web")
        if not property_id:
            return JSONResponse({"error": "no_property", "message": f"{profile} profili tanımlı değil"}, status_code=404)

    def _produce():
        try:
            result = fetch_realtime_top_events(
                property_id,
                window_minutes=min(window, 30),
                limit=min(limit, 250),
            )
            result["site_id"] = site_id
            result["profile"] = profile
            return result
        except Exception as exc:
            LOGGER.exception("Top events hatası [site=%s, profile=%s]", site_id, profile)
            # GA4 Standard property'lerde belirli pencere/dim kombinasyonları desteklenmez;
            # frontend 500 yerine boş veriyi sorunsuz işleyebilsin.
            return {
                "site_id": site_id,
                "profile": profile,
                "events": [],
                "window_minutes": min(window, 30),
                "total_event_count": 0,
                "error": "api_error",
                "message": str(exc),
            }

    result = get_or_call(
        f"rt:events:{site_id}:{profile}:{window}",
        settings.ga4_realtime_list_cache_seconds,
        _produce,
        is_error=lambda r: bool(r.get("error")),
        last_good_ttl=settings.ga4_realtime_last_good_seconds,
    )
    return JSONResponse(result)


@app.post("/api/ga4/realtime/check-all")
def api_ga4_realtime_check_all(request: Request):
    """Tüm aktif siteleri kontrol et — manuel tetik veya scheduler'dan çağrılır."""
    from backend.services.ga4_realtime import run_all_sites_realtime_check

    with SessionLocal() as db:
        results = run_all_sites_realtime_check(db)
    alarm_total = sum(r.get("alarm_count", 0) for r in results if isinstance(r, dict))
    return JSONResponse({
        "checked": len(results),
        "alarm_total": alarm_total,
        "results": results,
    })


# ── /settings (devam) ────────────────────────────────────────────────────────

@app.get("/settings/alert-thresholds")
def settings_alert_thresholds(request: Request):
    # HTMX ile alert threshold tablosunu yeniler.
    with SessionLocal() as db:
        alert_rules = get_alert_rules(db)
    return templates.TemplateResponse(request, "partials/alert_thresholds.html", context={"request": request, "alert_rules": alert_rules})


@app.get("/backlinks")
def backlinks_page(request: Request):
    with SessionLocal() as db:
        external_ids = _external_site_ids(db)
        sites = [
            s
            for s in db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.display_name.asc()).all()
            if s.id not in external_ids
        ]
    default_site_id: int | None = None
    if sites:
        rows = [
            {"id": s.id, "domain": s.domain, "display_name": s.display_name or s.domain}
            for s in sites
        ]
        default_site_id = _default_internal_site_id(rows)
        sites = sorted(sites, key=lambda s: (0 if s.id == default_site_id else 1, (s.display_name or s.domain or "").lower()))
    return templates.TemplateResponse(
        request,
        "backlinks.html",
        context={
            "request": request,
            "sites": sites,
            "default_site_id": default_site_id,
            "site_name": "Backlinks",
        },
    )


def _search_console_page_context(db: Session) -> dict[str, Any]:
    return {
        "site_name": "Search Console",
        "sites": get_sidebar_sites(),
        "oauth_ready": oauth_is_configured(),
        "oauth_redirect_uri": settings.google_oauth_redirect_uri,
        "site_list_mode": "lazy",
        "sc_extra_views": sc_extra_views_for_nav(),
        "default_site_id": _default_active_site_id(db),
    }


@app.get("/search-console")
def search_console_page(request: Request):
    with SessionLocal() as db:
        ctx = _search_console_page_context(db)
    return templates.TemplateResponse(
        request,
        "search_console.html",
        context={"request": request, **ctx},
        headers=_SC_HTML_NO_CACHE_HEADERS,
    )


@app.get("/search-console/site-list")
def search_console_site_list(request: Request):
    """Site listesini anlık render eder; her kart lazy HTMX ile ayrı yüklenir."""
    mode = str(request.query_params.get("mode") or "lazy").strip().lower()
    view = str(request.query_params.get("view") or "performance").strip() or "performance"
    if view not in SC_VIEW_SPECS:
        return HTMLResponse("Gecersiz gorunum", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)
    view_spec = SC_VIEW_SPECS[view]
    with SessionLocal() as db:
        external_ids = _external_site_ids(db)
        sites = [s for s in db.query(Site).order_by(Site.created_at.desc()).all() if s.id not in external_ids]
        sites.sort(key=lambda s: _preferred_site_order_key(s.domain, s.display_name))
        if view_spec.get("kind") != "performance":
            lazy_site_ids = [(s.id, s.display_name) for s in sites]
            return templates.TemplateResponse(
                request,
                "partials/sc_extras_site_list.html",
                context={
                    "request": request,
                    "lazy_mode": True,
                    "lazy_site_ids": lazy_site_ids,
                    "oauth_ready": oauth_is_configured(),
                    "sc_view": view,
                    "sc_view_item": view_spec,
                },
                headers=_SC_HTML_NO_CACHE_HEADERS,
            )
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
                headers=_SC_HTML_NO_CACHE_HEADERS,
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
        headers=_SC_HTML_NO_CACHE_HEADERS,
    )


@app.get("/search-console/extras/{view_slug}/site/{site_id}", response_class=HTMLResponse)
def search_console_extras_site_card(request: Request, view_slug: str, site_id: int):
    """Ek SC gorunumleri icin tek site karti (canli API)."""
    if view_slug not in SC_VIEW_SPECS:
        return HTMLResponse("", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)
    spec = SC_VIEW_SPECS[view_slug]
    if spec.get("kind") == "performance":
        return HTMLResponse("", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)
    try:
        with SessionLocal() as db:
            site = db.query(Site).filter(Site.id == site_id).first()
            if site is None:
                return HTMLResponse("", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)
            site_id_val = site.id
            display_name = site.display_name
            domain = site.domain
            connection = get_search_console_connection_status(db, site.id)
            report: dict[str, Any] | None = None
            error: str | None = None
            kind = str(spec.get("kind") or "")
            if kind == "analytics":
                try:
                    report = fetch_sc_analytics_report(
                        db,
                        site.id,
                        str(spec.get("report_key") or ""),
                    )
                except Exception as exc:
                    logging.exception("sc_extras analytics site_id=%s view=%s", site_id, view_slug)
                    from backend.services.search_console_auth import (
                        SearchConsoleOAuthError,
                        format_search_console_error_for_ui,
                        record_search_console_oauth_revoked,
                    )

                    if isinstance(exc, SearchConsoleOAuthError):
                        record_search_console_oauth_revoked(db, site.id, str(exc))
                    error = format_search_console_error_for_ui(str(exc))[:400]
            elif kind == "sitemaps":
                try:
                    report = fetch_sc_sitemaps(db, site.id)
                except Exception as exc:
                    logging.exception("sc_extras sitemaps site_id=%s", site_id)
                    error = str(exc)[:400]
            elif kind == "inspection":
                from backend.collectors.url_inspection import _normalize_url

                report = {
                    "default_url": _normalize_url(site.domain),
                }
            if not sc_extra_card_should_render(
                spec,
                connection=connection,
                report=report,
                error=error,
            ):
                return HTMLResponse("", headers=_SC_HTML_OMIT_HEADERS)
        return templates.TemplateResponse(
            request,
            "partials/sc_extras_site_card.html",
            context={
                "request": request,
                "site_id": site_id_val,
                "display_name": display_name,
                "domain": domain,
                "connection": connection,
                "oauth_ready": oauth_is_configured(),
                "sc_view": view_slug,
                "sc_view_item": spec,
                "report": report,
                "error": error,
            },
            headers=_SC_HTML_NO_CACHE_HEADERS,
        )
    except Exception as exc:
        logging.exception("sc_extras_site_card fatal site_id=%s view=%s", site_id, view_slug)
        return templates.TemplateResponse(
            request,
            "partials/sc_extras_site_card.html",
            context={
                "request": request,
                "site_id": site_id,
                "display_name": f"Site #{site_id}",
                "domain": "",
                "connection": {"connected": False, "label": "—"},
                "oauth_ready": oauth_is_configured(),
                "sc_view": view_slug,
                "sc_view_item": spec,
                "report": None,
                "error": str(exc)[:400],
            },
            headers=_SC_HTML_NO_CACHE_HEADERS,
        )


@app.post("/search-console/extras/url-inspection/site/{site_id}", response_class=HTMLResponse)
def search_console_inspect_url_post(
    request: Request,
    site_id: int,
    inspection_url: str = Form(""),
):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)
        try:
            result = inspect_sc_url(db, site.id, inspection_url)
            summary = result.get("summary") or {}
            error = None
        except Exception as exc:
            logging.exception("sc_inspect site_id=%s", site_id)
            summary = {}
            error = str(exc)[:400]
    return templates.TemplateResponse(
        request,
        "partials/sc_inspection_result.html",
        context={
            "request": request,
            "site_id": site.id,
            "summary": summary,
            "error": error,
        },
        headers=_SC_HTML_NO_CACHE_HEADERS,
    )


@app.get("/api/search-console/{site_id}/analytics-report")
def api_search_console_analytics_report(
    site_id: int,
    report_key: str,
    days: int = 28,
    row_limit: int = 250,
):
    if not report_key:
        return JSONResponse({"error": "report_key gerekli"}, status_code=400)
    with SessionLocal() as db:
        try:
            payload = fetch_sc_analytics_report(
                db,
                site_id,
                report_key,
                days=days,
                row_limit=row_limit,
            )
            return JSONResponse(payload)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        except Exception as exc:
            logging.exception("api_sc_analytics site_id=%s key=%s", site_id, report_key)
            return JSONResponse({"error": str(exc)}, status_code=400)


@app.get("/api/search-console/{site_id}/sitemaps")
def api_search_console_sitemaps(site_id: int):
    with SessionLocal() as db:
        try:
            return JSONResponse(fetch_sc_sitemaps(db, site_id))
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        except Exception as exc:
            logging.exception("api_sc_sitemaps site_id=%s", site_id)
            return JSONResponse({"error": str(exc)}, status_code=400)


@app.post("/api/search-console/{site_id}/inspect")
async def api_search_console_inspect(site_id: int, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    inspection_url = str(body.get("inspection_url") or body.get("url") or "").strip()
    with SessionLocal() as db:
        try:
            return JSONResponse(inspect_sc_url(db, site_id, inspection_url))
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        except Exception as exc:
            logging.exception("api_sc_inspect site_id=%s", site_id)
            return JSONResponse({"error": str(exc)}, status_code=400)


@app.get("/search-console/site/{site_id}", response_class=HTMLResponse)
def search_console_single_site_card(request: Request, site_id: int):
    """HTMX lazy loading ile tek site kartını tam veriyle render eder."""
    from backend.services.analytics_compare import parse_compare_options

    q = request.query_params
    compare_enabled = str(q.get("compare") or "").lower() in ("1", "true", "yes", "on")
    compare_opts = parse_compare_options(
        enabled=compare_enabled,
        mode=q.get("compare_mode"),
        custom_start=q.get("compare_start"),
        custom_end=q.get("compare_end"),
    )
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)
        try:
            # site_count: tek COUNT sorgusu, tüm objeleri çekme
            external_ids = _external_site_ids(db)
            from sqlalchemy import func as sqlfunc
            site_count = db.query(sqlfunc.count(Site.id)).filter(Site.id.notin_(external_ids)).scalar() or 1
            schedule_label = (
                f"{int(settings.search_console_scheduled_refresh_hour):02d}:"
                f"{int(settings.search_console_scheduled_refresh_minute):02d}"
            )
            site_data = _search_console_single_site_data(
                db, site, schedule_label, compare_opts=compare_opts
            )
        except Exception as exc:
            logging.exception("search_console_single_site_card site_id=%s hata", site_id)
            import html as _html

            from backend.services.search_console_auth import format_search_console_error_for_ui

            err_msg = _html.escape(format_search_console_error_for_ui(str(exc)) or f"{type(exc).__name__}: {exc}")
            return HTMLResponse(
                f'<section id="sc-card-{site_id}" class="rounded-3xl border border-red-300 dark:border-red-700 '
                f'bg-red-50 dark:bg-red-900/30 p-5 text-sm text-red-700 dark:text-red-300">'
                f'<p class="font-semibold">Kart yüklenemedi</p>'
                f'<p class="mt-1 text-xs">Site #{site_id} verisi hazırlanırken hata oluştu. '
                f'Sayfayı yenileyerek tekrar deneyin.</p>'
                f'<p class="mt-2 text-xs opacity-70 font-mono break-all">{err_msg}</p></section>',
                status_code=200,
                headers=_SC_HTML_NO_CACHE_HEADERS,
            )
    return templates.TemplateResponse(
        request,
        "partials/sc_single_site_card.html",
        context={
            "request": request,
            "site": site_data,
            "oauth_ready": oauth_is_configured(),
            "site_count": site_count,
            "analytics_compare": compare_opts,
        },
        headers=_SC_HTML_NO_CACHE_HEADERS,
    )


@app.get("/search-console/health")
def search_console_health():
    # UI yanlışlıkla /search-console/health çağırırsa, /health ile uyumlu cevap ver.
    return {"status": "ok"}


_CWV_VARIANT_ORDER = ("full", "mobile", "desktop", "extra")


def _validate_cwv_screenshot_bytes(content: bytes, filename: str) -> str | None:
    """Geçerliyse None; aksi halde kullanıcıya gösterilecek kısa hata metni."""
    name = (filename or "").lower()
    if not (name.endswith(".png") or name.endswith(".jpg") or name.endswith(".jpeg") or name.endswith(".webp")):
        return "Sadece png/jpg/webp kabul edilir."
    if not content:
        return "Boş dosya."
    if len(content) > 10 * 1024 * 1024:
        return "Dosya çok büyük (max 10MB)."
    if not (
        content.startswith(b"\x89PNG\r\n\x1a\n")
        or content.startswith(b"\xff\xd8\xff")
        or content.startswith(b"RIFF")  # webp container
    ):
        return "Dosya tipi doğrulanamadı."
    return None


_CWV_FILENAME_HINTS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("mobile", re.compile(r"(?:^|[^a-z0-9])mobile(?:[^a-z0-9]|$)", re.I)),
    ("desktop", re.compile(r"(?:^|[^a-z0-9])desktop(?:[^a-z0-9]|$)", re.I)),
    ("extra", re.compile(r"(?:^|[^a-z0-9])extra(?:[^a-z0-9]|$)", re.I)),
    ("full", re.compile(r"(?:^|[^a-z0-9])full(?:[^a-z0-9]|$)", re.I)),
)


def _cwv_variant_from_filename(name: str) -> str | None:
    """Dosya adından varyant tahmini (extranet → extra yanlış eşleşmesin diye kelime sınırı)."""
    n = name or ""
    for variant, rx in _CWV_FILENAME_HINTS:
        if rx.search(n):
            return variant
    return None


def _occupied_cwv_slots(db, site_id: int, domain_slug: str) -> set[str]:
    """DB veya diskte dolu CWV slotları."""
    from backend.services import gsc_cwv_storage

    occ: set[str] = set()
    for v in _CWV_VARIANT_ORDER:
        row = gsc_cwv_storage.load_screenshot(db, site_id=site_id, variant=v)
        if row and row.image_data:
            occ.add(v)
    for v in _CWV_VARIANT_ORDER:
        if v in occ:
            continue
        p = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-{v}.png"
        try:
            if p.exists() and p.stat().st_size > 0:
                occ.add(v)
        except OSError:
            continue
    return occ


def _pair_cwv_uploads_to_variants(
    files: list[UploadFile], db, site_id: int, domain_slug: str
) -> list[tuple[str, UploadFile]]:
    """Çoklu yüklemede dosyaları varyantlara eşle (dosya adı ipucu + diskte boş slota sırayla).

    İsim ipucu yoksa: önce bu istekteki explicit slotlar, sonra diskte dolu olanlar \"dolu\"
    sayılır; kalan ilk boş slot (full→mobile→desktop→extra) kullanılır. Tek tek yüklemede
    ikinci dosya artık birincinin üzerine yazılmaz.
    """
    if not files:
        raise ValueError("Dosya seçilmedi.")
    if len(files) > 4:
        raise ValueError("En fazla 4 dosya yükleyebilirsin (mobile, desktop, full, extra).")
    explicit: dict[str, UploadFile] = {}
    implicit: list[UploadFile] = []
    for f in files:
        v = _cwv_variant_from_filename(f.filename or "")
        if v:
            if v in explicit:
                raise ValueError(f"İki dosya aynı varyantı işaret ediyor: {v}. Dosya adlarını ayırın.")
            explicit[v] = f
        else:
            implicit.append(f)
    taken: set[str] = set(explicit.keys()) | _occupied_cwv_slots(db, site_id, domain_slug)
    for f in implicit:
        placed = False
        for slot in _CWV_VARIANT_ORDER:
            if slot not in taken:
                explicit[slot] = f
                taken.add(slot)
                placed = True
                break
        if not placed:
            # Dört slot da dolu: tam sayfa görselini güncelle (tek “ana” slot)
            explicit["full"] = f
            taken.add("full")
    return [(v, explicit[v]) for v in _CWV_VARIANT_ORDER if v in explicit]


@app.post("/search-console/cwv-screenshot/upload-batch/{site_id}", response_class=HTMLResponse)
async def search_console_upload_cwv_screenshot_batch(
    request: Request,
    site_id: int,
    files: list[UploadFile] = File(...),
):
    """Birden fazla CWV ekran görüntüsü (aynı istekte mobile/desktop/full/extra)."""
    from backend.services import gsc_cwv_storage

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        domain_slug = _gsc_domain_slug(site.domain)

        try:
            pairs = _pair_cwv_uploads_to_variants(files, db, site_id, domain_slug)
        except ValueError as exc:
            return HTMLResponse(str(exc), status_code=400)

        GSC_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        for variant, upload in pairs:
            content = await upload.read()
            err = _validate_cwv_screenshot_bytes(content, upload.filename or "")
            if err:
                return HTMLResponse(f"{upload.filename or 'dosya'}: {err}", status_code=400)
            gsc_cwv_storage.upsert_screenshot(
                db,
                site_id=site_id,
                variant=variant,
                data=content,
                filename=upload.filename or "",
            )
            gsc_cwv_storage.write_disk_copy(domain_slug, variant, content, gsc_dir=GSC_SCREENSHOT_DIR)

    return search_console_single_site_card(request, site_id)


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

    from backend.services import gsc_cwv_storage

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        domain_slug = _gsc_domain_slug(site.domain)

        hint = _cwv_variant_from_filename(file.filename or "")
        if hint:
            variant = hint
        elif variant == "full":
            occupied = _occupied_cwv_slots(db, site_id, domain_slug)
            for slot in _CWV_VARIANT_ORDER:
                if slot not in occupied:
                    variant = slot
                    break

        content = await file.read()
        err = _validate_cwv_screenshot_bytes(content, file.filename or "")
        if err:
            return HTMLResponse(err, status_code=400)

        gsc_cwv_storage.upsert_screenshot(
            db, site_id=site_id, variant=variant, data=content, filename=file.filename or ""
        )
        GSC_SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        gsc_cwv_storage.write_disk_copy(domain_slug, variant, content, gsc_dir=GSC_SCREENSHOT_DIR)

    return search_console_single_site_card(request, site_id)


@app.get("/search-console/cwv-image/{site_id}/{variant}")
def search_console_cwv_image(site_id: int, variant: str):
    """Postgres veya diskten CWV görseli (Railway kalıcı URL)."""
    from fastapi.responses import Response

    from backend.services import gsc_cwv_storage

    variant = str(variant or "").strip().lower()
    if variant not in gsc_cwv_storage.CWV_VARIANTS:
        return HTMLResponse("Geçersiz variant.", status_code=404)
    with SessionLocal() as db:
        row = gsc_cwv_storage.load_screenshot(db, site_id=site_id, variant=variant)
        if row and row.image_data:
            return Response(
                content=bytes(row.image_data),
                media_type=row.content_type or "image/png",
                headers={"Cache-Control": "public, max-age=86400"},
            )
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        domain_slug = _gsc_domain_slug(site.domain)
    path = GSC_SCREENSHOT_DIR / f"{domain_slug}-cwv-{variant}.png"
    if not path.exists():
        return HTMLResponse("Görsel yok.", status_code=404)
    data = path.read_bytes()
    return Response(content=data, media_type="image/png", headers={"Cache-Control": "public, max-age=3600"})


@app.post("/search-console/cwv-screenshot/delete/{site_id}", response_class=HTMLResponse)
def search_console_delete_cwv_screenshot(request: Request, site_id: int, variant: str = "full"):
    """Manuel CWV görselini DB ve diskten siler."""
    from backend.services import gsc_cwv_storage

    variant = str(variant or "full").strip().lower()
    if variant not in gsc_cwv_storage.CWV_VARIANTS:
        return HTMLResponse("Geçersiz variant.", status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadı.", status_code=404)
        domain_slug = _gsc_domain_slug(site.domain)
        gsc_cwv_storage.delete_screenshot(db, site_id=site_id, variant=variant)
        gsc_cwv_storage.delete_disk_copy(domain_slug, variant, gsc_dir=GSC_SCREENSHOT_DIR)

    return search_console_single_site_card(request, site_id)


def _refresh_one_site_for_sc_batch(site_id: int) -> tuple[int, dict]:
    """Tek bir sitenin SC verisini kendi DB session'ında yeniler (thread-safe)."""
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return (site_id, {"state": "failed", "error": "Site bulunamadı."})
        try:
            results = _refresh_site_detail_measurements(
                db,
                site,
                include_pagespeed=False,
                include_crawler=False,
                include_search_console=True,
                force=True,
                send_notifications=True,
            )
            _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
            sc = results.get("search_console")
            if isinstance(sc, dict):
                return (site_id, sc)
            return (site_id, {"state": "failed", "error": "Search Console ölçümü dönmedi (içerik yok veya boş)."})
        except Exception as exc:  # noqa: BLE001
            try:
                db.rollback()
            except Exception:
                pass
            return (site_id, {"state": "failed", "error": str(exc)})


def _sc_refresh_all_set_progress(*, done: int, total: int, label: str = "") -> None:
    with _SC_REFRESH_ALL_LOCK:
        job = _SC_REFRESH_ALL_JOB
        if not job or job.get("status") != "running":
            return
        job["progress"] = {"done": int(done), "total": int(total), "label": str(label or "")}


def _compute_search_console_refresh_all_payload(*, job_id: str | None = None) -> dict:
    """LIVE_REFRESH açıkken toplu GSC çekimi; JSON gövdesi (live_refresh_enabled=True).

    Site bazlı çekimler paralel çalıştırılır (her thread kendi DB session'ında).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with SessionLocal() as db:
        external = _external_site_ids(db)
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.asc(), Site.id.asc()).all()
        eligible: list[int] = []
        not_connected = 0
        for site in sites:
            if site.id in external:
                continue
            connection = get_search_console_connection_status(db, site.id)
            if not connection.get("connected"):
                not_connected += 1
                continue
            eligible.append(site.id)
        n_active_non_external = len([s for s in sites if s.id not in external])

    _sc_refresh_all_set_progress(done=0, total=len(eligible), label="Başlatılıyor")

    # Paralel çekim — her site kendi session'ında, max 4 eşzamanlı (Google API kotası ve
    # DB lock'ları için makul bir tavan; tipik kurulumda site sayısı zaten <4).
    site_results: dict[int, dict] = {}
    if eligible:
        max_workers = min(4, len(eligible))
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="sc-refresh-all") as pool:
            futures = {pool.submit(_refresh_one_site_for_sc_batch, sid): sid for sid in eligible}
            for fut in as_completed(futures):
                try:
                    sid, result = fut.result()
                except Exception as exc:  # noqa: BLE001
                    sid = futures[fut]
                    result = {"state": "failed", "error": str(exc)}
                site_results[sid] = result
                completed += 1
                with SessionLocal() as db:
                    site = db.query(Site).filter(Site.id == sid).first()
                site_label = (site.display_name or site.domain) if site else f"site #{sid}"
                _sc_refresh_all_set_progress(done=completed, total=len(eligible), label=site_label)

    refreshed_ok = sum(1 for r in site_results.values() if str(r.get("state") or "").lower() != "failed")
    failed = sum(1 for r in site_results.values() if str(r.get("state") or "").lower() == "failed")

    # Email için sc_batch — fresh session ile Site objelerini topla
    with SessionLocal() as db:
        sites_by_id = {
            s.id: s
            for s in db.query(Site).filter(Site.id.in_(list(site_results.keys()) or [0])).all()
        }
        sc_batch: list[tuple[Site, dict]] = [
            (sites_by_id[sid], result) for sid, result in site_results.items() if sid in sites_by_id
        ]
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

    if refreshed_ok == 0 and failed == 0:
        if n_active_non_external == 0:
            title, detail = "Yenilenecek site yok", "Aktif, internal (external olmayan) site bulunamadı."
        elif not_connected > 0:
            title, detail = (
                "API çağrılmadı",
                f"Bağlı GSC (OAuth) sitesi yok: {not_connected} aktif site Search Console property’si ile eşleşmiyor.",
            )
        else:
            title, detail = (
                "API sonucu yok",
                "Search Console ölçümü dönmedi; ayrıntı için uygulama loglarına bakın.",
            )
    elif failed and refreshed_ok:
        title = "Kısmi yenileme"
        detail = f"{refreshed_ok} site güncellendi, {failed} sitede hata."
    elif failed and not refreshed_ok:
        title = "Yenileme başarısız"
        detail = f"{failed} site için Search Console verisi alınamadı."
    else:
        title = "Tüm siteler güncellendi"
        detail = f"{refreshed_ok} site Search Console API verisiyle güncellendi."
        if not_connected:
            detail += f" {not_connected} site GSC’ye bağlı olmadığı için atlandı."
    return {
        "ok": True,
        "live_refresh_enabled": True,
        "refreshed": refreshed_ok,
        "failed": failed,
        "not_connected": not_connected,
        "title": title,
        "detail": detail,
    }


def _search_console_refresh_all_job_finish(job_id: str) -> None:
    global _SC_REFRESH_ALL_JOB
    try:
        payload = _compute_search_console_refresh_all_payload(job_id=job_id)
        with _SC_REFRESH_ALL_LOCK:
            job = _SC_REFRESH_ALL_JOB
            if job and job.get("id") == job_id:
                job["status"] = "done"
                job["result"] = payload
                job["progress"] = None
    except Exception as exc:
        LOGGER.exception("Search Console refresh-all background job failed")
        with _SC_REFRESH_ALL_LOCK:
            job = _SC_REFRESH_ALL_JOB
            if job and job.get("id") == job_id:
                job["status"] = "error"
                job["error"] = str(exc).strip() or "Arka plan işinde beklenmeyen hata; loglara bakın."
                job["progress"] = None


def _start_search_console_refresh_all_job(job_id: str) -> None:
    """Toplu GSC yenilemesini ayrı thread'de çalıştır (uvicorn event loop'u bloklamasın)."""
    threading.Thread(
        target=_search_console_refresh_all_job_finish,
        args=(job_id,),
        daemon=True,
        name=f"sc-refresh-all-{job_id[:8]}",
    ).start()


@app.post("/search-console/refresh-all")
def search_console_refresh_all(request: Request):
    """Toplu GSC çekimi. Uzun sürdüğü için hemen yanıt + /status ile izlenir (bağlantı kesilmesini önler)."""
    global _SC_REFRESH_ALL_JOB
    if not settings.live_refresh_enabled:
        return JSONResponse(
            {
                "ok": True,
                "live_refresh_enabled": False,
                "refreshed": 0,
                "failed": 0,
                "not_connected": 0,
                "title": "Canlı yenileme kapalı",
                "detail": "LIVE_REFRESH_ENABLED=0. Google Search Console API çağrılmadı; ekran yalnızca veritabanındaki mevcut veriyle tazelenecek.",
            },
            headers=_SC_JSON_NO_CACHE_HEADERS,
        )
    with _SC_REFRESH_ALL_LOCK:
        cur = _SC_REFRESH_ALL_JOB
        if cur and cur.get("status") == "running":
            started = float(cur.get("started") or 0.0)
            age = time.time() - started if started > 0 else 0.0
            if age < _SC_REFRESH_ALL_STALE_SECONDS:
                return JSONResponse(
                    {
                        "ok": False,
                        "detail": "Başka bir toplu Search Console yenilemesi hâlâ çalışıyor; bitene kadar bekleyin.",
                    },
                    status_code=409,
                    headers=_SC_JSON_NO_CACHE_HEADERS,
                )
            cur["status"] = "error"
            cur["error"] = "Önceki toplu yenileme zaman aşımına uğradı; yeni işlem başlatıldı."
        job_id = str(uuid4())
        _SC_REFRESH_ALL_JOB = {
            "id": job_id,
            "status": "running",
            "result": None,
            "error": None,
            "started": time.time(),
            "progress": {"done": 0, "total": 0, "label": ""},
        }
    _start_search_console_refresh_all_job(job_id)
    return JSONResponse(
        {
            "ok": True,
            "async": True,
            "job_id": job_id,
            "title": "Toplu yenileme başladı",
            "detail": "İşlem arka planda sürüyor; birkaç dakika sürebilir — sayfayı kapatmayın.",
        },
        headers=_SC_JSON_NO_CACHE_HEADERS,
    )


@app.get("/search-console/refresh-all/status/{job_id}")
def search_console_refresh_all_status(job_id: str):
    """Arka plandaki toplu GSC yenileme durumu (kısa JSON; tarayıcı sık sık çağırabilir)."""
    with _SC_REFRESH_ALL_LOCK:
        job = _SC_REFRESH_ALL_JOB
        if not job or job.get("id") != job_id:
            return JSONResponse(
                {
                    "ok": False,
                    "done": True,
                    "detail": "İşlem bulunamadı (sunucu yeniden başladıysa yeniden deneyin).",
                },
                status_code=404,
                headers=_SC_JSON_NO_CACHE_HEADERS,
            )
        if job.get("status") == "running":
            body: dict = {"ok": True, "done": False, "job_id": job_id}
            progress = job.get("progress")
            if isinstance(progress, dict):
                body["progress"] = progress
            return JSONResponse(body, headers=_SC_JSON_NO_CACHE_HEADERS)
        if job.get("status") == "error":
            return JSONResponse(
                {
                    "ok": False,
                    "done": True,
                    "detail": job.get("error") or "Arka plan işi başarısız.",
                },
                headers=_SC_JSON_NO_CACHE_HEADERS,
            )
        payload = job.get("result") or {}
        return JSONResponse({"ok": True, "done": True, **payload}, headers=_SC_JSON_NO_CACHE_HEADERS)


@app.post("/search-console/refresh/{site_id}")
def search_console_manual_refresh(request: Request, site_id: int):
    wjson = _search_console_request_wants_json(request)
    try:
        with SessionLocal() as db:
            site = db.query(Site).filter(Site.id == site_id).first()
            if site is None:
                if wjson:
                    return JSONResponse(
                        {"ok": False, "detail": "Site bulunamadı."},
                        status_code=404,
                        headers=_SC_JSON_NO_CACHE_HEADERS,
                    )
                return HTMLResponse("Site bulunamadı.", status_code=404)
            if _is_external_site(db, site.id):
                if wjson:
                    return JSONResponse(
                        {
                            "ok": False,
                            "detail": "Bu site external profilde; Search Console raporu yok.",
                        },
                        status_code=404,
                        headers=_SC_JSON_NO_CACHE_HEADERS,
                    )
                return HTMLResponse("Bu site için Search Console raporu gönderilmez (external).", status_code=404)
            results = _refresh_site_detail_measurements(
                db,
                site,
                include_pagespeed=False,
                include_crawler=False,
                include_search_console=True,
                force=True,
                send_notifications=True,
            )
            try:
                _commit_with_lock_retry(db, attempts=8, base_wait=0.2)
            except OperationalError as exc:
                db.rollback()
                if _is_sqlite_lock_error(exc):
                    if wjson:
                        return JSONResponse(
                            {"ok": False, "detail": "Veritabanı meşgul, lütfen tekrar deneyin."},
                            status_code=503,
                            headers=_SC_JSON_NO_CACHE_HEADERS,
                        )
                    return HTMLResponse("Veritabanı meşgul, lütfen tekrar deneyin.", status_code=503)
                raise
            try:
                notify_result_map(
                    trigger_source="manual",
                    site=site,
                    results=results,
                    action_label="Search Console verisini yenile",
                )
            except Exception:
                logging.exception(
                    "Search Console manual refresh: notify_result_map failed site_id=%s",
                    site_id,
                )
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
                headers=_SC_HTML_NO_CACHE_HEADERS,
            )
    except Exception as exc:  # noqa: BLE001
        logging.exception("search_console_manual_refresh site_id=%s", site_id)
        if wjson:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "detail": f"{type(exc).__name__}: {str(exc)[:2000]}"},
                headers=_SC_JSON_NO_CACHE_HEADERS,
            )
        return HTMLResponse("Search Console yenilenemedi.", status_code=500)


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
def search_console_oauth_start(request: Request, site_id: int, next: str = "/settings"):
    if not oauth_is_configured():
        return HTMLResponse("Google OAuth ayarlari eksik. GOOGLE_CLIENT_ID ve GOOGLE_CLIENT_SECRET gerekli.", status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

    state = encode_oauth_state(site_id, return_path=next, request=request)
    flow = build_oauth_flow(state=state, request=request)
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
        payload = decode_oauth_state(state, request=request)
    except ValueError as exc:
        return HTMLResponse(str(exc), status_code=400)

    import os as _os
    _os.environ.setdefault("OAUTHLIB_RELAX_TOKEN_SCOPE", "1")

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == int(payload["site_id"])).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

        try:
            flow = build_oauth_flow(state=state, request=request)
            flow.fetch_token(authorization_response=str(request.url))
        except Exception as exc:
            return HTMLResponse(
                f"OAuth token alınamadı: {exc}",
                status_code=400,
            )
        creds = flow.credentials
        if not creds.refresh_token:
            return HTMLResponse(
                "Google yeni refresh token vermedi. Önce «Bağlantıyı Kaldır», ardından tekrar "
                "«Google ile Bağlan» (tüm izinleri onaylayın).",
                status_code=400,
            )
        save_oauth_credentials(db, site.id, creds)
        connected_site_id = site.id

    import threading as _sc_oauth_threading

    def _sc_collect_after_oauth(site_id: int) -> None:
        import time as _t

        _t.sleep(0.8)
        try:
            with SessionLocal() as bg_db:
                bg_site = bg_db.query(Site).filter(Site.id == site_id).first()
                if bg_site is None:
                    return
                collect_search_console_metrics(bg_db, bg_site, send_notifications=False)
                bg_db.commit()
                LOGGER.info("Search Console OAuth sonrası otomatik çekim bitti site_id=%s", site_id)
        except Exception as exc:
            LOGGER.warning("Search Console OAuth sonrası çekim başarısız site_id=%s: %s", site_id, exc)

    _sc_oauth_threading.Thread(
        target=_sc_collect_after_oauth,
        args=(connected_site_id,),
        daemon=True,
        name=f"sc-oauth-collect-{connected_site_id}",
    ).start()

    return_path = str(payload.get("return_path") or "/settings")
    sep = "&" if "?" in return_path else "?"
    return RedirectResponse(f"{return_path}{sep}sc_oauth_ok=1", status_code=302)


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


@app.get("/search-console/performance")
def search_console_legacy_performance():
    return RedirectResponse(url="/search-console", status_code=301)


@app.get("/search-console/{view_slug}")
def search_console_legacy_view(view_slug: str):
    slug = str(view_slug or "").strip()
    if slug == "countries":
        return RedirectResponse(url="/search-console", status_code=301)
    if slug in SC_VIEW_SPECS:
        fragment = "" if slug == "performance" else f"#sc-{slug}"
        return RedirectResponse(url=f"/search-console{fragment}", status_code=301)
    return HTMLResponse("Gorunum bulunamadi.", status_code=404, headers=_SC_HTML_NO_CACHE_HEADERS)


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
        (Ga4ReportSnapshot, [Ga4ReportSnapshot.profile, Ga4ReportSnapshot.period_days], "ga4_report_snapshots"),
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
            (RealtimeSnapshot, RealtimeSnapshot.collected_at, settings.db_retention_realtime_snapshot_days, "realtime_snapshots"),
            (RealtimeAlarmLog, RealtimeAlarmLog.triggered_at, settings.db_retention_realtime_alarm_log_days, "realtime_alarm_logs"),
            (RealtimePageSnapshot, RealtimePageSnapshot.collected_at, 3, "realtime_page_snapshots"),
            (RealtimeNewsSnapshot, RealtimeNewsSnapshot.collected_at, 3, "realtime_news_snapshots"),
            (AppStoreRankSnapshot, AppStoreRankSnapshot.collected_at, 30, "app_store_rank_snapshots"),
            (AiDailyBriefReport, AiDailyBriefReport.created_at, settings.db_retention_ai_report_days, "ai_daily_brief_reports"),
            (AiBriefRunLog, AiBriefRunLog.created_at, settings.db_retention_ai_report_days, "ai_brief_run_logs"),
            (AppIntelRawCache, AppIntelRawCache.updated_at, settings.db_retention_app_intel_cache_days, "app_intel_raw_cache"),
            (SupportInboxThread, SupportInboxThread.last_synced_at, 90, "support_inbox_threads"),
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

        # PostgreSQL: isteğe bağlı tam VACUUM ANALYZE (varsayılan kapalı — bkz. settings.db_retention_run_vacuum)
        if not _IS_SQLITE and settings.db_retention_run_vacuum:
            try:
                from backend.database import engine
                with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                    conn.execute(text("VACUUM ANALYZE"))
                stats["vacuum"] = 0
            except Exception:
                logging.exception("VACUUM ANALYZE hatası")
                stats["vacuum"] = -1
        elif not _IS_SQLITE:
            stats["vacuum"] = 0

    total_deleted = sum(v for v in stats.values() if v > 0)
    logging.info("DB retention cleanup tamamlandı — toplam %d satır silindi: %s", total_deleted, stats)
    return stats


def _run_ga4_realtime_check_job(force_run: bool = False) -> dict[str, Any]:
    """APScheduler: periyodik GA4 Realtime karşılaştırma & alarm kontrolü.
    00:00–06:00 arası yalnızca KPI snapshot (alarm/sayfa/haber yok); gündüz tam döngü."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZoneInfo
    tz_name = getattr(settings, "report_calendar_timezone", "Europe/Istanbul")
    now_local = _dt.now(_ZoneInfo(tz_name))
    hour = now_local.hour
    is_night = (0 <= hour < 6)
    
    LOGGER.info(">>> GA4 Realtime Job HEARTBEAT: Kontrol döngüsü BAŞLADI (local_time=%s, force=%s)", now_local.isoformat(), force_run)
    try:
        from backend.services.ga4_realtime import (
            run_all_sites_realtime_check,
            run_news_alarm_check_all_sites,
            run_page_alarm_check_all_sites,
            run_404_spike_check_all_sites,
            run_app_event_spike_check_all_sites,
        )
        from backend.services.mailer import (
            realtime_email_batch_begin,
            realtime_email_batch_flush,
            realtime_email_batch_take_pending_marks,
        )
        from backend.services.ga4_realtime import apply_realtime_batch_email_marks

        total_site_alarms = 0
        total_page_alarms = 0
        total_news_alarms = 0

        # Tüm alarm tiplerinden gelen e-postalar tek bir mailde toplanır.
        # Alarm tespiti / DB mantığına dokunulmaz; sadece gönderim batche alınır.
        realtime_email_batch_begin()

        # 1. Site-level KPI (gece: yalnızca snapshot, alarm/mail yok — 24s grafik boşluğu azalır)
        with SessionLocal() as db:
            results = run_all_sites_realtime_check(
                db,
                window_minutes=settings.ga4_realtime_window_minutes,
                skip_alarms=is_night,
                skip_emails=is_night,
            )

        for res in results:
            if isinstance(res, dict) and res.get("alarms"):
                total_site_alarms += len(res["alarms"])

        if is_night and not force_run:
            flushed = realtime_email_batch_flush()
            if flushed:
                marks = realtime_email_batch_take_pending_marks()
                if marks:
                    with SessionLocal() as db:
                        apply_realtime_batch_email_marks(db, marks)
            LOGGER.info(
                "GA4 Realtime: Gece modu — %d KPI snapshot güncellendi (alarm/sayfa/haber atlandı).",
                len(results),
            )
            return {"total_alarms": 0, "status": "night_mode_snapshot_only", "site_check_count": len(results)}

        # 2. Sayfa bazlı alarmlar
        if settings.ga4_realtime_page_alerts_enabled:
            with SessionLocal() as db:
                page_alarms = run_page_alarm_check_all_sites(
                    db, window_minutes=settings.ga4_realtime_window_minutes,
                    skip_emails=False,
                )
            total_page_alarms = len(page_alarms) if page_alarms else 0

        # 3. Haber alarmları
        if settings.ga4_realtime_news_alerts_enabled:
            with SessionLocal() as db:
                news_alarms = run_news_alarm_check_all_sites(db, skip_emails=False)
            total_news_alarms = len(news_alarms) if news_alarms else 0

        # 4. Realtime 404 spike kontrolü
        total_404_alarms = 0
        if getattr(settings, "ga4_realtime_404_enabled", True):
            with SessionLocal() as db:
                spike_results = run_404_spike_check_all_sites(db, skip_emails=True)
            total_404_alarms = sum(1 for r in spike_results if r.get("severity"))

        # 5. App event spike (android/ios)
        total_app_event_alarms = 0
        try:
            with SessionLocal() as db:
                app_event_results = run_app_event_spike_check_all_sites(db, skip_emails=False)
            total_app_event_alarms = sum(len(r.get("alarms") or []) for r in app_event_results if isinstance(r, dict))
        except Exception as exc:
            LOGGER.warning("App event check hatası: %s", exc)

        # Tüm alarmlar toplandı — tek mail olarak gönder
        flushed = realtime_email_batch_flush()
        if flushed:
            marks = realtime_email_batch_take_pending_marks()
            if marks:
                with SessionLocal() as db:
                    apply_realtime_batch_email_marks(db, marks)

        total = total_site_alarms + total_page_alarms + total_news_alarms + total_404_alarms + total_app_event_alarms
        LOGGER.info(
            "<<< GA4 Realtime Job BİTTİ. Site: %d, Sayfa: %d, Haber: %d, 404 Spike: %d, App Event: %d alarm",
            total_site_alarms, total_page_alarms, total_news_alarms, total_404_alarms, total_app_event_alarms,
        )
        return {
            "total_alarms": total,
            "site_alarms": total_site_alarms,
            "page_alarms": total_page_alarms,
            "news_alarms": total_news_alarms,
            "404_alarms": total_404_alarms,
            "app_event_alarms": total_app_event_alarms,
            "site_check_count": len(results),
            "status": "completed",
        }
    except Exception as exc:
        import traceback
        logging.exception("GA4 Realtime check hatası")
        return {"status": "error", "message": str(exc), "traceback": traceback.format_exc()}


def _run_tmdb_cache_refresh_job() -> None:
    """TMDB combined cache'ini günlük tazeler (02:30)."""
    try:
        from backend.services.tmdb import refresh_combined_cache
        result = refresh_combined_cache(months_ahead=5)
        LOGGER.info(
            "TMDB cache refresh tamamlandı: theatrical=%d streaming=%d tv=%d",
            len(result.get("theatrical", [])),
            len(result.get("streaming", [])),
            len(result.get("tv_series", [])),
        )
    except Exception as exc:
        LOGGER.error("TMDB cache refresh hatası: %s", exc)


def _get_error_summary_for_card(db, site_id: int, days: int = 7) -> dict:
    """GA4 site kartı için hızlı hata özeti — son N günün top 5 URL'si."""
    try:
        from backend.services.error_monitor import get_error_summary
        return get_error_summary(db, site_id, days=days)
    except Exception:
        return {"total_404": 0, "total_5xx": 0, "total_users": 0, "errors": [], "days": days}


def _run_error_detection_job() -> None:
    """Günlük GA4 hata tespiti — tüm siteler için 1/7/14/30g periyotlarını DB'ye yazar."""
    try:
        from backend.services.error_monitor import run_error_detection_all_sites, _GA4_PERIODS
        with SessionLocal() as db:
            results = run_error_detection_all_sites(db)
        ok = [r for r in results if isinstance(r, dict) and r.get("status") == "ok"]
        total_found = sum(r.get("found", 0) for r in ok)
        site_count = len({r.get("domain") for r in ok if r.get("domain")})
        LOGGER.info(
            "Hata tespiti tamamlandı: %d site, %d periyot, toplam %d hata URL",
            site_count, len(_GA4_PERIODS), total_found,
        )
    except Exception as exc:
        LOGGER.error("Hata tespiti job hatası: %s", exc)


def _run_meta_audit_snapshot_job() -> None:
    """Günlük meta tag snapshot + kritik regresyon alarmı — 02:15."""
    try:
        from backend.services.meta_audit import take_daily_snapshot, get_changes, cleanup_old_snapshots
        from backend.services.mailer import send_email
        from backend.models import Site

        with SessionLocal() as db:
            external_ids = _external_site_ids(db)
            sites = [s for s in db.query(Site).order_by(Site.id).all() if s.id not in external_ids]

        all_critical: list[dict] = []
        for site in sites:
            try:
                with SessionLocal() as db:
                    saved = take_daily_snapshot(db, site.id)
                    changes = get_changes(db, site.id, days=1)
                critical = [c for c in changes if any(d.get("severity") == "critical" for d in c["changes"])]
                for c in critical:
                    c["domain"] = site.domain
                all_critical.extend(critical)
                LOGGER.info("Meta snapshot: site=%s, %d URL kaydedildi, %d değişiklik", site.domain, saved, len(changes))
            except Exception as exc:
                LOGGER.warning("Meta snapshot hatası [site=%s]: %s", site.domain, exc)

        with SessionLocal() as db:
            cleanup_old_snapshots(db, retention_days=90)

        if not all_critical:
            return

        # Kritik değişiklik maili
        from datetime import datetime as _dt
        from backend.services.ga4_page_urls import absolute_audit_href

        now_str = _dt.now().strftime("%d.%m.%Y %H:%M")
        pre_parts = [absolute_audit_href(c.get("domain"), c.get("url"))[:50] for c in all_critical[:3]]
        filler = "&nbsp;" * 80
        preheader = f'{len(all_critical)} kritik değişiklik · ' + " · ".join(pre_parts)
        rows_html = ""
        for c in all_critical[:20]:
            for d in c["changes"]:
                if d.get("severity") != "critical":
                    continue
                field_label = {"noindex": "Noindex eklendi", "canonical": "Canonical değişti"}.get(d["field"], d["field"])
                href = absolute_audit_href(c.get("domain"), c.get("url"))
                link_text = (c.get("url") or href)[:60]
                rows_html += (
                    f'<tr style="border-bottom:1px solid #fee2e2">'
                    f'<td style="padding:8px 10px 8px 0;font-family:monospace;font-size:12px;color:#dc2626">'
                    f'<a href="{href}" style="color:#dc2626">{link_text}</a></td>'
                    f'<td style="padding:8px;font-size:12px;color:#64748b">{field_label}</td>'
                    f'<td style="padding:8px 0;font-size:11px;color:#94a3b8">{str(d.get("old",""))[:40]} → {str(d.get("new",""))[:40]}</td>'
                    f'</tr>'
                )
        html = (
            f'<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:640px">'
            f'<span style="display:none;font-size:1px;color:#fafafa;max-height:0;overflow:hidden">{preheader}{filler}</span>'
            f'<p style="font-size:15px;font-weight:700;color:#dc2626;margin:0 0 4px">⚠ SEO Kritik Değişiklik</p>'
            f'<p style="font-size:12px;color:#64748b;margin:0 0 16px">{now_str} · {len(all_critical)} sayfa etkilendi</p>'
            f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>'
            f'<p style="font-size:11px;color:#94a3b8;margin-top:16px">SEO Agent · <a href="/seo-audit" style="color:#94a3b8">detaylar</a></p>'
            f'</div>'
        )
        subject = f"⚠ SEO Kritik Değişiklik · {len(all_critical)} sayfa · {now_str}"
        send_email(subject, html)
        LOGGER.info("Meta audit kritik alarm maili gönderildi: %d değişiklik", len(all_critical))
    except Exception as exc:
        LOGGER.error("Meta audit snapshot job hatası: %s", exc)


def _run_seo_audit_job() -> None:
    """Günlük SEO meta tag taraması — GA4 top 250 web + 250 mweb (arka plan thread)."""
    import threading

    if not settings.seo_audit_scheduled_enabled:
        LOGGER.info("SEO audit job skipped: scheduled refresh disabled.")
        return

    def _worker() -> None:
        from backend.models import Site
        from backend.services.seo_audit_runner import execute_seo_audit_for_site

        if not SEO_AUDIT_JOB_LOCK.acquire(blocking=False):
            LOGGER.info("SEO audit job skipped: previous run still in progress.")
            return
        try:
            with SessionLocal() as db:
                external_ids = _external_site_ids(db)
                sites = [
                    s for s in db.query(Site).order_by(Site.id).all()
                    if s.id not in external_ids
                ]

            for site in sites:
                site_id = site.id
                site_domain = site.domain or ""

                if _seo_audit_progress.get(site_id, {}).get("running"):
                    LOGGER.info("SEO audit job: site=%s zaten taranıyor, atlandı", site_domain)
                    continue

                prog = {
                    "running": True,
                    "total": 0,
                    "done": 0,
                    "ok": 0,
                    "error": 0,
                    "current": "Job başladı",
                }
                _seo_audit_progress[site_id] = prog
                LOGGER.info("SEO audit job başladı: site=%s", site_domain)
                try:
                    with SessionLocal() as db:
                        site_row = db.query(Site).filter(Site.id == site_id).first()
                        if site_row:
                            execute_seo_audit_for_site(
                                db,
                                site_row,
                                trigger_source="scheduled",
                                progress=prog,
                                sitemap_source="job",
                            )
                except Exception as exc:
                    LOGGER.exception("SEO audit job hatası [%s]: %s", site_domain, exc)
        except Exception as exc:
            LOGGER.error("SEO audit job: site listesi alınamadı: %s", exc)
        finally:
            SEO_AUDIT_JOB_LOCK.release()

    threading.Thread(target=_worker, daemon=True, name="seo-audit-scheduled-job").start()


def _run_omdb_enrichment_job() -> None:
    """APScheduler: Günlük OMDB zenginleştirme (max 999 film)."""
    try:
        from backend.services.omdb import run_daily_omdb_enrichment
        with SessionLocal() as db:
            result = run_daily_omdb_enrichment(db)
        LOGGER.info("OMDB zenginleştirme: %s", result)
    except Exception:
        LOGGER.exception("OMDB enrichment job failed")


def _run_inbox_scheduled_sync_job() -> None:
    """APScheduler: periyodik inbox Gmail senkronu (tüm sekmeler)."""
    if not INBOX_SYNC_LOCK.acquire(blocking=False):
        LOGGER.info("Inbox scheduled sync skipped: previous run still active.")
        return
    try:
        from backend.services.inbox_summary import run_inbox_scheduled_sync

        with SessionLocal() as db:
            run_inbox_scheduled_sync(db)
    except Exception as exc:
        if "invalid_grant" in str(exc) or type(exc).__name__ == "RefreshError":
            LOGGER.warning(
                "Inbox scheduled sync job atlandı: Gmail OAuth yenilenemedi — panelden Gmail yeniden bağlayın."
            )
        else:
            LOGGER.exception("Inbox scheduled sync job failed")
    finally:
        INBOX_SYNC_LOCK.release()


def _run_inbox_summary_email_job() -> None:
    """APScheduler: 2 saatte bir 4 sekmeli inbox özet maili (:15)."""
    try:
        from backend.services.inbox_summary import run_inbox_summary_email

        with SessionLocal() as db:
            run_inbox_summary_email(db)
    except Exception:
        LOGGER.exception("Inbox summary email job failed")


def _run_inbox_summary_job() -> None:
    """Geriye uyumluluk: eski job adı → özet maili."""
    _run_inbox_summary_email_job()


def _run_inbox_firebase_sync_job() -> None:
    """APScheduler: firebase-noreply@google.com uyarılarını hızlı çeker."""
    if not INBOX_SYNC_LOCK.acquire(blocking=False):
        LOGGER.debug("Inbox Firebase sync skipped: full inbox sync in progress.")
        return
    try:
        from backend.services import inbox_gmail_auth, inbox_sync

        with SessionLocal() as db:
            if inbox_gmail_auth.get_inbox_credential_row(db) is None:
                LOGGER.debug("Inbox Firebase sync atlandı: Gmail henüz bağlı değil.")
                return
            out = inbox_sync.sync_firebase_inbox_threads(db, max_threads=30)
            LOGGER.info("Inbox Firebase sync: synced=%s", out.get("synced_threads"))
    except RuntimeError as exc:
        if "bağlı değil" in str(exc).lower():
            LOGGER.debug("Inbox Firebase sync atlandı: %s", exc)
            return
        LOGGER.warning("Inbox Firebase sync: %s", exc)
    except Exception:
        LOGGER.exception("Inbox Firebase sync job failed")
    finally:
        INBOX_SYNC_LOCK.release()


def _run_news_intelligence_job() -> None:
    """APScheduler: Google News istihbarat taraması."""
    try:
        from backend.services.news_intelligence import run_news_intelligence_job
        run_news_intelligence_job()
    except Exception:
        LOGGER.exception("News intelligence job failed")


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


@app.get("/api/admin/test-realtime-mail")
def admin_test_realtime_mail(db: Session = Depends(get_db)):
    """Realtime e-posta sistemini teşhis eder ve test mailleri gönderir."""
    try:
        from backend.services.mailer import default_mail_recipients, is_realtime_mail_ready, send_realtime_email, _smtp_configured
        from backend.services import inbox_gmail_auth
        from backend.services.ga4_realtime import send_realtime_summary_email, get_recent_alarms
        from backend.models import Site as SiteModel

        recipient_list = default_mail_recipients()

        inbox_creds = inbox_gmail_auth.load_inbox_credentials(db)
        inbox_row = inbox_gmail_auth.get_inbox_credential_row(db)

        results = {
            "ga4_realtime_email_enabled": settings.ga4_realtime_email_enabled,
            "ga4_realtime_page_alert_email": settings.ga4_realtime_page_alert_email,
            "smtp_configured": _smtp_configured(),
            "smtp_host": settings.smtp_host,
            "smtp_port": settings.smtp_port,
            "mail_to": settings.mail_to,
            "mail_to_list": recipient_list,
            "is_ready": is_realtime_mail_ready(),
            "inbox": {
                "is_connected": inbox_creds is not None,
                "account_email": inbox_row.account_email if inbox_row else None,
                "has_refresh_token": bool(inbox_creds.refresh_token) if inbox_creds else False,
            }
        }

        # 0. Doğrudan SMTP Bağlantı Testi
        import smtplib
        smtp_debug = {}
        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=15) as smtp:
                smtp_debug["connect"] = "ok"
                smtp.starttls()
                smtp_debug["starttls"] = "ok"
                smtp.login(settings.smtp_user, settings.smtp_password)
                smtp_debug["login"] = "ok"
        except Exception as e:
            smtp_debug["error"] = str(e)
            smtp_debug["type"] = type(e).__name__
        results["smtp_debug"] = smtp_debug

        # 1. Bireysel test
        subject = "SEO Agent TEST: Bireysel Alarm"
        body = "<h3>Bireysel Mail Testi</h3><p>Sistem bireysel alarm gönderebiliyor.</p>"
        ok = send_realtime_email(subject, body, thread_kind="test", thread_key="test-key-indiv")
        results["individual_send_success"] = ok
        
        # 2. Özet test (son 15 dk alarmları varsa)
        recent_alarms = []
        sites = db.query(SiteModel).filter(SiteModel.is_active.is_(True)).all()
        for s in sites:
            alarms = get_recent_alarms(db, s.id, limit=5)
            recent_alarms.extend(alarms)
        
        results["found_recent_alarms"] = len(recent_alarms)
        if recent_alarms:
            results["summary_send_success"] = send_realtime_summary_email(recent_alarms)
        else:
            # Yapay bir alarm listesi ile dene
            fake_alarms = [{
                "domain": "test.com", "metric": "activeUsers", "profile": "web",
                "current_value": 100, "previous_value": 200, "change_pct": -50.0
            }]
            results["summary_fake_send_success"] = send_realtime_summary_email(fake_alarms)
            
        return JSONResponse(results)

    except Exception as exc:
        import traceback
        return JSONResponse({
            "status": "error",
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc()
        }, status_code=500)


@app.get("/api/admin/run-realtime-job-now")
def admin_run_realtime_job_now():
    """Realtime alarm kontrol işini manuel, senkron ve detaylı hata raporuyla çalıştırır."""
    try:
        results = _run_ga4_realtime_check_job(force_run=True)
        return {
            "status": "ok", 
            "message": "GA4 Realtime kontrol işi SENKRON olarak tamamlandı.",
            "details": results
        }
    except Exception as exc:
        import traceback
        return JSONResponse({
            "status": "error",
            "message": str(exc),
            "traceback": traceback.format_exc()
        }, status_code=500)


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


@app.get("/api/admin/force-test-alarm-emails")
def admin_force_test_alarm_emails():
    """Gerçek verilerle %50 sahte drop yaratarak web, mweb, ios, android mailleri atar."""
    from backend.services.ga4_realtime import (
        fetch_realtime_top_pages_with_app_fallback,
        _send_news_alarm_email,
        get_ga4_credentials_record,
        load_ga4_properties
    )
    from backend.services.mailer import realtime_email_batch_begin, realtime_email_batch_flush
    from backend.models import Site

    with SessionLocal() as db:
        sites = db.query(Site).filter(Site.is_active.is_(True)).all()
        if not sites:
            return {"error": "Aktif site bulunamadı."}

        realtime_email_batch_begin()
        logs = []

        for site in sites:
            logs.append(f"--- {site.domain} ---")
            record = get_ga4_credentials_record(db, site.id)
            properties = load_ga4_properties(record)

            for profile in ("web", "mweb", "ios", "android"):
                prop_id = str(properties.get(profile, "")).strip()
                if not prop_id:
                    logs.append(f"Profil: {profile} için property_id yok.")
                    continue

                try:
                    result = fetch_realtime_top_pages_with_app_fallback(
                        prop_id, profile=profile, window_minutes=15, limit=15, sort_by="activeUsers"
                    )
                    pages = result.get("pages", [])
                    
                    if not pages:
                        logs.append(f"{profile} için aktif sayfa/trafik bulunamadı.")
                        continue

                    fake_alarms = []
                    for p in pages:
                        cur = p.get("activeUsers", 0)
                        if cur == 0: continue
                        prev = cur * 2 # %50 drop
                        pct = ((cur - prev) / prev) * 100
                        fake_alarms.append({
                            "rule_id": "news_traffic_drop",
                            "severity": "critical",
                            "page": p["page"],
                            "profile": profile,
                            "domain": site.domain,
                            "current_users": cur,
                            "previous_users": prev,
                            "change_pct": round(pct, 1),
                            "message": f"{p['page']} — trafik düştü (TEST)",
                        })
                    
                    if fake_alarms:
                        site_kpi = {
                            "current": sum(a["current_users"] for a in fake_alarms),
                            "previous": sum(a["previous_users"] for a in fake_alarms),
                            "change_pct": -50.0
                        }
                        _send_news_alarm_email(site.domain, profile, fake_alarms, site_kpi=site_kpi)
                        logs.append(f"✅ {profile} için test maili sıraya alındı ({len(fake_alarms)} içerik)")

                except Exception as e:
                    logs.append(f"Hata {profile}: {str(e)}")

        realtime_email_batch_flush()
        logs.append("Posta kuyruğu boşaltıldı (Mailler gönderildi).")
        return {"status": "ok", "logs": logs}


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
    result: dict[str, object] = {"status": "ok"}
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT pg_database_size(current_database())")).fetchone()
            result["total_mb"] = round(row[0] / 1024 / 1024, 2) if row else 0

            try:
                wal_row = conn.execute(
                    text("SELECT COALESCE(SUM(size), 0) FROM pg_ls_waldir()")
                ).fetchone()
                result["wal_size_mb"] = round((wal_row[0] or 0) / 1024 / 1024, 2) if wal_row else 0
            except Exception:
                result["wal_size_mb"] = None

            tables = conn.execute(
                text(
                    "SELECT relname, pg_total_relation_size(relid) AS size "
                    "FROM pg_catalog.pg_statio_user_tables ORDER BY size DESC"
                )
            ).fetchall()
            result["tables"] = [
                {"table": t[0], "size_mb": round(t[1] / 1024 / 1024, 2)} for t in tables
            ]
    except Exception as exc:
        logging.exception("admin/db-size sorgusu başarısız")
        return JSONResponse(
            {
                "status": "error",
                "detail": str(exc),
                "hint": "Postgres volume dolu veya recovery modunda olabilir; Railway Postgres restart + volume artır.",
            },
            status_code=503,
        )
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


def _build_sc_service_and_targets(db, site_id: int):
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise ValueError("Site bulunamadi.")

    credential = get_search_console_credentials_record(db, site.id)
    if credential is None:
        raise ValueError("Search Console baglantisi yok.")

    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError("Google Search Console istemcisi yuklu degil.") from exc

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

    service = build("searchconsole", "v1", credentials=credentials, cache_discovery=False)
    targets = _resolve_search_console_targets(service, site)
    return site, service, targets


@app.get("/admin/sc/raw/sites/{site_id}")
def admin_sc_raw_sites(site_id: int):
    """Google Search Console sites.list ham cevabi."""
    db = SessionLocal()
    try:
        site, service, targets = _build_sc_service_and_targets(db, site_id)
        response = service.sites().list().execute()
        return JSONResponse(
            {
                "site_id": site.id,
                "domain": site.domain,
                "resolved_targets": targets,
                "raw": response,
            }
        )
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=400)
    finally:
        db.close()


@app.get("/admin/sc/raw/query/{site_id}")
def admin_sc_raw_query(
    site_id: int,
    start_date: str,
    end_date: str,
    dimensions: str = "query,device",
    row_limit: int = 250,
    start_row: int = 0,
    device: str = "",
    search_type: str = "",
):
    """Google Search Console searchanalytics.query ham cevabi."""
    db = SessionLocal()
    try:
        site, service, targets = _build_sc_service_and_targets(db, site_id)
        dim_list = [d.strip() for d in dimensions.split(",") if d.strip()]
        if not dim_list:
            dim_list = ["query", "device"]
        safe_row_limit = max(1, min(25000, int(row_limit)))
        safe_start_row = max(0, int(start_row))
        safe_device = str(device or "").strip().upper()

        results: list[dict] = []
        for target in targets:
            property_url = str(target.get("property_url") or "")
            body: dict = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": dim_list,
                "rowLimit": safe_row_limit,
                "startRow": safe_start_row,
            }
            if search_type:
                body["type"] = str(search_type)
            if safe_device:
                body["dimensionFilterGroups"] = [
                    {
                        "filters": [
                            {
                                "dimension": "device",
                                "operator": "equals",
                                "expression": safe_device,
                            }
                        ]
                    }
                ]
            raw = (
                service.searchanalytics()
                .query(
                    siteUrl=property_url,
                    body=body,
                )
                .execute()
            )
            results.append(
                {
                    "target_device": target.get("device"),
                    "property_url": property_url,
                    "request_body": body,
                    "raw": raw,
                }
            )

        return JSONResponse(
            {
                "site_id": site.id,
                "domain": site.domain,
                "results": results,
            }
        )
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=400)
    finally:
        db.close()


@app.get("/admin/sc/raw/url-inspection/{site_id}")
def admin_sc_raw_url_inspection(site_id: int, inspection_url: str = "", language_code: str = "tr-TR"):
    """Google URL Inspection API ham cevabi."""
    db = SessionLocal()
    try:
        site, service, targets = _build_sc_service_and_targets(db, site_id)
        property_url = str((targets[0] or {}).get("property_url") or "")
        normalized_inspection_url = inspection_url.strip() or (
            site.domain if str(site.domain).startswith(("http://", "https://")) else f"https://{site.domain}"
        )
        raw = (
            service.urlInspection()
            .index()
            .inspect(
                body={
                    "inspectionUrl": normalized_inspection_url,
                    "siteUrl": property_url,
                    "languageCode": language_code,
                }
            )
            .execute()
        )
        return JSONResponse(
            {
                "site_id": site.id,
                "domain": site.domain,
                "property_url": property_url,
                "inspection_url": normalized_inspection_url,
                "raw": raw,
            }
        )
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"status": "error", "detail": str(exc)}, status_code=400)
    finally:
        db.close()


@app.get("/admin/sc/raw/catalog")
def admin_sc_raw_catalog():
    """SC'den sentetik olmayan yeni sekme/basliklar icin veri katalogu."""
    return JSONResponse(
        {
            "searchanalytics": {
                "metrics": ["clicks", "impressions", "ctr", "position"],
                "dimensions": [
                    "date",
                    "query",
                    "page",
                    "device",
                    "searchAppearance",
                    "type",
                ],
                "ready_tabs": [
                    {
                        "tab_key": "pages",
                        "title": "Top Pages",
                        "dimensions": ["page"],
                        "description": "Landing URL bazinda tiklama/gosterim/CTR/pozisyon dagilimi.",
                    },
                    {
                        "tab_key": "devices",
                        "title": "Devices",
                        "dimensions": ["device"],
                        "description": "Mobile/Desktop ayriminda gercek Search Console verisi.",
                    },
                    {
                        "tab_key": "search_types",
                        "title": "Search Type Split",
                        "dimensions": ["type"],
                        "description": "web/image/video/news kesitleri.",
                    },
                    {
                        "tab_key": "page_query_matrix",
                        "title": "Page x Query Matrix",
                        "dimensions": ["page", "query"],
                        "description": "Hangi query hangi landing page'e trafik tasiyor.",
                    },
                    {
                        "tab_key": "appearance",
                        "title": "Search Appearance",
                        "dimensions": ["searchAppearance"],
                        "description": "Rich result, AMP vb. gorunum tipleri etkisi.",
                    },
                ],
            },
            "url_inspection": {
                "key_fields": [
                    "coverageState",
                    "indexingState",
                    "pageFetchState",
                    "robotsTxtState",
                    "googleCanonical",
                    "userCanonical",
                    "lastCrawlTime",
                    "mobileUsabilityResult.verdict",
                    "richResultsResult.verdict",
                ]
            },
            "notes": [
                "Tum basliklar sentetik degil; Search Console API ham response alanlarina dayanir.",
                "Yeni sekmeleri dogrudan /admin/sc/raw/query/{site_id} endpoint'i ile prototipleyebilirsin.",
            ],
        }
    )


@app.get("/health")
def health_check():
    # Basit sağlık kontrol endpoint'i JSON döner.
    # rev: Yerelde `git pull` sonrası UI değişmiyorsa çoğunlukla Docker imajı yeniden build edilmemiştir (bak: docker-compose.yml).
    return JSONResponse(
        {
            "status": "ok",
            "host": settings.app_host,
            "rev": get_app_revision(),
            "docker": _is_docker_runtime(),
        }
    )


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- GITLAB BOARDS ---

from backend.services.gitlab_board import (
    create_issue_async,
    fetch_board_project_bundle_async,
    fetch_gitlab_version_async,
    fetch_project_board_async,
    get_board_column_orders,
    get_board_project_settings,
    move_issue_async,
    reorder_issue_async,
    save_board_column_order,
    save_board_project_settings,
    sync_columns_order_to_gitlab,
    sync_open_issues_order_to_gitlab,
    update_issue_async,
    normalize_board_move_labels,
)

@app.get("/boards", response_class=HTMLResponse)
def page_boards(request: Request):
    """GitLab Kanban Board ana sayfası."""
    token = os.environ.get("GITLAB_PRIVATE_TOKEN", "")
    projects = [
        {"name": "Döviz Web", "path": "nokta/doviz", "platform": "web", "product": "doviz"},
        {"name": "Döviz iOS", "path": "ios/doviz", "platform": "ios", "product": "doviz"},
        {"name": "Döviz Android", "path": "android/doviz", "platform": "android", "product": "doviz"},
        {"name": "Sinemalar Web", "path": "nokta/sinemalar", "platform": "web", "product": "sinemalar"},
    ]
    return templates.TemplateResponse(
        request, "pages/boards.html",
        context={
            "request": request,
            "projects": projects,
            "token": token,
            "default_board_project": "ios/doviz",
        },
    )

@app.get("/api/boards/content")
async def api_boards_content(request: Request, project_path: str):
    """Belirli bir proje için board ve issue'ları getirir."""
    data = await fetch_project_board_async(project_path)
    return templates.TemplateResponse(
        request, "partials/boards/board_content.html",
        context={"request": request, "data": data, "project_path": project_path}
    )


@app.get("/api/boards/gitlab-ping")
async def api_boards_gitlab_ping():
    """Tarayıcıdan GitLab'e doğrudan gitmek yerine bağlantı kontrolü."""
    return JSONResponse(await fetch_gitlab_version_async())


@app.get("/api/boards/project-bundle")
async def api_boards_project_bundle(
    project_path: str,
    opened_order_by: str = "relative_position",
    opened_sort: str = "asc",
):
    """Kanban: board + sayfalı issue listeleri (CORS/VPN bypass — sunucu proxy)."""
    data = await fetch_board_project_bundle_async(
        project_path,
        opened_order_by=opened_order_by,
        opened_sort=opened_sort,
    )
    if data.get("error"):
        return JSONResponse(data, status_code=502)
    return JSONResponse(data)


@app.post("/api/boards/issues")
async def api_boards_create_issue(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    project_path = str(body.get("project_path") or "").strip()
    title = str(body.get("title") or "").strip()
    labels = body.get("labels")
    labels_str = str(labels).strip() if labels is not None and str(labels).strip() else None
    if not project_path or not title:
        return JSONResponse({"error": "missing_fields"}, status_code=400)
    issue, detail = await create_issue_async(project_path, title, labels=labels_str)
    if issue is None:
        return JSONResponse({"error": detail or "create_failed"}, status_code=502)
    return JSONResponse({"ok": True, "issue": issue})

@app.get("/api/boards/order")
def api_boards_order(project_path: str, db: Session = Depends(get_db)):
    """Kayıtlı board sütun sıraları."""
    orders = get_board_column_orders(db, project_path)
    return JSONResponse({"project_path": project_path, "orders": orders})


@app.put("/api/boards/order")
async def api_boards_save_order(request: Request, db: Session = Depends(get_db)):
    """Sütun içi issue sırasını kalıcı kaydet."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    project_path = str(body.get("project_path") or "").strip()
    list_key = str(body.get("list_key") or "").strip()
    issue_iids = body.get("issue_iids") or []
    if not project_path or not list_key:
        return JSONResponse({"error": "missing_project_or_list"}, status_code=400)
    if not isinstance(issue_iids, list):
        return JSONResponse({"error": "issue_iids_must_be_list"}, status_code=400)

    save_board_column_order(db, project_path, list_key, issue_iids)
    return JSONResponse({"ok": True, "project_path": project_path, "list_key": list_key, "count": len(issue_iids)})


@app.get("/api/boards/sort-settings")
def api_boards_sort_settings(project_path: str, db: Session = Depends(get_db)):
    """Proje için madde sıralama modu."""
    return JSONResponse(get_board_project_settings(db, project_path))


@app.put("/api/boards/sort-settings")
async def api_boards_save_sort_settings(request: Request, db: Session = Depends(get_db)):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    project_path = str(body.get("project_path") or "").strip()
    sort_mode = str(body.get("sort_mode") or "").strip()
    if not project_path:
        return JSONResponse({"error": "missing_project"}, status_code=400)
    saved = save_board_project_settings(db, project_path, sort_mode)
    return JSONResponse({"ok": True, **saved})


@app.post("/api/boards/sync-sort")
async def api_boards_sync_sort(request: Request):
    """Seçili sıralamayı GitLab relative_position olarak uygular (sütun bazlı)."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)
    project_path = str(body.get("project_path") or "").strip()
    columns = body.get("columns")
    ordered = body.get("ordered") or []
    if not project_path:
        return JSONResponse({"error": "missing_project"}, status_code=400)
    if columns is not None:
        if not isinstance(columns, list):
            return JSONResponse({"error": "columns_must_be_list"}, status_code=400)
        result = await sync_columns_order_to_gitlab(project_path, columns)
        return JSONResponse(result)
    if not isinstance(ordered, list):
        return JSONResponse({"error": "ordered_must_be_list"}, status_code=400)
    result = await sync_open_issues_order_to_gitlab(project_path, ordered)
    return JSONResponse(result)


@app.post("/api/boards/reorder")
async def api_boards_reorder(request: Request):
    """GitLab relative_position reorder proxy."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    project_path = str(body.get("project_path") or "").strip()
    issue_iid = body.get("issue_iid")
    if not project_path or issue_iid is None:
        return JSONResponse({"error": "missing_fields"}, status_code=400)

    move_after_id = body.get("move_after_id")
    move_before_id = body.get("move_before_id")
    updated = await reorder_issue_async(
        project_path,
        int(issue_iid),
        move_after_id=int(move_after_id) if move_after_id is not None else None,
        move_before_id=int(move_before_id) if move_before_id is not None else None,
    )
    if updated is None and (move_after_id is not None or move_before_id is not None):
        return JSONResponse({"ok": False, "issue": None}, status_code=200)
    return JSONResponse({"ok": True, "issue": updated})


@app.post("/api/boards/move")
async def api_boards_move(request: Request):
    """Sürükle-bırak: etiket + durum güncellemesi."""
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        project_path = str(body.get("project_path") or "").strip()
        issue_iid = body.get("issue_iid")
        from_label = str(body.get("from_label") or "")
        to_label = str(body.get("to_label") or "")
        remove_labels_raw = body.get("remove_labels")
        state_event = body.get("state_event")
    else:
        form = await request.form()
        project_path = str(form.get("project_path") or "").strip()
        issue_iid_str = form.get("issue_iid")
        if not issue_iid_str:
            return JSONResponse({"error": "missing_issue_iid"}, status_code=400)
        issue_iid = int(issue_iid_str)
        from_label = str(form.get("from_label", ""))
        to_label = str(form.get("to_label", ""))
        remove_labels_raw = form.getlist("remove_labels") if hasattr(form, "getlist") else None
        state_event = form.get("state_event")

    if not project_path or issue_iid is None:
        return JSONResponse({"error": "missing_fields"}, status_code=400)

    remove_labels_list: list[str] | None = None
    if isinstance(remove_labels_raw, list):
        remove_labels_list = [str(x) for x in remove_labels_raw]
    add_labels, remove_labels = normalize_board_move_labels(
        from_label=from_label,
        to_label=to_label,
        remove_labels=remove_labels_list,
    )
    state_ev = str(state_event).strip() if state_event else None
    if state_ev not in ("close", "reopen"):
        state_ev = None

    updated, detail = await update_issue_async(
        project_path,
        int(issue_iid),
        add_labels=add_labels or None,
        remove_labels=remove_labels or None,
        state_event=state_ev,
    )
    if updated is None:
        return JSONResponse(
            {"error": "update_failed", "detail": detail or "GitLab issue güncellenemedi"},
            status_code=502,
        )
    return JSONResponse({"ok": True, "issue": updated})



# ── Policy Center CSV Import ──────────────────────────────────────────────────

@app.get("/policy", response_class=HTMLResponse)
def policy_page(
    request: Request,
    db: Session = Depends(get_db),
):
    from backend.services import policy_csv as pcsv

    try:
        stats = pcsv.get_stats(db)
        last_upload = pcsv.get_latest_upload(db)
        # Son CSV upload'ından önce eklenmiş satırları belirlemek için 60 sn tolerans
        # (import_rows save_csv_blob'dan birkaç ms önce çalışır).
        new_threshold = None
        if last_upload and last_upload.uploaded_at:
            new_threshold = last_upload.uploaded_at - timedelta(seconds=60)
        violations = pcsv.get_violations(
            db, status="all", category="all", order_by="ad_requests",
            new_threshold=new_threshold,
        )
        title_job = pcsv.get_title_job_state()
    except Exception as _e:
        LOGGER.exception("policy_page hata: %s", _e)
        stats = {"total": 0, "new": 0, "with_title": 0, "without_title": 0,
                 "total_ad_requests_7d": 0, "last_fetch": None, "by_category": {}, "by_status": {}}
        violations = []
        last_upload = None
        title_job = {"running": False, "done": 0, "total": 0}

    last_upload_info = None
    last_upload_iso = None
    if last_upload:
        last_upload_info = {
            "filename": last_upload.filename,
            "row_count": last_upload.row_count,
            "new_count": last_upload.new_count,
            "updated_count": last_upload.updated_count,
            "uploaded_at": last_upload.uploaded_at.strftime("%Y-%m-%d %H:%M") if last_upload.uploaded_at else "",
        }
        if last_upload.uploaded_at:
            # Yeni satırları işaretlemek için ISO timestamp (template karşılaştırmasında kullanılır)
            last_upload_iso = last_upload.uploaded_at.isoformat()

    ctx = {
        "request": request,
        "stats": stats,
        "violations": violations,
        "last_upload": last_upload_info,
        "last_upload_iso": last_upload_iso,
        "title_job": title_job,
    }
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse("partials/policy_content.html", ctx)
    return templates.TemplateResponse("policy.html", ctx)


@app.post("/api/policy/upload")
async def api_policy_upload(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """CSV yükle → DB'ye UPSERT et → arka planda sayfa başlıklarını çek."""
    from backend.services import policy_csv as pcsv

    content = await file.read()
    if not content:
        return JSONResponse({"ok": False, "error": "Boş dosya."}, status_code=400)
    if len(content) > 20 * 1024 * 1024:
        return JSONResponse({"ok": False, "error": "Dosya 20MB'dan büyük."}, status_code=400)

    rows, headers, err = pcsv.parse_csv(content)
    if err:
        return JSONResponse({"ok": False, "error": err, "headers": headers}, status_code=400)
    if not rows:
        return JSONResponse({"ok": False, "error": "CSV'de işlenebilir satır yok."}, status_code=400)

    new_count, upd_count = pcsv.import_rows(db, rows)
    pcsv.save_csv_blob(
        db, filename=file.filename or "policy.csv", content=content,
        row_count=len(rows), new_count=new_count, updated_count=upd_count,
    )

    # Arka planda eksik sayfa başlıklarını çek
    pcsv.start_title_job(SessionLocal, only_missing=True)

    return JSONResponse({
        "ok": True,
        "row_count": len(rows),
        "new_count": new_count,
        "updated_count": upd_count,
        "title_job_started": True,
    })


@app.post("/api/policy/fetch-titles")
def api_policy_fetch_titles(only_missing: bool = True):
    """Tüm satırlar için sayfa başlıklarını arka planda yeniden çek."""
    from backend.services import policy_csv as pcsv
    started = pcsv.start_title_job(SessionLocal, only_missing=only_missing)
    return JSONResponse({"ok": True, "started": started, "state": pcsv.get_title_job_state()})


@app.get("/api/policy/fetch-titles/status")
def api_policy_fetch_titles_status():
    from backend.services import policy_csv as pcsv
    return JSONResponse(pcsv.get_title_job_state())


@app.post("/api/policy/violations/{vid}/fetch-title")
def api_policy_fetch_single_title(vid: int, db: Session = Depends(get_db)):
    from backend.services import policy_csv as pcsv
    title = pcsv.refresh_single_title(db, vid)
    if title is None:
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    return JSONResponse({"ok": True, "page_title": title})


@app.get("/api/policy/export.xlsx")
def api_policy_export(db: Session = Depends(get_db)):
    from backend.services import policy_csv as pcsv
    violations = pcsv.get_violations(db, status="all", category="all", order_by="ad_requests", limit=10000)
    blob = pcsv.build_xlsx(violations)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return Response(
        content=blob,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="policy_ihlalleri_{today}.xlsx"',
        },
    )


@app.get("/api/policy/last-csv")
def api_policy_last_csv(db: Session = Depends(get_db)):
    """Son yüklenen CSV'yi indir."""
    from backend.services import policy_csv as pcsv
    upload = pcsv.get_latest_upload(db)
    if not upload:
        return JSONResponse({"ok": False, "error": "Henüz CSV yüklenmemiş."}, status_code=404)
    return Response(
        content=upload.content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{upload.filename}"',
        },
    )


# ── ProjectControl AI Ajan ────────────────────────────────────────────────────

@app.get("/api/agent/test")
def api_agent_test():
    """LLM sağlayıcı bağlantı testi (Gemini listesi + Groq/OpenAI ping)."""
    import httpx
    from backend.config import settings
    from backend.services.llm_provider_chain import agent_provider_try_chain, llm_provider_keys

    keys = llm_provider_keys()
    chain = agent_provider_try_chain()
    out: dict[str, Any] = {
        "keys": keys,
        "agent_failover_chain": [{"provider": p, "model": m} for p, m in chain],
        "gemini": {"ok": False},
        "groq": {"ok": False},
        "openai": {"ok": False},
    }
    if not any(keys.values()):
        out["ok"] = False
        out["error"] = "GROQ_API_KEY, GEMINI_API_KEY veya OPENAI_API_KEY gerekli"
        return JSONResponse(out)

    gkey = (settings.gemini_api_key or "").strip()
    if gkey:
        try:
            r = httpx.get(
                f"https://generativelanguage.googleapis.com/v1beta/models?key={gkey}",
                timeout=30,
            )
            data = r.json()
            if r.status_code == 200:
                models = [
                    m["name"]
                    for m in data.get("models", [])
                    if "generateContent" in m.get("supportedGenerationMethods", [])
                ]
                out["gemini"] = {"ok": True, "available_models": models[:12]}
            else:
                out["gemini"] = {"ok": False, "status": r.status_code, "error": data}
        except Exception as e:
            out["gemini"] = {"ok": False, "error": str(e)}

    groq_k = (settings.groq_api_key or "").strip()
    if groq_k:
        try:
            model = (settings.ai_daily_brief_groq_model or "llama-3.3-70b-versatile").strip()
            r = httpx.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {groq_k}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 8,
                },
                timeout=30,
            )
            out["groq"] = {"ok": r.status_code < 400, "status": r.status_code}
            if r.status_code >= 400:
                out["groq"]["error"] = r.text[:200]
        except Exception as e:
            out["groq"] = {"ok": False, "error": str(e)}

    oai_k = (settings.openai_api_key or "").strip()
    if oai_k:
        try:
            model = (settings.ai_daily_brief_openai_model or "gpt-4.1-mini").strip()
            r = httpx.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {oai_k}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": "ping"}],
                    "max_tokens": 8,
                },
                timeout=30,
            )
            out["openai"] = {"ok": r.status_code < 400, "status": r.status_code}
            if r.status_code >= 400:
                out["openai"]["error"] = r.text[:200]
        except Exception as e:
            out["openai"] = {"ok": False, "error": str(e)}

    out["ok"] = bool(chain) and any(
        out.get(name, {}).get("ok") for name in ("groq", "gemini", "openai") if keys.get(name)
    )
    return JSONResponse(out)

@app.post("/api/agent/chat")
async def api_agent_chat(request: Request):
    """AI ajan SSE endpoint — streaming tool-use yanıtı."""
    import asyncio
    from starlette.responses import StreamingResponse as StarletteStreamingResponse
    from backend.services.project_agent import stream_agent_response

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Geçersiz JSON gövdesi."}, status_code=400)

    messages = body.get("messages", [])
    if not messages or not isinstance(messages, list):
        return JSONResponse({"error": "messages dizisi gerekli."}, status_code=400)

    # Sadece role/content kabul et
    clean_messages = []
    for m in messages[-20:]:  # Max 20 mesaj geçmişi
        role = m.get("role", "")
        content = m.get("content", "")
        if role in ("user", "assistant") and content:
            clean_messages.append({"role": role, "content": str(content)[:8000]})

    if not clean_messages or clean_messages[-1]["role"] != "user":
        return JSONResponse({"error": "Son mesaj user rolünde olmalı."}, status_code=400)

    page_context = body.get("page_context")
    if page_context is not None and not isinstance(page_context, dict):
        page_context = None
    if isinstance(page_context, dict):
        # Boyut sınırı — token patlamasını önle
        import json as _json
        try:
            if len(_json.dumps(page_context, ensure_ascii=False, default=str)) > 16000:
                page_context = {
                    k: page_context.get(k)
                    for k in ("path", "page_id", "label", "title", "query", "filters", "custom")
                    if k in page_context
                }
        except Exception:
            page_context = {"path": str(page_context.get("path", ""))[:200]}

    # Bağlam hafızası: frontend az mesaj gönderiyorsa (sayfa yenileme) DB'den tamamla
    session_id = (body.get("session_id") or "").strip()
    if session_id and len(clean_messages) <= 2:
        from backend.services.ai_talk_history_auth import is_ai_talk_history_authenticated
        from backend.services.agent_tools import ai_talk_get_messages

        if is_ai_talk_history_authenticated(request):
            history = ai_talk_get_messages(session_id)
            if history and len(history) > len(clean_messages):
                # Geçmiş mesajları + mevcut yeni mesajı birleştir
                # Son mesaj zaten clean_messages'da, geçmişe eklenmesin
                prefix = [m for m in history if m not in clean_messages]
                clean_messages = (prefix + clean_messages)[-30:]

    return StarletteStreamingResponse(
        stream_agent_response(clean_messages, session_id=session_id, page_context=page_context),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/agent/history/{session_id}")
def api_agent_history(request: Request, session_id: str):
    """Sayfa yenileme sonrası sohbet geçmişini döner."""
    from backend.services.ai_talk_history_auth import (
        ai_talk_history_password_configured,
        is_ai_talk_history_authenticated,
    )
    from backend.services.agent_tools import ai_talk_get_messages

    if ai_talk_history_password_configured() and not is_ai_talk_history_authenticated(request):
        messages = ai_talk_get_messages(session_id)
        if messages:
            return JSONResponse(
                {"messages": [], "count": 0, "requires_auth": True},
                status_code=403,
            )
        return JSONResponse({"messages": [], "count": 0, "requires_auth": True})
    messages = ai_talk_get_messages(session_id)
    return JSONResponse({"messages": messages, "count": len(messages), "requires_auth": False})


@app.delete("/api/agent/history/{session_id}")
def api_agent_history_clear(request: Request, session_id: str):
    """Sohbet geçmişini siler."""
    from backend.services.ai_talk_history_auth import require_ai_talk_history_auth
    from backend.services.agent_tools import ai_talk_clear_messages

    require_ai_talk_history_auth(request)
    return JSONResponse(ai_talk_clear_messages(session_id))


@app.post("/api/agent/history-auth")
async def api_agent_history_auth(request: Request):
    """AI Talk geçmişi için Settings / Inbox şifresi doğrulama."""
    from backend.services.ai_talk_history_auth import (
        AI_TALK_HISTORY_AUTH_COOKIE,
        ai_talk_history_password_configured,
        issue_ai_talk_history_cookie_token,
        verify_ai_talk_history_password,
    )

    if not ai_talk_history_password_configured():
        return JSONResponse({"ok": True, "password_required": False})

    password = ""
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            body = await request.json()
            password = str((body or {}).get("password") or "")
        except Exception:
            password = ""
    else:
        form = await request.form()
        password = str(form.get("password") or "")

    if not verify_ai_talk_history_password(password):
        return JSONResponse({"ok": False, "error": "Yanlış şifre"}, status_code=401)

    token = issue_ai_talk_history_cookie_token()
    resp = JSONResponse({"ok": True, "password_required": True})
    resp.set_cookie(
        key=AI_TALK_HISTORY_AUTH_COOKIE,
        value=token,
        httponly=True,
        secure=_admin_auth_cookie_secure(request),
        samesite="lax",
        max_age=60 * 60 * 8,
        path="/",
    )
    return resp


@app.get("/api/agent/history-auth/status")
def api_agent_history_auth_status(request: Request):
    """AI Talk geçmişi şifre koruması durumu."""
    from backend.services.ai_talk_history_auth import (
        ai_talk_history_password_configured,
        is_ai_talk_history_authenticated,
    )

    configured = ai_talk_history_password_configured()
    return JSONResponse(
        {
            "password_required": configured,
            "authenticated": is_ai_talk_history_authenticated(request),
        }
    )


@app.get("/api/agent/alerts")
def api_agent_alerts():
    """Okunmamış proaktif uyarıları döner."""
    from backend.services.agent_tools import get_unread_alerts
    alerts = get_unread_alerts(limit=20)
    return JSONResponse({"alerts": alerts, "count": len(alerts)})


@app.post("/api/agent/alerts/{alert_id}/read")
def api_agent_alert_read(alert_id: int):
    """Uyarıyı okundu işaretler."""
    from backend.services.agent_tools import mark_alert_read
    return JSONResponse(mark_alert_read(alert_id))
