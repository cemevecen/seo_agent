"""FastAPI uygulama giriş noktası."""
import json
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from ipaddress import ip_address, ip_network
from pathlib import Path

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
from backend.collectors.search_console import get_top_queries
from backend.collectors.search_console import collect_search_console_metrics
from backend.collectors.url_inspection import collect_url_inspection
from backend.config import settings
from backend.database import SessionLocal, init_db
from backend.models import PageSpeedPayloadSnapshot, Site
from backend.rate_limiter import limiter
from backend.services.alert_engine import ensure_site_alerts, get_alert_rules, get_recent_alerts
from backend.services.metric_store import get_latest_metrics, get_metric_history
from backend.services.quota_guard import get_quota_status
from backend.services.search_console_auth import build_oauth_flow, decode_oauth_state, delete_oauth_credentials, encode_oauth_state, get_search_console_connection_status, oauth_is_configured, save_oauth_credentials
from backend.services.pagespeed_analyzer import analyze_pagespeed_alerts
from backend.services.pagespeed_detailed import analyze_pagespeed_detailed
from backend.services.lighthouse_analyzer import get_lighthouse_analysis
from backend.services.warehouse import (
    get_latest_crux_snapshot,
    get_latest_search_console_rows,
    get_latest_url_inspection_snapshot,
    get_site_warehouse_summary,
)

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

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
        return "Henuz veri yok"
    return metric.collected_at.strftime("%d.%m.%Y %H:%M")


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
    init_db()


def get_sidebar_sites() -> list[dict]:
    # Sidebar için aktif siteler veritabanından okunur.
    with SessionLocal() as db:
        sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.created_at.desc()).all()
        return [{"domain": site.domain, "label": site.display_name} for site in sites]


def _settings_sites_payload(db) -> list[dict]:
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    rows: list[dict] = []
    for site in sites:
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
) -> dict[str, dict]:
    if not settings.live_refresh_enabled:
        return {}
    results: dict[str, dict] = {}
    if include_pagespeed:
        try:
            results["pagespeed"] = collect_pagespeed_metrics(db, site)
        except Exception as exc:  # noqa: BLE001
            results["pagespeed"] = {"errors": {"exception": str(exc)}}
    if include_crawler:
        try:
            results["crawler"] = collect_crawler_metrics(db, site)
        except Exception as exc:  # noqa: BLE001
            results["crawler"] = {"errors": {"exception": str(exc)}}
    if include_search_console:
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
                "title": "Dokunulabilirlik ve okunabilirlik iyilestirilmeli" if scope == "mobile" else "Masaustu okunabilirlik iyilestirilmeli",
                "category": "Accessibility",
                "priority": "HIGH" if accessibility_score < 85 else "MEDIUM",
                "problem": (
                    f"{scope_label} audit skorunda erisilebilirlik {accessibility_score}. "
                    + (
                        "Mobil ekranda dokunma alanlari, kucuk metinler veya kontrast sorunlari olasi gorunuyor."
                        if scope == "mobile"
                        else "Buyuk ekran duzeninde kontrast, tablo yogunlugu veya odak gorunurlugu sorunlari olasi gorunuyor."
                    )
                ),
                "impact": f"{scope_label} kullanicilarinda okunabilirlik ve etkileşim kalitesi dusebilir.",
                "solution": [],
                "expected_result": f"{scope_label} erisilebilirlik skorunda artis beklenir.",
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
                "title": "Mobil yukleme pratikleri optimize edilmeli" if scope == "mobile" else "Desktop bundle ve tarayici davranisi optimize edilmeli",
                "category": "Best Practices",
                "priority": "HIGH" if practices_score < 75 else "MEDIUM",
                "problem": (
                    "Mobil tarafta agir gorsel/script kullanimi ya da gereksiz kaynaklar gorunuyor."
                    if scope == "mobile"
                    else "Desktop tarafta gereksiz bundle, console warning ya da tarayici uyumluluk konusu gorunuyor."
                ),
                "impact": f"{scope_label} best practices skoru {practices_score} seviyesinde kalir.",
                "solution": [],
                "expected_result": f"{scope_label} icin daha stabil sayfa davranisi.",
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
                "title": "Mobil SERP gorunumu duzenlenmeli" if scope == "mobile" else "Desktop SEO sinyalleri guclendirilmeli",
                "category": "SEO",
                "priority": "LOW",
                "problem": (
                    "Mobil arama sonucunda baslik veya snippet gorunumu iyilestirilebilir."
                    if scope == "mobile"
                    else "Desktop arama sonucunda teknik sinyaller daha da guclendirilebilir."
                ),
                "impact": f"{scope_label} SEO skoru {seo_score}.",
                "solution": [],
                "expected_result": f"{scope_label} SEO skorunda marjinal artis beklenir.",
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
        "summary": f"{scope_label} fallback audit ozeti",
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


def _pagespeed_strategy_status(latest: dict[str, object], strategy: str, alert_messages: list[str]) -> dict[str, object]:
    metric = latest.get(f"pagespeed_{strategy}_score")
    has_metric = metric is not None
    is_stale = _metric_is_stale(latest, f"pagespeed_{strategy}_score") if has_metric else True
    has_fetch_error = any(f"{strategy} PageSpeed" in message for message in alert_messages)

    if has_metric and not is_stale and not has_fetch_error:
        state = "live"
        label = "Live"
        badge_class = "border-emerald-200 bg-emerald-50 text-emerald-700"
        description = "Canli ve guncel veri"
    elif has_metric:
        state = "stale"
        label = "Stale"
        badge_class = "border-amber-200 bg-amber-50 text-amber-800"
        description = "Son basarili olcum gosteriliyor"
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
        label = "Stale"
        badge_class = "border-amber-200 bg-amber-50 text-amber-800"
        description = "Son basarili Search Console snapshot'i"
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
            "label": "Stale",
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
        mobile_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="mobile")
        desktop_crux = get_latest_crux_snapshot(db, site_id=site.id, form_factor="desktop")
        mobile_pagespeed_current = _latest_pagespeed_field_metrics(db, site.id, "mobile")
        desktop_pagespeed_current = _latest_pagespeed_field_metrics(db, site.id, "desktop")
        inspection = get_latest_url_inspection_snapshot(db, site_id=site.id)

        mobile_state = _data_state_badge(
            "live" if mobile_crux else "failed",
            "CrUX guncel kaydi ve history serisi mevcut",
            "Son başarılı CrUX snapshot gösteriliyor",
            "CrUX history verisi henüz yok",
        )
        desktop_state = _data_state_badge(
            "live" if desktop_crux else "failed",
            "CrUX guncel kaydi ve history serisi mevcut",
            "Son başarılı CrUX snapshot gösteriliyor",
            "CrUX history verisi henüz yok",
        )
        inspection_state = _data_state_badge(
            "live" if inspection else "failed",
            "URL Inspection snapshot mevcut",
            "Son başarılı URL Inspection snapshot gösteriliyor",
            "URL Inspection verisi henüz yok",
        )

        return {
            "site_name": f"Data Explorer - {site.display_name}",
            "sites": get_sidebar_sites(),
            "domain": site.domain,
            "warehouse_summary": warehouse,
            "crux_mobile": mobile_crux,
            "crux_desktop": desktop_crux,
            "crux_mobile_series": _format_crux_series(mobile_crux, mobile_pagespeed_current),
            "crux_desktop_series": _format_crux_series(desktop_crux, desktop_pagespeed_current),
            "url_inspection": inspection,
            "crux_mobile_status": mobile_state,
            "crux_desktop_status": desktop_state,
            "url_inspection_status": inspection_state,
        }


def _dashboard_cards() -> list[dict]:
    # Dashboard için tüm site kartı özetlerini üretir.
    with SessionLocal() as db:
        sites = db.query(Site).order_by(Site.created_at.desc()).all()
        return [_build_dashboard_card(db, site) for site in sites]


def _build_dashboard_card(db, site: Site, flash_message: str | None = None) -> dict:
    ensure_site_alerts(db, site)
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    pagespeed_metric = latest.get("pagespeed_mobile_score") or latest.get("pagespeed_desktop_score")
    crawler_checks = [
        latest.get("crawler_robots_accessible"),
        latest.get("crawler_sitemap_exists"),
        latest.get("crawler_schema_found"),
        latest.get("crawler_canonical_found"),
    ]
    available_metrics = [metric for metric in latest.values()]
    last_updated = max((metric.collected_at for metric in available_metrics), default=site.created_at)
    pagespeed_score = float(pagespeed_metric.value) if pagespeed_metric else 0.0
    recent_site_alerts = [alert for alert in get_recent_alerts(db, limit=20) if alert["domain"] == site.domain][:5]
    pagespeed_status_alerts = [
        alert["message"]
        for alert in recent_site_alerts
        if alert["alert_type"] in {"pagespeed_mobile_fetch_error", "pagespeed_desktop_fetch_error"}
    ]
    mobile_status = _pagespeed_strategy_status(latest, "mobile", pagespeed_status_alerts)
    desktop_status = _pagespeed_strategy_status(latest, "desktop", pagespeed_status_alerts)
    return {
        "id": site.id,
        "domain": site.domain,
        "pagespeed_score": round(pagespeed_score),
        "pagespeed_color": _score_color(pagespeed_score),
        "crawler_ok": all(metric and metric.value >= 1 for metric in crawler_checks if metric is not None),
        "check_count": len(available_metrics),
        "last_updated": last_updated.strftime("%d.%m.%Y %H:%M"),
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
        parts.append("PageSpeed olcumu tamamlandi")
    elif pagespeed_result.get("errors"):
        parts.append("PageSpeed kismi olarak guncellendi")

    if crawler_result.get("metrics"):
        parts.append("crawler kontrolleri yenilendi")

    if search_console_result.get("blocked"):
        parts.append("Search Console kota nedeniyle atlandi")
    elif search_console_result.get("summary"):
        parts.append("Search Console verisi yenilendi")

    return ". ".join(parts) + "." if parts else "Olcum tetiklendi."


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
        if settings.live_refresh_enabled and (
            not _pagespeed_strategy_is_complete(latest, "mobile")
            or not _pagespeed_strategy_is_complete(latest, "desktop")
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
    period, period_days = _resolve_period(request.query_params.get("period"))
    payload = {
        "site_name": "SEO Agent Dashboard",
        "sites": get_sidebar_sites(),
        "period": period,
        "site_cards": _dashboard_cards(),
        "comparison_data": _dashboard_comparison_data(period_days),
        "trend_data": _dashboard_trend_data(period, period_days),
    }
    with SessionLocal() as db:
        payload["recent_alerts"] = get_recent_alerts(db, limit=6)
    return templates.TemplateResponse(request, "dashboard.html", context={"request": request, **payload})


@app.post("/dashboard/cards/{site_id}/measure", response_class=HTMLResponse)
def dashboard_measure_site(request: Request, site_id: int):
    period, _ = _resolve_period(request.query_params.get("period"))
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

        results = {
            "pagespeed": collect_pagespeed_metrics(db, site),
            "crawler": collect_crawler_metrics(db, site),
            "search_console": collect_search_console_metrics(db, site),
        }
        card = _build_dashboard_card(db, site, flash_message=_summarize_manual_measurement(results))
    return templates.TemplateResponse(
        request,
        "partials/dashboard_site_card.html",
        context={"request": request, "card": card, "period": period},
    )


@app.get("/site/{domain}", response_class=HTMLResponse)
def site_detail(request: Request, domain: str):
    # Site detay ekranını HTMX ve tam sayfa modunda sunar.
    period, period_days = _resolve_period(request.query_params.get("period"))
    try:
        payload = _site_detail_context(domain, period, period_days)
    except ValueError:
        return HTMLResponse("Site bulunamadı.", status_code=404)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "partials/site_detail_content.html", context={"request": request, **payload})
    return templates.TemplateResponse(request, "site_detail.html", context={"request": request, **payload})


@app.get("/data-explorer/{domain}", response_class=HTMLResponse)
def data_explorer(request: Request, domain: str):
    try:
        payload = _data_explorer_context(domain)
    except ValueError:
        return HTMLResponse("Site bulunamadı.", status_code=404)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "partials/data_explorer_content.html", context={"request": request, **payload})
    return templates.TemplateResponse(request, "data_explorer.html", context={"request": request, **payload})


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
def api_get_live_lighthouse_scores(domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

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
def api_get_live_lighthouse_scores_by_strategy(domain: str, strategy: str):
    normalized_strategy = (strategy or "").strip().lower()
    if normalized_strategy not in {"mobile", "desktop"}:
        return JSONResponse({"error": "Invalid strategy"}, status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

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
                "scores": {
                    "performance": _build_lighthouse_score("performance", "Performance", "Performans", scores.get("performance", 0.0), normalized_strategy),
                    "accessibility": _build_lighthouse_score("accessibility", "Accessibility", "Erişilebilirlik", scores.get("accessibility", 0.0), normalized_strategy),
                    "practices": _build_lighthouse_score("practices", "Best Practices", "En İyi Uygulamalar", scores.get("best_practices", 0.0), normalized_strategy),
                    "seo": _build_lighthouse_score("seo", "SEO", "Arama Motoru", scores.get("seo", 0.0), normalized_strategy),
                },
            }
        )


@app.post("/api/site/{domain}/data-explorer/refresh")
def api_refresh_data_explorer(domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        results = {
            "crux_history": collect_crux_history(db, site),
            "url_inspection": collect_url_inspection(db, site),
        }
        db.commit()
        return JSONResponse(
            {
                "site": site.domain,
                "refreshed": True,
                "results": results,
                "warehouse": get_site_warehouse_summary(db, site_id=site.id),
            }
        )


@app.post("/api/site/{domain}/refresh")
def api_refresh_site_metrics(domain: str):
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.domain == domain).first()
        if site is None:
            return JSONResponse({"error": "Site not found"}, status_code=404)

        results = _refresh_site_detail_measurements(
            db,
            site,
            include_pagespeed=True,
            include_crawler=True,
            include_search_console=True,
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
        }
    return templates.TemplateResponse(request, "alerts.html", context={"request": request, **payload})


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


@app.get("/api/search-console/oauth/start/{site_id}")
def search_console_oauth_start(site_id: int):
    if not oauth_is_configured():
        return HTMLResponse("Google OAuth ayarlari eksik. GOOGLE_CLIENT_ID ve GOOGLE_CLIENT_SECRET gerekli.", status_code=400)

    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return HTMLResponse("Site bulunamadi.", status_code=404)

    state = encode_oauth_state(site_id)
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
    return RedirectResponse("/settings", status_code=302)


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
