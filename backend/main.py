"""FastAPI uygulama giriş noktası."""
import os
from ipaddress import ip_address, ip_network
from pathlib import Path

# Localhost development için insecure OAuth transport'u allow et
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi import _rate_limit_exceeded_handler
import requests

from backend.api.alerts import router as alerts_router
from backend.api.metrics import router as metrics_router
from backend.api.sites import router as sites_router
from backend.collectors.crawler import collect_crawler_metrics
from backend.collectors.pagespeed import collect_pagespeed_metrics
from backend.collectors.search_console import get_top_queries
from backend.collectors.search_console import collect_search_console_metrics
from backend.config import settings
from backend.database import SessionLocal, init_db
from backend.models import Site
from backend.rate_limiter import limiter
from backend.services.alert_engine import ensure_site_alerts, get_alert_rules, get_recent_alerts
from backend.services.metric_store import get_latest_metrics, get_metric_history
from backend.services.quota_guard import get_quota_status
from backend.services.search_console_auth import build_oauth_flow, decode_oauth_state, delete_oauth_credentials, encode_oauth_state, get_search_console_connection_status, oauth_is_configured, save_oauth_credentials
from backend.services.technical_seo import run_technical_seo_audit
from backend.services.pagespeed_analyzer import analyze_pagespeed_alerts
from backend.services.pagespeed_detailed import analyze_pagespeed_detailed

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
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
    return {
        "id": site.id,
        "domain": site.domain,
        "pagespeed_score": round(pagespeed_score),
        "pagespeed_color": _score_color(pagespeed_score),
        "crawler_ok": all(metric and metric.value >= 1 for metric in crawler_checks if metric is not None),
        "check_count": len(available_metrics),
        "last_updated": last_updated.strftime("%d.%m.%Y %H:%M"),
        "pagespeed_status": {
            "mobile_updated_at": _format_metric_timestamp(latest.get("pagespeed_mobile_score")),
            "desktop_updated_at": _format_metric_timestamp(latest.get("pagespeed_desktop_score")),
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
        history = get_metric_history(db, site.id, days=period_days)
        top_queries = get_top_queries(db, site, limit=50)

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

        # Teknik SEO kontrolleri otomatik yapıl
        page_html = ""
        try:
            resp = requests.get(f"https://{site.domain}", timeout=10)
            page_html = resp.text if resp.status_code == 200 else ""
        except:
            pass
        
        crawler_checks = run_technical_seo_audit(site.domain, page_html)
        
        # PageSpeed comprehensive analysis
        pagespeed_analysis = analyze_pagespeed_detailed(int(mobile_score), int(desktop_score))
        
        recent_site_alerts = [alert for alert in get_recent_alerts(db, limit=20) if alert["domain"] == site.domain][:5]
        pagespeed_status_alerts = [
            alert["message"]
            for alert in recent_site_alerts
            if alert["alert_type"] in {"pagespeed_mobile_fetch_error", "pagespeed_desktop_fetch_error"}
        ]

        return {
            "site_name": site.display_name,
            "sites": get_sidebar_sites(),
            "domain": site.domain,
            "period": period,
            "mobile_score": mobile_score,
            "mobile_color": _score_color(mobile_score),
            "mobile_lcp": float((latest.get("pagespeed_mobile_lcp").value if latest.get("pagespeed_mobile_lcp") else 0.0)),
            "mobile_cls": float((latest.get("pagespeed_mobile_cls").value if latest.get("pagespeed_mobile_cls") else 0.0)),
            "mobile_inp": float((latest.get("pagespeed_mobile_inp").value if latest.get("pagespeed_mobile_inp") else 0.0)),
            "desktop_score": desktop_score,
            "desktop_color": _score_color(desktop_score),
            "desktop_lcp": float((latest.get("pagespeed_desktop_lcp").value if latest.get("pagespeed_desktop_lcp") else 0.0)),
            "desktop_cls": float((latest.get("pagespeed_desktop_cls").value if latest.get("pagespeed_desktop_cls") else 0.0)),
            "desktop_inp": float((latest.get("pagespeed_desktop_inp").value if latest.get("pagespeed_desktop_inp") else 0.0)),
            "pagespeed_status": {
                "mobile_updated_at": _format_metric_timestamp(latest.get("pagespeed_mobile_score")),
                "desktop_updated_at": _format_metric_timestamp(latest.get("pagespeed_desktop_score")),
                "messages": pagespeed_status_alerts,
            },
            "crawler_checks": crawler_checks,
            "pagespeed_analysis": pagespeed_analysis,
            "site_alerts": recent_site_alerts,
            "top_queries": top_queries,
            "search_summary": {
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
            },
            "trend_data": {
                "labels": trend_labels,
                "mobile": mobile_trend,
                "desktop": desktop_trend,
            },
            "search_trend_data": {
                "labels": search_trend_labels,
                "clicks": [item["value"] for item in search_clicks_history],
                "impressions": [item["value"] for item in search_impressions_history],
                "avg_ctr": [item["value"] for item in search_ctr_history],
                "avg_position": [item["value"] for item in search_position_history],
            },
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
    return templates.TemplateResponse("dashboard.html", {"request": request, **payload})


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
        "partials/dashboard_site_card.html",
        {"request": request, "card": card, "period": period},
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
        return templates.TemplateResponse("partials/site_detail_content.html", {"request": request, **payload})
    return templates.TemplateResponse("site_detail.html", {"request": request, **payload})


@app.get("/alerts")
def alerts_page(request: Request):
    # Son alarm kayıtlarını listeler.
    with SessionLocal() as db:
        payload = {
            "site_name": "Uyarılar",
            "sites": get_sidebar_sites(),
            "recent_alerts": get_recent_alerts(db, limit=30),
        }
    return templates.TemplateResponse("alerts.html", {"request": request, **payload})


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
    return templates.TemplateResponse("settings.html", {"request": request, **payload})


@app.get("/settings/site-list")
def settings_site_list(request: Request):
    # HTMX istekleri için sadece site listesini döner.
    with SessionLocal() as db:
        sites = _settings_sites_payload(db)
        return templates.TemplateResponse(
            "partials/site_list.html",
            {"request": request, "sites": sites, "oauth_ready": oauth_is_configured()},
        )


@app.get("/settings/alert-thresholds")
def settings_alert_thresholds(request: Request):
    # HTMX ile alert threshold tablosunu yeniler.
    with SessionLocal() as db:
        alert_rules = get_alert_rules(db)
    return templates.TemplateResponse("partials/alert_thresholds.html", {"request": request, "alert_rules": alert_rules})


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
        {"request": request, "sites": sites, "oauth_ready": oauth_is_configured()},
    )


@app.get("/health")
def health_check():
    # Basit sağlık kontrol endpoint'i JSON döner.
    return JSONResponse({"status": "ok", "host": settings.app_host})
