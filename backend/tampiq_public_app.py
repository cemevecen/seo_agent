"""TAMPIQ public Support / Privacy site (Apple App Store Connect + Google Play).

Isolated ASGI app — no ProjectControl routes, auth, DB, or navigation.

Railway service:
  PUBLIC_TAMPIQ_SITE=true
  uvicorn backend.tampiq_public_app:app --host 0.0.0.0 --port $PORT
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from backend.tampiq_i18n import get_catalog
from backend.tampiq_i18n.locales import (
    COOKIE_NAME,
    DEFAULT_LOCALE,
    locale_info,
    locale_selector_items,
    resolve_locale,
)

_ROOT = Path(__file__).resolve().parent.parent
_TEMPLATES = Jinja2Templates(directory=str(_ROOT / "templates" / "tampiq"))

_TRUE = {"1", "true", "yes", "on"}
_DEFAULT_PUBLIC_EMAIL = "netbaboli@gmail.com"
_BLOCKED_PUBLIC_EMAILS = {
    "cemevecen@gmail.com",
    "support@tampiq.app",
}
# Reviewed Android legal/support copy exists only for these locales.
_ANDROID_LOCALES = frozenset({"en-US", "tr"})
_SUPPORTED_PLATFORMS = frozenset({"ios", "android"})


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _truthy(name: str) -> bool:
    return _env(name).lower() in _TRUE


def _public_email(name: str) -> str:
    """Public Help Center contact — never expose personal/legacy mailboxes."""
    raw = _env(name, _DEFAULT_PUBLIC_EMAIL)
    if not raw or raw.lower() in _BLOCKED_PUBLIC_EMAILS:
        return _DEFAULT_PUBLIC_EMAIL
    return raw


def _site_identity() -> dict[str, str]:
    support_email = _public_email("TAMPIQ_SUPPORT_EMAIL")
    privacy_email = _public_email("TAMPIQ_PRIVACY_EMAIL")
    if privacy_email.lower() in _BLOCKED_PUBLIC_EMAILS:
        privacy_email = support_email
    return {
        "app_name": _env("TAMPIQ_APP_NAME", "TAMPIQ"),
        "company_name": _env("TAMPIQ_COMPANY_NAME", "TAMPIQ"),
        "support_email": support_email,
        "privacy_email": privacy_email,
        "site_url": _env("TAMPIQ_PUBLIC_BASE_URL", "").rstrip("/"),
        "publisher_name": _env("TAMPIQ_PUBLISHER_NAME", "TAMPIQ") or "TAMPIQ",
        "publisher_country": _env("TAMPIQ_PUBLISHER_COUNTRY", "Türkiye"),
    }


def _cookie_locale(request: Request) -> str | None:
    return request.cookies.get(COOKIE_NAME)


def resolve_request_locale(request: Request) -> str:
    return resolve_locale(
        query_lang=request.query_params.get("lang"),
        cookie_lang=_cookie_locale(request),
        accept_language=request.headers.get("accept-language"),
    )


def resolve_platform(request: Request) -> str:
    """Platform selector for privacy/support copy.

    Default is ``ios`` so existing `/privacy?lang=…` and `/support?lang=…`
    URLs keep serving the published iOS Help Center content.
    Unknown values fall back to ``ios`` (backward compatible, no blank page).
    """
    raw = (request.query_params.get("platform") or "").strip().lower()
    if raw in _SUPPORTED_PLATFORMS:
        return raw
    return "ios"


def _apply_platform_catalog(catalog: dict, *, locale: str, platform: str) -> dict:
    """Swap privacy/support bodies for Android when requested.

    Incomplete Android locales intentionally use reviewed English Android copy
    (never another language's iOS-specific text).
    """
    if platform != "android":
        return catalog

    source_locale = locale if locale in _ANDROID_LOCALES else DEFAULT_LOCALE
    android_catalog = get_catalog(source_locale)
    privacy = android_catalog.get("privacy_android")
    support = android_catalog.get("support_android")
    if not isinstance(privacy, dict) or not isinstance(support, dict):
        return catalog

    out = dict(catalog)
    out["privacy"] = privacy
    out["support"] = support
    meta = dict(out.get("meta") or {})
    # Prefer Android-specific titles/descriptions when present.
    en_meta = get_catalog(DEFAULT_LOCALE).get("meta") or {}
    src_meta = android_catalog.get("meta") or {}
    for key, fallback_key in (
        ("privacy_title", "privacy_title_android"),
        ("support_title", "support_title_android"),
        ("privacy_description", "privacy_description_android"),
        ("support_description", "support_description_android"),
    ):
        value = src_meta.get(fallback_key) or en_meta.get(fallback_key)
        if value:
            meta[key] = value
    notice = None
    if locale not in _ANDROID_LOCALES:
        notice = src_meta.get("android_locale_notice") or en_meta.get("android_locale_notice")
    out["meta"] = meta
    out["android_locale_notice"] = notice
    return out


def page_context(request: Request, *, active: str) -> dict:
    locale = resolve_request_locale(request)
    platform = resolve_platform(request)
    info = locale_info(locale)
    catalog = _apply_platform_catalog(get_catalog(locale), locale=locale, platform=platform)
    identity = _site_identity()
    # Allow env overrides for publisher display while keeping catalog defaults.
    # Never allow a personal legal name to leak onto the public site.
    publisher = identity["publisher_name"]
    if (
        "evecen" in publisher.lower()
        or "cem gürsoy" in publisher.lower()
        or "cem gursoy" in publisher.lower()
    ):
        publisher = "TAMPIQ"
    if catalog.get("support"):
        catalog = {
            **catalog,
            "support": {
                **catalog["support"],
                "developer_name": publisher,
                "country_value": identity["publisher_country"],
            },
        }
    platform_qs = f"&platform={platform}" if platform != "ios" else ""
    return {
        "request": request,
        "active": active,
        "locale": info.code,
        "text_dir": info.dir,
        "locale_items": locale_selector_items(info.code),
        "platform": platform,
        "platform_qs": platform_qs,
        "android_locale_notice": catalog.get("android_locale_notice"),
        "t": catalog,
        **identity,
    }


def _html_response(request: Request, template: str, active: str) -> Response:
    locale = resolve_request_locale(request)
    response = _TEMPLATES.TemplateResponse(
        request,
        template,
        page_context(request, active=active),
    )
    # Mirror selected locale for next navigation (localStorage also set client-side).
    response.set_cookie(
        key=COOKIE_NAME,
        value=locale,
        max_age=60 * 60 * 24 * 365,
        httponly=False,
        samesite="lax",
        path="/",
    )
    return response


app = FastAPI(
    title="TAMPIQ Support",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

_static_dir = _ROOT / "static" / "tampiq"
if _static_dir.is_dir():
    app.mount("/static/tampiq", StaticFiles(directory=str(_static_dir)), name="tampiq_static")


@app.get("/health", include_in_schema=False)
def health() -> dict[str, str]:
    return {"status": "ok", "site": "tampiq-public"}


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def home(request: Request):
    return _html_response(request, "home.html", "home")


@app.get("/support", response_class=HTMLResponse, include_in_schema=False)
def support(request: Request):
    return _html_response(request, "support.html", "support")


@app.get("/privacy", response_class=HTMLResponse, include_in_schema=False)
def privacy(request: Request):
    return _html_response(request, "privacy.html", "privacy")


@app.get("/robots.txt", response_class=PlainTextResponse, include_in_schema=False)
def robots() -> str:
    return "User-agent: *\nDisallow: /\n"


@app.get("/{path:path}", include_in_schema=False)
def catch_all(path: str):
    return PlainTextResponse("Not Found", status_code=404)


def is_public_tampiq_mode() -> bool:
    return _truthy("PUBLIC_TAMPIQ_SITE")


def public_site_context() -> dict[str, str]:
    """Backward-compatible helper for older tests."""
    return _site_identity() | {
        "effective_date": _env("TAMPIQ_POLICY_EFFECTIVE_DATE", "30 September 2026"),
    }
