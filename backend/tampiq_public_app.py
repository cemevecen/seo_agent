"""TAMPIQ public Support / Privacy site (Apple App Store Connect).

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


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _truthy(name: str) -> bool:
    return _env(name).lower() in _TRUE


def _site_identity() -> dict[str, str]:
    support_email = _env("TAMPIQ_SUPPORT_EMAIL", "support@tampiq.app")
    privacy_email = _env("TAMPIQ_PRIVACY_EMAIL", support_email)
    return {
        "app_name": _env("TAMPIQ_APP_NAME", "TAMPIQ"),
        "company_name": _env("TAMPIQ_COMPANY_NAME", "TAMPIQ"),
        "support_email": support_email,
        "privacy_email": privacy_email,
        "site_url": _env("TAMPIQ_PUBLIC_BASE_URL", "").rstrip("/"),
        "publisher_name": _env("TAMPIQ_PUBLISHER_NAME", "Cem Gürsoy EVECEN"),
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


def page_context(request: Request, *, active: str) -> dict:
    locale = resolve_request_locale(request)
    info = locale_info(locale)
    catalog = get_catalog(locale)
    identity = _site_identity()
    # Allow env overrides for publisher display while keeping catalog defaults.
    if catalog.get("support"):
        catalog = {
            **catalog,
            "support": {
                **catalog["support"],
                "developer_name": identity["publisher_name"],
                "country_value": identity["publisher_country"],
            },
        }
    return {
        "request": request,
        "active": active,
        "locale": info.code,
        "text_dir": info.dir,
        "locale_items": locale_selector_items(info.code),
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
