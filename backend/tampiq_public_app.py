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
from starlette.types import ASGIApp, Receive, Scope, Send

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
    """Swap privacy/support/delete-* bodies for Android when requested.

    Incomplete Android locales intentionally use reviewed English Android copy
    (never another language's iOS-specific text).
    """
    if platform != "android":
        return catalog

    source_locale = locale if locale in _ANDROID_LOCALES else DEFAULT_LOCALE
    android_catalog = get_catalog(source_locale)
    privacy = android_catalog.get("privacy_android")
    support = android_catalog.get("support_android")
    delete_account = android_catalog.get("delete_account_android")
    delete_data = android_catalog.get("delete_data_android")
    if (
        not isinstance(privacy, dict)
        or not isinstance(support, dict)
        or not isinstance(delete_account, dict)
        or not isinstance(delete_data, dict)
    ):
        return catalog

    out = dict(catalog)
    out["privacy"] = privacy
    out["support"] = support
    out["delete_account"] = delete_account
    out["delete_data"] = delete_data
    meta = dict(out.get("meta") or {})
    # Prefer Android-specific titles/descriptions when present.
    en_meta = get_catalog(DEFAULT_LOCALE).get("meta") or {}
    src_meta = android_catalog.get("meta") or {}
    for key, fallback_key in (
        ("privacy_title", "privacy_title_android"),
        ("support_title", "support_title_android"),
        ("privacy_description", "privacy_description_android"),
        ("support_description", "support_description_android"),
        ("delete_account_title", "delete_account_title_android"),
        ("delete_account_description", "delete_account_description_android"),
        ("delete_data_title", "delete_data_title_android"),
        ("delete_data_description", "delete_data_description_android"),
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

# Public HTML/text paths that store crawlers and browsers may request with a
# trailing slash or via HEAD. Exact paths stay canonical; slash → 308 redirect.
_PUBLIC_SLASH_REDIRECTS = frozenset(
    {
        "/support",
        "/privacy",
        "/delete-account",
        "/delete-data",
        "/health",
        "/robots.txt",
    }
)


class _PublicPathCompatMiddleware:
    """Trailing-slash redirect + HEAD→GET for public Help Center paths.

    Independent checkers sometimes hit ``/delete-account/`` (404 via catch-all)
    or probe with HEAD (FastAPI returns 405 when only GET is registered).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path") or ""
        method = (scope.get("method") or "GET").upper()

        if path != "/" and path.endswith("/"):
            bare = path.rstrip("/") or "/"
            if bare in _PUBLIC_SLASH_REDIRECTS:
                qs = scope.get("query_string") or b""
                location = bare.encode("ascii")
                if qs:
                    location = location + b"?" + qs
                await send(
                    {
                        "type": "http.response.start",
                        "status": 308,
                        "headers": [
                            (b"content-length", b"0"),
                            (b"location", location),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": b"", "more_body": False})
                return

        # Treat HEAD like GET for routing, then drop the body (store probes).
        if method == "HEAD":
            scope = dict(scope)
            scope["method"] = "GET"
            started: dict | None = None

            async def send_head(message: dict) -> None:
                nonlocal started
                if message["type"] == "http.response.start":
                    headers = [
                        (k, v)
                        for k, v in message.get("headers", [])
                        if k.lower() != b"content-length"
                    ]
                    headers.append((b"content-length", b"0"))
                    started = {"type": "http.response.start", "status": message["status"], "headers": headers}
                    await send(started)
                elif message["type"] == "http.response.body":
                    await send({"type": "http.response.body", "body": b"", "more_body": False})

            await self.app(scope, receive, send_head)
            return

        await self.app(scope, receive, send)


app.add_middleware(_PublicPathCompatMiddleware)

_static_dir = _ROOT / "static" / "tampiq"
if _static_dir.is_dir():
    app.mount("/static/tampiq", StaticFiles(directory=str(_static_dir)), name="tampiq_static")


@app.api_route("/health", methods=["GET", "HEAD"], include_in_schema=False)
def health() -> dict[str, str]:
    return {"status": "ok", "site": "tampiq-public"}


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse, include_in_schema=False)
def home(request: Request):
    return _html_response(request, "home.html", "home")


@app.api_route("/support", methods=["GET", "HEAD"], response_class=HTMLResponse, include_in_schema=False)
def support(request: Request):
    return _html_response(request, "support.html", "support")


@app.api_route("/privacy", methods=["GET", "HEAD"], response_class=HTMLResponse, include_in_schema=False)
def privacy(request: Request):
    return _html_response(request, "privacy.html", "privacy")


@app.api_route("/delete-account", methods=["GET", "HEAD"], response_class=HTMLResponse, include_in_schema=False)
def delete_account(request: Request):
    return _html_response(request, "delete_account.html", "delete-account")


@app.api_route("/delete-data", methods=["GET", "HEAD"], response_class=HTMLResponse, include_in_schema=False)
def delete_data(request: Request):
    return _html_response(request, "delete_data.html", "delete-data")


@app.api_route("/robots.txt", methods=["GET", "HEAD"], response_class=PlainTextResponse, include_in_schema=False)
def robots() -> str:
    # Public Help Center must remain fetchable for store review (Google Play / App Store).
    # Historical blanket "Disallow: /" blocked crawlers from reading /privacy and /support.
    # Keep everything else disallowed; this site has no private API surface beyond catch-all 404.
    # Allow both bare and trailing-slash forms so slash redirects remain crawlable.
    return (
        "User-agent: *\n"
        "Allow: /privacy\n"
        "Allow: /privacy/\n"
        "Allow: /support\n"
        "Allow: /support/\n"
        "Allow: /delete-account\n"
        "Allow: /delete-account/\n"
        "Allow: /delete-data\n"
        "Allow: /delete-data/\n"
        "Allow: /robots.txt\n"
        "Allow: /health\n"
        "Allow: /static/tampiq/\n"
        "Disallow: /\n"
    )


@app.api_route("/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
def catch_all(path: str):
    return PlainTextResponse("Not Found", status_code=404)


def is_public_tampiq_mode() -> bool:
    return _truthy("PUBLIC_TAMPIQ_SITE")


def public_site_context() -> dict[str, str]:
    """Backward-compatible helper for older tests."""
    return _site_identity() | {
        "effective_date": _env("TAMPIQ_POLICY_EFFECTIVE_DATE", "30 September 2026"),
    }
