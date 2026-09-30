"""TAMPIQ public Support / Privacy site (Apple App Store Connect).

Isolated ASGI app — no ProjectControl routes, auth, DB, or navigation.

Railway service `tampiq-support`:
  PUBLIC_TAMPIQ_SITE=true
  start: uvicorn backend.tampiq_public_app:app --host 0.0.0.0 --port $PORT

ProjectControl production must keep PUBLIC_TAMPIQ_SITE unset and continue
using backend.main:app so these pages are never linked or mounted there.
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

_ROOT = Path(__file__).resolve().parent.parent
_TEMPLATES = Jinja2Templates(directory=str(_ROOT / "templates" / "tampiq"))

_TRUE = {"1", "true", "yes", "on"}


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _truthy(name: str) -> bool:
    return _env(name).lower() in _TRUE


def public_site_context() -> dict[str, str]:
    app_name = _env("TAMPIQ_APP_NAME", "TAMPIQ")
    support_email = _env("TAMPIQ_SUPPORT_EMAIL", "support@tampiq.app")
    privacy_email = _env("TAMPIQ_PRIVACY_EMAIL", support_email)
    company = _env("TAMPIQ_COMPANY_NAME", "TAMPIQ")
    site_url = _env("TAMPIQ_PUBLIC_BASE_URL", "").rstrip("/")
    return {
        "app_name": app_name,
        "company_name": company,
        "support_email": support_email,
        "privacy_email": privacy_email,
        "site_url": site_url,
        "effective_date": _env("TAMPIQ_POLICY_EFFECTIVE_DATE", "30 September 2026"),
    }


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
    return _TEMPLATES.TemplateResponse(
        request,
        "home.html",
        {"request": request, **public_site_context()},
    )


@app.get("/support", response_class=HTMLResponse, include_in_schema=False)
def support(request: Request):
    return _TEMPLATES.TemplateResponse(
        request,
        "support.html",
        {"request": request, **public_site_context()},
    )


@app.get("/privacy", response_class=HTMLResponse, include_in_schema=False)
def privacy(request: Request):
    return _TEMPLATES.TemplateResponse(
        request,
        "privacy.html",
        {"request": request, **public_site_context()},
    )


@app.get("/robots.txt", response_class=PlainTextResponse, include_in_schema=False)
def robots() -> str:
    return "User-agent: *\nAllow: /\nAllow: /support\nAllow: /privacy\nDisallow: /docs\nDisallow: /redoc\n"


@app.get("/{path:path}", include_in_schema=False)
def catch_all(path: str):
    """Unknown paths stay undiscoverable — no ProjectControl fallback."""
    return PlainTextResponse("Not Found", status_code=404)


def is_public_tampiq_mode() -> bool:
    """True when this process is intended as the isolated TAMPIQ public host."""
    return _truthy("PUBLIC_TAMPIQ_SITE")
