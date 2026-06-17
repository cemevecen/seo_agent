"""Panel (uygulama) giriş kapısı — middleware ve sayfa koruması."""

from __future__ import annotations

from typing import TYPE_CHECKING

from backend.config import host_requires_panel_auth, is_railway_runtime, settings

if TYPE_CHECKING:
    from starlette.requests import Request


def request_host(request: Request) -> str:
    raw = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").strip()
    return raw.split(",")[0].strip().lower().split(":")[0]


def is_loopback_client(request: Request) -> bool:
    host = request_host(request)
    if host in ("127.0.0.1", "localhost", "::1"):
        return True
    client = (request.client.host if request.client else "") or ""
    return client in ("127.0.0.1", "::1")


def auth_gate_enabled(request: Request) -> bool:
    """Panel girişi zorunlu mu? Yalnızca yerel + ADMIN_AUTH_ENFORCED=false ile kapatılır."""
    if is_railway_runtime():
        return True
    if host_requires_panel_auth(request_host(request)):
        return True
    if not settings.admin_auth_enforced and is_loopback_client(request):
        return False
    return True


def panel_session_granted(
    *,
    password_ready: bool,
    admin_authenticated: bool,
    member_authenticated: bool,
) -> bool:
    if not (admin_authenticated or member_authenticated):
        return False
    if member_authenticated:
        return True
    return bool(password_ready and admin_authenticated)
