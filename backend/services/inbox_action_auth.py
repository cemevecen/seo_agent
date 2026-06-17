"""Inbox yazma/AI/Gmail aksiyonları için ikinci katman doğrulama."""

from __future__ import annotations

import hashlib
import hmac
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.config import settings

if TYPE_CHECKING:
    from starlette.requests import Request

INBOX_ACTION_AUTH_COOKIE = "seo_inbox_action_auth"


def inbox_action_password_configured() -> bool:
    return bool((getattr(settings, "inbox_action_password", "") or "").strip())


def inbox_action_token(raw_pwd: str) -> str:
    secret = str(getattr(settings, "secret_key", "") or "").encode("utf-8")
    return hmac.new(secret, ("inbox_action:" + raw_pwd).encode("utf-8"), digestmod=hashlib.sha256).hexdigest()


def _cookie_token_valid(request: Request, raw_pwd: str) -> bool:
    token = str(request.cookies.get(INBOX_ACTION_AUTH_COOKIE) or "")
    if not token:
        return False
    expected = inbox_action_token(raw_pwd)
    return hmac.compare_digest(token, expected)


def _admin_panel_authenticated(request: Request) -> bool:
    """Admin şifresi veya Google üye oturumu — INBOX_ACTION_PASSWORD bypass (site OAuth değil)."""
    try:
        from backend.main import _is_app_panel_authenticated

        return bool(_is_app_panel_authenticated(request))
    except Exception:
        return False


def is_inbox_action_authenticated(request: Request) -> bool:
    """INBOX_ACTION_PASSWORD yoksa açık; panel oturumu veya aksiyon cookie'si geçerliyse True."""
    raw_pwd = (getattr(settings, "inbox_action_password", "") or "").strip()
    if not raw_pwd:
        return True
    if _admin_panel_authenticated(request):
        return True
    return _cookie_token_valid(request, raw_pwd)


def require_inbox_action_auth(request: Request) -> None:
    if is_inbox_action_authenticated(request):
        return
    raise HTTPException(status_code=403, detail="inbox_action_auth_required")
