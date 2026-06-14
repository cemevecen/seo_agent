"""AI Talk sohbet geçmişi görüntüleme — Settings / Inbox şifresi ile ikinci katman."""

from __future__ import annotations

import hashlib
import hmac
from typing import TYPE_CHECKING

from fastapi import HTTPException

from backend.config import settings

if TYPE_CHECKING:
    from starlette.requests import Request

AI_TALK_HISTORY_AUTH_COOKIE = "seo_ai_talk_history_auth"


def ai_talk_history_password_configured() -> bool:
    settings_pwd = (getattr(settings, "settings_password", "") or "").strip()
    inbox_pwd = (getattr(settings, "inbox_action_password", "") or "").strip()
    return bool(settings_pwd or inbox_pwd)


def _primary_history_password() -> str:
    settings_pwd = (getattr(settings, "settings_password", "") or "").strip()
    if settings_pwd:
        return settings_pwd
    return (getattr(settings, "inbox_action_password", "") or "").strip()


def ai_talk_history_token(raw_pwd: str) -> str:
    secret = str(getattr(settings, "secret_key", "") or "").encode("utf-8")
    return hmac.new(secret, ("ai_talk_history:" + raw_pwd).encode("utf-8"), digestmod=hashlib.sha256).hexdigest()


def verify_ai_talk_history_password(input_pwd: str) -> bool:
    raw = str(input_pwd or "").strip()
    if not raw:
        return False
    for env_key in ("settings_password", "inbox_action_password"):
        expected = (getattr(settings, env_key, "") or "").strip()
        if expected and hmac.compare_digest(raw, expected):
            return True
    return False


def _cookie_token_valid(request: Request) -> bool:
    raw_pwd = _primary_history_password()
    if not raw_pwd:
        return False
    token = str(request.cookies.get(AI_TALK_HISTORY_AUTH_COOKIE) or "")
    if not token:
        return False
    expected = ai_talk_history_token(raw_pwd)
    return hmac.compare_digest(token, expected)


def _settings_authenticated(request: Request) -> bool:
    try:
        from backend.main import _is_settings_authenticated

        return bool(_is_settings_authenticated(request))
    except Exception:
        return False


def _inbox_action_authenticated(request: Request) -> bool:
    try:
        from backend.main import _is_inbox_action_authenticated

        return bool(_is_inbox_action_authenticated(request))
    except Exception:
        return False


def is_ai_talk_history_authenticated(request: Request) -> bool:
    """Şifre tanımlı değilse açık; settings/inbox/ai-talk cookie geçerliyse True."""
    if not ai_talk_history_password_configured():
        return True
    if _settings_authenticated(request):
        return True
    if _inbox_action_authenticated(request):
        return True
    return _cookie_token_valid(request)


def require_ai_talk_history_auth(request: Request) -> None:
    if is_ai_talk_history_authenticated(request):
        return
    raise HTTPException(status_code=403, detail="ai_talk_history_auth_required")


def issue_ai_talk_history_cookie_token() -> str:
    return ai_talk_history_token(_primary_history_password())
