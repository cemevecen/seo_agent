"""TMDB vizyon takvimi — paylaşımlı misafir linki (yalnızca /tmdb-upcoming)."""

from __future__ import annotations

import hashlib
import hmac
from typing import TYPE_CHECKING

from backend.config import settings

if TYPE_CHECKING:
    from starlette.requests import Request

TMDB_GUEST_COOKIE = "seo_tmdb_guest"
TMDB_GUEST_PATH = "/tmdb-upcoming"

_GUEST_STATIC_PREFIXES = (
    "/static/",
    "/health",
    "/favicon",
    "/apple-touch-icon",
)


def guest_access_configured() -> bool:
    return bool((getattr(settings, "tmdb_guest_access_token", "") or "").strip())


def _configured_token() -> str:
    return (getattr(settings, "tmdb_guest_access_token", "") or "").strip()


def guest_cookie_value() -> str:
    raw = _configured_token()
    secret = str(getattr(settings, "secret_key", "") or "").encode("utf-8")
    return hmac.new(secret, ("tmdb_guest:" + raw).encode("utf-8"), digestmod=hashlib.sha256).hexdigest()


def access_query_matches(query_token: str) -> bool:
    expected = _configured_token()
    if not expected or not query_token:
        return False
    return hmac.compare_digest(str(query_token).strip(), expected)


def is_tmdb_guest_authenticated(request: Request) -> bool:
    if not guest_access_configured():
        return False
    token = str(request.cookies.get(TMDB_GUEST_COOKIE) or "")
    if not token:
        return False
    return hmac.compare_digest(token, guest_cookie_value())


def guest_path_allowed(path: str) -> bool:
    p = (path or "").strip()
    if p == TMDB_GUEST_PATH or p.startswith(TMDB_GUEST_PATH + "/"):
        return True
    return any(p.startswith(prefix) for prefix in _GUEST_STATIC_PREFIXES)


def tmdb_only_member_path_allowed(path: str) -> bool:
    """Google üyesi — yalnızca vizyon takvimi sayfası + gerekli API + çıkış."""
    p = (path or "").strip()
    if guest_path_allowed(p):
        return True
    if p.startswith("/api/tmdb-upcoming/"):
        return True
    if p in ("/auth/logout",):
        return True
    return False
