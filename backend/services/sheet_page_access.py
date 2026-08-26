"""Sheet (/sheet) — cemevecen + sheet-only üyeler."""

from __future__ import annotations

from backend.services.app_member_auth import _normalize_email

SHEET_ONLY_MEMBER_EMAILS = frozenset(
    {
        "evecensema@gmail.com",
    }
)

SHEET_ALLOWED_EMAILS = frozenset(
    {
        "cemevecen@nokta.com",
        "cemevecen@gmail.com",
    }
) | SHEET_ONLY_MEMBER_EMAILS

SHEET_ONLY_HOME_PATH = "/sheet"

_SHEET_ONLY_STATIC_PREFIXES = (
    "/static/",
    "/health",
    "/favicon",
    "/apple-touch-icon",
)


def is_sheet_only_member_email(email: str | None) -> bool:
    em = _normalize_email(email or "")
    return bool(em) and em in SHEET_ONLY_MEMBER_EMAILS


def sheet_only_home_path() -> str:
    return SHEET_ONLY_HOME_PATH


def is_sheet_page_allowed_email(email: str | None) -> bool:
    em = _normalize_email(email or "")
    return bool(em) and em in SHEET_ALLOWED_EMAILS


def resolve_sheet_menu_visible(*, member_email: str | None) -> bool:
    return is_sheet_page_allowed_email(member_email)


def is_sheet_page_path(path: str) -> bool:
    p = (path or "").split("?", 1)[0].rstrip("/") or "/"
    if p == "/sheet":
        return True
    return p.startswith("/api/sheet/")


def sheet_only_member_path_allowed(path: str) -> bool:
    """Google üyesi — yalnızca /sheet + API + giriş/çıkış."""
    p = (path or "").strip().split("?", 1)[0].rstrip("/") or "/"
    if p == "/sheet" or p.startswith("/sheet/"):
        return True
    if p.startswith("/api/sheet/"):
        return True
    if p in (
        "/auth/logout",
        "/admin/login",
        "/auth/google/start",
        "/auth/google/callback",
    ):
        return True
    return any(p.startswith(prefix) for prefix in _SHEET_ONLY_STATIC_PREFIXES)


def member_denied_sheet_access(member_email: str | None) -> bool:
    em = _normalize_email(member_email or "")
    return bool(em) and not is_sheet_page_allowed_email(em)
