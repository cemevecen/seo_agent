"""IPO (/ipo) — yalnızca cemevecen admin üye hesapları."""

from __future__ import annotations

from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS, _normalize_email


def is_ipo_page_allowed_email(email: str | None) -> bool:
    em = _normalize_email(email or "")
    return bool(em) and em in ADMIN_MEMBER_EMAILS


def resolve_ipo_menu_visible(*, member_email: str | None) -> bool:
    return is_ipo_page_allowed_email(member_email)


def is_ipo_page_path(path: str) -> bool:
    p = (path or "").split("?", 1)[0].rstrip("/") or "/"
    if p == "/ipo":
        return True
    return p.startswith("/api/ipo/")


def member_denied_ipo_access(member_email: str | None) -> bool:
    em = _normalize_email(member_email or "")
    return bool(em) and not is_ipo_page_allowed_email(em)
