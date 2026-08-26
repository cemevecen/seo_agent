"""Sheet (/sheet) — yalnızca cemevecen hesapları."""

from __future__ import annotations

from backend.services.app_member_auth import _normalize_email

SHEET_ALLOWED_EMAILS = frozenset(
    {
        "cemevecen@nokta.com",
        "cemevecen@gmail.com",
    }
)


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


def member_denied_sheet_access(member_email: str | None) -> bool:
    em = _normalize_email(member_email or "")
    return bool(em) and not is_sheet_page_allowed_email(em)
