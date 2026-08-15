"""X-Data (/x-data, /metrik) — yalnızca admin üye e-postaları.

Empower metrik API'leri (/api/empower-intel/…) burada kilitlenmez; overlay/metrik
verisi diğer panellerde herkese açık kalır.
"""

from __future__ import annotations

from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS, _normalize_email


def is_xdata_page_allowed_email(email: str | None) -> bool:
    em = _normalize_email(email or "")
    return bool(em) and em in ADMIN_MEMBER_EMAILS


def resolve_xdata_menu_visible(*, member_email: str | None) -> bool:
    """Üst menüde X-Data linki: yalnızca admin hesaplar."""
    return is_xdata_page_allowed_email(member_email)


def is_xdata_page_path(path: str) -> bool:
    """Yalnızca HTML sayfa yolları — API'ler dahil değil."""
    p = (path or "").split("?", 1)[0].rstrip("/") or "/"
    return p in ("/x-data", "/metrik")


def member_denied_xdata_access(member_email: str | None) -> bool:
    em = _normalize_email(member_email or "")
    return bool(em) and not is_xdata_page_allowed_email(em)
