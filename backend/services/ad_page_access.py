"""Monetizasyon (/ad) — yalnızca tanımlı e-postalar."""

from __future__ import annotations

from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS, _normalize_email

# Kullanıcı isteği: yalnızca bu iki hesap /ad verisini görsün.
AD_PAGE_ALLOWED_EMAILS = frozenset(ADMIN_MEMBER_EMAILS)  # cemevecen@nokta.com, cemevecen@gmail.com


def is_ad_page_allowed_email(email: str | None) -> bool:
    em = _normalize_email(email or "")
    return bool(em) and em in AD_PAGE_ALLOWED_EMAILS


def resolve_ad_menu_visible(*, member_email: str | None) -> bool:
    """Üst menüde ad linki: yalnızca allowlist Google üyesi."""
    if member_email:
        return is_ad_page_allowed_email(member_email)
    return False


def is_ad_page_path(path: str) -> bool:
    """HTML /ad sayfaları, GA4 banner paneli ve monetizasyon API'leri."""
    p = (path or "").split("?", 1)[0]
    if p == "/ad" or p.startswith("/ad/"):
        return True
    if p == "/ad-virgul/app-banner" or p.startswith("/ad-virgul/app-banner/"):
        return True
    if p.startswith("/api/mz-analytics"):
        return True
    return False


def member_denied_ad_access(member_email: str | None) -> bool:
    em = _normalize_email(member_email or "")
    return bool(em) and not is_ad_page_allowed_email(em)
