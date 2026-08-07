"""Monetizasyon (Virgül /ad-virgul) — @nokta.com (Gözde/Beren hariç) + admin."""

from __future__ import annotations

from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS, _normalize_email

# Bu hesaplar menü ve monetizasyon yollarından dışarıda.
AD_PAGE_DENIED_EMAILS = frozenset(
    {
        "gozdeunaldi@nokta.com",
        "berendemirci@gmail.com",
    }
)


def is_ad_page_allowed_email(email: str | None) -> bool:
    em = _normalize_email(email or "")
    if not em or em in AD_PAGE_DENIED_EMAILS:
        return False
    if em in ADMIN_MEMBER_EMAILS:
        return True
    return em.endswith("@nokta.com")


def resolve_ad_menu_visible(*, member_email: str | None) -> bool:
    """Üst menüde virgül linki: izinli monetizasyon hesapları."""
    if member_email:
        return is_ad_page_allowed_email(member_email)
    return False


def is_ad_page_path(path: str) -> bool:
    """HTML monetizasyon sayfaları, GA4 banner ve ilgili API'ler."""
    p = (path or "").split("?", 1)[0]
    if p == "/ad" or p.startswith("/ad/"):
        return True
    if p == "/ad-virgul" or p.startswith("/ad-virgul/"):
        return True
    if p.startswith("/api/mz-analytics"):
        return True
    if p.startswith("/api/virgul-analytics"):
        return True
    return False


def member_denied_ad_access(member_email: str | None) -> bool:
    em = _normalize_email(member_email or "")
    return bool(em) and not is_ad_page_allowed_email(em)
