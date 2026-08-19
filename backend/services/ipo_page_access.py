"""IPO (/ipo) — panelin tüm üyelerine açık.

Sekme başlangıçta yalnız cemevecen hesaplarındaydı; artık herkese açık.
Fonksiyonlar korunuyor: middleware ve şablonlar bunları çağırıyor, ayrıca
ileride yeniden kısıtlamak gerekirse tek yerden dönülür.
"""

from __future__ import annotations


def is_ipo_page_allowed_email(email: str | None) -> bool:
    """IPO artık herkese açık; e-posta kısıtı yok."""
    return True


def resolve_ipo_menu_visible(*, member_email: str | None) -> bool:
    return True


def is_ipo_page_path(path: str) -> bool:
    p = (path or "").split("?", 1)[0].rstrip("/") or "/"
    if p == "/ipo":
        return True
    return p.startswith("/api/ipo/")


def member_denied_ipo_access(member_email: str | None) -> bool:
    return False
