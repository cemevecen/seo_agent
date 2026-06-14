"""Menü, filtre ve dahili raporlardan gizlenecek domain/kaynak adları."""

from __future__ import annotations

_MENU_EXCLUDED_PARTS = ("canlidoviz",)


def is_menu_excluded_label(label: str | None) -> bool:
    """Domain veya haber kaynağı adında canlidoviz vb. geçiyorsa True."""
    normalized = str(label or "").lower().replace("ı", "i").replace("ö", "o")
    return any(part in normalized for part in _MENU_EXCLUDED_PARTS)
