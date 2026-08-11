"""Owner-only PM lab (/pm-lab) — yalnızca cemevecen hesapları."""

from __future__ import annotations

from backend.services.panel_visitor_alerts import is_owner_email


def is_pm_lab_allowed_email(email: str | None) -> bool:
    return is_owner_email(email or "")


def resolve_pm_lab_visible(*, member_email: str | None) -> bool:
    return is_pm_lab_allowed_email(member_email)


def is_pm_lab_path(path: str) -> bool:
    p = (path or "").split("?", 1)[0]
    if p == "/pm-lab" or p.startswith("/pm-lab/"):
        return True
    if p.startswith("/api/pm-lab/") and not p.startswith("/api/pm-lab/ingest"):
        return True
    return False
