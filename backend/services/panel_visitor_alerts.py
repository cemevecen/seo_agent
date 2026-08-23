"""Owner-only (cem) ziyaretçi uyarıları ve çıkış kullanım özeti."""

from __future__ import annotations

import html
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")

OWNER_ALERT_TO = "cemevecen@nokta.com"
_ALERT_COOLDOWN_SEC = 20 * 60
_lock = threading.Lock()
_last_visitor_alert_at: dict[str, float] = {}


def _norm(email: str) -> str:
    return str(email or "").strip().lower()


def is_owner_email(email: str, owner_emails: frozenset[str] | set[str] | None = None) -> bool:
    em = _norm(email)
    if not em or "@" not in em:
        return False
    # Yerel kısım cemevecen → her zaman owner (gmail/nokta vb.)
    local = em.split("@", 1)[0]
    if local == "cemevecen":
        return True
    if owner_emails is None:
        try:
            from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS

            owner_emails = ADMIN_MEMBER_EMAILS
        except Exception:  # noqa: BLE001
            owner_emails = frozenset()
    return em in {_norm(e) for e in owner_emails}


def _fmt_tr(dt: datetime | None) -> str:
    if not dt:
        return "—"
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return aware.astimezone(_TR).strftime("%d.%m.%Y %H:%M:%S")


def _duration_tr(start: datetime | None, end: datetime | None) -> str:
    if not start or not end:
        return "—"
    sec = max(0, int((end - start).total_seconds()))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h} sa {m} dk {s} sn"
    if m:
        return f"{m} dk {s} sn"
    return f"{s} sn"


def owners_currently_online(
    sessions: dict[str, dict[str, Any]] | list[dict[str, Any]],
    owner_emails: frozenset[str] | set[str],
    *,
    idle_minutes: int = 30,
) -> list[str]:
    owners = {_norm(e) for e in owner_emails}
    now = datetime.utcnow()
    cutoff = now.timestamp() - idle_minutes * 60
    found: set[str] = set()
    rows = sessions.values() if isinstance(sessions, dict) else sessions
    for s in rows:
        em = _norm(str(s.get("email") or ""))
        if em not in owners:
            continue
        last = s.get("last_seen")
        if isinstance(last, datetime):
            if last.timestamp() < cutoff:
                continue
        found.add(em)
    return sorted(found)


def maybe_alert_visitor_joined(
    sessions: dict[str, dict[str, Any]],
    *,
    email: str,
    session: dict[str, Any],
    owner_emails: frozenset[str] | set[str],
) -> bool:
    """Panel bağlantı e-postası kapalı."""
    _ = (sessions, email, session, owner_emails)
    return False


def send_usage_summary_email(
    *,
    email: str,
    display_name: str = "",
    session: dict[str, Any] | None = None,
    paths: list[dict[str, Any]] | None = None,
) -> bool:
    """Çıkış kullanım özeti e-postası kapalı."""
    _ = (email, display_name, session, paths)
    return False
