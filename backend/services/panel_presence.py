"""Panel çevrimiçi üye listesi (yalnızca yetkili izleyiciler için)."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def _normalize_email_key(email: str) -> str:
    return str(email or "").strip().lower()


def collect_member_emails(sessions: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for s in sessions:
        em = _normalize_email_key(str(s.get("email") or ""))
        if em and "@" in em:
            out.add(em)
    return out


def dedupe_online_users(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aynı e-postanın birden fazla sekmesini tek satırda birleştir."""
    best: dict[str, dict[str, Any]] = {}
    for s in sessions:
        em = str(s.get("email") or "").strip()
        if not em or "@" not in em:
            continue
        key = em.lower()
        last = s.get("last_seen")
        if not isinstance(last, datetime):
            last = None
        prev = best.get(key)
        prev_last = prev.get("_last_seen") if prev else None
        if prev is None or (last is not None and (prev_last is None or last > prev_last)):
            best[key] = {
                "email": em,
                "display_name": str(s.get("label") or s.get("display_name") or em).strip() or em,
                "last_seen_tr": str(s.get("last_seen_tr") or ""),
                "is_current": bool(s.get("is_current")),
                "_last_seen": last,
            }
    out = [
        {k: v for k, v in row.items() if k != "_last_seen"}
        for row in best.values()
    ]
    out.sort(key=lambda r: str(r.get("email") or "").lower())
    return out


def build_online_presence_api_payload(
    sessions: list[dict[str, Any]],
    *,
    tracked_emails: frozenset[str],
) -> dict[str, Any]:
    """
    Yalnızca tracked_emails içindeki üyeler listelenir.
    Başka üyeler çevrimiçi olsa bile gösterge gizlenmez (onlar listede görünmez).
    """
    tracked = {_normalize_email_key(e) for e in tracked_emails if _normalize_email_key(e)}
    users = dedupe_online_users(sessions)
    users = [u for u in users if _normalize_email_key(u.get("email") or "") in tracked]
    return {"show": True, "users": users, "count": len(users)}
