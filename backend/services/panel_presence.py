"""Panel çevrimiçi üye listesi (yalnızca yetkili izleyiciler için)."""

from __future__ import annotations

from datetime import datetime
from typing import Any


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
