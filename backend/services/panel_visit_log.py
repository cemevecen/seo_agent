"""Panel ziyaret günlüğü — giriş, gezilen sayfalar, çıkış."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import PanelVisitLog
from backend.services.admin_access_log import admin_path_label, should_track_admin_path

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")
_MAX_PAGES = 80
_KEEP_ROWS = 400


def _utcnow() -> datetime:
    return datetime.utcnow()


def format_tr_sec(dt: datetime | None) -> str:
    if not dt:
        return "—"
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return aware.astimezone(_TR).strftime("%d.%m.%Y %H:%M:%S")


def _load_pages(raw: str) -> list[dict[str, str]]:
    try:
        data = json.loads(raw or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    out: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip()
        if not path:
            continue
        out.append(
            {
                "path": path[:120],
                "label": str(item.get("label") or path)[:80],
                "at_tr": str(item.get("at_tr") or "")[:32],
            }
        )
    return out


def _dump_pages(pages: list[dict[str, str]]) -> str:
    return json.dumps(pages[-_MAX_PAGES:], ensure_ascii=False)


def _trim_old(db: Session) -> None:
    keep_ids = [
        r[0]
        for r in (
            db.query(PanelVisitLog.id)
            .order_by(PanelVisitLog.logged_in_at.desc(), PanelVisitLog.id.desc())
            .limit(_KEEP_ROWS)
            .all()
        )
    ]
    if not keep_ids:
        return
    db.query(PanelVisitLog).filter(PanelVisitLog.id.notin_(keep_ids)).delete(synchronize_session=False)


def _open_row(db: Session, session_key: str) -> PanelVisitLog | None:
    return (
        db.query(PanelVisitLog)
        .filter(PanelVisitLog.session_key == session_key, PanelVisitLog.logged_out_at.is_(None))
        .order_by(PanelVisitLog.id.desc())
        .with_for_update()
        .first()
    )


def touch_visit(
    *,
    session_key: str,
    email: str = "",
    display_name: str = "",
    session_kind: str = "",
    ip: str = "",
    device: str = "",
    path: str = "",
) -> None:
    key = (session_key or "").strip()
    if not key:
        return
    now = _utcnow()
    page_path = (path or "").split("?")[0] or "/"
    track = should_track_admin_path(page_path)
    try:
        with SessionLocal() as db:
            row = _open_row(db, key)
            if row is None:
                row = PanelVisitLog(
                    session_key=key[:80],
                    email=(email or "")[:255],
                    display_name=(display_name or "")[:255],
                    session_kind=(session_kind or "")[:20],
                    ip=(ip or "")[:64],
                    device_label=(device or "")[:120],
                    logged_in_at=now,
                    last_seen_at=now,
                    pages_json="[]",
                )
                db.add(row)
                db.flush()
            else:
                row.last_seen_at = now
                if ip:
                    row.ip = ip[:64]
                if device:
                    row.device_label = device[:120]
                if email:
                    row.email = email[:255]
                if display_name:
                    row.display_name = display_name[:255]
            if track:
                pages = _load_pages(row.pages_json)
                last = pages[-1]["path"] if pages else None
                if last != page_path:
                    pages.append(
                        {
                            "path": page_path[:120],
                            "label": admin_path_label(page_path),
                            "at_tr": datetime.now(_TR).strftime("%H:%M:%S"),
                        }
                    )
                    row.pages_json = _dump_pages(pages)
            db.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel visit touch atlandı: %s", exc)


def close_visit(session_key: str, *, reason: str = "logout") -> None:
    key = (session_key or "").strip()
    if not key:
        return
    now = _utcnow()
    why = (reason or "logout")[:20]
    try:
        with SessionLocal() as db:
            row = _open_row(db, key)
            if row is None:
                return
            row.logged_out_at = now
            row.last_seen_at = now
            row.end_reason = why
            db.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel visit close atlandı: %s", exc)


def close_sessions(session_keys: list[str], *, reason: str = "idle") -> None:
    for key in session_keys:
        close_visit(key, reason=reason)


def expire_idle(*, idle_minutes: int = 30) -> None:
    cutoff = _utcnow() - timedelta(minutes=max(1, int(idle_minutes)))
    try:
        with SessionLocal() as db:
            rows = (
                db.query(PanelVisitLog)
                .filter(PanelVisitLog.logged_out_at.is_(None), PanelVisitLog.last_seen_at < cutoff)
                .all()
            )
            now = _utcnow()
            for row in rows:
                row.logged_out_at = row.last_seen_at or now
                row.end_reason = "idle"
            if rows:
                db.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel visit idle expire atlandı: %s", exc)


def recent_visits(*, limit: int = 80) -> list[dict[str, Any]]:
    expire_idle()
    cap = max(1, min(int(limit), 200))
    try:
        with SessionLocal() as db:
            _trim_old(db)
            db.commit()
            rows = (
                db.query(PanelVisitLog)
                .order_by(PanelVisitLog.logged_in_at.desc(), PanelVisitLog.id.desc())
                .limit(cap)
                .all()
            )
            out: list[dict[str, Any]] = []
            for row in rows:
                pages = _load_pages(row.pages_json)
                open_now = row.logged_out_at is None
                reason = row.end_reason or ("open" if open_now else "logout")
                who = (row.display_name or "").strip() or (row.email or "").strip() or (
                    "Admin şifre" if row.session_kind == "admin" else "Üye"
                )
                out.append(
                    {
                        "id": row.id,
                        "who": who,
                        "email": row.email,
                        "session_kind": row.session_kind,
                        "ip": row.ip,
                        "device": row.device_label,
                        "logged_in_at": row.logged_in_at,
                        "logged_in_tr": format_tr_sec(row.logged_in_at),
                        "logged_out_at": row.logged_out_at,
                        "logged_out_tr": format_tr_sec(row.logged_out_at) if not open_now else "Açık",
                        "is_open": open_now,
                        "end_reason": reason,
                        "end_label": "Açık" if open_now else ("Çıkış" if reason == "logout" else "Hareketsizlik"),
                        "pages": pages,
                        "page_count": len(pages),
                        "page_summary": " · ".join(p["label"] for p in pages[:12]),
                    }
                )
            return out
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel visit list atlandı: %s", exc)
        return []
