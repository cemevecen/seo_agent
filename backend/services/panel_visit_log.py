"""Panel ziyaret günlüğü — gerçek auth girişi, gezilen sayfalar/özellikler, çıkış."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import case
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import PanelVisitLog
from backend.services.admin_access_log import admin_path_label, should_track_admin_path

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")
_MAX_PAGES = 120
_KEEP_ROWS = 400


def _utcnow() -> datetime:
    return datetime.utcnow()


def format_tr_sec(dt: datetime | None) -> str:
    if not dt:
        return "—"
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return aware.astimezone(_TR).strftime("%d.%m.%Y %H:%M:%S")


def member_session_key_from_token(token: str) -> str:
    tok = (token or "").strip()
    if not tok:
        return ""
    return "m:" + hashlib.sha256(tok.encode()).hexdigest()[:16]


def admin_session_key_from_token(token: str) -> str:
    tok = (token or "").strip()
    if not tok:
        return ""
    return "a:" + hashlib.sha256(tok.encode()).hexdigest()[:16]


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
        label = str(item.get("label") or path).strip()
        if not path and not label:
            continue
        kind = str(item.get("kind") or "page").strip() or "page"
        out.append(
            {
                "path": (path or label)[:120],
                "label": (label or path)[:80],
                "at_tr": str(item.get("at_tr") or "")[:32],
                "kind": kind[:20],
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
        .first()
    )


def _close_open_row(row: PanelVisitLog, *, reason: str, when: datetime | None = None) -> None:
    now = when or _utcnow()
    row.logged_out_at = now
    row.last_seen_at = now
    row.end_reason = (reason or "logout")[:20]


def _insert_auth_visit_row(
    db: Session,
    *,
    session_key: str,
    email: str = "",
    display_name: str = "",
    session_kind: str = "",
    ip: str = "",
    device: str = "",
    path: str = "/",
    logged_in_at: datetime | None = None,
    close_previous: bool = True,
) -> PanelVisitLog | None:
    key = (session_key or "").strip()
    if not key:
        return None
    now = logged_in_at or _utcnow()
    page_path = (path or "/").split("?")[0] or "/"
    if close_previous:
        prev = _open_row(db, key)
        if prev is not None:
            _close_open_row(prev, reason="relogin", when=now)
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
        start_reason="auth",
        end_reason="",
    )
    db.add(row)
    db.flush()
    if should_track_admin_path(page_path):
        _append_activity(
            row,
            path=page_path,
            label=admin_path_label(page_path),
            kind="page",
        )
    if email:
        try:
            from backend.services import app_member_auth as ama

            ama.touch_member_last_login(db, email, now)
        except Exception:  # noqa: BLE001
            LOGGER.debug("üye son giriş güncellenemedi", exc_info=True)
    return row


def _sync_visits_from_login_events(db: Session, *, limit: int = 60) -> int:
    """OAuth visit satırı hiç yazılmadıysa admin_login_events'ten geriye dönük doldur."""
    from backend.models import AdminLoginEvent

    events = (
        db.query(AdminLoginEvent)
        .filter(AdminLoginEvent.event_type.in_(("member_login_ok", "member_register_ok")))
        .order_by(AdminLoginEvent.created_at.desc(), AdminLoginEvent.id.desc())
        .limit(max(1, min(int(limit), 200)))
        .all()
    )
    created = 0
    window = timedelta(minutes=5)
    for ev in events:
        email = (ev.actor_email or "").strip()
        if not email:
            continue
        backfill_key = f"login:{ev.id}"[:80]
        if db.query(PanelVisitLog.id).filter(PanelVisitLog.session_key == backfill_key).first():
            continue
        t0 = ev.created_at - window
        t1 = ev.created_at + window
        live = (
            db.query(PanelVisitLog.id)
            .filter(
                PanelVisitLog.email == email,
                PanelVisitLog.logged_in_at >= t0,
                PanelVisitLog.logged_in_at <= t1,
                PanelVisitLog.start_reason == "auth",
                ~PanelVisitLog.session_key.like("login:%"),
            )
            .first()
        )
        if live:
            continue
        db.add(
            PanelVisitLog(
                session_key=backfill_key,
                email=email[:255],
                display_name=email[:255],
                session_kind="member",
                ip=(ev.ip or "")[:64],
                device_label=(ev.device_label or "")[:120],
                logged_in_at=ev.created_at,
                last_seen_at=ev.created_at,
                logged_out_at=ev.created_at,
                pages_json="[]",
                start_reason="auth",
                end_reason="sync",
            )
        )
        created += 1
    return created


def _append_activity(
    row: PanelVisitLog,
    *,
    path: str,
    label: str,
    kind: str = "page",
) -> None:
    pages = _load_pages(row.pages_json)
    last = pages[-1] if pages else None
    if last and last.get("path") == path and last.get("kind") == kind:
        return
    pages.append(
        {
            "path": path[:120],
            "label": (label or path)[:80],
            "at_tr": datetime.now(_TR).strftime("%H:%M:%S"),
            "kind": (kind or "page")[:20],
        }
    )
    row.pages_json = _dump_pages(pages)


def open_auth_visit(
    *,
    session_key: str,
    email: str = "",
    display_name: str = "",
    session_kind: str = "",
    ip: str = "",
    device: str = "",
    path: str = "/",
) -> None:
    """Yalnızca gerçek Google/admin girişi sonrası çağrılır — Visit log «Signed in»."""
    key = (session_key or "").strip()
    if not key:
        return
    try:
        with SessionLocal() as db:
            _insert_auth_visit_row(
                db,
                session_key=key,
                email=email,
                display_name=display_name,
                session_kind=session_kind,
                ip=ip,
                device=device,
                path=path,
            )
            db.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("panel auth visit open failed: %s", exc, exc_info=True)


def ensure_auth_visit_open(
    *,
    session_key: str,
    email: str = "",
    display_name: str = "",
    session_kind: str = "",
    ip: str = "",
    device: str = "",
    path: str = "/",
) -> bool:
    """OAuth callback kaçırdıysa ilk panel isteğinde auth ziyaret satırı aç."""
    key = (session_key or "").strip()
    if not key or not key.startswith(("m:", "a:")):
        return False
    try:
        with SessionLocal() as db:
            if _open_row(db, key) is not None:
                return False
            _insert_auth_visit_row(
                db,
                session_key=key,
                email=email,
                display_name=display_name,
                session_kind=session_kind,
                ip=ip,
                device=device,
                path=path,
                close_previous=False,
            )
            db.commit()
            return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("ensure auth visit open failed: %s", exc, exc_info=True)
        return False


def touch_visit(
    *,
    session_key: str,
    email: str = "",
    display_name: str = "",
    session_kind: str = "",
    ip: str = "",
    device: str = "",
    path: str = "",
    allow_open: bool = False,
) -> None:
    """Mevcut açık ziyarete sayfa ekle / last_seen güncelle.

    allow_open=False (varsayılan): yeni «Signed in» satırı açmaz — yanlış pozitif yok.
    """
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
                if not allow_open:
                    return
                # Geriye dönük / test uyumu — üretim middleware allow_open=False kullanır
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
                    start_reason="legacy",
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
                _append_activity(
                    row,
                    path=page_path,
                    label=admin_path_label(page_path),
                    kind="page",
                )
            db.commit()
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel visit touch atlandı: %s", exc)


def record_feature_activity(
    *,
    session_key: str,
    feature: str,
    label: str = "",
    path: str = "",
) -> bool:
    """Menü / özellik kullanımı — açık auth ziyaretine eklenir."""
    key = (session_key or "").strip()
    feat = (feature or "").strip()
    if not key or not feat:
        return False
    now = _utcnow()
    lab = (label or feat).strip() or feat
    p = (path or f"feature:{feat}").split("?")[0][:120] or f"feature:{feat}"
    try:
        with SessionLocal() as db:
            row = _open_row(db, key)
            if row is None:
                return False
            row.last_seen_at = now
            _append_activity(row, path=p, label=lab[:80], kind="feature")
            db.commit()
            return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel feature activity atlandı: %s", exc)
        return False


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
            _close_open_row(row, reason=why, when=now)
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


def recent_visits(*, limit: int = 80, auth_only: bool = True) -> list[dict[str, Any]]:
    expire_idle()
    cap = max(1, min(int(limit), 200))
    try:
        with SessionLocal() as db:
            _trim_old(db)
            _sync_visits_from_login_events(db)
            db.commit()
            # Online (Open) üstte; grup içinde en son giriş önce
            q = db.query(PanelVisitLog).order_by(
                case((PanelVisitLog.logged_out_at.is_(None), 0), else_=1),
                PanelVisitLog.logged_in_at.desc(),
                PanelVisitLog.id.desc(),
            )
            if auth_only:
                # Middleware ile açılmış yanlış «Signed in» satırlarını gizle
                q = q.filter(PanelVisitLog.start_reason == "auth")
            rows = q.limit(cap).all()
            out: list[dict[str, Any]] = []
            for row in rows:
                pages = _load_pages(row.pages_json)
                open_now = row.logged_out_at is None
                reason = row.end_reason or ("open" if open_now else "logout")
                start = (row.start_reason or "").strip() or "auth"
                who = (row.display_name or "").strip() or (row.email or "").strip() or (
                    "Admin şifre" if row.session_kind == "admin" else "Üye"
                )
                end_label = "Open" if open_now else ("Logout" if reason == "logout" else "Idle")
                if reason == "relogin":
                    end_label = "Re-login"
                elif reason == "sync":
                    end_label = "Synced"
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
                        "logged_out_tr": format_tr_sec(row.logged_out_at) if not open_now else "Open",
                        "is_open": open_now,
                        "end_reason": reason,
                        "end_label": end_label,
                        "start_reason": start,
                        "start_label": "Google sign-in" if start == "auth" else start,
                        "pages": pages,
                        "page_count": len(pages),
                        "page_summary": " · ".join(
                            (p["label"] + (" ★" if p.get("kind") == "feature" else ""))
                            for p in pages[:12]
                        ),
                    }
                )
            return out
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("panel visit list failed: %s", exc)
        return []
