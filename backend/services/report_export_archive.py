"""İndirilen rapor arşivi — 7 gün DB; Settings'te yalnızca allowlist."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.models import ReportExportArchive
from backend.services.panel_visit_log import format_tr_sec

LOGGER = logging.getLogger(__name__)

RETENTION_DAYS = 7

REPORT_KIND_LABELS: dict[str, str] = {
    "sheet_ayilma": "Ayılma çizelgesi",
}


def _purge_expired(db: Session) -> None:
    cutoff = datetime.utcnow()
    db.query(ReportExportArchive).filter(ReportExportArchive.expires_at < cutoff).delete(
        synchronize_session=False
    )


def save_export(
    db: Session,
    *,
    report_kind: str,
    export_format: str,
    filename: str,
    content: bytes,
    media_type: str,
    actor_email: str,
    actor_display_name: str,
    client_ip: str,
    meta: dict[str, Any] | None = None,
) -> int:
    """İndirilen raporu kaydet; süresi dolmuş kayıtları sil."""
    now = datetime.utcnow()
    row = ReportExportArchive(
        report_kind=(report_kind or "").strip()[:64],
        export_format=(export_format or "").strip()[:16],
        filename=(filename or "export.bin").strip()[:255],
        media_type=(media_type or "application/octet-stream").strip()[:128],
        content=content,
        actor_email=(actor_email or "").strip()[:255],
        actor_display_name=(actor_display_name or "").strip()[:255],
        client_ip=(client_ip or "").strip()[:64],
        meta_json=json.dumps(meta or {}, ensure_ascii=False),
        downloaded_at=now,
        expires_at=now + timedelta(days=RETENTION_DAYS),
    )
    db.add(row)
    db.flush()
    export_id = int(row.id)
    _purge_expired(db)
    db.commit()
    return export_id


def list_recent(*, limit: int = 80) -> list[dict[str, Any]]:
    from backend.database import SessionLocal

    with SessionLocal() as db:
        _purge_expired(db)
        db.commit()
        rows = (
            db.query(ReportExportArchive)
            .order_by(ReportExportArchive.downloaded_at.desc(), ReportExportArchive.id.desc())
            .limit(max(1, min(limit, 200)))
            .all()
        )
        return [_row_payload(r, include_meta=True) for r in rows]


def get_export_bytes(export_id: int) -> tuple[bytes, str, str] | None:
    from backend.database import SessionLocal

    with SessionLocal() as db:
        row = db.query(ReportExportArchive).filter(ReportExportArchive.id == export_id).first()
        if row is None or row.expires_at < datetime.utcnow():
            return None
        return row.content, row.filename, row.media_type


def _row_payload(row: ReportExportArchive, *, include_meta: bool) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    if include_meta:
        try:
            meta = json.loads(row.meta_json or "{}")
        except json.JSONDecodeError:
            meta = {}
    who = (row.actor_display_name or "").strip() or row.actor_email or "—"
    kind = row.report_kind or ""
    return {
        "id": row.id,
        "report_kind": kind,
        "report_label": REPORT_KIND_LABELS.get(kind, kind or "Rapor"),
        "export_format": row.export_format,
        "filename": row.filename,
        "actor_email": row.actor_email,
        "actor_display_name": row.actor_display_name,
        "who": who,
        "client_ip": row.client_ip,
        "downloaded_at_tr": format_tr_sec(row.downloaded_at),
        "expires_at_tr": format_tr_sec(row.expires_at),
        "meta": meta,
    }
