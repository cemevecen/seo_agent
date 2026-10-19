"""Empower Creative Review — pending job + last scan/block sonuçları."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import EmpowerCreativeReviewState

STATE_ID = 1


def _loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _row(db: Session) -> EmpowerCreativeReviewState:
    row = db.get(EmpowerCreativeReviewState, STATE_ID)
    if row is None:
        row = EmpowerCreativeReviewState(id=STATE_ID)
        db.add(row)
        db.flush()
    return row


def get_state(db: Session) -> dict[str, Any]:
    row = _row(db)
    return {
        "ok": True,
        "status": row.status or "",
        "message": row.message or "",
        "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        "pending": _loads(row.pending_json),
        "last_scan": _loads(row.last_scan_json),
        "last_block": _loads(row.last_block_json),
    }


def set_pending(
    db: Session,
    *,
    action: str,
    keyword: str = "",
    ids: list[str] | None = None,
    requested_by: str = "",
) -> dict[str, Any]:
    action = (action or "").strip().lower()
    if action not in ("scan", "block"):
        return {"ok": False, "message": "action scan|block olmalı"}
    kw = (keyword or "").strip()
    id_list = [str(x).strip().lower() for x in (ids or []) if str(x).strip()]
    if action == "scan" and len(kw) < 2:
        return {"ok": False, "message": "keyword en az 2 karakter"}
    if action == "block" and not id_list:
        return {"ok": False, "message": "engellenecek id yok"}
    row = _row(db)
    pending = {
        "action": action,
        "keyword": kw,
        "ids": id_list,
        "requested_by": (requested_by or "")[:120],
        "requested_at": datetime.utcnow().isoformat() + "Z",
    }
    row.pending_json = json.dumps(pending, ensure_ascii=False)
    row.status = "queued"
    row.message = f"{action} kuyrukta · {kw or (str(len(id_list)) + ' id')}"
    row.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "pending": pending, "message": row.message}


def pop_pending(db: Session) -> dict[str, Any] | None:
    row = _row(db)
    pending = _loads(row.pending_json)
    if not pending.get("action"):
        return None
    row.pending_json = "{}"
    row.status = "running"
    row.message = f"{pending.get('action')} çalışıyor…"
    row.updated_at = datetime.utcnow()
    db.commit()
    return pending


def peek_pending(db: Session) -> dict[str, Any]:
    return _loads(_row(db).pending_json)


def save_scan(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    row = _row(db)
    matches = payload.get("matches") if isinstance(payload.get("matches"), list) else []
    scanned = int(payload.get("scanned") or payload.get("scanned_count") or 0)
    body = {
        "keyword": str(payload.get("keyword") or ""),
        "scanned": scanned,
        "match_count": len(matches),
        "matches": matches[:500],
        "pages": int(payload.get("pages") or 0),
        "scraped_at": str(payload.get("scraped_at") or datetime.utcnow().isoformat() + "Z"),
        "source": str(payload.get("source") or "empower_creative_bridge")[:64],
        "message": str(payload.get("message") or "")[:500],
    }
    row.last_scan_json = json.dumps(body, ensure_ascii=False)
    row.status = "scan_done" if payload.get("ok", True) else "scan_error"
    row.message = body["message"] or f"Tarama · {body['match_count']} eşleşme / {scanned} kart"
    row.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "last_scan": body, "message": row.message}


def save_block(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    row = _row(db)
    blocked = payload.get("blocked") if isinstance(payload.get("blocked"), list) else []
    failed = payload.get("failed") if isinstance(payload.get("failed"), list) else []
    body = {
        "keyword": str(payload.get("keyword") or ""),
        "blocked": blocked[:500],
        "failed": failed[:200],
        "blocked_count": len(blocked),
        "failed_count": len(failed),
        "scraped_at": str(payload.get("scraped_at") or datetime.utcnow().isoformat() + "Z"),
        "source": str(payload.get("source") or "empower_creative_bridge")[:64],
        "message": str(payload.get("message") or "")[:500],
    }
    row.last_block_json = json.dumps(body, ensure_ascii=False)
    row.status = "block_done" if payload.get("ok", True) else "block_error"
    row.message = body["message"] or f"Engelleme · {len(blocked)} ok / {len(failed)} hata"
    row.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True, "last_block": body, "message": row.message}


def set_status(db: Session, *, status: str, message: str = "") -> None:
    row = _row(db)
    row.status = (status or "")[:32]
    row.message = (message or "")[:500]
    row.updated_at = datetime.utcnow()
    db.commit()
