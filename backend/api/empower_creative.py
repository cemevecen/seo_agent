"""Empower Creative Review — panel API (scan/block kuyruk + sonuç)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import empower_creative_store as store

router = APIRouter(tags=["empower-creative"])


def _check_ingest_token(
    authorization: str | None,
    x_notification_ingest_token: str | None,
) -> None:
    expected = (settings.notification_ingest_token or "").strip()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="NOTIFICATION_INGEST_TOKEN tanımlı değil (Railway Variables).",
        )
    got = (x_notification_ingest_token or "").strip()
    if not got and authorization:
        raw = authorization.strip()
        if raw.lower().startswith("bearer "):
            got = raw[7:].strip()
        else:
            got = raw
    if not got or got != expected:
        raise HTTPException(status_code=401, detail="Geçersiz ingest token.")


def _caller_email(request: Request) -> str:
    try:
        from backend.services import app_member_auth as ama

        member = ama.member_from_request(request)
        if member and member.email:
            return str(member.email).strip()
    except Exception:
        pass
    return ""


class PendingBody(BaseModel):
    action: str = "scan"
    keyword: str = ""
    ids: list[str] = Field(default_factory=list)


class ScanIngestBody(BaseModel):
    ok: bool = True
    keyword: str = ""
    scanned: int = 0
    pages: int = 0
    matches: list[dict[str, Any]] = Field(default_factory=list)
    scraped_at: str = ""
    source: str = "empower_creative_bridge"
    message: str = ""


class BlockIngestBody(BaseModel):
    ok: bool = True
    keyword: str = ""
    blocked: list[dict[str, Any]] = Field(default_factory=list)
    failed: list[dict[str, Any]] = Field(default_factory=list)
    scraped_at: str = ""
    source: str = "empower_creative_bridge"
    message: str = ""


@router.get("/empower-creative/state")
def empower_creative_state(db: Session = Depends(get_db)) -> dict[str, Any]:
    return store.get_state(db)


@router.post("/empower-creative/pending")
def empower_creative_pending(
    body: PendingBody,
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    out = store.set_pending(
        db,
        action=body.action,
        keyword=body.keyword,
        ids=body.ids,
        requested_by=_caller_email(request),
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("message") or "pending failed")
    return out


@router.get("/empower-creative/pending")
def empower_creative_pending_get(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
    pop: bool = False,
) -> dict[str, Any]:
    """Bridge: bekleyen işi oku; pop=1 ile alıp running yap."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    if pop:
        pending = store.pop_pending(db)
        return {"ok": True, "pending": pending}
    return {"ok": True, "pending": store.peek_pending(db)}


@router.post("/empower-creative/ingest-scan")
def empower_creative_ingest_scan(
    body: ScanIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    return store.save_scan(db, body.model_dump())


@router.post("/empower-creative/ingest-block")
def empower_creative_ingest_block(
    body: BlockIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    return store.save_block(db, body.model_dump())


@router.post("/empower-creative/status")
def empower_creative_status(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
    status: str = "",
    message: str = "",
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    store.set_status(db, status=status, message=message)
    return {"ok": True}
