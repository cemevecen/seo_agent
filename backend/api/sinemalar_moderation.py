"""Sinemalar moderasyon özeti ingest + panel API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import sinemalar_moderation as mod

router = APIRouter(tags=["sinemalar-moderation"])


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


class ModerationDayBlock(BaseModel):
    date: str
    rows: list[dict[str, Any]] = Field(default_factory=list)


class ModerationIngestBody(BaseModel):
    source: str = "sinemalar_moderation"
    mode: str = "incremental"
    scraped_at: str = ""
    days: list[ModerationDayBlock] = Field(default_factory=list)
    backfill_complete: bool = False
    backfill_cursor: str | None = None


@router.post("/sinemalar-moderation/ingest")
def sinemalar_moderation_ingest(
    body: ModerationIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    payload = body.model_dump()
    payload["days"] = [{"date": d["date"], "rows": d["rows"]} for d in payload.get("days") or []]
    result = mod.ingest_backfill_payload(db, payload)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    return result


@router.get("/sinemalar-moderation/panel")
def sinemalar_moderation_panel(
    db: Session = Depends(get_db),
    start: str | None = Query(None),
    end: str | None = Query(None),
) -> dict[str, Any]:
    return mod.get_panel_payload(db, start=start, end=end)


@router.get("/sinemalar-moderation/meta")
def sinemalar_moderation_meta(db: Session = Depends(get_db)) -> dict[str, Any]:
    return mod.get_meta_summary(db)
