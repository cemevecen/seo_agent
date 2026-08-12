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


class ModerationDetailBatch(BaseModel):
    user_id: int
    username: str = ""
    metric_type: str
    source_url: str | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)


class ModerationIngestBody(BaseModel):
    source: str = "sinemalar_moderation"
    mode: str = "incremental"
    scraped_at: str = ""
    days: list[ModerationDayBlock] = Field(default_factory=list)
    detail_batches: list[ModerationDetailBatch] = Field(default_factory=list)
    range_start: str | None = None
    range_end: str | None = None
    backfill_complete: bool = False
    backfill_cursor: str | None = None
    purge_first: bool = False


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
    payload["detail_batches"] = [
        {
            "user_id": b["user_id"],
            "username": b.get("username") or "",
            "metric_type": b["metric_type"],
            "source_url": b.get("source_url"),
            "items": b.get("items") or [],
            "_recompute_daily": b.get("_recompute_daily"),
        }
        for b in payload.get("detail_batches") or []
    ]
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


@router.get("/sinemalar-moderation/details")
def sinemalar_moderation_details(
    db: Session = Depends(get_db),
    start: str | None = Query(None),
    end: str | None = Query(None),
    user_id: int | None = Query(None),
    metric_type: str | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    return mod.get_detail_payload(
        db,
        start=start,
        end=end,
        user_id=user_id,
        metric_type=metric_type,
        limit=limit,
        offset=offset,
    )


@router.post("/sinemalar-moderation/purge")
def sinemalar_moderation_purge(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    return mod.purge_all_data(db)


@router.get("/sinemalar-moderation/meta")
def sinemalar_moderation_meta(db: Session = Depends(get_db)) -> dict[str, Any]:
    return mod.get_meta_summary(db)
