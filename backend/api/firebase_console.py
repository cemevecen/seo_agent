"""Firebase Console Crashlytics scrape ingest + snapshot + filtered query API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.firebase_console_store import (
    firebase_console_payload,
    ingest_firebase_console_payload,
    query_firebase_console,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["firebase-console"])


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


class FirebaseConsoleIngestBody(BaseModel):
    metrics: list[dict[str, Any]] = Field(default_factory=list)
    panels: dict[str, Any] = Field(default_factory=dict)
    raw_network: list[dict[str, Any]] = Field(default_factory=list)
    source: str | None = "firebase_console_bridge"
    source_url: str | None = None
    sync_ok: bool = True
    sync_message: str | None = None
    sync_mode: str | None = "crashlytics_scrape"
    scrape_days: int = 365


@router.get("/firebase-console/snapshot")
def get_firebase_console_snapshot(db: Session = Depends(get_db)):
    return firebase_console_payload(db)


@router.get("/firebase-console/query")
def get_firebase_console_query(
    db: Session = Depends(get_db),
    platform: str = Query("all"),
    days: int = Query(30, ge=1, le=365),
    version: str | None = None,
    device: str | None = None,
):
    return query_firebase_console(
        db,
        platform=platform,
        days=days,
        version=version,
        device=device,
    )


@router.post("/firebase-console/ingest")
def post_firebase_console_ingest(
    body: FirebaseConsoleIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
):
    _check_ingest_token(authorization, x_notification_ingest_token)
    try:
        return ingest_firebase_console_payload(db, body.model_dump())
    except Exception as exc:  # noqa: BLE001
        logger.exception("firebase-console ingest failed")
        raise HTTPException(status_code=500, detail=str(exc)[:200]) from exc
