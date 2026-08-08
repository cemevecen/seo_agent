"""Play Console scrape ingest + UI API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.play_console_store import ingest_play_console_payload, play_console_payload

logger = logging.getLogger(__name__)

router = APIRouter(tags=["play-console"])


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


class PlayConsoleIngestBody(BaseModel):
    metrics: list[dict[str, Any]] = Field(default_factory=list)
    reviews: list[dict[str, Any]] = Field(default_factory=list)
    rating_summary: dict[str, Any] = Field(default_factory=dict)
    raw_network: list[dict[str, Any]] = Field(default_factory=list)
    source: str | None = "play_console_bridge"
    source_url: str | None = None
    package_name: str | None = "com.Doviz"
    app_id: str | None = None
    sync_ok: bool = True
    sync_message: str | None = None
    sync_mode: str | None = "dashboard_reviews"


@router.get("/play-console/snapshot")
def get_play_console_snapshot(db: Session = Depends(get_db)):
    return play_console_payload(db)


@router.post("/play-console/ingest")
def post_play_console_ingest(
    body: PlayConsoleIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
):
    _check_ingest_token(authorization, x_notification_ingest_token)
    try:
        return ingest_play_console_payload(
            db,
            metrics=body.metrics,
            reviews=body.reviews,
            rating_summary=body.rating_summary,
            raw_network=body.raw_network,
            source=body.source or "play_console_bridge",
            source_url=body.source_url,
            package_name=body.package_name,
            app_id=body.app_id,
            sync_ok=body.sync_ok,
            sync_message=body.sync_message,
            sync_mode=body.sync_mode or "dashboard_reviews",
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("play-console ingest failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
