"""App Store Connect scrape ingest + snapshot API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.asc_console_store import asc_console_payload, ingest_asc_console_payload

logger = logging.getLogger(__name__)

router = APIRouter(tags=["asc-console"])


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


class AscConsoleIngestBody(BaseModel):
    metrics: list[dict[str, Any]] = Field(default_factory=list)
    panels: dict[str, Any] = Field(default_factory=dict)
    raw_network: list[dict[str, Any]] = Field(default_factory=list)
    source: str | None = "asc_console_bridge"
    source_url: str | None = None
    bundle_id: str | None = "com.nokta.Finans.Takip"
    app_id: str | None = "465599322"
    sync_ok: bool = True
    sync_message: str | None = None
    sync_mode: str | None = "analytics_scrape"


@router.get("/asc-console/snapshot")
def get_asc_console_snapshot(db: Session = Depends(get_db)):
    return asc_console_payload(db)


@router.post("/asc-console/sync-store-reviews")
def post_sync_asc_store_reviews(
    db: Session = Depends(get_db),
    days: int = 365,
    app_id: str = "465599322",
    quick: bool = False,
):
    """App Store (iTunes RSS) yorumlarını çekip ASC workspace’e yazar."""
    from backend.services.app_store_reviews import sync_app_store_reviews_to_workspace

    try:
        return sync_app_store_reviews_to_workspace(
            db,
            track_id=(app_id or "465599322").strip() or "465599322",
            days=max(28, min(400, int(days or 365))),
            quick=bool(quick),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("asc sync-store-reviews failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/asc-console/ingest")
def post_asc_console_ingest(
    body: AscConsoleIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
):
    _check_ingest_token(authorization, x_notification_ingest_token)
    try:
        result = ingest_asc_console_payload(
            db,
            metrics=body.metrics,
            panels=body.panels,
            raw_network=body.raw_network,
            source=body.source or "asc_console_bridge",
            source_url=body.source_url,
            bundle_id=body.bundle_id,
            app_id=body.app_id,
            sync_ok=body.sync_ok,
            sync_message=body.sync_message,
            sync_mode=body.sync_mode or "analytics_scrape",
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("asc-console ingest failed")
        try:
            from backend.services.scrape_telemetry import record_scrape_ingest

            record_scrape_ingest(
                db,
                source="asc_console",
                target=body.bundle_id or "ASC",
                status="error",
                message=str(exc)[:500],
                commit=True,
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    try:
        from backend.services.scrape_telemetry import record_scrape_ingest

        record_scrape_ingest(
            db,
            source="asc_console",
            target=body.bundle_id or "ASC",
            status="success" if body.sync_ok else "error",
            row_count=len(body.metrics or []),
            message=body.sync_message or "",
            detail={"sync_mode": body.sync_mode},
            commit=True,
        )
    except Exception:
        pass
    return result


@router.get("/asc-console/coverage")
def get_asc_console_coverage(
    start: str | None = Query(None, description="YYYY-MM-DD (varsayılan: mühür+1)"),
    end: str | None = Query(None, description="YYYY-MM-DD (varsayılan: dün)"),
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
):
    """«Elimde şu günler var» — köprü boşluk doldurmak için bunu sorar."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    from backend.services.console_coverage import asc_coverage

    return asc_coverage(db, start=start, end=end)
