"""Owner PM lab ingest + snapshot API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.pm_lab_store import (
    PM_LAB_REFRESH_JOBS,
    claim_pm_lab_refresh,
    enqueue_pm_lab_refresh,
    ingest_pm_lab_payload,
    load_payload,
    pm_lab_refresh_status,
    serp_cycle_meta,
)

router = APIRouter(tags=["pm-lab"])


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


class PmLabIngestBody(BaseModel):
    sections: dict[str, Any] = Field(default_factory=dict)
    scraped_at: str = ""
    source: str = "pm_lab_scrape"
    sync_ok: bool = True
    sync_message: str = ""
    replace: bool = False


@router.get("/pm-lab/state")
def pm_lab_state(db: Session = Depends(get_db)) -> dict[str, Any]:
    payload = load_payload(db)
    sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}
    slim: dict[str, Any] = {}
    for key, block in sections.items():
        if not isinstance(block, dict):
            continue
        copy = dict(block)
        copy.pop("shots", None)
        if key == "serp":
            meta = serp_cycle_meta(copy)
            if meta["missing_batches"]:
                copy["serp_missing_batches"] = meta["missing_batches"]
            copy["serp_cycle_resume"] = bool(meta["resume"])
            copy["serp_cycle_stale"] = bool(meta["stale"])
        slim[key] = copy
    status = pm_lab_refresh_status(payload)
    return {
        "ok": True,
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "sections": slim,
        "queued": status["queued"],
        "running": status["running"],
    }


class PmLabRefreshBody(BaseModel):
    job: str = ""


@router.post("/pm-lab/refresh")
def pm_lab_refresh(body: PmLabRefreshBody, db: Session = Depends(get_db)) -> dict[str, Any]:
    job = str(body.job or "").strip()
    if job not in PM_LAB_REFRESH_JOBS:
        raise HTTPException(status_code=400, detail="unknown job")
    return enqueue_pm_lab_refresh(db, job)


@router.get("/pm-lab/claim-refresh")
def pm_lab_claim_refresh(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    job = claim_pm_lab_refresh(db)
    return {"ok": True, "job": job or ""}


@router.post("/pm-lab/ingest")
def pm_lab_ingest(
    body: PmLabIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = ingest_pm_lab_payload(db, body.model_dump())
    incoming = body.sections if isinstance(body.sections, dict) else {}
    if "competitors" in incoming:
        try:
            from backend.services.pm_lab_sapma_alerts import notify_competitor_sapma

            notify_competitor_sapma(db, incoming.get("competitors") if isinstance(incoming.get("competitors"), dict) else {})
        except Exception:
            pass
    try:
        from backend.services.scrape_telemetry import record_scrape_ingest

        record_scrape_ingest(
            db,
            source="pm_lab",
            target="owner-pm-lab",
            status="success" if result.get("ok") else "error",
            row_count=int(result.get("section_count") or 0),
            message=str(result.get("keys") or ""),
            detail={"keys": result.get("keys")},
            commit=True,
        )
    except Exception:
        pass
    return result
