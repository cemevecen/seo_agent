"""Owner PM lab ingest + snapshot API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.pm_lab_store import ingest_pm_lab_payload, load_payload

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
        shots = copy.get("shots") if isinstance(copy.get("shots"), dict) else {}
        copy["shots"] = sorted(str(k) for k in shots.keys())
        slim[key] = copy
    return {
        "ok": True,
        "updated_at": payload.get("updated_at"),
        "scraped_at": payload.get("scraped_at"),
        "sync_ok": payload.get("sync_ok", True),
        "sync_message": payload.get("sync_message") or "",
        "sections": slim,
    }


@router.post("/pm-lab/ingest")
def pm_lab_ingest(
    body: PmLabIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = ingest_pm_lab_payload(db, body.model_dump())
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
