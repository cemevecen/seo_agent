"""Empower Intelligence scrape ingest API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import empower_intel_store as store
from backend.services.empower_intel_config import meta_payload

router = APIRouter(tags=["empower-intel"])


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


class EmpowerIntelIngestBody(BaseModel):
    project: str = "doviz"
    source: str = "empower_intel_bridge"
    scraped_at: str = ""
    sync_message: str = ""
    platforms: list[dict[str, Any]] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    snapshots: list[dict[str, Any]] = Field(default_factory=list)


@router.post("/empower-intel/ingest")
def empower_intel_ingest(
    body: EmpowerIntelIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    payload = body.model_dump()
    result = store.ingest_payload(db, payload)
    try:
        from backend.services.scrape_telemetry import record_scrape_ingest

        plats = body.platforms or body.snapshots or []
        targets = sorted(
            {
                str(p.get("platform") or "").strip()
                for p in plats
                if isinstance(p, dict) and str(p.get("platform") or "").strip()
            }
        )
        record_scrape_ingest(
            db,
            source="empower_intel",
            target=f"{body.project} · {', '.join(targets)}"[:128] or body.project,
            status="success" if result.get("ok") else "error",
            row_count=int(result.get("row_count") or 0),
            message=str(result.get("message") or body.sync_message or ""),
            scraped_at=body.scraped_at or None,
            commit=True,
        )
    except Exception:
        pass
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    return result


@router.get("/empower-intel/meta")
def empower_intel_meta() -> dict[str, Any]:
    return meta_payload()


@router.get("/empower-intel/summary")
def empower_intel_summary(
    project: str = Query("doviz"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    return store.summary(db, project=project)


@router.get("/empower-intel/rows")
def empower_intel_rows(
    project: str = Query("doviz"),
    platform: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
    limit: int = Query(5000, ge=1, le=50000),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return store.query_rows(
            db,
            project=project,
            platform=platform,
            start=start,
            end=end,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/empower-intel/series")
def empower_intel_series(
    project: str = Query("doviz"),
    platform: str = Query("android"),
    metric: str = Query(""),
    start: str | None = Query(None),
    end: str | None = Query(None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return store.query_series(
            db,
            project=project,
            platform=platform,
            metric=metric,
            start=start,
            end=end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
