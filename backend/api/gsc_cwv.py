"""GSC Core Web Vitals + AMP scrape ingest API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import gsc_cwv_scrape_store as store

router = APIRouter(tags=["gsc-cwv"])


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


class GscCwvIngestBody(BaseModel):
    source: str = "gsc_cwv_scrape"
    scraped_at: str = ""
    snapshots: list[dict[str, Any]] = Field(default_factory=list)


@router.post("/gsc-cwv/ingest")
def gsc_cwv_ingest(
    body: GscCwvIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = store.ingest_gsc_cwv_payload(
        db,
        {
            "source": body.source,
            "scraped_at": body.scraped_at,
            "snapshots": body.snapshots,
        },
    )
    try:
        from backend.services.scrape_telemetry import record_scrape_ingest

        snaps = body.snapshots or []
        targets = sorted({str(s.get("domain") or s.get("site") or "").strip() for s in snaps if isinstance(s, dict)})
        targets = [t for t in targets if t] or ["gsc_cwv"]
        vol = int(result.get("row_count") or result.get("saved") or len(snaps) or 0)
        record_scrape_ingest(
            db,
            source="gsc_cwv",
            target=", ".join(targets)[:128],
            status="success" if result.get("ok") else "error",
            row_count=vol,
            message=str(result.get("message") or ""),
            scraped_at=body.scraped_at or None,
            detail={"snapshots": len(snaps)},
            commit=True,
        )
    except Exception:
        pass
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    return result
