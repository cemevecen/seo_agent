"""Scrape ingest report API — bridge başarısız turları ve telemetri yazımı."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import scrape_telemetry as st

router = APIRouter(tags=["scrape-telemetry"])


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


class ScrapeRunReportBody(BaseModel):
    source: str
    target: str = ""
    status: str = "error"
    row_count: int = 0
    message: str = ""
    scraped_at: str = ""
    detail: dict[str, Any] = Field(default_factory=dict)


@router.post("/scrape-runs/report")
def scrape_runs_report(
    body: ScrapeRunReportBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Bridge tarafı başarısız/başarılı tur kaydı (payload olmadan)."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    src = (body.source or "").strip().lower()
    if not src:
        raise HTTPException(status_code=400, detail="source gerekli")
    row = st.record_scrape_ingest(
        db,
        source=src[:64],
        target=(body.target or "")[:128],
        status=body.status,
        row_count=body.row_count,
        message=body.message,
        detail=body.detail or {"via": "scrape_runs_report"},
        scraped_at=body.scraped_at or None,
        commit=True,
    )
    return {"ok": True, "id": row.id if row else None}
