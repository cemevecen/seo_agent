"""Ad Manager Policy Center scrape ingest API (Mac bridge)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import SessionLocal, get_db
from backend.services import policy_csv as pcsv

router = APIRouter(tags=["policy"])


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


class PolicyIngestBody(BaseModel):
    source: str = "admanager_policy_scrape"
    scraped_at: str = ""
    message: str = ""
    network_id: str = ""
    site_filter: str = "sinemalar.com"
    method: str = ""
    rows: list[dict[str, Any]] = Field(default_factory=list)
    csv_base64: str | None = None


class NoAdsIngestBody(BaseModel):
    source: str = "sinemalar_noads"
    scraped_at: str = ""
    message: str = ""
    entries: list[dict[str, Any] | str] = Field(default_factory=list)
    urls: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


@router.post("/policy/ingest")
def policy_ingest(
    body: PolicyIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = pcsv.ingest_scrape_payload(
        db,
        {
            "source": body.source,
            "scraped_at": body.scraped_at,
            "message": body.message,
            "network_id": body.network_id,
            "site_filter": body.site_filter,
            "method": body.method,
            "rows": body.rows,
            "csv_base64": body.csv_base64,
        },
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    # Arka planda eksik başlıkları çek
    try:
        pcsv.start_title_job(SessionLocal, only_missing=True)
    except Exception:
        pass
    return result


@router.post("/policy/noads/ingest")
def policy_noads_ingest(
    body: NoAdsIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Sinemalar management/noAds listesi → policy URL eşleştirme + alarm."""
    from backend.services import sinemalar_noads as noads

    _check_ingest_token(authorization, x_notification_ingest_token)
    entries: list[Any] = list(body.entries or [])
    if body.urls:
        entries.extend(body.urls)
    if body.rows:
        entries.extend(body.rows)
    result = noads.ingest_noads_payload(
        db,
        {
            "source": body.source,
            "scraped_at": body.scraped_at,
            "message": body.message,
            "entries": entries,
        },
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "noads ingest failed")
    return result
