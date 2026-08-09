"""GSC Links scrape ingest API (Mac bridge)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.models import ExternalSite, Site
from backend.services import gsc_links_scrape_store as store

router = APIRouter(tags=["gsc-links"])


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


class GscLinksIngestBody(BaseModel):
    source: str = "gsc_links_bridge"
    scraped_at: str = ""
    message: str = ""
    snapshots: list[dict[str, Any]] = Field(default_factory=list)


@router.post("/gsc-links/ingest")
def gsc_links_ingest(
    body: GscLinksIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = store.ingest_gsc_links_payload(
        db,
        {
            "source": body.source,
            "scraped_at": body.scraped_at,
            "message": body.message,
            "snapshots": body.snapshots,
        },
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    return result


@router.get("/gsc-links/properties")
def gsc_links_properties(
    site_id: int = Query(...),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    if db.query(ExternalSite.site_id).filter(ExternalSite.site_id == site_id).first() is not None:
        raise HTTPException(status_code=403, detail="Harici siteler bu özellikte kullanılamaz.")
    props = store.properties_for_site(site)
    return {
        "site_id": site_id,
        "domain": site.domain,
        "properties": props,
        "report_types": [
            {"id": "external", "label": "External (top linked pages)", "gsc_type": "EXTERNAL"},
            {"id": "domain", "label": "Linking sites", "gsc_type": "DOMAIN"},
            {"id": "anchor_text", "label": "Anchor text", "gsc_type": "ANCHOR_TEXT"},
            {"id": "internal", "label": "Internal links", "gsc_type": "INTERNAL"},
        ],
    }


@router.get("/gsc-links/changes")
def gsc_links_changes(
    site_id: int = Query(...),
    report_type: str = Query("external"),
    gsc_resource_id: str = Query(""),
    window: str = Query("daily"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    if db.query(ExternalSite.site_id).filter(ExternalSite.site_id == site_id).first() is not None:
        raise HTTPException(status_code=403, detail="Harici siteler bu özellikte kullanılamaz.")
    return store.build_change_window(
        db,
        site_id=site_id,
        report_type=report_type,
        gsc_resource_id=gsc_resource_id,
        window=window,
    )
