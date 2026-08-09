"""GSC backlink dashboard + risk analizi API (scrape ingest: /api/gsc-links)."""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import ExternalSite, Site
from backend.rate_limiter import limiter
from backend.services import backlink_csv

router = APIRouter(tags=["backlinks"])


def _require_internal_site(db: Session, site_id: int) -> Site:
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    if (
        db.query(ExternalSite.site_id).filter(ExternalSite.site_id == site_id).first()
        is not None
    ):
        raise HTTPException(status_code=403, detail="Harici siteler bu özellikte kullanılamaz.")
    return site


class DomainActionBody(BaseModel):
    site_id: int
    domain: str
    action: str = Field(description="ignore|monitor|review|disavow")


class BacklinkImportBody(BaseModel):
    """Legacy body — endpoints return 410; scrape ingest uses /api/gsc-links/ingest."""

    site_id: int = 0
    report_type: str = "external"
    csv_text: str | None = None
    sheets_url: str | None = None
    source_filename: str | None = None


@router.get("/backlinks/report-types")
@limiter.limit("120/minute")
def backlinks_report_types(request: Request) -> dict[str, Any]:
    return {
        "items": [
            {"id": "external", "label": "External (top linked pages)"},
            {"id": "domain", "label": "Linking sites"},
            {"id": "anchor_text", "label": "Anchor text"},
            {"id": "internal", "label": "Internal links"},
        ]
    }


@router.get("/backlinks/dashboard")
@limiter.limit("120/minute")
def backlinks_dashboard(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("external"),
    gsc_resource_id: str = Query(""),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _require_internal_site(db, site_id)
    return backlink_csv.build_dashboard(
        db,
        site_id=site_id,
        report_type=report_type,
        gsc_resource_id=gsc_resource_id,
    )


@router.get("/backlinks/domain-links")
@limiter.limit("60/minute")
def backlinks_domain_links(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("external"),
    domain: str = Query(..., min_length=1),
    limit: int = Query(0, ge=0, description="0 = sınırsız"),
    all_link_imports: bool = Query(False, description="Tüm link importları (top target pages hariç)"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _require_internal_site(db, site_id)
    try:
        return backlink_csv.list_domain_links(
            db,
            site_id=site_id,
            report_type=report_type,
            domain=domain,
            limit=limit,
            all_link_imports=all_link_imports,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/backlinks/target-page-links")
@limiter.limit("60/minute")
def backlinks_target_page_links(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("external"),
    target_url: str = Query(..., min_length=1),
    link_kind: str = Query("all", description="all|external|internal"),
    limit: int = Query(0, ge=0, description="0 = sınırsız"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _require_internal_site(db, site_id)
    try:
        return backlink_csv.list_target_page_links(
            db,
            site_id=site_id,
            report_type=report_type,
            target_url=target_url,
            link_kind=link_kind,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backlinks/import-json")
@limiter.limit("20/minute")
def backlinks_import_json(
    request: Request,
    body: BacklinkImportBody,
) -> dict[str, Any]:
    """Manuel CSV/Sheets kapatıldı — yalnızca GSC Links scrape."""
    _ = body
    raise HTTPException(
        status_code=410,
        detail=(
            "Manuel CSV / Sheets import kapatıldı. "
            "Veri GSC Links scrape ile gelir (Mac bridge: POST :18765/sync-gsc-links)."
        ),
    )


@router.post("/backlinks/import")
@limiter.limit("20/minute")
async def backlinks_import(
    request: Request,
) -> dict[str, Any]:
    """Manuel dosya yükleme kapatıldı — yalnızca GSC Links scrape."""
    raise HTTPException(
        status_code=410,
        detail=(
            "Manuel dosya yükleme kapatıldı. "
            "Veri GSC Links scrape ile gelir (Mac bridge: POST :18765/sync-gsc-links)."
        ),
    )


@router.patch("/backlinks/domain-action")
@limiter.limit("60/minute")
def backlinks_domain_action(
    request: Request,
    body: DomainActionBody,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _require_internal_site(db, body.site_id)
    try:
        return backlink_csv.set_domain_action(
            db, site_id=body.site_id, domain=body.domain, action=body.action
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/backlinks/imports/{import_id}")
@limiter.limit("30/minute")
def backlinks_delete_import(
    request: Request,
    import_id: int,
    site_id: int = Query(..., ge=1),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    _require_internal_site(db, site_id)
    try:
        return backlink_csv.delete_backlink_import(db, site_id=site_id, import_id=import_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/backlinks/disavow.txt", response_class=PlainTextResponse)
@limiter.limit("30/minute")
def backlinks_disavow_txt(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("domain"),
    db: Session = Depends(get_db),
) -> PlainTextResponse:
    _require_internal_site(db, site_id)
    text = backlink_csv.build_disavow_text(db, site_id=site_id, report_type=report_type)
    return PlainTextResponse(text, media_type="text/plain; charset=utf-8")
