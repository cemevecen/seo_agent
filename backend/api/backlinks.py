"""GSC backlink import + risk analizi API."""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import Site
from backend.rate_limiter import limiter
from backend.services import backlink_csv

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["backlinks"])


class DomainActionBody(BaseModel):
    site_id: int
    domain: str
    action: str = Field(description="ignore|monitor|review|disavow")


@router.get("/backlinks/report-types")
@limiter.limit("120/minute")
def backlinks_report_types(request: Request) -> dict[str, Any]:
    return {
        "items": [
            {"id": "latest_links", "label": "Latest links"},
            {"id": "more_sample", "label": "More sample links"},
            {"id": "top_linking_sites", "label": "Top linking sites"},
        ]
    }


@router.get("/backlinks/dashboard")
@limiter.limit("120/minute")
def backlinks_dashboard(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("latest_links"),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    return backlink_csv.build_dashboard(db, site_id=site_id, report_type=report_type)


@router.post("/backlinks/import")
@limiter.limit("20/minute")
async def backlinks_import(
    request: Request,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    form = await request.form()
    try:
        site_id = int(form.get("site_id"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="site_id gerekli.") from exc
    report_type = str(form.get("report_type") or "latest_links")
    csv_text = str(form.get("csv_text") or "").strip()
    sheets_url = str(form.get("sheets_url") or "").strip()

    text = csv_text
    fname = ""
    kind = "csv_paste"
    upload = form.get("file")
    if upload is not None and getattr(upload, "filename", None):
        raw = await upload.read()
        if len(raw) > 15_000_000:
            raise HTTPException(status_code=413, detail="Dosya çok büyük (max ~15MB).")
        text = raw.decode("utf-8", errors="replace")
        fname = str(getattr(upload, "filename", None) or "upload.csv")
        kind = "csv_upload"
    elif sheets_url:
        try:
            text = backlink_csv.fetch_public_sheet_csv(sheets_url)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"Sheets CSV alınamadı: {exc}") from exc
        fname = "google_sheets.csv"
        kind = "google_sheets"
    if not text:
        raise HTTPException(status_code=400, detail="CSV dosyası, yapıştırılmış metin veya Sheets URL gerekli.")
    try:
        return backlink_csv.import_backlink_csv(
            db,
            site_id=site_id,
            report_type=report_type,
            csv_text=text,
            source_filename=fname,
            source_kind=kind,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("backlinks import failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.patch("/backlinks/domain-action")
@limiter.limit("60/minute")
def backlinks_domain_action(
    request: Request,
    body: DomainActionBody,
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return backlink_csv.set_domain_action(
            db, site_id=body.site_id, domain=body.domain, action=body.action
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/backlinks/disavow.txt", response_class=PlainTextResponse)
@limiter.limit("30/minute")
def backlinks_disavow_txt(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("latest_links"),
    db: Session = Depends(get_db),
) -> PlainTextResponse:
    site = db.query(Site).filter(Site.id == site_id).first()
    if site is None:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    text = backlink_csv.build_disavow_text(db, site_id=site_id, report_type=report_type)
    return PlainTextResponse(text, media_type="text/plain; charset=utf-8")
