"""GSC backlink import + risk analizi API."""

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import ExternalSite, Site
from backend.rate_limiter import limiter
from backend.services import backlink_csv

LOGGER = logging.getLogger(__name__)
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
    site_id: int
    report_type: str = "latest_links"
    csv_text: str | None = None
    sheets_url: str | None = None
    source_filename: str | None = None


def _run_backlink_import(
    db: Session,
    *,
    site_id: int,
    report_type: str,
    csv_text: str = "",
    sheets_url: str = "",
    source_filename: str = "",
    source_kind: str = "csv_paste",
) -> dict[str, Any]:
    _require_internal_site(db, site_id)
    paste = (csv_text or "").strip()
    url = (sheets_url or "").strip()
    if paste:
        text = paste
        fname = (source_filename or "")[:255]
        kind = source_kind or "csv_paste"
    elif url:
        try:
            text = backlink_csv.fetch_public_sheet_csv(url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"Sheets CSV alınamadı: {exc}") from exc
        fname = "google_sheets.csv"
        kind = "google_sheets"
    else:
        raise HTTPException(
            status_code=400,
            detail="CSV dosyası, yapıştırılmış metin veya Sheets URL gerekli.",
        )
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


@router.get("/backlinks/report-types")
@limiter.limit("120/minute")
def backlinks_report_types(request: Request) -> dict[str, Any]:
    return {
        "items": [
            {"id": "latest_links", "label": "Latest links"},
            {"id": "more_sample", "label": "More sample links"},
            {"id": "top_linking_sites", "label": "Top linking sites"},
            {"id": "top_target_pages", "label": "Top external links"},
            {"id": "top_target_pages_internal", "label": "Top internal links"},
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
    _require_internal_site(db, site_id)
    return backlink_csv.build_dashboard(db, site_id=site_id, report_type=report_type)


@router.get("/backlinks/domain-links")
@limiter.limit("60/minute")
def backlinks_domain_links(
    request: Request,
    site_id: int = Query(..., ge=1),
    report_type: str = Query("latest_links"),
    domain: str = Query(..., min_length=1),
    limit: int = Query(10000, ge=1, le=50000),
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
    report_type: str = Query("latest_links"),
    target_url: str = Query(..., min_length=1),
    link_kind: str = Query("all", description="all|external|internal"),
    limit: int = Query(10000, ge=1, le=50000),
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
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """Büyük CSV için JSON gövdesi (multipart 1MB parça limitinden kaçınır)."""
    csv_text = (body.csv_text or "").strip()
    if len(csv_text.encode("utf-8", errors="replace")) > 15_000_000:
        raise HTTPException(status_code=413, detail="CSV metni çok büyük (max ~15MB).")
    kind = "csv_upload" if (body.source_filename or "").strip() else "csv_paste"
    return _run_backlink_import(
        db,
        site_id=body.site_id,
        report_type=body.report_type or "latest_links",
        csv_text=csv_text,
        sheets_url=(body.sheets_url or "").strip(),
        source_filename=(body.source_filename or "")[:255],
        source_kind=kind,
    )


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

    upload = form.get("file")
    if upload is not None and getattr(upload, "filename", None):
        raw = await upload.read()
        if len(raw) > 15_000_000:
            raise HTTPException(status_code=413, detail="Dosya çok büyük (max ~15MB).")
        csv_text = raw.decode("utf-8", errors="replace")
        return _run_backlink_import(
            db,
            site_id=site_id,
            report_type=report_type,
            csv_text=csv_text,
            sheets_url="",
            source_filename=str(getattr(upload, "filename", None) or "upload.csv"),
            source_kind="csv_upload",
        )
    return _run_backlink_import(
        db,
        site_id=site_id,
        report_type=report_type,
        csv_text=csv_text,
        sheets_url=sheets_url,
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
    report_type: str = Query("latest_links"),
    db: Session = Depends(get_db),
) -> PlainTextResponse:
    _require_internal_site(db, site_id)
    text = backlink_csv.build_disavow_text(db, site_id=site_id, report_type=report_type)
    return PlainTextResponse(text, media_type="text/plain; charset=utf-8")
