"""Sheet — ayılma çizelgesi API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from backend.services.ayilma_schedule import (
    build_ayilma_csv_bytes,
    build_ayilma_docx_bytes,
    build_ayilma_xlsx_bytes,
    generate_ayilma_schedule,
    roster_defaults,
)
from backend.services.sheet_page_access import is_sheet_page_allowed_email

router = APIRouter(tags=["sheet"])
LOGGER = logging.getLogger(__name__)


def _require_sheet(request: Request) -> None:
    from backend.main import _local_panel_open
    from backend.services.app_member_auth import member_from_request

    if _local_panel_open(request):
        return
    member = member_from_request(request)
    em = member.email if member else None
    if not is_sheet_page_allowed_email(em):
        raise HTTPException(status_code=403, detail="Sheet bu hesap için kapalı.")


class SpecialRuleBody(BaseModel):
    name: str
    mode: str  # work | avoid
    dates: list[str] = Field(default_factory=list)
    weekly: bool = False


class GenerateBody(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    month: int = Field(..., ge=1, le=12)
    leaves: dict[str, dict[str, str]] = Field(default_factory=dict)
    day_only: list[str] = Field(default_factory=list)
    special_rules: list[SpecialRuleBody] = Field(default_factory=list)
    variant: int = Field(0, ge=0, le=9999)


class ExportBody(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    month: int = Field(..., ge=1, le=12)
    days: list[dict[str, Any]] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


def _export_attachment(body: ExportBody) -> tuple[int, int, list, list]:
    if not body.days or not body.rows:
        raise HTTPException(status_code=400, detail="Boş çizelge — önce takvim/çizelge yükleyin.")
    return body.year, body.month, body.days, body.rows


def _archive_export(
    request: Request,
    *,
    export_format: str,
    filename: str,
    content: bytes,
    media_type: str,
    year: int,
    month: int,
) -> None:
    try:
        from backend.database import SessionLocal
        from backend.main import _extract_client_ip, _local_panel_open
        from backend.services import report_export_archive as rea
        from backend.services.app_member_auth import member_from_request

        member = member_from_request(request)
        if member is not None:
            actor_email = member.email or ""
            actor_name = member.display_name or ""
        elif _local_panel_open(request):
            actor_email = "local@panel"
            actor_name = "Yerel panel"
        else:
            actor_email = ""
            actor_name = ""

        with SessionLocal() as db:
            rea.save_export(
                db,
                report_kind="sheet_ayilma",
                export_format=export_format,
                filename=filename,
                content=content,
                media_type=media_type,
                actor_email=actor_email,
                actor_display_name=actor_name,
                client_ip=_extract_client_ip(request),
                meta={"year": year, "month": month},
            )
    except Exception:
        LOGGER.exception("sheet export archive failed")


def _export_response(
    request: Request,
    data: bytes,
    fname: str,
    media_type: str,
    *,
    export_format: str,
    year: int,
    month: int,
) -> Response:
    _archive_export(
        request,
        export_format=export_format,
        filename=fname,
        content=data,
        media_type=media_type,
        year=year,
        month=month,
    )
    return Response(
        content=data,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "no-store",
        },
    )


@router.get("/sheet/ayilma/meta")
def sheet_ayilma_meta(request: Request) -> dict[str, Any]:
    _require_sheet(request)
    return {"ok": True, **roster_defaults()}


@router.post("/sheet/ayilma/generate")
def sheet_ayilma_generate(request: Request, body: GenerateBody) -> dict[str, Any]:
    _require_sheet(request)
    try:
        rules = [r.model_dump() for r in body.special_rules]
        return generate_ayilma_schedule(
            body.year,
            body.month,
            leaves=body.leaves,
            day_only=body.day_only,
            prefer_48h_after_24=True,
            special_rules=rules,
            variant=body.variant,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/sheet/ayilma/export.xlsx")
def sheet_ayilma_export_xlsx(request: Request, body: ExportBody) -> Response:
    """Görüntülenen ay tablosunu .xlsx indir (Windows Excel + Mac Excel/Numbers)."""
    _require_sheet(request)
    year, month, days, rows = _export_attachment(body)
    data = build_ayilma_xlsx_bytes(year=year, month=month, days=days, rows=rows)
    fname = f"ayilma_cizelge_{year}-{month:02d}.xlsx"
    return _export_response(
        request,
        data,
        fname,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        export_format="xlsx",
        year=year,
        month=month,
    )


@router.post("/sheet/ayilma/export.csv")
def sheet_ayilma_export_csv(request: Request, body: ExportBody) -> Response:
    """UTF-8 BOM + noktalı virgül — Windows Excel TR ve Android Sheets."""
    _require_sheet(request)
    year, month, days, rows = _export_attachment(body)
    data = build_ayilma_csv_bytes(year=year, month=month, days=days, rows=rows)
    fname = f"ayilma_cizelge_{year}-{month:02d}.csv"
    return _export_response(
        request,
        data,
        fname,
        "text/csv; charset=utf-8",
        export_format="csv",
        year=year,
        month=month,
    )


@router.post("/sheet/ayilma/export.docx")
def sheet_ayilma_export_docx(request: Request, body: ExportBody) -> Response:
    """Word .docx — Windows Word / Android Office & Google Docs."""
    _require_sheet(request)
    year, month, days, rows = _export_attachment(body)
    data = build_ayilma_docx_bytes(year=year, month=month, days=days, rows=rows)
    fname = f"ayilma_cizelge_{year}-{month:02d}.docx"
    return _export_response(
        request,
        data,
        fname,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        export_format="docx",
        year=year,
        month=month,
    )
