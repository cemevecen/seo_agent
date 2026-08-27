"""Sheet — ayılma çizelgesi API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from backend.services.ayilma_schedule import (
    build_ayilma_xlsx_bytes,
    generate_ayilma_schedule,
    roster_defaults,
)
from backend.services.sheet_page_access import is_sheet_page_allowed_email

router = APIRouter(tags=["sheet"])


def _require_sheet(request: Request) -> None:
    from backend.services.app_member_auth import member_from_request

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
    prefer_48h_after_24: bool = True
    special_rules: list[SpecialRuleBody] = Field(default_factory=list)


class ExportBody(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    month: int = Field(..., ge=1, le=12)
    days: list[dict[str, Any]] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


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
            prefer_48h_after_24=body.prefer_48h_after_24,
            special_rules=rules,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/sheet/ayilma/export.xlsx")
def sheet_ayilma_export_xlsx(request: Request, body: ExportBody) -> Response:
    """Görüntülenen ay tablosunu .xlsx indir (Windows Excel + Mac Excel/Numbers)."""
    _require_sheet(request)
    if not body.days or not body.rows:
        raise HTTPException(status_code=400, detail="Boş çizelge — önce takvim/çizelge yükleyin.")
    data = build_ayilma_xlsx_bytes(
        year=body.year,
        month=body.month,
        days=body.days,
        rows=body.rows,
    )
    fname = f"ayilma_cizelge_{body.year}-{body.month:02d}.xlsx"
    return Response(
        content=data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "no-store",
        },
    )