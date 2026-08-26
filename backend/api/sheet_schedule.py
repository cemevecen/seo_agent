"""Sheet — ayılma çizelgesi API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.services.ayilma_schedule import generate_ayilma_schedule, roster_defaults
from backend.services.sheet_page_access import is_sheet_page_allowed_email

router = APIRouter(tags=["sheet"])


def _require_sheet(request: Request) -> None:
    from backend.services.app_member_auth import member_from_request

    member = member_from_request(request)
    em = member.email if member else None
    if not is_sheet_page_allowed_email(em):
        raise HTTPException(status_code=403, detail="Sheet bu hesap için kapalı.")


class GenerateBody(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    month: int = Field(..., ge=1, le=12)
    leaves: dict[str, dict[str, str]] = Field(default_factory=dict)
    day_only: list[str] = Field(default_factory=list)
    prefer_48h_after_24: bool = True


@router.get("/sheet/ayilma/meta")
def sheet_ayilma_meta(request: Request) -> dict[str, Any]:
    _require_sheet(request)
    return {"ok": True, **roster_defaults()}


@router.post("/sheet/ayilma/generate")
def sheet_ayilma_generate(request: Request, body: GenerateBody) -> dict[str, Any]:
    _require_sheet(request)
    try:
        return generate_ayilma_schedule(
            body.year,
            body.month,
            leaves=body.leaves,
            day_only=body.day_only,
            prefer_48h_after_24=body.prefer_48h_after_24,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
