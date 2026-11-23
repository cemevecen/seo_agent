"""Borsa tatil takvimi — Mac bridge ingest + JSON okuma."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.holiday_calendar import (
    build_holidays_page_context,
    load_holiday_payload,
    save_holiday_payload,
)

router = APIRouter(tags=["holiday-calendar"])


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


class HolidayRowIn(BaseModel):
    date_tr: str = ""
    country: str = ""
    country_flag: str = ""
    country_href: str = ""
    exchange: str = ""
    holiday: str = ""


class HolidayIngestBody(BaseModel):
    source: str = ""
    country: str = "all"
    date_from: str = ""
    date_to: str = ""
    fetched_at: str = ""
    rows: list[HolidayRowIn] = Field(default_factory=list)


@router.post("/holiday-calendar/ingest")
def post_holiday_calendar_ingest(
    body: HolidayIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    payload = body.model_dump()
    result = save_holiday_payload(db, payload, commit=True)
    try:
        from backend.services.scrape_telemetry import record_scrape_ingest

        record_scrape_ingest(
            db,
            source="investing_holiday",
            target="tr.investing.com/holiday-calendar",
            status="success" if result.get("ok") else "error",
            row_count=int(result.get("row_count") or 0),
            message=f"{result.get('row_count') or 0} tatil satırı",
            detail={"updated_at": result.get("updated_at")},
            commit=True,
        )
    except Exception:
        pass
    if not result.get("row_count"):
        raise HTTPException(status_code=400, detail="Tatil takvimi ingest boş")
    return result


@router.get("/holiday-calendar")
def get_holiday_calendar(db: Session = Depends(get_db)) -> dict[str, Any]:
    payload = load_holiday_payload(db)
    if not payload:
        return {"ok": False, "rows": [], "row_count": 0, "message": "Henüz veri yok"}
    return {"ok": True, **payload}


@router.get("/holiday-calendar/meta")
def get_holiday_calendar_meta(db: Session = Depends(get_db)) -> dict[str, Any]:
    ctx = build_holidays_page_context(db)
    return {
        "ok": not ctx.get("empty"),
        "row_count": ctx.get("row_count") or 0,
        "country_count": ctx.get("country_count") or 0,
        "fetched_at": ctx.get("fetched_at") or "",
        "updated_at": ctx.get("updated_at") or "",
        "date_from": ctx.get("date_from") or "",
        "date_to": ctx.get("date_to") or "",
        "source_url": ctx.get("source_url") or "",
    }
