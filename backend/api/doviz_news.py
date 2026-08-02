"""Doviz News — Google Sheets haber yayın raporu API."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services.doviz_news_sheet import doviz_news_payload

logger = logging.getLogger(__name__)

router = APIRouter(tags=["doviz-news"])


@router.get("/doviz-news/report")
def get_doviz_news_report(
    category: str | None = Query(None, description="Kategori filtresi (boş = tümü)"),
    period: str | None = Query(
        "last_7d",
        description="Dönem: all | today | yesterday | last_7d | prev_week | this_month | last_month",
    ),
    force: bool = Query(False, description="Google Sheet önbelleğini atla ve yeniden çek"),
    items_limit: int = Query(80, ge=1, le=500),
    include_traffic: bool = Query(True, description="GA4 + GSC trafik zenginleştirmesi"),
    site_id: int = Query(1, ge=1, description="Site ID (GA4/GSC)"),
    db: Session = Depends(get_db),
):
    try:
        return doviz_news_payload(
            category=category,
            period=period,
            force=force,
            items_limit=items_limit,
            db=db,
            include_traffic=include_traffic,
            site_id=site_id,
        )
    except Exception as exc:
        logger.exception("doviz news report failed")
        raise HTTPException(status_code=400, detail=str(exc) or "Doviz News tablosu yüklenemedi") from exc
