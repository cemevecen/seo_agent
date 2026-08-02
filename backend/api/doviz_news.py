"""Doviz News — Google Sheets haber yayın raporu API."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

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
):
    try:
        return doviz_news_payload(
            category=category,
            period=period,
            force=force,
            items_limit=items_limit,
        )
    except Exception as exc:
        logger.exception("doviz news report failed")
        raise HTTPException(status_code=400, detail=str(exc) or "Doviz News tablosu yüklenemedi") from exc
