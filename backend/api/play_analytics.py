"""Play analytics API — tarih / kırılım / karşılaştırma."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Query

from backend.services.play_analytics_warehouse import play_analytics_status, query_play_analytics

logger = logging.getLogger(__name__)

router = APIRouter(tags=["play-analytics"])


@router.get("/play-analytics/status")
def get_play_analytics_status() -> dict[str, Any]:
    return play_analytics_status()


@router.get("/play-analytics/query")
def get_play_analytics_query(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str = Query(default="installs"),
    breakdown: str = Query(default="date"),
    dim: str = Query(default="overview"),
    segment: str | None = Query(default=None),
    compare: str | None = Query(default="previous_period"),
) -> dict[str, Any]:
    try:
        return query_play_analytics(
            start=start,
            end=end,
            metric=metric,
            breakdown=breakdown,
            dim=dim,
            segment=segment,
            compare=compare,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("play-analytics query failed")
        return {"ok": False, "message": str(exc), "series": [], "total": 0}
