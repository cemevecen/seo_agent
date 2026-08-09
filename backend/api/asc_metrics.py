"""App Store Connect Metrikler API — /api/asc-metrics/* (Android play-analytics paralel)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Query

from backend.services.asc_metrics_warehouse import (
    asc_metrics_status,
    metric_catalog,
    query_asc_metric,
    query_asc_overview,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["asc-metrics"])


@router.get("/asc-metrics/status")
def get_asc_metrics_status() -> dict[str, Any]:
    return asc_metrics_status()


@router.get("/asc-metrics/metrics")
def get_asc_metrics_list() -> dict[str, Any]:
    return {"ok": True, "metrics": metric_catalog()}


@router.get("/asc-metrics/query")
def get_asc_metrics_query(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str = Query(default="units"),
    bundle_id: str | None = Query(default=None),
    compare: str | None = Query(default=None),
    breakdown: str = Query(default="date"),
) -> dict[str, Any]:
    try:
        return query_asc_metric(
            start=start,
            end=end,
            metric=metric,
            bundle_id=bundle_id,
            compare=compare,
            breakdown=breakdown,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("asc-metrics query failed")
        return {"ok": False, "message": str(exc), "series": [], "total": 0}


@router.get("/asc-metrics/overview")
def get_asc_metrics_overview(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metrics: str = Query(
        default="units,redownloads,impressions,page_views,conversion_rate,iap,paying_users,proceeds,active_subscriptions",
        description="Virgülle ayrılmış metrik listesi",
    ),
    bundle_id: str | None = Query(default=None),
) -> dict[str, Any]:
    metric_list = [m.strip() for m in (metrics or "").split(",") if m.strip()]
    try:
        return query_asc_overview(
            start=start, end=end, metrics=metric_list, bundle_id=bundle_id
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("asc-metrics overview failed")
        return {"ok": False, "message": str(exc), "bundles": []}
