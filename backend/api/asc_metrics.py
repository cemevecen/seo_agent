"""App Store Connect Metrikler API — /api/asc-metrics/* (Android play-analytics paralel)."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services.asc_metrics_warehouse import (
    asc_metrics_status,
    metric_catalog,
    query_asc_metric,
    query_asc_overview,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["asc-metrics"])


@router.get("/asc-metrics/viz20/meta")
def get_asc_viz20_meta() -> dict[str, Any]:
    from backend.services.asc_viz20 import build_asc_viz20_meta

    return build_asc_viz20_meta()


@router.get("/asc-metrics/viz20/{viz_id}")
def get_asc_viz20_data(
    viz_id: str,
    db: Session = Depends(get_db),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str | None = Query(default=None),
    metric_left: str | None = Query(default=None),
    metric_right: str | None = Query(default=None),
    metrics: str | None = Query(default=None),
    etype: str = Query(default="CRASH"),
    limit: int = Query(default=15, ge=3, le=50),
) -> dict[str, Any]:
    from backend.services.asc_viz20 import build_asc_viz20_data

    return build_asc_viz20_data(
        db,
        viz_id=viz_id,
        start=start,
        end=end,
        metric=metric,
        metric_left=metric_left,
        metric_right=metric_right,
        metrics=metrics,
        etype=etype,
        limit=limit,
    )


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
    dim: str = Query(default="overview"),
    segment: str = Query(default="all"),
) -> dict[str, Any]:
    try:
        return query_asc_metric(
            start=start,
            end=end,
            metric=metric,
            bundle_id=bundle_id,
            compare=compare,
            breakdown=breakdown,
            dim=dim,
            segment=segment,
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
