"""x-ga4 endpoint'i — GA4'ün kullanılmayan boyut/metrikleri."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.rate_limiter import limiter

router = APIRouter(tags=["x-ga4"])


@router.get("/x-ga4/report")
@limiter.limit("30/minute")
def x_ga4_report(
    request: Request,
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(15, ge=5, le=50),
    profile: str = Query("hepsi", description="hepsi | web | mweb | android | ios"),
    force: bool = Query(False, description="5 dk önbelleği atla"),
    site_id: int = Query(1, ge=1),
    db: Session = Depends(get_db),
):
    from backend.services.x_ga4 import build_x_ga4_report

    return build_x_ga4_report(
        db, site_id=site_id, days=days, limit=limit, profile=profile, force=force
    )
