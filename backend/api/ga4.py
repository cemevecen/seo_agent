"""GA4 metric endpoint'leri."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from backend.collectors.ga4 import collect_ga4_channel_sessions
from backend.database import get_db
from backend.models import Site
from backend.rate_limiter import limiter
from backend.services.ga4_auth import get_ga4_connection_status
from backend.services.metric_store import get_latest_metrics

router = APIRouter(tags=["ga4"])


@router.get("/ga4/{site_id}/status")
@limiter.limit("60/minute")
def ga4_status(request: Request, site_id: int, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    return {"site_id": site.id, "domain": site.domain, "ga4": get_ga4_connection_status(db, site.id)}


@router.post("/ga4/{site_id}/refresh")
@limiter.limit("60/minute")
def ga4_refresh(request: Request, site_id: int, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")
    profile = (request.query_params.get("profile") or "").strip().lower() or None
    raw_days = (request.query_params.get("days") or "").strip()
    try:
        days = int(raw_days) if raw_days else 30
    except ValueError:
        days = 30
    result = collect_ga4_channel_sessions(db, site, profile=profile, days=days)
    db.commit()
    return {"site_id": site.id, "domain": site.domain, "result": result}


@router.get("/ga4/{site_id}/latest")
@limiter.limit("60/minute")
def ga4_latest(request: Request, site_id: int, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    latest = {m.metric_type: m for m in get_latest_metrics(db, site_id)}
    items = []
    for key, metric in latest.items():
        if key.startswith("ga4_") and "_sessions_" in key:
            items.append(
                {
                    "metric_type": metric.metric_type,
                    "value": metric.value,
                    "collected_at": metric.collected_at.isoformat(),
                }
            )
    items.sort(key=lambda row: row["metric_type"])
    return {"site_id": site.id, "domain": site.domain, "items": items}

