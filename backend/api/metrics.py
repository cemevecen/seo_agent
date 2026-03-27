"""Metric okuma endpoint'leri."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import Site
from backend.rate_limiter import limiter
from backend.services.metric_store import get_latest_metrics, get_metric_history

router = APIRouter(tags=["metrics"])


def _period_to_days(period: str | None) -> int | None:
    normalized = (period or "").strip().lower()
    if not normalized:
        return None
    mapping = {
        "day": 1,
        "daily": 1,
        "week": 7,
        "weekly": 7,
        "month": 30,
        "monthly": 30,
    }
    return mapping.get(normalized)


@router.get("/metrics/{site_id}")
@limiter.limit("60/minute")
def get_site_metrics(request: Request, site_id: int, db: Session = Depends(get_db)):
    # Her metric_type için son kaydı döndürür.
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    latest_metrics = get_latest_metrics(db, site_id)
    return {
        "site": {"id": site.id, "domain": site.domain, "display_name": site.display_name},
        "items": [
            {
                "metric_type": metric.metric_type,
                "value": metric.value,
                "collected_at": metric.collected_at.isoformat(),
            }
            for metric in latest_metrics
        ],
    }


@router.get("/metrics/{site_id}/history")
@limiter.limit("60/minute")
def get_site_metrics_history(request: Request, site_id: int, db: Session = Depends(get_db)):
    # Trend verisi için metrikleri metric_type bazında döndürür.
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    period = request.query_params.get("period")
    days = _period_to_days(period)

    return {
        "site": {"id": site.id, "domain": site.domain, "display_name": site.display_name},
        "period": period or "all",
        "history": get_metric_history(db, site_id, days=days),
    }