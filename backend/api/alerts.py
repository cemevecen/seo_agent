"""Alert yönetimi API endpoint'leri."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from backend.database import get_db
from backend.models import Alert, AlertLog, Site, Metric
from backend.rate_limiter import limiter
from backend.services.alert_engine import DEFAULT_ALERT_RULES, ALERT_DESCRIPTIONS

router = APIRouter(tags=["alerts"])


@router.get("/alerts")
@limiter.limit("60/minute")
def list_alerts(request: Request, db: Session = Depends(get_db)):
    # Tüm alarm kurallarını site bazında döndürür.
    alerts = db.query(Alert).order_by(Alert.site_id.asc(), Alert.alert_type.asc()).all()
    return {
        "items": [
            {
                "id": alert.id,
                "site_id": alert.site_id,
                "alert_type": alert.alert_type,
                "threshold": alert.threshold,
                "is_active": alert.is_active,
            }
            for alert in alerts
        ]
    }


@router.patch("/alerts/{alert_id}")
@limiter.limit("60/minute")
async def update_alert(request: Request, alert_id: int, db: Session = Depends(get_db)):
    # Alert threshold ve aktiflik bilgisini günceller.
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert bulunamadı.")

    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        payload = await request.json()
        threshold = float(payload.get("threshold", alert.threshold))
        is_active = bool(payload.get("is_active", alert.is_active))
    else:
        form = await request.form()
        threshold = float(form.get("threshold", alert.threshold))
        is_active = str(form.get("is_active", "false")).lower() in {"true", "1", "on", "yes"}

    alert.threshold = threshold
    alert.is_active = is_active
    db.commit()
    db.refresh(alert)
    return {
        "item": {
            "id": alert.id,
            "site_id": alert.site_id,
            "alert_type": alert.alert_type,
            "threshold": alert.threshold,
            "is_active": alert.is_active,
        }
    }


@router.get("/alert-details/{alert_log_id}")
@limiter.limit("60/minute")
def get_alert_details(request: Request, alert_log_id: int, db: Session = Depends(get_db)):
    """Alert log detaylarını açıklamalar, trend ve önerilerle döner."""
    alert_log = db.query(AlertLog).filter(AlertLog.id == alert_log_id).first()
    if not alert_log:
        raise HTTPException(status_code=404, detail="Alert bulunamadı.")
    
    alert = alert_log.alert
    site = alert.site
    rule = next((r for r in DEFAULT_ALERT_RULES if r.metric_type == alert.alert_type), None)
    desc = ALERT_DESCRIPTIONS.get(alert.alert_type, {})
    
    # Search Console uyarıları için query name'i extract et
    query_name_filter = None
    if alert.alert_type in ["search_console_dropped_queries", "search_console_biggest_drop"]:
        # Message format: "doviz.com için ... : 'query_name'. ..."
        import re
        match = re.search(r":\s*'([^']+)'", alert_log.message)
        if match:
            query_name_filter = match.group(1)
    
    # Trend: Son 10 alert log'u - aynı query'e ait
    recent_logs = (
        db.query(AlertLog)
        .filter(AlertLog.alert_id == alert.id)
        .order_by(AlertLog.triggered_at.desc())
        .limit(10)
        .all()
    )
    
    # Search Console alertleri için, aynı query'e ait olanları filtre et
    if query_name_filter:
        filtered_logs = []
        for log in recent_logs:
            match = re.search(r":\s*'([^']+)'", log.message)
            if match and match.group(1) == query_name_filter:
                filtered_logs.append(log)
        recent_logs = filtered_logs
    
    # Metrik history: Son 30 gün
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    metrics_history = (
        db.query(Metric)
        .filter(
            Metric.site_id == site.id,
            Metric.metric_type == alert.alert_type,
            Metric.collected_at >= thirty_days_ago
        )
        .order_by(Metric.collected_at.desc())
        .limit(30)
        .all()
    )
    
    # Metrik istatistikleri
    metric_values = [m.value for m in metrics_history]
    metric_stats = {
        "current": metric_values[0] if metric_values else None,
        "min": min(metric_values) if metric_values else None,
        "max": max(metric_values) if metric_values else None,
        "avg": sum(metric_values) / len(metric_values) if metric_values else None,
    }
    
    return {
        "alert_log": {
            "id": alert_log.id,
            "message": alert_log.message,
            "triggered_at": alert_log.triggered_at.strftime("%d.%m.%Y %H:%M:%S"),
            "sent_mail": alert_log.sent_mail,
        },
        "alert": {
            "id": alert.id,
            "type": alert.alert_type,
            "threshold": alert.threshold,
            "is_active": alert.is_active,
        },
        "site": {
            "id": site.id,
            "domain": site.domain,
        },
        "rule": {
            "title": rule.title if rule else "",
            "description_short": desc.get("what_means", ""),
            "description_short_en": desc.get("what_means_en", ""),
            "description_detailed_tr": desc.get("description_tr", ""),
            "description_detailed_en": desc.get("description_en", ""),
            "severity": desc.get("severity", "warning"),
        },
        "metrics": {
            "current_value": metric_stats["current"],
            "min_value": metric_stats["min"],
            "max_value": metric_stats["max"],
            "avg_value": metric_stats["avg"],
            "threshold_value": alert.threshold,
            "comparator": rule.comparator if rule else "",
        },
        "trend": [
            {
                "message": log.message,
                "triggered_at": log.triggered_at.strftime("%d.%m.%Y %H:%M:%S"),
                "sent_mail": log.sent_mail,
            }
            for log in recent_logs
        ]
    }