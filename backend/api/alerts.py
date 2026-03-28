"""Alert yönetimi API endpoint'leri."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import re

from backend.database import get_db
from backend.models import Alert, AlertLog, Site, Metric
from backend.rate_limiter import limiter
from backend.services.alert_engine import DEFAULT_ALERT_RULES, ALERT_DESCRIPTIONS

router = APIRouter(tags=["alerts"])


def _extract_query_details_from_message(message: str) -> dict | list[dict]:
    """Alert message'den query detaylarını extract et.
    
    Message formatı: "[NEGATIVE] search_console_position_change: 'query1'. Position: 5.0->8.0"
    Returns single dict if one query, list of dicts if multiple queries.
    """
    # Split by pipe to handle multiple queries
    query_segments = message.split(" | ")
    all_details = []
    
    for segment in query_segments:
        details = {}
        
        # Query name extract et
        query_match = re.search(r"'([^']+)'", segment)
        if query_match:
            details["query"] = query_match.group(1)
        
        # Position change extract et (handles both numeric and N/A)
        pos_match = re.search(r"Position:\s*([\d.]+|N/A)\s*->\s*([\d.]+|N/A)", segment)
        if pos_match:
            old_val = pos_match.group(1)
            new_val = pos_match.group(2)
            
            # Convert to float if not N/A
            try:
                details["old_position"] = float(old_val) if old_val != "N/A" else None
                details["new_position"] = float(new_val) if new_val != "N/A" else None
                
                # Calculate change only if both values exist
                if details["old_position"] is not None and details["new_position"] is not None:
                    details["change"] = details["new_position"] - details["old_position"]
                    details["is_improvement"] = details["change"] < 0  # Düşük position daha iyi
            except (ValueError, TypeError):
                pass
        
        # POSITIVE/NEGATIVE flag
        details["is_negative"] = "[NEGATIVE]" in message
        details["is_positive"] = "[POSITIVE]" in message
        
        if details.get("query"):  # Only add if query was found
            all_details.append(details)
    
    # Return single dict if one query, list if multiple
    if len(all_details) == 1:
        return all_details[0]
    elif len(all_details) > 1:
        return all_details
    else:
        return {}


def _calculate_comparison(db: Session, site_id: int, metric_type: str, triggered_at: datetime, comparison_type: str = "daily", alert_log_message: str = None) -> dict:
    """Metrikleri tarih bazında karşılaştırır (gün veya hafta bazında).
    
    Args:
        db: Database session
        site_id: Site ID
        metric_type: Metric type (metrik adı)
        triggered_at: Alert'in trigger edildiği tarih
        comparison_type: "daily" (dünle karşılaştır) veya "weekly" (geçen hafta aynı güne karşılaştır)
    
    Returns:
        {"message": "Karşılaştırmalı açıklama}
    """
    # Mevcut günün metriğini al
    current_start = triggered_at.replace(hour=0, minute=0, second=0, microsecond=0)
    current_end = current_start + timedelta(days=1)
    
    current_metrics = db.query(Metric).filter(
        Metric.site_id == site_id,
        Metric.metric_type == metric_type,
        Metric.collected_at >= current_start,
        Metric.collected_at < current_end
    ).all()
    
    current_value = sum(m.value for m in current_metrics) / len(current_metrics) if current_metrics else None
    
    if comparison_type == "weekly":
        # Geçen haftanın aynı günündeki veriler
        past_start = (triggered_at - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        past_end = past_start + timedelta(days=1)
        comparison_label = "Geçen haftanın aynı günü"
    else:
        # Dünün veriler (default: daily)
        past_start = (triggered_at - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        past_end = past_start + timedelta(days=1)
        comparison_label = "Dün"
    
    past_metrics = db.query(Metric).filter(
        Metric.site_id == site_id,
        Metric.metric_type == metric_type,
        Metric.collected_at >= past_start,
        Metric.collected_at < past_end
    ).all()
    
    past_value = sum(m.value for m in past_metrics) / len(past_metrics) if past_metrics else None
    
    # Mesaj oluştur
    result = {}
    
    if current_value is None or past_value is None:
        message = f"{comparison_label} ile karşılaştırma için yeterli veri yok."
    else:
        change = current_value - past_value
        change_pct = (change / past_value * 100) if past_value != 0 else 0
        direction = "↑ artış" if change > 0 else "↓ azalış" if change < 0 else "→ değişiklik yok"
        
        message = (
            f"<strong>{comparison_label} ile karşılaştırma:</strong><br>"
            f"Mevcut: {current_value:.2f} vs {comparison_label}: {past_value:.2f}<br>"
            f"Değişim: {direction} ({change:+.2f}, {change_pct:+.1f}%)"
        )
    
    result["message"] = message
    
    # Search Console alertları için query details ekle
    if alert_log_message and metric_type in ["search_console_dropped_queries", "search_console_biggest_drop"]:
        query_details = _extract_query_details_from_message(alert_log_message)
        if query_details:
            # Ensure query_details is always a list for consistent template handling
            if isinstance(query_details, dict):
                result["query_details"] = [query_details]  # Wrap single dict in list
            else:
                result["query_details"] = query_details
    
    return result


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
def get_alert_details(request: Request, alert_log_id: int, comparison: str = "daily", db: Session = Depends(get_db)):
    """Alert log detaylarını açıklamalar, trend, önerilerle döner ve tarih karşılaştırması yapar."""
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
    
    # Tarih karşılaştırması (gün veya hafta bazında)
    comparison_data = _calculate_comparison(db, site.id, alert.alert_type, alert_log.triggered_at, comparison, alert_log.message)
    
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
        ],
        "comparison": comparison_data
    }