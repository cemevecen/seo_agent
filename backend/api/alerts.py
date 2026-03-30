"""Alert yönetimi API endpoint'leri."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import re

from backend.database import get_db
from backend.models import Alert, AlertLog, Site, Metric
from backend.rate_limiter import limiter
from backend.services.alert_engine import DEFAULT_ALERT_RULES, ALERT_DESCRIPTIONS
from backend.services.timezone_utils import format_local_datetime
from backend.services.warehouse import get_latest_search_console_rows

router = APIRouter(tags=["alerts"])


def _extract_primary_query(message: str | None) -> str | None:
    match = re.search(r"'([^']+)'", message or "")
    if not match:
        return None
    query = str(match.group(1) or "").strip()
    return query or None


def _weighted_position(rows: list[dict]) -> float | None:
    weighted_total = 0.0
    total_impressions = 0.0
    fallback_total = 0.0
    fallback_count = 0
    for row in rows:
        impressions = float(row.get("impressions") or 0.0)
        position = float(row.get("position") or 0.0)
        if impressions > 0:
            weighted_total += position * impressions
            total_impressions += impressions
        elif position > 0:
            fallback_total += position
            fallback_count += 1
    if total_impressions > 0:
        return weighted_total / total_impressions
    if fallback_count > 0:
        return fallback_total / fallback_count
    return None


def _aggregate_search_console_query(rows: list[dict], query_name: str) -> dict:
    filtered = [row for row in rows if str(row.get("query") or "").strip() == query_name]
    clicks = sum(float(row.get("clicks") or 0.0) for row in filtered)
    impressions = sum(float(row.get("impressions") or 0.0) for row in filtered)
    ctr = (clicks / impressions * 100.0) if impressions > 0 else None
    position = _weighted_position(filtered)
    devices = sorted({str(row.get("device") or "").upper() for row in filtered if row.get("device")})
    property_urls = sorted({str(row.get("property_url") or "") for row in filtered if row.get("property_url")})
    return {
        "rows": filtered,
        "clicks": clicks,
        "impressions": impressions,
        "ctr": ctr,
        "position": position,
        "devices": devices,
        "property_urls": property_urls,
    }


def _format_decimal(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def _comparison_card(label: str, value: str, detail: str = "", tone: str = "slate") -> dict:
    return {
        "label": label,
        "value": value,
        "detail": detail,
        "tone": tone,
    }


def _position_change_state(change: float | None) -> str:
    if change is None:
        return "unknown"
    if change < 0:
        return "improved"
    if change > 0:
        return "worsened"
    return "neutral"


def _build_search_console_comparison(
    db: Session,
    site: Site,
    metric_type: str,
    comparison_type: str,
    alert_log_message: str | None,
) -> dict:
    query_name = _extract_primary_query(alert_log_message)
    if not query_name:
        fallback_label = "Dün" if comparison_type != "weekly" else "Geçen haftanın aynı günü"
        return {
            "message": (
                f"{fallback_label} bazlı ayrı snapshot bulunmuyor. "
                "Bu uyarı Search Console son 7 gün / önceki 7 gün karşılaştırmasından üretilir."
            ),
            "comparison_type": comparison_type,
        }

    current_label = "Son 7 Gun"
    previous_label = "Onceki 7 Gun"
    if comparison_type == "weekly":
        current_rows = get_latest_search_console_rows(db, site_id=site.id, data_scope="current_7d")
        previous_rows = get_latest_search_console_rows(db, site_id=site.id, data_scope="previous_7d")
    else:
        current_rows = get_latest_search_console_rows(db, site_id=site.id, data_scope="current_day")
        previous_rows = get_latest_search_console_rows(db, site_id=site.id, data_scope="previous_day")
        current_label = "Dun"
        previous_label = "Onceki Gun"
        if not current_rows and not previous_rows:
            return {
                "message": "Gunluk Search Console snapshot henuz hazir degil. Manuel yenile ya da sabah otomatik taramayi bekle.",
                "comparison_type": comparison_type,
                "query_details": [],
                "cards": [
                    _comparison_card("Durum", "Veri yok", "Dun / onceki gun snapshot bulunamadi.", "slate"),
                ],
            }

    current = _aggregate_search_console_query(current_rows, query_name)
    previous = _aggregate_search_console_query(previous_rows, query_name)
    has_meaningful_data = bool(current["rows"] or previous["rows"])

    cards: list[dict] = []
    query_details: list[dict] = []

    if metric_type == "search_console_ctr_drop":
        current_ctr = current.get("ctr")
        previous_ctr = previous.get("ctr")
        change = None if current_ctr is None or previous_ctr is None else current_ctr - previous_ctr
        message = (
            "CTR dun onceki gune gore daha dusuk."
            if comparison_type == "daily"
            else "CTR son 7 gunde onceki 7 gune gore daha dusuk."
        )
        cards = [
            _comparison_card(
                current_label,
                f"{_format_decimal(current_ctr, 3)} CTR",
                f"{int(current.get('clicks') or 0)} tiklama / {int(current.get('impressions') or 0)} gosterim",
                "blue",
            ),
            _comparison_card(
                previous_label,
                f"{_format_decimal(previous_ctr, 3)} CTR",
                f"{int(previous.get('clicks') or 0)} tiklama / {int(previous.get('impressions') or 0)} gosterim",
                "slate",
            ),
        ]
        if change is not None and previous_ctr not in (None, 0):
            change_pct = change / previous_ctr * 100.0
            cards.append(
                _comparison_card(
                    "Fark",
                    f"{change:+.3f} puan",
                    f"{change_pct:+.1f}%",
                    "red" if change < 0 else "green",
                )
            )
        else:
            cards.append(_comparison_card("Fark", "N/A", "", "slate"))
    elif metric_type == "search_console_impressions_drop":
        current_impressions = current.get("impressions")
        previous_impressions = previous.get("impressions")
        change = current_impressions - previous_impressions
        message = (
            "Gosterim dun onceki gune gore dusmus."
            if comparison_type == "daily"
            else "Gosterim son 7 gunde onceki 7 gune gore dusmus."
        )
        cards = [
            _comparison_card(current_label, f"{int(current_impressions or 0)}", "Gosterim", "blue"),
            _comparison_card(previous_label, f"{int(previous_impressions or 0)}", "Gosterim", "slate"),
        ]
        if previous_impressions:
            change_pct = change / previous_impressions * 100.0
            cards.append(
                _comparison_card(
                    "Fark",
                    f"{change:+.0f}",
                    f"{change_pct:+.1f}%",
                    "red" if change < 0 else "green",
                )
            )
        else:
            cards.append(_comparison_card("Fark", "N/A", "", "slate"))
    elif metric_type == "search_console_biggest_drop":
        previous_position = previous.get("position")
        current_position = current.get("position")
        change = None if current_position is None or previous_position is None else current_position - previous_position
        position_state = _position_change_state(change)
        query_details = [{
            "query": query_name,
            "old_position": previous_position,
            "new_position": current_position,
            "change": change,
            "is_improvement": True if position_state == "improved" else False if position_state == "worsened" else None,
        }]
        if comparison_type == "daily":
            message = (
                "Pozisyon dun ile onceki gun arasinda iyilesmis."
                if position_state == "improved"
                else "Pozisyon dun ile onceki gun arasinda kotulesmis."
                if position_state == "worsened"
                else "Pozisyon dun ile onceki gun arasinda degismemis."
            )
        else:
            message = (
                "Pozisyon son 7 gun ile onceki 7 gun arasinda iyilesmis."
                if position_state == "improved"
                else "Pozisyon son 7 gun ile onceki 7 gun arasinda kotulesmis."
                if position_state == "worsened"
                else "Pozisyon son 7 gun ile onceki 7 gun arasinda degismemis."
            )
        cards = [
            _comparison_card(current_label, _format_decimal(current_position, 1), "Ortalama pozisyon", "blue"),
            _comparison_card(previous_label, _format_decimal(previous_position, 1), "Ortalama pozisyon", "slate"),
        ]
        if change is not None:
            cards.append(
                _comparison_card(
                    "Fark",
                    f"{change:+.1f}",
                    "Pozisyon farki",
                    "green" if position_state == "improved" else "red" if position_state == "worsened" else "slate",
                )
            )
        else:
            cards.append(_comparison_card("Fark", "N/A", "", "slate"))
    elif metric_type == "search_console_dropped_queries":
        previous_position = previous.get("position")
        query_details = [{
            "query": query_name,
            "old_position": previous_position,
            "new_position": None,
            "change": None,
            "is_improvement": False,
        }]
        dropped_message = (
            "Gunluk karsilastirmada bu sorgu icin veri bulunamadi. Haftalik gorunum daha anlamli."
            if comparison_type == "daily" and not current["rows"] and not previous["rows"]
            else "Sorgu dunde gorunmuyor, onceki gunde vardi."
            if comparison_type == "daily" and previous["rows"] and not current["rows"]
            else "Sorgu dunde vardi, onceki gunde yoktu."
            if comparison_type == "daily" and current["rows"] and not previous["rows"]
            else "Sorgu onceki 7 gunde vardi, son 7 gunde gorunmuyor."
            if previous["rows"] and not current["rows"]
            else "Sorgu dusus adayi olarak isaretlendi."
        )
        message = dropped_message
        if comparison_type == "daily" and not current["rows"] and not previous["rows"]:
            delta_value = "Veri yok"
            delta_detail = "Haftalik gorunumu kullan"
            has_meaningful_data = False
        elif previous["rows"] and not current["rows"]:
            delta_value = "SERP disi"
            delta_detail = "Dunde yok" if comparison_type == "daily" else "Son 7 gunde yok"
        else:
            delta_value = "Dususte"
            delta_detail = "Kontrol et"
        cards = [
            _comparison_card(
                current_label,
                _format_decimal(current.get("position"), 1),
                f"{int(current.get('clicks') or 0)} tiklama / {int(current.get('impressions') or 0)} gosterim",
                "blue",
            ),
            _comparison_card(
                previous_label,
                _format_decimal(previous_position, 1),
                f"{int(previous.get('clicks') or 0)} tiklama / {int(previous.get('impressions') or 0)} gosterim",
                "slate",
            ),
            _comparison_card("Fark", delta_value, delta_detail, "red"),
        ]
    else:
        message = "Bu alert tipi Search Console snapshot karsilastirmasiyla aciklandi."
        cards = [_comparison_card("Durum", "Hazir", "", "blue")]

    return {
        "message": message,
        "comparison_type": comparison_type,
        "query_details": query_details,
        "cards": cards,
        "has_meaningful_data": has_meaningful_data,
    }


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
                    state = _position_change_state(details["change"])
                    details["is_improvement"] = True if state == "improved" else False if state == "worsened" else None
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
        alert_log_message: Alert message'i (extract query details için)
    
    Returns:
        {"message": "Karşılaştırmalı açıklama", "query_details": [...], "comparison_type": "daily|weekly"}
    """
    site = db.query(Site).filter(Site.id == site_id).first()
    if metric_type.startswith("search_console_"):
        if site is None:
            return {"message": "Site bulunamadi.", "comparison_type": comparison_type, "query_details": [], "cards": []}
        return _build_search_console_comparison(db, site, metric_type, comparison_type, alert_log_message)

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
    result["comparison_type"] = comparison_type  # Help frontend know which comparison this is
    
    # Search Console alertları için query details ekle
    # IMPORTANT: Parse message AND ADD TIME PERIOD INFO if applicable
    if alert_log_message and metric_type in ["search_console_dropped_queries", "search_console_biggest_drop"]:
        query_details = _extract_query_details_from_message(alert_log_message)
        if query_details:
            # Ensure query_details is always a list for consistent template handling
            if isinstance(query_details, dict):
                query_details = [query_details]
            else:
                query_details = list(query_details)
            
            # Add comparison_type to each query detail for clarity
            for detail in query_details:
                detail["comparison_type"] = comparison_type
            
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
            "triggered_at": format_local_datetime(alert_log.triggered_at, fmt="%d.%m.%Y %H:%M:%S"),
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
                "triggered_at": format_local_datetime(log.triggered_at, fmt="%d.%m.%Y %H:%M:%S"),
                "sent_mail": log.sent_mail,
            }
            for log in recent_logs
        ],
        "comparison": comparison_data
    }
