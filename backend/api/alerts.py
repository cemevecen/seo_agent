"""Alert yönetimi API endpoint'leri."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import Alert
from backend.rate_limiter import limiter

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