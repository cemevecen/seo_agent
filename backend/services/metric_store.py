"""Metric kayıtlarını tutarlı biçimde yazmak ve okumak için yardımcı fonksiyonlar."""

from collections import defaultdict
from datetime import datetime

from sqlalchemy.orm import Session

from backend.models import Metric


def save_metric(db: Session, site_id: int, metric_type: str, value: float, collected_at: datetime | None = None) -> Metric:
    """Tek bir metriği veritabanına kaydeder."""
    metric = Metric(
        site_id=site_id,
        metric_type=metric_type,
        value=float(value),
        collected_at=collected_at or datetime.utcnow(),
    )
    db.add(metric)
    return metric


def save_metrics(db: Session, site_id: int, metrics: dict[str, float], collected_at: datetime | None = None) -> list[Metric]:
    """Bir collector çıktısındaki metrikleri aynı zaman damgası ile kaydeder."""
    saved_metrics: list[Metric] = []
    timestamp = collected_at or datetime.utcnow()
    for metric_type, value in metrics.items():
        saved_metrics.append(save_metric(db, site_id, metric_type, value, timestamp))
    db.commit()
    for metric in saved_metrics:
        db.refresh(metric)
    return saved_metrics


def get_latest_metrics(db: Session, site_id: int) -> list[Metric]:
    """Her metric_type için en son kaydı döndürür."""
    rows = db.query(Metric).filter(Metric.site_id == site_id).order_by(Metric.collected_at.desc(), Metric.id.desc()).all()
    latest_by_type: dict[str, Metric] = {}
    for row in rows:
        if row.metric_type not in latest_by_type:
            latest_by_type[row.metric_type] = row
    return sorted(latest_by_type.values(), key=lambda item: item.metric_type)


def get_metric_history(db: Session, site_id: int) -> dict[str, list[dict]]:
    """Trend grafikleri için tüm metrikleri tür bazında gruplayarak döndürür."""
    rows = db.query(Metric).filter(Metric.site_id == site_id).order_by(Metric.collected_at.asc(), Metric.id.asc()).all()
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row.metric_type].append(
            {
                "value": row.value,
                "collected_at": row.collected_at.isoformat(),
            }
        )
    return dict(grouped)