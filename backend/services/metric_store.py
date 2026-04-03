"""Metric kayıtlarını tutarlı biçimde yazmak ve okumak için yardımcı fonksiyonlar."""

from collections import defaultdict
from datetime import datetime, timedelta

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
    """Her metric_type için en son kaydı döndürür — GROUP BY subquery ile."""
    from sqlalchemy import func

    subq = (
        db.query(Metric.metric_type, func.max(Metric.collected_at).label("max_ts"))
        .filter(Metric.site_id == site_id)
        .group_by(Metric.metric_type)
        .subquery("_lm_ts")
    )
    rows = (
        db.query(Metric)
        .join(
            subq,
            (Metric.metric_type == subq.c.metric_type) & (Metric.collected_at == subq.c.max_ts),
        )
        .filter(Metric.site_id == site_id)
        .all()
    )
    return sorted(rows, key=lambda m: m.metric_type)


def get_latest_metrics_batch(db: Session, site_ids: list[int]) -> "dict[int, dict[str, Metric]]":
    """Multiple sites için latest metrics — tek GROUP BY sorgusu (N sorgu yerine 1)."""
    if not site_ids:
        return {}
    from sqlalchemy import func, and_

    subq = (
        db.query(
            Metric.site_id,
            Metric.metric_type,
            func.max(Metric.collected_at).label("max_ts"),
        )
        .filter(Metric.site_id.in_(site_ids))
        .group_by(Metric.site_id, Metric.metric_type)
        .subquery("_lm_batch_ts")
    )
    rows = (
        db.query(Metric)
        .join(
            subq,
            and_(
                Metric.site_id == subq.c.site_id,
                Metric.metric_type == subq.c.metric_type,
                Metric.collected_at == subq.c.max_ts,
            ),
        )
        .all()
    )
    result: dict[int, dict[str, Metric]] = {sid: {} for sid in site_ids}
    for m in rows:
        result[m.site_id][m.metric_type] = m
    return result


def get_metric_history(db: Session, site_id: int, days: int | None = None) -> dict[str, list[dict]]:
    """Trend grafikleri için tüm metrikleri tür bazında gruplayarak döndürür."""
    query = db.query(Metric).filter(Metric.site_id == site_id)
    if days is not None and days > 0:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = query.filter(Metric.collected_at >= cutoff)
    rows = query.order_by(Metric.collected_at.asc(), Metric.id.asc()).all()
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row.metric_type].append(
            {
                "value": row.value,
                "collected_at": row.collected_at.isoformat(),
            }
        )
    return dict(grouped)