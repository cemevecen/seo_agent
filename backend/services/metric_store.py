"""Metric kayıtlarını tutarlı biçimde yazmak ve okumak için yardımcı fonksiyonlar."""

from collections import defaultdict
from datetime import date, datetime, timedelta

from sqlalchemy.orm import Session

from backend.models import Metric
from backend.services.timezone_utils import to_local_datetime, utc_naive_bounds_for_local_calendar_day

# Yalnızca ana performans skorları — trend + depo budaması bu türlerle sınırlı.
PAGESPEED_PERFORMANCE_SCORE_TYPES: frozenset[str] = frozenset(
    {"pagespeed_mobile_score", "pagespeed_desktop_score"}
)


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


def get_metric_latest_pair(db: Session, site_id: int, metric_type: str) -> tuple[float | None, float | None]:
    """Aynı metrik için (bir önceki ölçüm, son ölçüm). Yalnızca bir kayıt varsa (None, son)."""
    rows = (
        db.query(Metric)
        .filter(Metric.site_id == site_id, Metric.metric_type == metric_type)
        .order_by(Metric.collected_at.desc(), Metric.id.desc())
        .limit(2)
        .all()
    )
    if not rows:
        return None, None
    newest = float(rows[0].value)
    if len(rows) < 2:
        return None, newest
    older = float(rows[1].value)
    return older, newest


def get_metric_first_last_in_window(
    db: Session,
    site_id: int,
    metric_type: str,
    *,
    window_days: int,
) -> tuple[float | None, float | None, date | None, date | None]:
    """Son ``window_days`` gün içindeki aynı metrik türünün en eski ve en yeni örneği.

    PageSpeed gibi seyrek ölçümlerde eğilim göstermek için (günlük dışı dönemler).
    Dönüş: (ilk_değer, son_değer, ilk_yerel_gün, son_yerel_gün).
    """
    wd = int(window_days) if int(window_days) > 0 else 1
    hist = get_metric_history(db, site_id, days=wd)
    items = hist.get(metric_type) or []
    if not items:
        return None, None, None, None

    def _local_day(iso_ts: str) -> date | None:
        try:
            raw = str(iso_ts).replace("Z", "+00:00")
            parsed = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return None
        loc = to_local_datetime(parsed)
        return loc.date() if loc else None

    last_item = items[-1]
    last_val = float(last_item["value"])
    last_d = _local_day(str(last_item.get("collected_at") or ""))
    if len(items) < 2:
        return None, last_val, None, last_d

    first_item = items[0]
    first_val = float(first_item["value"])
    first_d = _local_day(str(first_item.get("collected_at") or ""))
    return first_val, last_val, first_d, last_d


def get_metric_day_over_day_score(
    db: Session, site_id: int, metric_type: str
) -> tuple[float | None, float | None, date | None]:
    """Son kaydın yerel takvim gününden bir önceki günün (o gün içindeki en geç) skoru.

    Dönüş: (dün_skoru, son_skor, dün_tarihi). Dün kaydı yoksa (None, son_skor, None).
    """
    latest = (
        db.query(Metric)
        .filter(Metric.site_id == site_id, Metric.metric_type == metric_type)
        .order_by(Metric.collected_at.desc(), Metric.id.desc())
        .first()
    )
    if latest is None:
        return None, None, None
    newest = float(latest.value)
    loc = to_local_datetime(latest.collected_at)
    if loc is None:
        return None, newest, None
    prev_calendar = loc.date() - timedelta(days=1)
    start_utc, end_utc = utc_naive_bounds_for_local_calendar_day(prev_calendar)
    prev_row = (
        db.query(Metric)
        .filter(
            Metric.site_id == site_id,
            Metric.metric_type == metric_type,
            Metric.collected_at >= start_utc,
            Metric.collected_at < end_utc,
        )
        .order_by(Metric.collected_at.desc(), Metric.id.desc())
        .first()
    )
    if prev_row is None:
        return None, newest, None
    return float(prev_row.value), newest, prev_calendar


def dedupe_pagespeed_performance_scores_for_local_calendar_day(
    db: Session, site_id: int, local_day: date
) -> int:
    """Aynı yerel gün içindeki yinelenen PageSpeed performans skoru satırlarını siler; en geç kayıt kalır."""
    start_utc, end_utc = utc_naive_bounds_for_local_calendar_day(local_day)
    removed = 0
    for mt in PAGESPEED_PERFORMANCE_SCORE_TYPES:
        rows = (
            db.query(Metric)
            .filter(
                Metric.site_id == site_id,
                Metric.metric_type == mt,
                Metric.collected_at >= start_utc,
                Metric.collected_at < end_utc,
            )
            .order_by(Metric.collected_at.desc(), Metric.id.desc())
            .all()
        )
        for row in rows[1:]:
            db.delete(row)
            removed += 1
    return removed


def prune_pagespeed_performance_scores_older_than_local_date(
    db: Session, site_id: int, keep_from_local_date: date
) -> int:
    """Yerel tarihi ``keep_from_local_date`` öncesindeki performans skorlarını siler (dün+bugün için ``keep_from_local_date = bugün - 1``).

    Böylece karşılaştırma için en fazla iki yerel gün tutulur; daha eski skor tekrarları temizlenir.
    """
    removed = 0
    rows = (
        db.query(Metric)
        .filter(
            Metric.site_id == site_id,
            Metric.metric_type.in_(PAGESPEED_PERFORMANCE_SCORE_TYPES),
        )
        .all()
    )
    for row in rows:
        loc = to_local_datetime(row.collected_at)
        if loc is None:
            continue
        if loc.date() < keep_from_local_date:
            db.delete(row)
            removed += 1
    return removed