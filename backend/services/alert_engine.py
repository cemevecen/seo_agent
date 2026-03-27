"""Metriklerden alarm üreten kural motoru."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from backend.models import Alert, AlertLog, Metric, Site
from backend.services.mailer import send_email
from backend.services.metric_store import get_latest_metrics


@dataclass(frozen=True)
class AlertRuleDefinition:
    # Varsayılan alarm kuralı tanımı.
    metric_type: str
    threshold: float
    comparator: str
    title: str


DEFAULT_ALERT_RULES: tuple[AlertRuleDefinition, ...] = (
    AlertRuleDefinition("pagespeed_mobile_score", 50.0, "lt", "Mobile PageSpeed kritik seviyede"),
    AlertRuleDefinition("pagespeed_desktop_score", 50.0, "lt", "Desktop PageSpeed kritik seviyede"),
    AlertRuleDefinition("crawler_robots_accessible", 1.0, "lt", "robots.txt erişilemiyor"),
    AlertRuleDefinition("crawler_sitemap_exists", 1.0, "lt", "sitemap.xml bulunamadı"),
    AlertRuleDefinition("crawler_schema_found", 1.0, "lt", "Schema markup bulunamadı"),
    AlertRuleDefinition("crawler_canonical_found", 1.0, "lt", "Canonical etiketi bulunamadı"),
    AlertRuleDefinition("search_console_dropped_queries", 1.0, "gt", "Düşen sorgu sayısı arttı"),
    AlertRuleDefinition("search_console_biggest_drop", 2.0, "gt", "Search Console sıralama düşüşü yüksek"),
)


def ensure_site_alerts(db: Session, site: Site) -> list[Alert]:
    # Site için varsayılan alarmlar yoksa oluşturur.
    alerts = db.query(Alert).filter(Alert.site_id == site.id).all()
    existing = {alert.alert_type for alert in alerts}
    created = False
    for rule in DEFAULT_ALERT_RULES:
        if rule.metric_type in existing:
            continue
        db.add(
            Alert(
                site_id=site.id,
                alert_type=rule.metric_type,
                threshold=rule.threshold,
                is_active=True,
            )
        )
        created = True
    if created:
        db.commit()
        alerts = db.query(Alert).filter(Alert.site_id == site.id).all()
    return alerts


def _is_triggered(metric_value: float, threshold: float, comparator: str) -> bool:
    # Alert eşiğinin aşılıp aşılmadığını kontrol eder.
    if comparator == "lt":
        return metric_value < threshold
    return metric_value > threshold


def _build_message(site: Site, alert: Alert, metric: Metric, rule: AlertRuleDefinition) -> str:
    # Alarm log mesajını okunur halde üretir.
    return (
        f"{site.domain} için {rule.title}. "
        f"Mevcut değer: {metric.value:.2f}, eşik: {alert.threshold:.2f}."
    )


def evaluate_site_alerts(db: Session, site: Site) -> list[AlertLog]:
    # Son metriklere göre aktif alarm kayıtlarını üretir.
    alerts = ensure_site_alerts(db, site)
    latest_metrics = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    rules = {rule.metric_type: rule for rule in DEFAULT_ALERT_RULES}
    created_logs: list[AlertLog] = []
    now = datetime.utcnow()

    for alert in alerts:
        if not alert.is_active:
            continue
        metric = latest_metrics.get(alert.alert_type)
        rule = rules.get(alert.alert_type)
        if metric is None or rule is None:
            continue
        if not _is_triggered(metric.value, alert.threshold, rule.comparator):
            continue

        message = _build_message(site, alert, metric, rule)
        last_log = (
            db.query(AlertLog)
            .filter(AlertLog.alert_id == alert.id)
            .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
            .first()
        )
        if last_log and last_log.message == message and last_log.triggered_at >= now - timedelta(hours=12):
            continue

        log = AlertLog(
            alert_id=alert.id,
            triggered_at=now,
            message=message,
            sent_mail=False,
        )
        db.add(log)
        created_logs.append(log)

    if created_logs:
        db.commit()
        for log in created_logs:
            db.refresh(log)
        _send_alert_emails(db, site, created_logs)
    return created_logs


def _send_alert_emails(db: Session, site: Site, logs: list[AlertLog]) -> None:
    # Yeni alarm kayıtlarını tek e-posta içinde yollar.
    if not logs:
        return

    html_items = "".join(f"<li>{log.message}</li>" for log in logs)
    subject = f"SEO Alert: {site.domain}"
    body = (
        f"<h2>{site.domain} için yeni uyarılar</h2>"
        f"<p>Aşağıdaki alarmlar tetiklendi:</p>"
        f"<ul>{html_items}</ul>"
    )
    sent = send_email(subject, body)
    if sent:
        for log in logs:
            log.sent_mail = True
        db.commit()


def get_recent_alerts(db: Session, limit: int = 20) -> list[dict]:
    # Dashboard ve alert sayfası için son alarm kayıtlarını döndürür.
    rows = (
        db.query(AlertLog, Alert, Site)
        .join(Alert, AlertLog.alert_id == Alert.id)
        .join(Site, Alert.site_id == Site.id)
        .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "site_id": site.id,
            "domain": site.domain,
            "alert_type": alert.alert_type,
            "message": log.message,
            "triggered_at": log.triggered_at.strftime("%d.%m.%Y %H:%M"),
            "sent_mail": log.sent_mail,
        }
        for log, alert, site in rows
    ]


def get_alert_rules(db: Session) -> list[dict]:
    # Settings ekranı için alert kural listesini döndürür.
    rows = db.query(Alert, Site).join(Site, Alert.site_id == Site.id).order_by(Site.domain.asc(), Alert.alert_type.asc()).all()
    return [
        {
            "id": alert.id,
            "domain": site.domain,
            "alert_type": alert.alert_type,
            "threshold": alert.threshold,
            "is_active": alert.is_active,
        }
        for alert, site in rows
    ]
