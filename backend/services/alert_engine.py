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
    description_tr: str = ""
    description_en: str = ""
    recommendations: str = ""
    severity: str = "warning"  # critical, warning, info


# Alert açıklamaları ve önerileri
ALERT_DESCRIPTIONS = {
    "pagespeed_mobile_score": {
        "description_tr": "Mobil cihazlardaki sayfa yükleme performansı Google tarafından 0-100 puan ile değerlendirilir. 50 puanın altı kritik seviye kabul edilir.",
        "description_en": "Mobile page loading performance is evaluated by Google on a 0-100 scale. Scores below 50 are considered critical.",
        "what_means": "Mobil kullanıcıların sitenizi açması yavaş, Google sıralamalarında kayıp yaşıyorsunuz.",
        "what_means_en": "Mobile users experience slow page loads, and you're losing Google search rankings.",
        "severity": "critical"
    },
    "pagespeed_desktop_score": {
        "description_tr": "Masaüstü cihazlardaki sayfa yükleme performansı. Masaüstü genelde mobil'den daha hızlı olmalı.",
        "description_en": "Desktop page loading performance. Desktop speeds should typically be faster than mobile.",
        "what_means": "Masaüstü kullanıcıları da sayfa açılış süreleri yüzünden siteyi terk edebilir.",
        "what_means_en": "Desktop users may abandon your site due to slow page speeds.",
        "severity": "warning"
    },
    "crawler_robots_accessible": {
        "description_tr": "robots.txt dosyası Google bots'ların sitenizin hangi bölümlerini tarayabileceğini belirler. Bu dosya bulunamazsa, arama motorları siteyi tam tarayamayabilir.",
        "description_en": "The robots.txt file tells Google bots which parts of your site can be crawled. If missing, search engines may not fully index your site.",
        "what_means": "Google sitenizin tüm sayfalarını indeksleme imkanı olmayabilir.",
        "what_means_en": "Google may not have the opportunity to index all pages of your site.",
        "severity": "critical"
    },
    "crawler_sitemap_exists": {
        "description_tr": "sitemap.xml arama motorlarına sitenizin tüm sayfalarını listeler. Bu dosya olmazsa, derin sayfalar indekslenmeyebilir.",
        "description_en": "sitemap.xml lists all pages on your site to search engines. Without it, deep pages may not be indexed.",
        "what_means": "Yeni eklenen sayfalarınız Google'a geç ulaşabilir.",
        "what_means_en": "Your new pages may reach Google slowly.",
        "severity": "warning"
    },
    "crawler_schema_found": {
        "description_tr": "Schema markup (Structured Data) arama motorlarına sayfanızın içeriğini anlamaya yardımcı olur. Ürün, makale, lokasyon gibi bilgiler bulunmalı.",
        "description_en": "Schema markup helps search engines understand your page content. Should include product, article, location information, etc.",
        "what_means": "Rich snippets (yıldız, fiyat vb.) Search sonuçlarında gösterilmeyen sayfaların tıklanma oranı düşük olabilir.",
        "what_means_en": "Without rich snippets in search results, your click-through rate may be lower.",
        "severity": "info"
    },
    "crawler_canonical_found": {
        "description_tr": "Canonical etiketi sitenizde çift içerik (duplicate content) problemi olup olmadığını arama motorlarına söyler.",
        "description_en": "Canonical tag tells search engines if duplicate content exists on your site.",
        "what_means": "Aynı içerik birden fazla URL'de yayınlanıyorsa, linking power dağılabilir veya yanlış sayfa sıralanabilir.",
        "what_means_en": "If the same content is published on multiple URLs, Google may split ranking power or rank the wrong page.",
        "severity": "warning"
    },
    "search_console_dropped_queries": {
        "description_tr": "Google Search Console'da sıralamadığınız (veya konumunuz düşen) arama terimleri sayısı arttığında tetiklenir.",
        "description_en": "Triggered when the number of search queries where you're not ranking (or your position dropped) increases.",
        "what_means": "Siteniz için önemli arama terimlerindeki görünürlüğünüz azalıyor.",
        "what_means_en": "Your visibility is decreasing for important search queries.",
        "severity": "critical"
    },
    "search_console_biggest_drop": {
        "description_tr": "En yüksek sıralama kaybı olan arama terimindeki pozisyon düşüşü. Hangi arama terimleri etkilendiğini görmek için detaylara bakınız.",
        "description_en": "The biggest ranking drop for your top search queries. See details section for affected search terms.",
        "what_means": "Trafik getiren ana arama terimlerindeki sıralamanız düşmüş. Detaylı bilgi için açılımı görebilirsiniz.",
        "what_means_en": "Your ranking has dropped for your main traffic-driving search queries. See expansion for detailed terms.",
        "severity": "critical"
    }
}


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


def _build_message(site: Site, alert: Alert, metric: Metric, rule: AlertRuleDefinition, db: Session = None) -> str:
    # Alarm log mesajını okunur halde üretir.
    
    # Search Console uyarıları için query detayları ekle
    if alert.alert_type in ["search_console_dropped_queries", "search_console_biggest_drop"] and db:
        try:
            from backend.collectors.search_console import get_top_queries
            
            # Düşen query'leri bul
            if alert.alert_type == "search_console_biggest_drop":
                queries = get_top_queries(db, site, limit=5, device="all")
                if queries and len(queries) > 0:
                    query_list = ", ".join([f"'{q['query']}'" for q in queries[:3]])
                    return (
                        f"{site.domain} için {rule.title}. "
                        f"Etkilenen arama terimleri: {query_list}. "
                        f"Mevcut sıralama düşüşü: {metric.value:.2f} pozisyon, eşik: {alert.threshold:.2f}."
                    )
            elif alert.alert_type == "search_console_dropped_queries":
                queries = get_top_queries(db, site, limit=5, device="all")
                if queries and len(queries) > 0:
                    query_list = ", ".join([f"'{q['query']}'" for q in queries[:3]])
                    return (
                        f"{site.domain} için {rule.title}. "
                        f"Düşen arama terimleri: {query_list}. "
                        f"Sayı: {metric.value:.0f}, eşik: {alert.threshold:.0f}."
                    )
        except:
            pass  # Search Console data not available, use default message
    
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

        message = _build_message(site, alert, metric, rule, db)
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
            "id": log.id,
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


def emit_custom_alert(
    db: Session,
    site: Site,
    alert_type: str,
    message: str,
    dedupe_hours: int = 6,
) -> AlertLog | None:
    # Metric dışı güvenlik/operasyon olayları için alarm log kaydı üretir.
    alert = db.query(Alert).filter(Alert.site_id == site.id, Alert.alert_type == alert_type).first()
    if alert is None:
        alert = Alert(site_id=site.id, alert_type=alert_type, threshold=0.0, is_active=True)
        db.add(alert)
        db.commit()
        db.refresh(alert)

    if not alert.is_active:
        return None

    now = datetime.utcnow()
    last_log = (
        db.query(AlertLog)
        .filter(AlertLog.alert_id == alert.id)
        .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
        .first()
    )
    if last_log and last_log.message == message and last_log.triggered_at >= now - timedelta(hours=dedupe_hours):
        return None

    log = AlertLog(
        alert_id=alert.id,
        triggered_at=now,
        message=message,
        sent_mail=False,
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    subject = f"SEO Quota Alert: {site.domain}"
    body = f"<h2>Quota Uyarisi</h2><p>{message}</p>"
    if send_email(subject, body):
        log.sent_mail = True
        db.commit()
        db.refresh(log)
    return log
