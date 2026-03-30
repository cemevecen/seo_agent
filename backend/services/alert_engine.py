"""Metriklerden alarm üreten kural motoru."""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
import re

from sqlalchemy.orm import Session

from backend.models import Alert, AlertLog, Metric, Site
from backend.services.email_templates import data_table, note_box, render_email_shell, section, status_chip, summary_table
from backend.services.mailer import send_email
from backend.services.metric_store import get_latest_metrics
from backend.services.timezone_utils import format_local_datetime
from backend.services.warehouse import get_latest_search_console_rows


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
    },
    "search_console_position_drop": {
        "description_tr": "Top 50 en önemli arama terimlerindeki pozisyon (ranking) düşüşü. Position sayısı arttıkça sıralam düşüyor (daha alt sırada çıkıyor).",
        "description_en": "Position drop in your top 50 most important search queries. Higher position number = lower ranking.",
        "what_means": "En yüksek hacimli arama terimlerinizde sıralama kaybı yaşanıyor.",
        "what_means_en": "Your ranking is dropping for high-volume search queries.",
        "severity": "critical"
    },
    "search_console_impressions_drop": {
        "description_tr": "Top 50 arama terimlerinde impression sayısı %10'dan fazla düştü. Daha az insan sitenizi SERP'de görüyor.",
        "description_en": "Top 50 search queries showing >10% drop in impressions. Fewer people are seeing your site in search results.",
        "what_means": "Arama motorlarında görünürlüğünüz azalıyor, organik trafik potansiyeli düşüyor.",
        "what_means_en": "Your visibility in search engines is decreasing, organic traffic potential is dropping.",
        "severity": "warning"
    },
    "search_console_ctr_drop": {
        "description_tr": "Top 50 arama terimlerinde tıklama oranı (CTR) %5'ten fazla düştü. Google'da göründüğünüz halde daha az insan tıklıyor.",
        "description_en": "Top 50 search queries showing >5% drop in CTR. People see your site in Google but click less often.",
        "what_means": "Title, description veya position'unuz kötüleşmiş olabilir. İyileştirme gerekli.",
        "what_means_en": "Your title, description, or position may have worsened. Optimization needed.",
        "severity": "warning"
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
    AlertRuleDefinition("search_console_position_drop", 1.0, "gt", "Top 50 keyword position düşüşü"),
    AlertRuleDefinition("search_console_impressions_drop", 10.0, "gt", "Top 50 keyword impressions düşüşü"),
    AlertRuleDefinition("search_console_ctr_drop", 5.0, "gt", "Top 50 keyword CTR düşüşü"),
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


def _weighted_position(rows: list[dict]) -> float:
    weighted_total = 0.0
    total_impressions = 0.0
    fallback_total = 0.0
    fallback_count = 0
    for row in rows:
        impressions = float(row.get("impressions", 0.0) or 0.0)
        position = float(row.get("position", 0.0) or 0.0)
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
    return 0.0


def _recent_duplicate_exists(
    db: Session,
    *,
    alert_id: int,
    message: str,
    now: datetime,
    dedupe_hours: int = 24,
) -> bool:
    return (
        db.query(AlertLog.id)
        .filter(
            AlertLog.alert_id == alert_id,
            AlertLog.message == message,
            AlertLog.triggered_at >= now - timedelta(hours=dedupe_hours),
        )
        .first()
        is not None
    )


def _device_scope_code(query_data: dict | None) -> str:
    devices = (query_data or {}).get("devices") or {}
    active_devices = []
    for device, payload in devices.items():
        if payload.get("current") or payload.get("previous"):
            active_devices.append(str(device).upper())
    active_devices = sorted(set(active_devices))
    if active_devices == ["DESKTOP"]:
        return "D"
    if active_devices == ["MOBILE"]:
        return "M"
    return ""


def _get_top_50_keywords_with_changes(db: Session, site: Site) -> dict:
    """Gerçek current_7d / previous_7d snapshot'larından top keyword değişimlerini al."""
    try:
        current_rows = [
            row
            for row in get_latest_search_console_rows(db, site_id=site.id, data_scope="current_7d")
            if str(row.get("device") or "").upper() in {"MOBILE", "DESKTOP"}
        ]
        previous_rows = [
            row
            for row in get_latest_search_console_rows(db, site_id=site.id, data_scope="previous_7d")
            if str(row.get("device") or "").upper() in {"MOBILE", "DESKTOP"}
        ]

        if not current_rows and not previous_rows:
            return {"top_50": [], "all_queries": [], "dropped_queries": []}

        query_aggregates: dict[str, dict[str, dict[str, dict]]] = {}

        for scope, rows in (("current", current_rows), ("previous", previous_rows)):
            for row in rows:
                query_name = str(row.get("query") or "").strip()
                device = str(row.get("device") or "DESKTOP").upper().strip()
                if not query_name or device not in {"MOBILE", "DESKTOP"}:
                    continue
                query_aggregates.setdefault(query_name, {}).setdefault(device, {})[scope] = {
                    "clicks": float(row.get("clicks", 0.0) or 0.0),
                    "impressions": float(row.get("impressions", 0.0) or 0.0),
                    "ctr": float(row.get("ctr", 0.0) or 0.0),
                    "position": float(row.get("position", 0.0) or 0.0),
                    "property_url": str(row.get("property_url") or ""),
                }

        query_metrics: list[dict] = []
        dropped_queries: list[dict] = []

        for query_name, devices in query_aggregates.items():
            current_device_rows = []
            previous_device_rows = []
            current_clicks = 0.0
            previous_clicks = 0.0

            for device, data in devices.items():
                current = data.get("current")
                previous = data.get("previous")
                if current:
                    current_device_rows.append(current)
                    current_clicks += float(current.get("clicks", 0.0) or 0.0)
                if previous:
                    previous_device_rows.append(previous)
                    previous_clicks += float(previous.get("clicks", 0.0) or 0.0)

            current_impressions = sum(float(item.get("impressions", 0.0) or 0.0) for item in current_device_rows)
            previous_impressions = sum(float(item.get("impressions", 0.0) or 0.0) for item in previous_device_rows)
            current_ctr = (current_clicks / current_impressions * 100.0) if current_impressions > 0 else 0.0
            previous_ctr = (previous_clicks / previous_impressions * 100.0) if previous_impressions > 0 else 0.0
            current_position = _weighted_position(current_device_rows)
            previous_position = _weighted_position(previous_device_rows)
            traffic_weight = max(current_clicks, previous_clicks)

            metric = {
                "query": query_name,
                "clicks": current_clicks,
                "previous_clicks": previous_clicks,
                "impressions": current_impressions,
                "previous_impressions": previous_impressions,
                "ctr": current_ctr,
                "previous_ctr": previous_ctr,
                "position": current_position,
                "previous_position": previous_position,
                "devices": devices,
                "traffic_weight": traffic_weight,
                "is_dropped": bool(previous_device_rows) and not bool(current_device_rows),
            }
            query_metrics.append(metric)

            if metric["is_dropped"]:
                dropped_queries.append(metric)

        query_metrics.sort(key=lambda item: item.get("traffic_weight", 0.0), reverse=True)
        dropped_queries.sort(key=lambda item: item.get("traffic_weight", 0.0), reverse=True)
        return {
            "top_50": query_metrics[:50],
            "all_queries": query_metrics,
            "dropped_queries": dropped_queries[:10],
        }
    except Exception as e:
        print(f"Error getting top 50 keywords: {e}")
        traceback.print_exc()
        return {"top_50": [], "all_queries": [], "dropped_queries": []}


def _detect_top50_drops(db: Session, site: Site, now: datetime) -> list[AlertLog]:
    """Top 50 keywords'teki position, impression, CTR drops'ı detekt et."""
    created_logs = []
    alerts = ensure_site_alerts(db, site)
    rules = {rule.metric_type: rule for rule in DEFAULT_ALERT_RULES}
    
    # Get top 50 keywords with their changes
    data = _get_top_50_keywords_with_changes(db, site)
    top_50 = data.get("top_50", [])
    
    if not top_50:
        return created_logs
    
    # Position drops
    position_drops = []
    impression_drops = []
    ctr_drops = []
    
    for query_data in top_50:
        query_name = query_data.get("query", "")
        position = query_data.get("position", 0)
        impressions = query_data.get("impressions", 0)
        ctr = query_data.get("ctr", 0)
        
        prev_position = float(query_data.get("previous_position", 0) or 0)
        prev_impressions = float(query_data.get("previous_impressions", 0) or 0)
        prev_ctr = float(query_data.get("previous_ctr", 0) or 0)
        
        # Position drop: position > prev_position (higher number = worse ranking)
        if prev_position > 0 and position > prev_position and (position - prev_position) >= 1.0:
            position_drops.append({
                "query": query_name,
                "old_position": prev_position,
                "new_position": position,
                "change": position - prev_position,
                "clicks": query_data.get("clicks", 0)
            })
        
        # Impression drop: >10% decrease
        if prev_impressions > 0 and impressions < prev_impressions * 0.9:
            impression_drops.append({
                "query": query_name,
                "old_impressions": prev_impressions,
                "new_impressions": impressions,
                "change_pct": ((impressions - prev_impressions) / prev_impressions) * 100,
                "clicks": query_data.get("clicks", 0)
            })
        
        # CTR drop: >5% decrease
        if prev_ctr > 0 and ctr < prev_ctr * 0.95:
            ctr_drops.append({
                "query": query_name,
                "old_ctr": prev_ctr,
                "new_ctr": ctr,
                "change_pct": ((ctr - prev_ctr) / prev_ctr) * 100 if prev_ctr > 0 else 0,
                "clicks": query_data.get("clicks", 0)
            })
    
    # Sort by click volume (importance)
    position_drops.sort(key=lambda x: x["clicks"], reverse=True)
    impression_drops.sort(key=lambda x: x["clicks"], reverse=True)
    ctr_drops.sort(key=lambda x: x["clicks"], reverse=True)
    
    # Create alerts for position drops
    if position_drops:
        alert = next((a for a in alerts if a.alert_type == "search_console_position_drop"), None)
        if alert and alert.is_active:
            top_3 = position_drops[:3]
            for drop in top_3:
                message = (
                    f"[NEGATIVE] search_console_position_drop: '{drop['query']}'. "
                    f"Position: {drop['old_position']:.1f}->{drop['new_position']:.1f}"
                )
                if not _recent_duplicate_exists(db, alert_id=alert.id, message=message, now=now):
                    log = AlertLog(
                        alert_id=alert.id,
                        domain=site.domain,
                        triggered_at=now,
                        message=message,
                        sent_mail=False,
                    )
                    db.add(log)
                    created_logs.append(log)
    
    # Create alerts for impression drops
    if impression_drops:
        alert = next((a for a in alerts if a.alert_type == "search_console_impressions_drop"), None)
        if alert and alert.is_active:
            top_3 = impression_drops[:3]
            for drop in top_3:
                message = (
                    f"[NEGATIVE] {site.domain} impression düşüşü - "
                    f"'{drop['query']}' ({drop['clicks']:.0f} clicks): "
                    f"Impressions {drop['old_impressions']:.0f} → {drop['new_impressions']:.0f} ({drop['change_pct']:+.1f}%)"
                )
                if not _recent_duplicate_exists(db, alert_id=alert.id, message=message, now=now):
                    log = AlertLog(
                        alert_id=alert.id,
                        domain=site.domain,
                        triggered_at=now,
                        message=message,
                        sent_mail=False,
                    )
                    db.add(log)
                    created_logs.append(log)
    
    # Create alerts for CTR drops
    if ctr_drops:
        alert = next((a for a in alerts if a.alert_type == "search_console_ctr_drop"), None)
        if alert and alert.is_active:
            top_3 = ctr_drops[:3]
            for drop in top_3:
                message = (
                    f"[NEGATIVE] {site.domain} CTR düşüşü - "
                    f"'{drop['query']}' ({drop['clicks']:.0f} clicks): "
                    f"CTR {drop['old_ctr']:.3f} → {drop['new_ctr']:.3f} ({drop['change_pct']:+.1f}%)"
                )
                if not _recent_duplicate_exists(db, alert_id=alert.id, message=message, now=now):
                    log = AlertLog(
                        alert_id=alert.id,
                        domain=site.domain,
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
    
    return created_logs


def _build_message(site: Site, alert: Alert, metric: Metric, rule: AlertRuleDefinition, query_name: str = None, query_data: dict = None) -> str:
    # Alarm log mesajını okunur halde üretir.
    
    # Search Console uyarıları için query detayları ekle - her sorgu için kendi verilerini kullan
    if alert.alert_type in ["search_console_dropped_queries", "search_console_biggest_drop"] and query_name and query_data:
        if alert.alert_type == "search_console_biggest_drop":
            position = query_data.get("position", 0)
            previous_position = query_data.get("previous_position", 0)
            delta = position - previous_position
            device_code = _device_scope_code(query_data)
            device_suffix = f" [{device_code}]" if device_code else ""
            # delta < 0 = pozisyon iyileşti (iyi) = positive sentiment
            # delta > 0 = pozisyon kötüleşti (kötü) = negative sentiment
            sentiment = "POSITIVE" if delta < 0 else "NEGATIVE"
            return (
                f"[{sentiment}] search_console_position_change: '{query_name}'. "
                f"Position: {previous_position:.1f}->{position:.1f}{device_suffix}"
            )
        elif alert.alert_type == "search_console_dropped_queries":
            position = query_data.get("previous_position", 0) or query_data.get("position", 0)
            device_code = _device_scope_code(query_data)
            device_suffix = f" [{device_code}]" if device_code else ""
            return (
                f"[NEGATIVE] search_console_dropped_queries: '{query_name}'. "
                f"Position: {position:.1f}->N/A{device_suffix}"
            )
    
    return (
        f"{site.domain} için {rule.title}. "
        f"Mevcut değer: {metric.value:.2f}, eşik: {alert.threshold:.2f}."
    )


def evaluate_site_alerts(db: Session, site: Site, *, send_notifications: bool = True) -> list[AlertLog]:
    # Son metriklere göre aktif alarm kayıtlarını üretir.
    alerts = ensure_site_alerts(db, site)
    latest_metrics = {metric.metric_type: metric for metric in get_latest_metrics(db, site.id)}
    rules = {rule.metric_type: rule for rule in DEFAULT_ALERT_RULES}
    created_logs: list[AlertLog] = []
    now = datetime.utcnow()
    search_console_changes = _get_top_50_keywords_with_changes(db, site)

    for alert in alerts:
        if not alert.is_active:
            continue
        metric = latest_metrics.get(alert.alert_type)
        rule = rules.get(alert.alert_type)
        if metric is None or rule is None:
            continue
        if not _is_triggered(metric.value, alert.threshold, rule.comparator):
            continue

        # Search Console uyarıları için her query'nin kendi log entry'si olmalı
        if alert.alert_type in ["search_console_dropped_queries", "search_console_biggest_drop"]:
            try:
                if alert.alert_type == "search_console_dropped_queries":
                    queries = search_console_changes.get("dropped_queries", [])
                else:
                    queries = [
                        query
                        for query in search_console_changes.get("top_50", [])
                        if float(query.get("previous_position", 0) or 0) > 0
                        and float(query.get("position", 0) or 0) > float(query.get("previous_position", 0) or 0)
                    ][:10]
                if queries:
                    for query in queries:
                        query_name = query.get("query", "")
                        if not query_name:
                            continue
                        
                        message = _build_message(site, alert, metric, rule, query_name, query_data=query)
                        
                        if _recent_duplicate_exists(db, alert_id=alert.id, message=message, now=now):
                            continue
                        
                        log = AlertLog(
                            alert_id=alert.id,
                            domain=site.domain,
                            triggered_at=now,
                            message=message,
                            sent_mail=False,
                        )
                        db.add(log)
                        created_logs.append(log)
            except Exception as e:
                # Search Console data not available, fallback to default message
                print(f"⚠️  get_top_queries failed for {site.domain}: {e}")
                traceback.print_exc()
                message = _build_message(site, alert, metric, rule)
                if not _recent_duplicate_exists(db, alert_id=alert.id, message=message, now=now):
                    log = AlertLog(
                        alert_id=alert.id,
                        domain=site.domain,
                        triggered_at=now,
                        message=message,
                        sent_mail=False,
                    )
                    db.add(log)
                    created_logs.append(log)
        else:
            # Diğer alert'ler (non-Search Console)
            message = _build_message(site, alert, metric, rule)
            if _recent_duplicate_exists(db, alert_id=alert.id, message=message, now=now):
                continue

            log = AlertLog(
                alert_id=alert.id,
                domain=site.domain,
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
        if send_notifications:
            _send_alert_emails(db, site, created_logs)
    
    # Also check for top 50 keyword drops (position, impressions, CTR)
    top50_logs = _detect_top50_drops(db, site, now)
    if top50_logs:
        created_logs.extend(top50_logs)
        if send_notifications:
            _send_alert_emails(db, site, top50_logs)
    
    return created_logs


def _send_alert_emails(db: Session, site: Site, logs: list[AlertLog]) -> None:
    # Yeni alarm kayıtlarını tek e-posta içinde yollar.
    if not logs:
        return

    rows = [_alert_email_row(log) for log in logs]
    subject = f"SEO Alert: {site.domain}"
    body = render_email_shell(
        eyebrow="SEO Agent Alerts",
        title=f"{site.domain} icin yeni uyarilar",
        intro="Asagidaki tablo son alarm turlerini, hangi sorgu veya metrikte degisim oldugunu ve degisim ozetini renkli satirlarla gosterir.",
        tone="rose",
        status_label="Alert",
        sections=[
            section(
                "Ozet",
                summary_table(
                    [
                        ("Site", site.domain),
                        ("Toplam yeni uyari", str(len(logs))),
                        ("Son tetikleme", format_local_datetime(max(log.triggered_at for log in logs))),
                    ]
                ),
                subtitle="Bu e-posta yeni olusan alarm loglarini tek paket halinde gonderir.",
            ),
            section(
                "Uyari Tablosu",
                data_table(
                    ["Durum", "Tur", "Sorgu / Alan", "Degisim"],
                    rows,
                ),
                subtitle="Her satir bir yeni alarm kaydini temsil eder.",
            ),
            section(
                "Yorum",
                note_box(
                    "Okuma Rehberi",
                    "CTR, impression veya pozisyon satirlarinda dusus renklerle vurgulanir. Sayisal degisim ilk inceleme icin yeterlidir; daha detayli karsilastirma uygulama icindeki alert ekraninda gorulur.",
                    tone="rose",
                ),
            ),
        ],
    )
    sent = send_email(subject, body)
    if sent:
        for log in logs:
            log.sent_mail = True
        db.commit()


def get_recent_alerts(db: Session, limit: int = 20) -> list[dict]:
    # Dashboard ve alert sayfası için son alarm kayıtlarını döndürür.
    def _has_same_position(message: str) -> bool:
        match = re.search(r"Position:\s*([\d.]+|N/A)\s*->\s*([\d.]+|N/A)", message or "")
        if not match:
            return False
        return match.group(1) == match.group(2)

    def _extract_device_code(message: str) -> str:
        match = re.search(r"\[(M|D)\]\s*$", message or "")
        return match.group(1) if match else ""

    def _present_alert(message: str, alert_type: str) -> dict:
        raw = str(message or "")
        clean = raw.replace("[POSITIVE]", "").replace("[NEGATIVE]", "").strip()
        clean = re.sub(r"\s+\[(M|D)\]\s*$", "", clean)

        title = "Uyarı"
        query = ""
        metric = clean
        tone = "slate"

        ctr_match = re.search(
            r"CTR düşüşü - '([^']+)' \(([\d.,]+) clicks\): CTR ([\d.]+)\s*→\s*([\d.]+)\s*\(([-+]?[\d.]+%)\)",
            clean,
        )
        if ctr_match:
            title = "CTR düşüşü"
            query = ctr_match.group(1)
            metric = f"CTR {ctr_match.group(3)} -> {ctr_match.group(4)}"
            tone = "rose"
            return {"title": title, "query": query, "metric": metric, "tone": tone}

        impression_match = re.search(
            r"impression düşüşü - '([^']+)' \(([\d.,]+) clicks\): Impressions ([\d.]+)\s*→\s*([\d.]+)\s*\(([-+]?[\d.]+%)\)",
            clean,
        )
        if impression_match:
            title = "Impression düşüşü"
            query = impression_match.group(1)
            metric = f"Impressions {impression_match.group(3)} -> {impression_match.group(4)}"
            tone = "amber"
            return {"title": title, "query": query, "metric": metric, "tone": tone}

        position_match = re.search(r"'([^']+)'.*Position:\s*([\d.]+|N/A)\s*->\s*([\d.]+|N/A)", clean)
        if position_match and alert_type in {
            "search_console_biggest_drop",
            "search_console_position_drop",
            "search_console_dropped_queries",
        }:
            title = "Pozisyon düşüşü"
            query = position_match.group(1)
            metric = f"Pozisyon {position_match.group(2)} -> {position_match.group(3)}"
            tone = "sky"
            return {"title": title, "query": query, "metric": metric, "tone": tone}

        return {
            "title": title,
            "query": query,
            "metric": metric,
            "tone": tone,
            "device_code": _extract_device_code(raw),
        }

    rows = (
        db.query(AlertLog, Alert)
        .join(Alert, AlertLog.alert_id == Alert.id)
        .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
        .all()
    )
    filtered_alerts = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    for log, alert in rows:
        if _has_same_position(log.message):
            continue
        presentation = _present_alert(log.message, alert.alert_type)
        semantic_key = (
            log.domain,
            alert.alert_type,
            presentation.get("title") or "",
            presentation.get("query") or "",
            presentation.get("metric") or "",
            presentation.get("device_code") or "",
        )
        if semantic_key in seen_keys:
            continue
        seen_keys.add(semantic_key)
        filtered_alerts.append(
            {
                "id": log.id,
                "alert_id": alert.id,
                "domain": log.domain,
                "alert_type": alert.alert_type,
                "message": log.message,
                "display_title": presentation["title"],
                "display_query": presentation["query"],
                "display_metric": presentation["metric"],
                "display_tone": presentation["tone"],
                "display_device_code": presentation.get("device_code") or "",
                "triggered_at": format_local_datetime(log.triggered_at),
                "sent_mail": log.sent_mail,
            }
        )
        if len(filtered_alerts) >= limit:
            break
    return filtered_alerts


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
        domain=site.domain,
        triggered_at=now,
        message=message,
        sent_mail=False,
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    subject = f"SEO Quota Alert: {site.domain}"
    body = render_email_shell(
        eyebrow="SEO Agent Quota",
        title=f"{site.domain} icin quota uyarisi",
        intro="Bu mail, entegrasyon kotasina yaklasildigini veya limitin asildigini haber verir.",
        tone="amber",
        status_label="Quota",
        sections=[
            section(
                "Ozet",
                summary_table(
                    [
                        ("Site", site.domain),
                        ("Zaman", format_local_datetime(log.triggered_at)),
                    ]
                ),
            ),
            section(
                "Detay",
                note_box("Quota Mesaji", message, tone="amber"),
            ),
        ],
    )
    if send_email(subject, body):
        log.sent_mail = True
        db.commit()
        db.refresh(log)
    return log


def _alert_email_row(log: AlertLog) -> list[str]:
    message = str(log.message or "")
    clean = message.replace("[POSITIVE]", "").replace("[NEGATIVE]", "").strip()
    tone = "rose" if "[NEGATIVE]" in message else "emerald" if "[POSITIVE]" in message else "amber"
    status = status_chip("Negatif" if tone == "rose" else "Pozitif" if tone == "emerald" else "Uyari", tone=tone)

    ctr_match = re.search(
        r"CTR düşüşü - '([^']+)' \(([\d.,]+) clicks\): CTR ([\d.]+)\s*→\s*([\d.]+)\s*\(([-+]?[\d.]+%)\)",
        clean,
    )
    if ctr_match:
        return [
            status,
            "CTR",
            ctr_match.group(1),
            f"{ctr_match.group(3)} -> {ctr_match.group(4)} ({ctr_match.group(5)})",
        ]

    impression_match = re.search(
        r"impression düşüşü - '([^']+)' \(([\d.,]+) clicks\): Impressions ([\d.]+)\s*→\s*([\d.]+)\s*\(([-+]?[\d.]+%)\)",
        clean,
    )
    if impression_match:
        return [
            status,
            "Impression",
            impression_match.group(1),
            f"{impression_match.group(3)} -> {impression_match.group(4)} ({impression_match.group(5)})",
        ]

    position_match = re.search(r"'([^']+)'.*Position:\s*([\d.]+|N/A)\s*->\s*([\d.]+|N/A)", clean)
    if position_match:
        return [
            status,
            "Pozisyon",
            position_match.group(1),
            f"{position_match.group(2)} -> {position_match.group(3)}",
        ]

    return [
        status,
        "Genel",
        log.domain,
        clean,
    ]
