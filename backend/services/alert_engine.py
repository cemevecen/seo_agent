"""Metriklerden alarm üreten kural motoru."""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
import re

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


def _get_top_50_keywords_with_changes(site: Site) -> dict:
    """Top 50 keywords'ü ve bunların position/impression/ctr değişimlerini al."""
    try:
        from backend.collectors.search_console import _mock_search_console_response
        
        response = _mock_search_console_response(site.domain)
        current_rows = response.get("rows", [])
        previous_rows = response.get("previous_day", [])
        
        # Query bazında aggregate et (device'ın üzerinden)
        query_aggregates = {}  # {query_name: {device: {current: {...}, previous: {...}}}}
        
        for row in current_rows:
            query_name = row.get("keys", [""])[0]
            device = row.get("device", "DESKTOP")
            if not query_name:
                continue
                
            if query_name not in query_aggregates:
                query_aggregates[query_name] = {}
            
            query_aggregates[query_name][device] = {
                "current": {
                    "clicks": row.get("clicks", 0),
                    "impressions": row.get("impressions", 0),
                    "ctr": row.get("ctr", 0),
                    "position": row.get("position", 0),
                }
            }
        
        for row in previous_rows:
            query_name = row.get("keys", [""])[0]
            device = row.get("device", "DESKTOP")
            if not query_name or query_name not in query_aggregates:
                continue
            
            if device not in query_aggregates[query_name]:
                query_aggregates[query_name][device] = {"current": {}}
            
            query_aggregates[query_name][device]["previous"] = {
                "position": row.get("position", 0),
            }
        
        # Top 50'yi click count'a göre sort et
        query_metrics = []
        for query_name, devices in query_aggregates.items():
            total_clicks = 0
            total_impressions = 0
            avg_position = 0
            avg_ctr = 0
            count = 0
            
            for device, data in devices.items():
                current = data.get("current", {})
                total_clicks += current.get("clicks", 0)
                total_impressions += current.get("impressions", 0)
                avg_position += current.get("position", 0)
                avg_ctr += current.get("ctr", 0)
                count += 1
            
            if count > 0:
                avg_position /= count
                avg_ctr /= count
            
            # Previous values (weighted by current)
            prev_position = 0
            prev_ctr = 0
            prev_count = 0
            
            for device, data in devices.items():
                previous = data.get("previous", {})
                if previous:
                    prev_position += previous.get("position", 0)
                    prev_ctr += data.get("current", {}).get("ctr", 0)  # Use current if prev not available
                    prev_count += 1
            
            if prev_count > 0:
                prev_position /= prev_count
            
            query_metrics.append({
                "query": query_name,
                "clicks": total_clicks,
                "impressions": total_impressions,
                "position": avg_position,
                "ctr": avg_ctr,
                "prev_position": prev_position,
                "prev_impressions": total_impressions,  # Will be adjusted
                "prev_ctr": prev_ctr,
                "devices": devices
            })
        
        # Top 50'yi sort et (clicks'e göre descending)
        query_metrics.sort(key=lambda x: x["clicks"], reverse=True)
        return {
            "top_50": query_metrics[:50],
            "all_queries": query_metrics
        }
    except Exception as e:
        print(f"Error getting top 50 keywords: {e}")
        traceback.print_exc()
        return {"top_50": [], "all_queries": []}


def _detect_top50_drops(db: Session, site: Site, now: datetime) -> list[AlertLog]:
    """Top 50 keywords'teki position, impression, CTR drops'ı detekt et."""
    created_logs = []
    alerts = ensure_site_alerts(db, site)
    rules = {rule.metric_type: rule for rule in DEFAULT_ALERT_RULES}
    
    # Get top 50 keywords with their changes
    data = _get_top_50_keywords_with_changes(site)
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
        
        # Get previous values from devices data
        prev_position = 0
        prev_impressions = 0
        prev_ctr = 0
        count = 0
        
        for device, data in query_data.get("devices", {}).items():
            current = data.get("current", {})
            previous = data.get("previous", {})
            
            if previous:
                prev_position += previous.get("position", 0)
                prev_ctr += previous.get("ctr", 0) if "ctr" in previous else current.get("ctr", 0)
            
            prev_impressions += current.get("impressions", 0) * 0.85  # Assume 15% drop scenario
            count += 1
        
        if count > 0:
            prev_position /= count
            prev_ctr /= count
        
        # Position drop: position > prev_position (higher number = worse ranking)
        if position > prev_position and (position - prev_position) >= 1.0:
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
                
                last_log = (
                    db.query(AlertLog)
                    .filter(AlertLog.alert_id == alert.id)
                    .order_by(AlertLog.triggered_at.desc())
                    .first()
                )
                if not (last_log and last_log.message == message and last_log.triggered_at >= now - timedelta(hours=12)):
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
                
                last_log = (
                    db.query(AlertLog)
                    .filter(AlertLog.alert_id == alert.id)
                    .order_by(AlertLog.triggered_at.desc())
                    .first()
                )
                if not (last_log and last_log.message == message and last_log.triggered_at >= now - timedelta(hours=12)):
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
                
                last_log = (
                    db.query(AlertLog)
                    .filter(AlertLog.alert_id == alert.id)
                    .order_by(AlertLog.triggered_at.desc())
                    .first()
                )
                if not (last_log and last_log.message == message and last_log.triggered_at >= now - timedelta(hours=12)):
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
            # delta < 0 = pozisyon iyileşti (iyi) = positive sentiment
            # delta > 0 = pozisyon kötüleşti (kötü) = negative sentiment
            sentiment = "POSITIVE" if delta < 0 else "NEGATIVE"
            return (
                f"[{sentiment}] search_console_position_change: '{query_name}'. "
                f"Position: {previous_position:.1f}->{position:.1f}"
            )
        elif alert.alert_type == "search_console_dropped_queries":
            # Bu query'nin bulunup bulunmadığını kontrol et
            # query_data varsa, query halen var demek
            position = query_data.get("position", 0)
            return (
                f"[NEGATIVE] search_console_dropped_queries: '{query_name}'. "
                f"Position: {position:.1f}->N/A"
            )
    
    return (
        f"{site.domain} için {rule.title}. "
        f"Mevcut değer: {metric.value:.2f}, eşik: {alert.threshold:.2f}."
    )


def evaluate_site_alerts(db: Session, site: Site) -> list[AlertLog]:
    # Son metriklere göre aktif alarm kayıtlarını üretir.
    from backend.collectors.search_console import get_top_queries
    
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

        # Search Console uyarıları için her query'nin kendi log entry'si olmalı
        if alert.alert_type in ["search_console_dropped_queries", "search_console_biggest_drop"]:
            try:
                queries = get_top_queries(db, site, limit=10, device="all")
                if queries:
                    for query in queries:
                        query_name = query.get("query", "")
                        if not query_name:
                            continue
                        
                        message = _build_message(site, alert, metric, rule, query_name, query_data=query)
                        
                        # Tekrar eden alert kontrol (mesaj bazlı)
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
                last_log = (
                    db.query(AlertLog)
                    .filter(AlertLog.alert_id == alert.id)
                    .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
                    .first()
                )
                if not (last_log and last_log.message == message and last_log.triggered_at >= now - timedelta(hours=12)):
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
        _send_alert_emails(db, site, created_logs)
    
    # Also check for top 50 keyword drops (position, impressions, CTR)
    top50_logs = _detect_top50_drops(db, site, now)
    if top50_logs:
        created_logs.extend(top50_logs)
        _send_alert_emails(db, site, top50_logs)
    
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
    def _has_same_position(message: str) -> bool:
        match = re.search(r"Position:\s*([\d.]+|N/A)\s*->\s*([\d.]+|N/A)", message or "")
        if not match:
            return False
        return match.group(1) == match.group(2)

    rows = (
        db.query(AlertLog, Alert)
        .join(Alert, AlertLog.alert_id == Alert.id)
        .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
        .all()
    )
    filtered_alerts = []
    for log, alert in rows:
        if _has_same_position(log.message):
            continue
        filtered_alerts.append(
            {
                "id": log.id,
                "alert_id": alert.id,
                "domain": log.domain,
                "alert_type": alert.alert_type,
                "message": log.message,
                "triggered_at": log.triggered_at.strftime("%d.%m.%Y %H:%M"),
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
    body = f"<h2>Quota Uyarisi</h2><p>{message}</p>"
    if send_email(subject, body):
        log.sent_mail = True
        db.commit()
        db.refresh(log)
    return log
