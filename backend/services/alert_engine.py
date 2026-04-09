"""Metriklerden alarm üreten kural motoru."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from html import escape
import re
import time

from sqlalchemy.exc import OperationalError

LOGGER = logging.getLogger(__name__)
from sqlalchemy.orm import Session

from backend.models import Alert, AlertLog, Metric, Site
from backend.services.email_templates import data_table, note_box, render_email_shell, section, stat_cards, status_chip, summary_table
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
        "description_tr": "Mobil cihazlardaki sayfa yükleme performansı Google tarafından 0-100 puan ile değerlendirilir. 60 puanın altı düşük performans kabul edilir.",
        "description_en": "Mobile page loading performance is evaluated by Google on a 0-100 scale. Scores below 60 are treated as low performance.",
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
    "crawler_broken_links_count": {
        "description_tr": "Site içi taranan URL'lerin bir kısmı 404, 410 veya benzeri hata ile açılmıyor.",
        "description_en": "Some sampled internal links from the homepage are returning 404, 410, or similar errors.",
        "what_means": "Kullanıcı deneyimi bozulur, crawl bütçesi boşa gider ve SEO değeri zayıflayabilir.",
        "what_means_en": "User experience degrades, crawl budget is wasted, and SEO value may weaken.",
        "severity": "critical"
    },
    "crawler_redirect_chain_count": {
        "description_tr": "Site içi taranan URL'lerin bir kısmı hedefe tek adımda gitmiyor; birden fazla yönlendirme üzerinden açılıyor.",
        "description_en": "Some internal links are not resolving directly and require multiple redirect hops.",
        "what_means": "Tarama verimi düşer, sayfa açılışı uzar ve link değeri gereksiz yönlendirmelerde yıpranır.",
        "what_means_en": "Crawl efficiency drops, page loads slow down, and link equity is diluted through unnecessary redirects.",
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
        "description_tr": "Top 50 arama terimlerinde impression sayısı %15'ten fazla düştü. Daha az insan sitenizi SERP'de görüyor.",
        "description_en": "Top 50 search queries showing >15% drop in impressions. Fewer people are seeing your site in search results.",
        "what_means": "Arama motorlarında görünürlüğünüz azalıyor, organik trafik potansiyeli düşüyor.",
        "what_means_en": "Your visibility in search engines is decreasing, organic traffic potential is dropping.",
        "severity": "warning"
    },
    "search_console_ctr_drop": {
        "description_tr": "Top 50 arama terimlerinde tıklama oranı (CTR) %10'dan fazla düştü. Google'da göründüğünüz halde daha az insan tıklıyor.",
        "description_en": "Top 50 search queries showing >10% drop in CTR. People see your site in Google but click less often.",
        "what_means": "Title, description veya position'unuz kötüleşmiş olabilir. İyileştirme gerekli.",
        "what_means_en": "Your title, description, or position may have worsened. Optimization needed.",
        "severity": "warning"
    }
}


DEFAULT_ALERT_RULES: tuple[AlertRuleDefinition, ...] = (
    AlertRuleDefinition("pagespeed_mobile_score", 60.0, "lt", "Mobile PageSpeed düşük"),
    AlertRuleDefinition("pagespeed_desktop_score", 70.0, "lt", "Desktop PageSpeed düşük"),
    AlertRuleDefinition("crawler_robots_accessible", 1.0, "lt", "robots.txt erişilemiyor"),
    AlertRuleDefinition("crawler_sitemap_exists", 1.0, "lt", "sitemap.xml bulunamadı"),
    AlertRuleDefinition("crawler_schema_found", 1.0, "lt", "Schema markup bulunamadı"),
    AlertRuleDefinition("crawler_canonical_found", 1.0, "lt", "Canonical etiketi bulunamadı"),
    AlertRuleDefinition("crawler_broken_links_count", 2.0, "gt", "Kırık iç link sayısı yüksek"),
    AlertRuleDefinition("crawler_redirect_chain_count", 1.0, "gt", "Redirect zinciri sayısı yüksek"),
    AlertRuleDefinition("search_console_dropped_queries", 3.0, "gt", "Düşen sorgu sayısı yüksek"),
    AlertRuleDefinition("search_console_biggest_drop", 3.0, "gt", "Search Console sıralama düşüşü yüksek"),
    AlertRuleDefinition("search_console_position_drop", 2.0, "gt", "Top 50 keyword position düşüşü"),
    AlertRuleDefinition("search_console_impressions_drop", 15.0, "gt", "Top 50 keyword impressions düşüşü"),
    AlertRuleDefinition("search_console_ctr_drop", 10.0, "gt", "Top 50 keyword CTR düşüşü"),
)


SUPPORTED_ALERT_TYPES = {rule.metric_type for rule in DEFAULT_ALERT_RULES}


def _is_supported_alert_type(alert_type: str) -> bool:
    metric_type = str(alert_type or "")
    if metric_type in SUPPORTED_ALERT_TYPES:
        return True
    if metric_type.startswith("pagespeed_") and metric_type.endswith("_fetch_error"):
        return True
    if metric_type.startswith("quota_"):
        return True
    return False


def ensure_site_alerts(db: Session, site: Site) -> list[Alert]:
    # Site için varsayılan alarmlar yoksa oluşturur.
    alerts = db.query(Alert).filter(Alert.site_id == site.id).all()
    by_type = {alert.alert_type: alert for alert in alerts}
    changed = False

    for alert in alerts:
        if _is_supported_alert_type(alert.alert_type):
            continue
        if alert.is_active:
            alert.is_active = False
            changed = True

    for rule in DEFAULT_ALERT_RULES:
        existing_alert = by_type.get(rule.metric_type)
        if existing_alert is None:
            db.add(
                Alert(
                    site_id=site.id,
                    alert_type=rule.metric_type,
                    threshold=rule.threshold,
                    is_active=True,
                )
            )
            changed = True
            continue

    if changed:
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


def _is_sqlite_locked_error(exc: Exception) -> bool:
    return "database is locked" in str(exc).lower()


def _commit_with_retry(db: Session, *, retries: int = 5, base_delay: float = 0.15) -> None:
    """SQLite database locked hatası için exponential backoff ile retry."""
    last_exc: OperationalError | None = None
    for attempt in range(retries):
        try:
            db.commit()
            return
        except OperationalError as exc:
            db.rollback()
            if not _is_sqlite_locked_error(exc):
                raise
            last_exc = exc
            if attempt == retries - 1:
                break
            delay = base_delay * (2 ** attempt)  # 0.15, 0.30, 0.60, 1.20, ...
            LOGGER.debug("SQLite locked, retry %d/%d, %.2fs bekleniyor.", attempt + 1, retries, delay)
            time.sleep(delay)
    if last_exc is not None:
        raise last_exc


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
                if not current or not previous:
                    continue
                if current:
                    current_device_rows.append(current)
                    current_clicks += float(current.get("clicks", 0.0) or 0.0)
                if previous:
                    previous_device_rows.append(previous)
                    previous_clicks += float(previous.get("clicks", 0.0) or 0.0)

            if not current_device_rows or not previous_device_rows:
                continue

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
                "is_dropped": False,
            }
            query_metrics.append(metric)

        query_metrics.sort(key=lambda item: item.get("traffic_weight", 0.0), reverse=True)
        dropped_queries.sort(key=lambda item: item.get("traffic_weight", 0.0), reverse=True)
        return {
            "top_50": query_metrics[:50],
            "all_queries": query_metrics,
            "dropped_queries": dropped_queries[:10],
        }
    except Exception:
        LOGGER.exception("Top 50 keyword değişim hesaplaması başarısız.")
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

    position_alert = next((a for a in alerts if a.alert_type == "search_console_position_drop"), None)
    impressions_alert = next((a for a in alerts if a.alert_type == "search_console_impressions_drop"), None)
    ctr_alert = next((a for a in alerts if a.alert_type == "search_console_ctr_drop"), None)
    position_drop_threshold = float(position_alert.threshold if position_alert else rules["search_console_position_drop"].threshold)
    impressions_drop_threshold_pct = float(impressions_alert.threshold if impressions_alert else rules["search_console_impressions_drop"].threshold)
    ctr_drop_threshold_pct = float(ctr_alert.threshold if ctr_alert else rules["search_console_ctr_drop"].threshold)
    
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
        if prev_position > 0 and position > prev_position and (position - prev_position) >= position_drop_threshold:
            position_drops.append({
                "query": query_name,
                "old_position": prev_position,
                "new_position": position,
                "change": position - prev_position,
                "clicks": query_data.get("clicks", 0)
            })
        
        # Impression drop: threshold-based percentage decrease
        impression_drop_pct = ((prev_impressions - impressions) / prev_impressions * 100.0) if prev_impressions > 0 else 0.0
        if prev_impressions > 0 and impression_drop_pct >= impressions_drop_threshold_pct:
            impression_drops.append({
                "query": query_name,
                "old_impressions": prev_impressions,
                "new_impressions": impressions,
                "change_pct": ((impressions - prev_impressions) / prev_impressions) * 100,
                "clicks": query_data.get("clicks", 0)
            })
        
        # CTR drop: threshold-based percentage decrease
        ctr_drop_pct = ((prev_ctr - ctr) / prev_ctr * 100.0) if prev_ctr > 0 else 0.0
        if prev_ctr > 0 and ctr_drop_pct >= ctr_drop_threshold_pct:
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
                    f"CTR {drop['old_ctr']:.3f}% → {drop['new_ctr']:.3f}% ({drop['change_pct']:+.1f}%)"
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
        if alert.alert_type == "search_console_dropped_queries":
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
            except Exception:
                # Search Console data mevcut değil, fallback mesajı kullan
                LOGGER.exception("get_top_queries başarısız: site=%s", site.domain)
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

    parsed_logs = [_parse_alert_message(log.message, domain=site.domain) for log in logs]
    rows = [_alert_email_row(parsed) for parsed in parsed_logs]
    overview_cards = _alert_overview_cards(parsed_logs)
    subject = f"SEO Alert: {site.domain}"
    body = render_email_shell(
        eyebrow="SEO Agent Alerts",
        title=f"{site.domain} icin yeni uyarilar",
        intro="",
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
            ),
            section(
                "Kritik Gorunum",
                stat_cards(overview_cards),
            ),
            section(
                "Uyari Tablosu",
                data_table(
                    ["Durum", "Olcum", "Sorgu / Alan", "Once", "Simdi", "Delta", "Ek Veri"],
                    rows,
                ),
            ),
        ],
    )
    sent = send_email(subject, body)
    if sent:
        for log in logs:
            log.sent_mail = True
        try:
            _commit_with_retry(db)
        except OperationalError as exc:
            db.rollback()
            # Mail gonderimi tamamlandi; lock nedeniyle sent_mail yazilamazsa ana akisi bozma.
            LOGGER.warning("Alert mail sent_mail güncellenemedi (DB lock): %s", exc)


def _metric_type_for_alert_filter(presentation: dict[str, object], alert_type: str) -> str:
    """Chip filtreleri için UI metrik adı — alert_type → kategori adı."""
    mt = str(presentation.get("metric_type") or "")
    # SC tipler
    sc_map = {
        "search_console_ctr_drop": "CTR",
        "search_console_impressions_drop": "Impression",
        "search_console_position_drop": "Pozisyon",
        "search_console_biggest_drop": "Pozisyon",
    }
    if alert_type in sc_map:
        return sc_map[alert_type]
    # Eğer presentation'dan gelen mt zaten SC chip adı ise kullan
    if mt in ("CTR", "Impression", "Pozisyon"):
        return mt
    # Crawler / PageSpeed tipleri
    other_map = {
        "crawler_schema_found": "Schema markup bulunamadı",
        "crawler_sitemap_exists": "sitemap.xml bulunamadı",
        "pagespeed_mobile_score": "Mobile PageSpeed düşük",
        "pagespeed_desktop_score": "Desktop PageSpeed düşük",
        "crawler_robots_accessible": "robots.txt erişilemiyor",
        "crawler_canonical_found": "Canonical etiketi bulunamadı",
        "crawler_broken_links_count": "Kırık iç link sayısı yüksek",
        "crawler_redirect_chain_count": "Redirect zinciri sayısı yüksek",
    }
    return other_map.get(alert_type, mt or "Genel")


def get_recent_alerts(
    db: Session,
    limit: int = 20,
    *,
    include_external: bool = False,
    site_id_filter: int | None = None,
) -> list[dict]:
    # Dashboard ve alert sayfası için son alarm kayıtlarını döndürür.
    from backend.models import ExternalSite  # local import to avoid circular
    external_site_ids: set[int] = {
        int(row.site_id) for row in db.query(ExternalSite.site_id).all()
    }
    query = db.query(AlertLog, Alert).join(Alert, AlertLog.alert_id == Alert.id)
    if site_id_filter is not None:
        query = query.filter(Alert.site_id == site_id_filter)
    rows = (
        query
        .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
        .limit(max(limit * 8, 200))  # DB-level limit: Python dedup için ~8× buffer
        .all()
    )
    filtered_alerts = []
    seen_keys: set[tuple[str, str, str, str, str, str]] = set()
    for log, alert in rows:
        if alert.alert_type == "search_console_dropped_queries":
            continue
        is_external = alert.site_id in external_site_ids
        if is_external and not include_external:
            continue
        presentation = _parse_alert_message(log.message, alert_type=alert.alert_type, domain=log.domain)
        if presentation.get("skip"):
            continue
        semantic_key = (
            log.domain,
            alert.alert_type,
            presentation.get("display_title") or "",
            presentation.get("display_query") or "",
            presentation.get("display_metric") or "",
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
                "display_title": presentation["display_title"],
                "display_query": presentation["display_query"],
                "display_metric": presentation["display_metric"],
                "display_tone": presentation["tone"],
                "display_device_code": presentation.get("device_code") or "",
                "triggered_at": format_local_datetime(log.triggered_at),
                "triggered_at_iso": log.triggered_at.strftime("%Y-%m-%dT%H:%M:%S"),
                "sent_mail": log.sent_mail,
                "metric_type": _metric_type_for_alert_filter(presentation, alert.alert_type),
                "is_external": is_external,
            }
        )
        if len(filtered_alerts) >= limit:
            break
    return filtered_alerts


def get_site_alerts(db: Session, *, site_id: int, limit: int = 500) -> dict:
    # Site bazli alert listesi ve kirilim ozetlerini dondurur.
    rows = (
        db.query(AlertLog, Alert)
        .join(Alert, AlertLog.alert_id == Alert.id)
        .filter(Alert.site_id == site_id)
        .order_by(AlertLog.triggered_at.desc(), AlertLog.id.desc())
        .all()
    )

    alerts: list[dict] = []
    tone_breakdown = {
        "rose": 0,
        "amber": 0,
        "sky": 0,
        "emerald": 0,
        "other": 0,
    }

    for log, alert in rows:
        if alert.alert_type == "search_console_dropped_queries":
            continue
        presentation = _parse_alert_message(log.message, alert_type=alert.alert_type, domain=log.domain)
        if presentation.get("skip"):
            continue

        tone = str(presentation.get("tone") or "other")
        if tone not in tone_breakdown:
            tone = "other"
        tone_breakdown[tone] += 1

        alerts.append(
            {
                "id": log.id,
                "alert_id": alert.id,
                "domain": log.domain,
                "alert_type": alert.alert_type,
                "message": log.message,
                "display_title": presentation["display_title"],
                "display_query": presentation["display_query"],
                "display_metric": presentation["display_metric"],
                "display_tone": tone,
                "display_device_code": presentation.get("device_code") or "",
                "triggered_at": format_local_datetime(log.triggered_at),
                "sent_mail": log.sent_mail,
            }
        )
        if len(alerts) >= limit:
            break

    return {
        "items": alerts,
        "total": len(alerts),
        "breakdown": tone_breakdown,
    }


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
        if _is_supported_alert_type(alert.alert_type)
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
        try:
            _commit_with_retry(db)
            db.refresh(log)
        except OperationalError as exc:
            db.rollback()
            LOGGER.warning("Kota alert mail sent_mail güncellenemedi (DB lock): %s", exc)
    return log


def _device_label(device_code: str) -> str:
    return {"M": "Mobil", "D": "Desktop"}.get(device_code or "", device_code or "-")


def _safe_float(value) -> float | None:
    if value in {None, "", "N/A"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_number(value, *, decimals: int = 0, suffix: str = "") -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "-"
    rendered = f"{numeric:,.{decimals}f}"
    rendered = rendered.replace(",", "__TMP__").replace(".", ",").replace("__TMP__", ".")
    if decimals > 0:
        rendered = rendered.rstrip("0").rstrip(",")
    return f"{rendered}{suffix}"


def _format_percent(value, *, decimals: int = 1) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "-"
    return f"{_format_number(numeric, decimals=decimals)}%"


def _format_delta(value, *, decimals: int = 1, suffix: str = "") -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return "-"
    rendered = f"{numeric:+,.{decimals}f}"
    rendered = rendered.replace(",", "__TMP__").replace(".", ",").replace("__TMP__", ".")
    if decimals > 0:
        rendered = rendered.rstrip("0").rstrip(",")
    return f"{rendered}{suffix}"


def _delta_html(text: str, *, tone: str) -> str:
    if text == "-":
        return text
    palette = {
        "rose": "#be123c",
        "emerald": "#047857",
        "amber": "#b45309",
        "sky": "#0369a1",
        "slate": "#334155",
    }
    return f'<span style="font-weight:800;color:{palette.get(tone, "#334155")};">{escape(text)}</span>'


def _parse_alert_message(message: str, *, alert_type: str = "", domain: str = "") -> dict[str, object]:
    raw = str(message or "")
    clean = raw.replace("[POSITIVE]", "").replace("[NEGATIVE]", "").strip()
    device_match = re.search(r"\s+\[(M|D)\]\s*$", clean)
    device_code = device_match.group(1) if device_match else ""
    clean = re.sub(r"\s+\[(M|D)\]\s*$", "", clean)
    tone = "rose" if "[NEGATIVE]" in raw else "emerald" if "[POSITIVE]" in raw else "amber"
    status_label = "Negatif" if tone == "rose" else "Pozitif" if tone == "emerald" else "Uyari"

    ctr_match = re.search(
        r"CTR düşüşü - '([^']+)' \(([\d.,]+) clicks\): CTR ([\d.]+)%?\s*→\s*([\d.]+)%?\s*\(([-+]?[\d.]+%)\)",
        clean,
    )
    if ctr_match:
        clicks = _safe_float(ctr_match.group(2).replace(",", ""))
        return {
            "tone": "rose",
            "status_label": "Negatif",
            "metric_type": "CTR",
            "query_or_area": ctr_match.group(1),
            "device_code": device_code,
            "before": _format_percent(ctr_match.group(3), decimals=3),
            "after": _format_percent(ctr_match.group(4), decimals=3),
            "delta": _format_percent(str(ctr_match.group(5)).replace("%", ""), decimals=1),
            "extra": f"{_format_number(clicks)} click",
            "extra_numeric": clicks,
            "delta_numeric": _safe_float(str(ctr_match.group(5)).replace("%", "")),
            "display_title": "CTR düşüşü",
            "display_query": ctr_match.group(1),
            "display_metric": (
                f"CTR {_format_percent(ctr_match.group(3), decimals=3)} -> "
                f"{_format_percent(ctr_match.group(4), decimals=3)} | "
                f"Click {_format_number(clicks)} | Delta {_format_percent(str(ctr_match.group(5)).replace('%', ''), decimals=1)}"
            ),
        }

    impression_match = re.search(
        r"impression düşüşü - '([^']+)' \(([\d.,]+) clicks\): Impressions ([\d.]+)\s*→\s*([\d.]+)\s*\(([-+]?[\d.]+%)\)",
        clean,
    )
    if impression_match:
        clicks = _safe_float(impression_match.group(2).replace(",", ""))
        return {
            "tone": "amber",
            "status_label": "Uyari",
            "metric_type": "Impression",
            "query_or_area": impression_match.group(1),
            "device_code": device_code,
            "before": _format_number(impression_match.group(3)),
            "after": _format_number(impression_match.group(4)),
            "delta": _format_percent(str(impression_match.group(5)).replace("%", ""), decimals=1),
            "extra": f"{_format_number(clicks)} click",
            "extra_numeric": clicks,
            "delta_numeric": _safe_float(str(impression_match.group(5)).replace("%", "")),
            "display_title": "Impression düşüşü",
            "display_query": impression_match.group(1),
            "display_metric": (
                f"Impression {_format_number(impression_match.group(3))} -> "
                f"{_format_number(impression_match.group(4))} | "
                f"Click {_format_number(clicks)} | Delta {_format_percent(str(impression_match.group(5)).replace('%', ''), decimals=1)}"
            ),
        }

    position_match = re.search(r"'([^']+)'.*Position:\s*([\d.]+|N/A)\s*->\s*([\d.]+|N/A)", clean)
    if position_match:
        before_value = _safe_float(position_match.group(2))
        after_value = _safe_float(position_match.group(3))
        if before_value is not None and after_value is not None and before_value == after_value:
            return {"skip": True}
        if before_value is None or after_value is None:
            delta_text = "Kayıp"
        else:
            delta_text = _format_delta(after_value - before_value, decimals=1)
        title = "Pozisyon değişimi"
        if after_value is None:
            title = "Sorgu kaybı"
        elif before_value is not None and after_value is not None and after_value > before_value:
            title = "Pozisyon düşüşü"
        elif before_value is not None and after_value is not None and after_value < before_value:
            title = "Pozisyon iyileşmesi"
        return {
            "tone": "rose" if after_value is None or (before_value is not None and after_value is not None and after_value > before_value) else "emerald",
            "status_label": "Negatif" if after_value is None or (before_value is not None and after_value is not None and after_value > before_value) else "Pozitif",
            "metric_type": "Pozisyon",
            "query_or_area": position_match.group(1),
            "device_code": device_code,
            "before": _format_number(position_match.group(2), decimals=1),
            "after": _format_number(position_match.group(3), decimals=1) if position_match.group(3) != "N/A" else "N/A",
            "delta": delta_text,
            "extra": _device_label(device_code),
            "extra_numeric": None,
            "delta_numeric": (after_value - before_value) if before_value is not None and after_value is not None else None,
            "display_title": title,
            "display_query": position_match.group(1),
            "display_metric": f"Pozisyon {position_match.group(2)} -> {position_match.group(3)} | {_device_label(device_code)}",
        }

    threshold_match = re.search(
        r"^(.*?) için (.*)\. Mevcut değer: ([\d.]+), eşik: ([\d.]+)\.",
        clean,
    )
    if threshold_match:
        threshold_title = threshold_match.group(2)
        current_value = _safe_float(threshold_match.group(3))
        threshold_value = _safe_float(threshold_match.group(4))
        delta_value = None
        if current_value is not None and threshold_value is not None:
            delta_value = current_value - threshold_value
        threshold_tone = "rose" if "kritik" in threshold_title.lower() else tone
        threshold_status = "Negatif" if threshold_tone == "rose" else status_label
        return {
            "tone": threshold_tone,
            "status_label": threshold_status,
            "metric_type": threshold_title,
            "query_or_area": threshold_match.group(1),
            "device_code": device_code,
            "before": f"Eşik {_format_number(threshold_value, decimals=2)}",
            "after": _format_number(current_value, decimals=2),
            "delta": _format_delta(delta_value, decimals=2),
            "extra": "Kritik eşik aşıldı" if threshold_tone == "rose" else "Eşik uyarısı",
            "extra_numeric": None,
            "delta_numeric": delta_value,
            "display_title": threshold_title,
            "display_query": threshold_match.group(1),
            "display_metric": f"Mevcut {_format_number(current_value, decimals=2)} | Eşik {_format_number(threshold_value, decimals=2)} | Fark {_format_delta(delta_value, decimals=2)}",
        }

    return {
        "tone": tone,
        "status_label": status_label,
        "metric_type": "Genel",
        "query_or_area": domain or "-",
        "device_code": device_code,
        "before": "-",
        "after": "-",
        "delta": "-",
        "extra": clean,
        "extra_numeric": None,
        "delta_numeric": None,
        "display_title": "Uyarı",
        "display_query": "",
        "display_metric": clean,
    }


def _alert_overview_cards(parsed_logs: list[dict[str, object]]) -> list[dict[str, str]]:
    unique_queries = {str(item.get("query_or_area") or "-") for item in parsed_logs if str(item.get("query_or_area") or "-") != "-"}
    click_values = [float(item["extra_numeric"]) for item in parsed_logs if item.get("extra_numeric") is not None]
    delta_values = [abs(float(item["delta_numeric"])) for item in parsed_logs if item.get("delta_numeric") is not None]
    negative_count = sum(1 for item in parsed_logs if item.get("tone") == "rose")
    return [
        {
            "label": "Toplam alarm",
            "value": str(len(parsed_logs)),
            "caption": "Bu pakette yer alan yeni uyari sayisi",
            "tone": "rose",
        },
        {
            "label": "Etkilenen alan",
            "value": str(len(unique_queries) or len(parsed_logs)),
            "caption": "Ayrik sorgu veya alan sayisi",
            "tone": "blue",
        },
        {
            "label": "En yuksek click",
            "value": _format_number(max(click_values)) if click_values else "-",
            "caption": "En cok click hacmine sahip etkilenen satir",
            "tone": "amber",
        },
        {
            "label": "En sert degisim",
            "value": _format_percent(max(delta_values)) if delta_values else "-",
            "caption": f"Negatif satir: {negative_count}",
            "tone": "emerald" if negative_count == 0 else "rose",
        },
    ]


def _alert_email_row(parsed: dict[str, object]) -> list[str]:
    tone = str(parsed.get("tone") or "amber")
    status = status_chip(str(parsed.get("status_label") or "Uyari"), tone=tone)
    area = escape(str(parsed.get("query_or_area") or "-"))
    device_code = str(parsed.get("device_code") or "")
    if device_code:
        area += " " + status_chip(_device_label(device_code), tone="slate")
    return [
        status,
        str(parsed.get("metric_type") or "Genel"),
        area,
        str(parsed.get("before") or "-"),
        str(parsed.get("after") or "-"),
        _delta_html(str(parsed.get("delta") or "-"), tone=tone),
        str(parsed.get("extra") or "-"),
    ]
