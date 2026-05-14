import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
import urllib.parse
from sqlalchemy.orm import Session
from backend.models import NewsIntelligenceItem
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

# Çok Kanallı Tarama: Her kategori için birden fazla kaynak ve arama sorgusu
CATEGORY_SOURCES = {
    "İş Dünyası": [
        "https://news.google.com/news/rss/headlines/section/topic/BUSINESS?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=ekonomi+finans+borsa+piyasalar+when:1h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=şirket+haberleri+yatırım+girişim+when:1h&hl=tr&gl=TR&ceid=TR:tr"
    ],
    "Finans & Borsa": [
        "https://www.bloomberght.com/rss",
        "https://news.google.com/rss/search?q=borsa+istanbul+hisse+analiz+temettü+when:1h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=kripto+para+bitcoin+ethereum+blokzincir+when:1h&hl=tr&gl=TR&ceid=TR:tr"
    ],
    "Rakip": [
        "https://news.google.com/rss/search?q=(site:ekonomim.com OR site:dunya.com OR site:bloomberght.com OR site:cnbce.com OR site:ntv.com.tr OR site:cnnturk.com OR site:foreks.com OR site:gazeteoksijen.com) (finans OR ekonomi OR politika) when:1h&hl=tr&gl=TR&ceid=TR:tr"
    ],
    "Yahoo Finance": [
        "https://finance.yahoo.com/news/rssindex",
        "https://news.google.com/rss/search?q=site:finance.yahoo.com+when:1h&hl=en-US&gl=US&ceid=US:en"
    ],
    "Dünya": [
        "https://news.google.com/news/rss/headlines/section/topic/WORLD?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=dünya+gündemi+uluslararası+manşetler+when:1h&hl=tr&gl=TR&ceid=TR:tr"
    ],
    "Türkiye": [
        "https://news.google.com/news/rss/headlines/section/topic/NATION?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=türkiye+gündemi+son+dakika+manşetler+when:1h&hl=tr&gl=TR&ceid=TR:tr"
    ],
    "Bilim ve Teknoloji": [
        "https://news.google.com/news/rss/headlines/section/topic/TECHNOLOGY?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/news/rss/headlines/section/topic/SCIENCE?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=yapay+zeka+teknoloji+dijital+yazılım+startup+when:1h&hl=tr&gl=TR&ceid=TR:tr"
    ]
}

# Filtreleme Anahtar Kelimeleri (Sadece etiketleme için kullanılır, engelleme yapmaz)
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım", "dolar", "euro", "piyasa", "yapay zeka", "teknoloji", "yazılım", "bilim", "startup", "inovasyon", "gündem", "haber"]

# Negatif Filtre: Bu kelimeleri içeren başlıklar "haber" sayılmaz ve elenir
EXCLUDE_KEYWORDS = ["canlı grafik", "hisse senedi canlı", "fiyatı canlı", "bist:"]

def fetch_and_sync_news_intelligence(db: Session, reset: bool = False):
    """Çok kanallı RSS üzerinden haberleri çeker ve DB ile senkronize eder."""
    logger.info("Starting Multi-Channel News Intelligence sync (reset=%s)...", reset)
    
    if reset:
        db.query(NewsIntelligenceItem).delete()
        db.commit()
        logger.info("Database cleared for news intelligence reset.")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Her kategori için tüm kaynakları tara
    for category, rss_urls in CATEGORY_SOURCES.items():
        logger.info("Scanning category: %s with %d sources", category, len(rss_urls))
        for rss_url in rss_urls:
            try:
                response = requests.get(rss_url, headers=headers, timeout=15)
                if response.status_code != 200:
                    logger.error("Failed to fetch RSS for %s from %s: HTTP %d", category, rss_url, response.status_code)
                    continue
                
                root = ET.fromstring(response.content)
                items = root.findall(".//item")
                
                new_count = 0
                for item in items:
                    title = item.find("title").text if item.find("title") is not None else ""
                    link = item.find("link").text if item.find("link") is not None else ""
                    pub_date_str = item.find("pubDate").text if item.find("pubDate") is not None else ""
                    
                    source_el = item.find("source")
                    source_name = source_el.text if source_el is not None else "Unknown"
                    source_url = source_el.get("url") if source_el is not None else None
                    
                    description = item.find("description").text if item.find("description") is not None else ""
                    
                    # Görsel (Thumbnail) Çekme - Kaldırıldı (Artık logo kullanılacak)
                    image_url = None
                    
                    # Negatif Filtre Kontrolü
                    lower_title = title.lower()
                    if any(ex in lower_title for ex in EXCLUDE_KEYWORDS):
                        continue

                    # Etiketleme için anahtar kelime kontrolü
                    combined_text = (title + " " + description).lower()
                    matched_topic = next((kw for kw in FILTER_KEYWORDS if kw in combined_text), None)
                    
                    # Her halükarda bir konu ismi veriyoruz
                    display_topic = matched_topic.capitalize() if matched_topic else category
                    
                    # Tarih dönüşümü
                    try:
                        # Thu, 14 May 2026 17:00:00 GMT
                        published_at = datetime.strptime(pub_date_str, "%a, %d %b %Y %H:%M:%S %Z")
                    except:
                        published_at = datetime.utcnow()

                    # DB'de var mı kontrol et
                    exists = db.query(NewsIntelligenceItem).filter(NewsIntelligenceItem.url == link).first()
                    if not exists:
                        new_item = NewsIntelligenceItem(
                            url=link,
                            headline=title,
                            content=description,
                            source_name=source_name,
                            source_url=source_url,
                            image_url=image_url,
                            category=category,
                            topic=display_topic,
                            published_at=published_at,
                            is_in_our_site=False,
                            ai_note=None
                        )
                        db.add(new_item)
                        new_count += 1
                
                db.commit()
                if new_count > 0:
                    logger.info("Synced %d new items for category: %s from source: %s", new_count, category, rss_url[:40])
            except Exception as e:
                logger.error("Error syncing news for category %s from %s: %s", category, rss_url, e)
                db.rollback()

    # 7 Günlük Temizlik
    try:
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=168)
        deleted_count = db.query(NewsIntelligenceItem).filter(NewsIntelligenceItem.published_at < cutoff).delete()
        db.commit()
        if deleted_count > 0:
            logger.info("Cleaned up %d old news items.", deleted_count)
    except Exception as e:
        logger.error("Error during news intelligence cleanup: %s", e)
        db.rollback()

def run_news_intelligence_job(reset: bool = False):
    """APScheduler wrapper."""
    with SessionLocal() as db:
        fetch_and_sync_news_intelligence(db, reset=reset)
