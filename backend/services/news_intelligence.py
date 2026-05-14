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

# Kategori bazlı arama sorguları (Daha zengin veri için)
SEARCH_QUERIES = {
    "İş Dünyası": "şirket haberleri ekonomi finans borsa",
    "Dünya": "global ekonomi dünya piyasaları fed faiz",
    "Türkiye": "türkiye ekonomisi döviz dolar borsa faiz"
}

# Filtreleme Anahtar Kelimeleri (Hala ekonomi odaklı kalması için)
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım", "dolar", "euro", "piyasa"]

def fetch_and_sync_news_intelligence(db: Session):
    """Google News Arama RSS üzerinden haberleri çeker ve DB ile senkronize eder."""
    logger.info("Starting Google News Search Intelligence sync...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for category, query in SEARCH_QUERIES.items():
        encoded_query = urllib.parse.quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=tr&gl=TR&ceid=TR:tr"
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=15)
            if response.status_code != 200:
                logger.error("Failed to fetch search RSS for %s: HTTP %d", category, response.status_code)
                continue
            
            root = ET.fromstring(response.content)
            items = root.findall(".//item")
            
            new_count = 0
            for item in items:
                title = item.find("title").text if item.find("title") is not None else ""
                link = item.find("link").text if item.find("link") is not None else ""
                pub_date_str = item.find("pubDate").text if item.find("pubDate") is not None else ""
                source = item.find("source").text if item.find("source") is not None else "Unknown"
                description = item.find("description").text if item.find("description") is not None else ""
                
                # Başlıkta veya açıklamada anahtar kelime kontrolü
                combined_text = (title + " " + description).lower()
                matched_topic = next((kw for kw in FILTER_KEYWORDS if kw in combined_text), None)
                
                # Arama sorgusu zaten kısıtlı olduğu için eşleşme şartını biraz esnetiyoruz ama yine de kontrol ediyoruz
                display_topic = matched_topic.capitalize() if matched_topic else "Ekonomi"
                
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
                        source_name=source,
                        category=category,
                        topic=display_topic,
                        published_at=published_at,
                        is_in_our_site=False,
                        ai_note=f"'{category}' odaklı piyasa haberi algılandı. Kaynak: {source}"
                    )
                    db.add(new_item)
                    new_count += 1
            
            db.commit()
            logger.info("Synced %d new items for category: %s", new_count, category)
            
        except Exception as e:
            logger.exception("Error syncing news intelligence search for %s", category)

def run_news_intelligence_job():
    """APScheduler wrapper."""
    with SessionLocal() as db:
        fetch_and_sync_news_intelligence(db)
