import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
from sqlalchemy.orm import Session
from backend.models import NewsIntelligenceItem
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

# Google News Topic IDs (Türkiye/Global için gerçek ID'ler)
TOPICS = {
    "İş Dünyası": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FuUnlHZ0pVVWlnQVAB",
    "Dünya": "CAAqJggKIiBDQkFTRWdvSUwyMHZNR3B4WmpVdU5EbHphR1p0Y0hKRE9BUnVfQW9G",
    "Türkiye": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FuUnlHZ0pVVWlnQVAB" # Türkiye için Business bazlı ekonomi süzgeci devam ediyor
}

# Filtreleme Anahtar Kelimeleri
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım"]

def fetch_and_sync_news_intelligence(db: Session):
    """Google News RSS üzerinden haberleri çeker ve DB ile senkronize eder."""
    logger.info("Starting Google News Intelligence sync...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for category, topic_id in TOPICS.items():
        rss_url = f"https://news.google.com/rss/topics/{topic_id}?hl=tr&gl=TR&ceid=TR:tr"
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=15)
            if response.status_code != 200:
                logger.error("Failed to fetch RSS for %s: HTTP %d", category, response.status_code)
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
                
                if not matched_topic:
                    continue # İlgisiz haberleri atla
                
                # Tarih dönüşümü
                try:
                    # Örn: Thu, 14 May 2026 17:00:00 GMT
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
                        topic=matched_topic.capitalize(),
                        published_at=published_at,
                        is_in_our_site=False, # Daha sonra sitemap taraması ile güncellenebilir
                        ai_note=f"Sektörel '{matched_topic}' haberi algılandı. Rakip kaynak: {source}"
                    )
                    db.add(new_item)
                    new_count += 1
            
            db.commit()
            logger.info("Synced %d new items for category: %s", new_count, category)
            
        except Exception as e:
            logger.exception("Error syncing news intelligence for %s", category)

def run_news_intelligence_job():
    """APScheduler wrapper."""
    with SessionLocal() as db:
        fetch_and_sync_news_intelligence(db)
