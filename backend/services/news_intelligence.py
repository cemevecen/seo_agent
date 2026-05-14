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
    "Türkiye": "türkiye ekonomisi döviz dolar borsa faiz",
    "Bilim ve Teknoloji": "yapay zeka teknoloji bilim yazılım uzay teknoloji haberleri"
}

# Filtreleme Anahtar Kelimeleri (Hala ekonomi odaklı kalması için)
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım", "dolar", "euro", "piyasa", "yapay zeka", "teknoloji", "yazılım", "bilim", "startup", "inovasyon"]

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
                
                source_el = item.find("source")
                source_name = source_el.text if source_el is not None else "Unknown"
                source_url = source_el.get("url") if source_el is not None else None
                
                description = item.find("description").text if item.find("description") is not None else ""
                
                # Görsel (Thumbnail) Çekme
                image_url = None
                # 1. media:content kontrolü (Namespace ile)
                media_content = item.find("{http://search.yahoo.com/mrss/}content")
                if media_content is not None:
                    image_url = media_content.get("url")
                
                # 2. Description içinde img tagı kontrolü (Eğer media:content yoksa)
                if not image_url and "<img" in description:
                    img_match = re.search(r'src="([^"]+)"', description)
                    if img_match:
                        image_url = img_match.group(1)
                
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
                        source_name=source_name,
                        source_url=source_url,
                        image_url=image_url,
                        category=category,
                        topic=display_topic,
                        published_at=published_at,
                        is_in_our_site=False,
                        ai_note=None # SEO Notu ibaresi kaldırılacağı için None yapıyoruz
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
