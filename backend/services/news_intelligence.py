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

# Kategori bazlı arama sorguları (when:9h ile sadece son 9 saatin en taze haberleri)
SEARCH_QUERIES = {
    "İş Dünyası": "finans ekonomi borsa son dakika haber when:9h",
    "Dünya": "dünya gündemi manşet haberler when:9h",
    "Türkiye": "türkiye gündemi son dakika haberler when:9h",
    "Bilim ve Teknoloji": "teknoloji bilim yapay zeka yeni gelişmeler when:9h"
}

# Filtreleme Anahtar Kelimeleri (Sadece etiketleme için kullanılır, engelleme yapmaz)
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım", "dolar", "euro", "piyasa", "yapay zeka", "teknoloji", "yazılım", "bilim", "startup", "inovasyon", "gündem", "haber"]

def fetch_and_sync_news_intelligence(db: Session):
    """Google News Arama RSS üzerinden haberleri çeker ve DB ile senkronize eder."""
    logger.info("Starting Google News Search Intelligence sync...")
    
    # Zaman bazlı 'when' parametresi belirleme
    # Eğer saat 07:01 civarıysa (ilk çalışma) 2 saat, değilse 1 saat
    now = datetime.now()
    if now.hour == 7 and now.minute <= 10:
        freshness = "2h"
        logger.info("First run of the day (07:01), using when:2h")
    else:
        freshness = "1h"
        logger.info(f"Routine run, using when:{freshness}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Her kategori için aramayı yap
    for category, base_query in SEARCH_QUERIES.items():
        # Query'deki eski when:9h kısmını temizleyip dinamik olanı ekleyelim
        clean_query = base_query.split(" when:")[0]
        final_query = f"{clean_query} when:{freshness}"
        
        encoded_query = urllib.parse.quote(final_query)
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
                # media:content kontrolü
                media_content = item.find("{http://search.yahoo.com/mrss/}content")
                if media_content is not None:
                    image_url = media_content.get("url")
                
                # img tagı kontrolü
                if not image_url and "<img" in description:
                    img_match = re.search(r'src="([^"]+)"', description)
                    if img_match:
                        image_url = img_match.group(1)
                
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
            logger.info("Synced %d new items for category: %s", new_count, category)
            
        except Exception as e:
            logger.exception("Error syncing news intelligence search for %s", category)

    # 24 Saatlik Temizlik: 24 saatten eski tüm kayıtları sil
    try:
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=24)
        deleted_count = db.query(NewsIntelligenceItem).filter(NewsIntelligenceItem.published_at < cutoff).delete()
        db.commit()
        if deleted_count > 0:
            logger.info("Cleaned up %d news items older than 24 hours.", deleted_count)
    except Exception as e:
        logger.error("Error during news intelligence cleanup: %s", e)
        db.rollback()

def run_news_intelligence_job():
    """APScheduler wrapper."""
    with SessionLocal() as db:
        fetch_and_sync_news_intelligence(db)
