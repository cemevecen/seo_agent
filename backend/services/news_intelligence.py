import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from backend.models import NewsIntelligenceItem
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

def extract_image_from_url(url: str) -> str:
    """Haber sayfasını ziyaret edip og:image veya twitter:image meta etiketlerini arar."""
    if not url:
        return None
    try:
        # User-agent önemli
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        # Timeout'u kısa tutalım ki sync kilitlenmesin
        resp = requests.get(url, headers=headers, timeout=5, allow_redirects=True)
        if resp.status_code != 200:
            return None
        
        html = resp.text
        # og:image kontrolü
        og_match = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html)
        if not og_match:
            og_match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', html)
        
        if og_match:
            return og_match.group(1)
            
        # twitter:image kontrolü
        tw_match = re.search(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', html)
        if tw_match:
            return tw_match.group(1)
            
        return None
    except Exception as e:
        logger.debug(f"Failed to extract image from {url}: {e}")
        return None

# Kategori bazlı resmi Google News Headlines RSS linkleri
CATEGORY_URLS = {
    "İş Dünyası": "https://news.google.com/news/rss/headlines/section/topic/BUSINESS?hl=tr&gl=TR&ceid=TR:tr",
    "Dünya": "https://news.google.com/news/rss/headlines/section/topic/WORLD?hl=tr&gl=TR&ceid=TR:tr",
    "Türkiye": "https://news.google.com/news/rss/headlines/section/topic/NATION?hl=tr&gl=TR&ceid=TR:tr",
    "Bilim ve Teknoloji": "https://news.google.com/news/rss/headlines/section/topic/TECHNOLOGY?hl=tr&gl=TR&ceid=TR:tr"
}

# Filtreleme Anahtar Kelimeleri (Sadece etiketleme için kullanılır, engelleme yapmaz)
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım", "dolar", "euro", "piyasa", "yapay zeka", "teknoloji", "yazılım", "bilim", "startup", "inovasyon", "gündem", "haber"]

def fetch_and_sync_news_intelligence(db: Session, reset: bool = False):
    """Google News Arama RSS üzerinden haberleri çeker ve DB ile senkronize eder."""
    logger.info("Starting Google News Search Intelligence sync (reset=%s)...", reset)
    
    if reset:
        db.query(NewsIntelligenceItem).delete()
        db.commit()
        logger.info("Database cleared for news intelligence reset.")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    # Her kategori için ana akışı tara
    for category, rss_url in CATEGORY_URLS.items():
        logger.info("Fetching curated RSS for %s", category)
        
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
                
                # img tagı kontrolü (regex iyileştirildi)
                if not image_url and description:
                    # Headlines RSS içinde genelde <img src="..."> şeklinde olur
                    img_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', description)
                    if img_match:
                        image_url = img_match.group(1)
                        if any(x in image_url.lower() for x in ["/pixel/", "1x1", "tracking", "google-analytics"]):
                            image_url = None
                        elif image_url.startswith("//"):
                            image_url = "https:" + image_url
                
                # Google News spesifik thumbnail kontrolü (enclosure & media:thumbnail)
                if not image_url:
                    enclosure = item.find("enclosure")
                    if enclosure is not None:
                        image_url = enclosure.get("url")
                    else:
                        # Namespace ile media:thumbnail araması
                        media_thumb = item.find("{http://search.yahoo.com/mrss/}thumbnail")
                        if media_thumb is not None:
                            image_url = media_thumb.get("url")
                
                # Hiç görsel bulunamadıysa DERİN TARAMA (Deep Fetch) yap
                if not image_url and link:
                    # Sadece yeni eklenecekse derin tarama yapalım (DB yükünü azaltmak için)
                    exists = db.query(NewsIntelligenceItem).filter(NewsIntelligenceItem.url == link).first()
                    if not exists:
                        logger.info("RSS lacks image for %s, attempting deep fetch...", title[:50])
                        image_url = extract_image_from_url(link)
                
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
            logger.error("Error syncing news for category %s: %s", category, e)
            db.rollback()

    # 7 Günlük Temizlik: Geçmişe dönük tarama için süreyi uzatıyoruz
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
