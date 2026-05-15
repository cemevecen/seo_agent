import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
import urllib.parse
from sqlalchemy.orm import Session
from deep_translator import GoogleTranslator
from backend.models import NewsIntelligenceItem
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

# Çok Kanallı Tarama: Her kategori için birden fazla kaynak ve arama sorgusu
CATEGORY_SOURCES = {
    "Türkiye": [
        "https://www.cnnturk.com/feed/rss/all/news",
        "https://www.aa.com.tr/tr/rss/default?cat=guncel",
        "https://www.sabah.com.tr/rss/anasayfa.xml",
        "https://www.milliyet.com.tr/rss/rssNew/gundem.xml",
    ],
    "Genel": [
        "https://www.ekonomim.com/rss",
        "https://www.dunya.com/rss",
        "https://www.bloomberght.com/rss",
        "https://news.google.com/rss/search?q=site:gazeteoksijen.com+when:6h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "İş Dünyası": [
        "https://www.dunya.com/rss",
        "https://www.ekonomim.com/rss",
        "https://news.google.com/news/rss/headlines/section/topic/BUSINESS?hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Finans & Borsa": [
        "https://www.bloomberght.com/rss",
        "https://www.foreks.com/rss",
        "https://news.google.com/rss/search?q=borsa+istanbul+hisse+analiz+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=kripto+bitcoin+ethereum+when:6h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Dünya": [
        "https://www.aa.com.tr/tr/rss/default?cat=dunya",
        "https://www.cnnturk.com/feed/rss/all/news",
        "https://news.google.com/news/rss/headlines/section/topic/WORLD?hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Yahoo Finance": [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^DJI&region=US&lang=en-US",
    ],
    "Bilim ve Teknoloji": [
        "https://news.google.com/news/rss/headlines/section/topic/TECHNOLOGY?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/news/rss/headlines/section/topic/SCIENCE?hl=tr&gl=TR&ceid=TR:tr",
    ],
}

# Feed başına işlenecek maksimum item sayısı
MAX_ITEMS_PER_FEED = 30

# Filtreleme Anahtar Kelimeleri (Sadece etiketleme için kullanılır, engelleme yapmaz)
FILTER_KEYWORDS = ["döviz", "finans", "ekonomi", "iş dünyası", "borsa", "faiz", "enflasyon", "merkez bankası", "şirket", "yatırım", "dolar", "euro", "piyasa", "yapay zeka", "teknoloji", "yazılım", "bilim", "startup", "inovasyon", "gündem", "haber"]

# Negatif Filtre: Bu kelimeleri içeren başlıklar "haber" sayılmaz ve elenir
EXCLUDE_KEYWORDS = [
    "canlı grafik", "hisse senedi canlı", "fiyatı canlı", "bist:", 
    "teknik analiz", "günlük bülten", "sabah bülteni", "akşam bülteni", 
    "varant bülteni", "foreks haber", "cnbc-e haber", "piyasa ekranı",
    "reklam", "sponsorlu", "yol durumu", "hava durumu", "namaz vakitleri",
    "astroloji", "burç", "gün sonu bülteni", "piyasa özeti", "piyasa bülteni",
    "ekonomi takvimi", "ajanda", "şifreli kanal", "yayın akışı", "izle", "canlı izle"
]

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
                items = root.findall(".//item")[:MAX_ITEMS_PER_FEED]

                # Channel-level kaynak bilgisi — item'da <source> yoksa fallback
                channel = root.find("channel")
                ch_title = (channel.findtext("title") or "").strip() if channel is not None else ""
                ch_link  = (channel.findtext("link")  or "").strip() if channel is not None else ""
                # Google News RSS channel title'larını temizle (TR/EN)
                for _suffix in (" - Google News", " - Google Haberler", " - En yeni", " - Latest"):
                    if _suffix in ch_title:
                        ch_title = ch_title.replace(_suffix, "").strip()
                # "site:xxx.com" formatındaki arama sorgularından domain çıkar
                if ch_title.startswith('"site:'):
                    import re as _re2
                    _m = _re2.search(r'site:([a-z0-9.-]+)', ch_title)
                    if _m:
                        ch_title = _m.group(1).replace("www.", "").split(".")[0].capitalize()

                # --- Adım 1: Ham parse (çeviri öncesi) ---
                pending_items = []  # (title, link, source_name, source_url, description, display_topic, published_at)
                for item in items:
                    title = item.find("title").text if item.find("title") is not None else ""
                    link = item.find("link").text if item.find("link") is not None else ""

                    if category == "Yahoo Finance" and "/personal-finance/" in link.lower():
                        continue

                    pub_date_str = item.find("pubDate").text if item.find("pubDate") is not None else ""

                    source_el = item.find("source")
                    if source_el is not None:
                        source_name = (source_el.text or "").strip() or ch_title or "Bilinmiyor"
                        source_url  = source_el.get("url") or ch_link or None
                    else:
                        source_name = ch_title or "Bilinmiyor"
                        source_url  = ch_link or None

                    description = item.find("description").text if item.find("description") is not None else ""
                    image_url = None

                    title = title.strip()
                    for suffix in [f" - {source_name}", f" | {source_name}", f" - {source_name.upper()}", f" | {source_name.upper()}"]:
                        if title.endswith(suffix):
                            title = title[:-len(suffix)].strip()

                    if len(title) < 50:
                        continue
                    lower_title = title.lower()
                    if lower_title == source_name.lower() or lower_title == category.lower():
                        continue
                    if any(ex in lower_title for ex in EXCLUDE_KEYWORDS):
                        continue

                    combined_text = (title + " " + description).lower()
                    matched_topic = next((kw for kw in FILTER_KEYWORDS if kw in combined_text), None)
                    display_topic = matched_topic.capitalize() if matched_topic else category

                    published_at = None
                    for fmt in ["%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
                                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            published_at = datetime.strptime(pub_date_str, fmt)
                            break
                        except:
                            continue
                    if not published_at:
                        published_at = datetime.utcnow()

                    if db.query(NewsIntelligenceItem).filter(NewsIntelligenceItem.url == link).first():
                        continue

                    pending_items.append((title, link, source_name, source_url, description,
                                          display_topic, published_at, image_url))

                # --- Adım 2: Yahoo için toplu çeviri (tek API çağrısı) ---
                if category == "Yahoo Finance" and pending_items:
                    SEP = " ||| "
                    raw_titles = [p[0] for p in pending_items]
                    try:
                        from deep_translator import GoogleTranslator
                        joined = SEP.join(raw_titles)
                        translated_joined = GoogleTranslator(source="en", target="tr").translate(joined)
                        translated_parts = [t.strip() for t in (translated_joined or "").split(SEP.strip())]
                        if len(translated_parts) == len(raw_titles):
                            pending_items = [
                                (translated_parts[i],) + pending_items[i][1:]
                                for i in range(len(pending_items))
                            ]
                    except Exception as te:
                        logger.warning("Yahoo toplu çeviri başarısız, orijinal başlıklar kullanılacak: %s", te)

                # --- Adım 3: Kaydet ---
                new_count = 0
                for (title, link, source_name, source_url, description,
                     display_topic, published_at, image_url) in pending_items:
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
