import gzip
import logging
import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from sqlalchemy.orm import Session

from backend.models import NewsIntelligenceItem
from backend.database import SessionLocal

logger = logging.getLogger(__name__)

# Çok Kanallı Tarama: Her kategori için birden fazla kaynak ve arama sorgusu.
#
# NOT: Aşağıdaki kaynaklar kasıtlı olarak çıkarıldı çünkü sürekli "not well-formed"
# parse hatası veriyor (Cloudflare bot koruması, Brotli sıkıştırması veya bozuk XML):
#   - sozcu.com.tr/rss.xml
#   - cumhuriyet.com.tr/rss/son_dakika.xml
#   - ntv.com.tr/gundem.rss
#   - dunya.com/rss
#   - ekonomim.com/rss
#   - techcrunch.com/feed/
#   - theverge.com/rss/index.xml
# Bunlar tekrar denenmemeli; çöp veri üretiyorlardı.
CATEGORY_SOURCES = {
    "Türkiye": [
        # Ajanslar
        "https://www.aa.com.tr/tr/rss/default?cat=guncel",
        # Gazeteler (sadece çalışanlar)
        "https://www.cnnturk.com/feed/rss/all/news",
        "https://www.sabah.com.tr/rss/anasayfa.xml",
        "https://www.hurriyet.com.tr/rss/anasayfa",
        "https://www.milliyet.com.tr/rss/rssNew/gundem.xml",
        "https://www.haberturk.com/rss",
        # Google News
        "https://news.google.com/rss/search?q=türkiye+gündem+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=ankara+siyaset+meclis+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=erdoğan+hükümet+when:3h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Genel": [
        "https://www.aa.com.tr/tr/rss/default?cat=guncel",
        "https://www.cnnturk.com/feed/rss/all/news",
        "https://www.bloomberght.com/rss",
        # Google News
        "https://news.google.com/rss/headlines/section/topic/TOP_STORIES?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=son+dakika+türkiye+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=türkiye+ekonomi+dünya+when:3h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "İş Dünyası": [
        "https://www.bloomberght.com/rss",
        # Google News
        "https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=şirket+ihracat+yatırım+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=startup+girişim+inovasyon+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=ipo+halka+arz+özelleştirme+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=ihracat+ithalat+ticaret+when:6h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Finans & Borsa": [
        "https://www.bloomberght.com/rss",
        # Google News — piyasa odaklı
        "https://news.google.com/rss/search?q=borsa+istanbul+bist100+hisse+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=merkez+bankası+faiz+enflasyon+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=dolar+euro+kur+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=altın+ons+fiyat+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=kripto+bitcoin+ethereum+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=fed+ecb+para+politikası+when:6h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Dünya": [
        "https://www.aa.com.tr/tr/rss/default?cat=dunya",
        "https://www.cnnturk.com/feed/rss/all/news",
        # Uluslararası
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://feeds.bbci.co.uk/news/business/rss.xml",
        # Google News
        "https://news.google.com/rss/headlines/section/topic/WORLD?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=avrupa+abd+rusya+çin+when:3h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=nato+g7+bm+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=ukrayna+orta+doğu+when:3h&hl=tr&gl=TR&ceid=TR:tr",
    ],
    "Teknoloji": [
        # Google News TR
        "https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=yapay+zeka+ai+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=teknoloji+yazılım+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=siber+güvenlik+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        # Google News EN
        "https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=en&gl=US&ceid=US:en",
        # Tech medya (çalışan)
        "https://feeds.arstechnica.com/arstechnica/index",
        "https://www.wired.com/feed/rss",
        # Türkçe tech
        "https://news.google.com/rss/search?q=openai+google+microsoft+apple+when:6h&hl=tr&gl=TR&ceid=TR:tr",
        "https://news.google.com/rss/search?q=uzay+nasa+spacex+when:6h&hl=tr&gl=TR&ceid=TR:tr",
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

    # Kronik bozulan kaynaklardan kalan eski item'leri her sync'te temizle.
    # Bu kaynaklar artık fetch edilmiyor; DB'de duran kayıtları da çöpe atalım.
    _BLACKLISTED_DOMAINS = (
        "sozcu.com.tr",
        "cumhuriyet.com.tr",
        "ntv.com.tr",
        "dunya.com",
        "ekonomim.com",
        "techcrunch.com",
        "theverge.com",
    )
    try:
        from sqlalchemy import or_
        conds = []
        for dom in _BLACKLISTED_DOMAINS:
            pat = f"%{dom}%"
            conds.append(NewsIntelligenceItem.url.like(pat))
            conds.append(NewsIntelligenceItem.source_url.like(pat))
        deleted = db.query(NewsIntelligenceItem).filter(or_(*conds)).delete(synchronize_session=False)
        if deleted:
            db.commit()
            logger.info("News intelligence: %d eski item kara liste kaynaklardan silindi.", deleted)
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        logger.warning("News intelligence kara liste temizlik hatası: %s", exc)

    _BASE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, application/atom+xml, */*;q=0.8",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.7,en;q=0.5",
        # br (Brotli) çıkarıldı — requests Brotli'yi otomatik açamıyor, gzip+deflate yeterli
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    _GNEWS_HEADERS = {
        **_BASE_HEADERS,
        # Google News için Referer eklemek gerekiyor aksi halde HTML redirect alıyoruz
        "Referer": "https://news.google.com/",
    }

    import time as _time

    def _fetch_rss(url: str) -> bytes | None:
        """RSS içeriğini çek; HTML veya parse edilemeyen içerik dönerse None."""
        hdrs = _GNEWS_HEADERS if "news.google.com" in url else _BASE_HEADERS
        try:
            resp = requests.get(url, headers=hdrs, timeout=15)
            if resp.status_code != 200:
                logger.error("RSS HTTP %d: %s", resp.status_code, url[:80])
                return None
            ct = resp.headers.get("Content-Type", "")
            # Google bazen HTML döndürüyor (bot tespiti / consent sayfası)
            if "html" in ct and "xml" not in ct:
                logger.warning("RSS kaynak HTML döndürdü (bot bloğu?): %s", url[:80])
                return None
            # Ham içeriği al; gzip magic bytes varsa manuel decompress et
            raw = resp.content
            if raw[:2] == b"\x1f\x8b":
                try:
                    raw = gzip.decompress(raw)
                except Exception as gz_exc:
                    logger.warning("RSS gzip decompress hatası (%s): %s", gz_exc, url[:80])
                    return None
            # Content-Type olmasa bile içerik kontrolü yap
            sniff = raw[:50].lstrip()
            if sniff.startswith(b"<!") or sniff.lower().startswith(b"<html"):
                logger.warning("RSS içerik HTML (Content-Type: %s): %s", ct[:40], url[:80])
                return None
            return raw
        except requests.RequestException as exc:
            logger.warning("RSS fetch hatası: %s — %s", url[:80], exc)
            return None

    # Her kategori için tüm kaynakları tara
    _gnews_count = 0  # Google News istekleri arasında throttle
    for category, rss_urls in CATEGORY_SOURCES.items():
        logger.info("Scanning category: %s with %d sources", category, len(rss_urls))
        for rss_url in rss_urls:
            try:
                # Google News rate limiting: istekler arasına küçük gecikme koy
                if "news.google.com" in rss_url:
                    if _gnews_count > 0:
                        _time.sleep(3.0)
                    _gnews_count += 1

                content = _fetch_rss(rss_url)
                if content is None:
                    continue

                try:
                    root = ET.fromstring(content)
                except ET.ParseError as exc:
                    logger.warning("RSS XML parse hatası (%s): %s", exc, rss_url[:80])
                    continue
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
                    title = (item.find("title").text or "") if item.find("title") is not None else ""
                    link = (item.find("link").text or "") if item.find("link") is not None else ""

                    pub_date_str = (item.find("pubDate").text or "") if item.find("pubDate") is not None else ""

                    source_el = item.find("source")
                    if source_el is not None:
                        source_name = (source_el.text or "").strip() or ch_title or "Bilinmiyor"
                        source_url  = source_el.get("url") or ch_link or None
                    else:
                        source_name = ch_title or "Bilinmiyor"
                        source_url  = ch_link or None

                    # Description için 3 kaynak: <description>, <content:encoded>, <summary>
                    # Bazı kaynaklar (TechCrunch, ArsTechnica, Wired vb.) description yerine
                    # content:encoded kullanır; bazı atom-style RSS'lerde summary olur.
                    description = ""
                    for _tag in (
                        "description",
                        "{http://purl.org/rss/1.0/modules/content/}encoded",
                        "{http://www.w3.org/2005/Atom}summary",
                        "summary",
                    ):
                        _el = item.find(_tag)
                        if _el is not None and _el.text:
                            description = _el.text or ""
                            if description.strip():
                                break
                    image_url = None

                    title = title.strip()
                    for suffix in [f" - {source_name}", f" | {source_name}", f" - {source_name.upper()}", f" | {source_name.upper()}"]:
                        if title.endswith(suffix):
                            title = title[:-len(suffix)].strip()

                    if len(title) < 25:
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

                # --- Adım 2: İngilizce kaynaklar için toplu çeviri ---
                _en_domains = ("techcrunch.com", "arstechnica.com", "theverge.com",
                               "wired.com", "bbc.co.uk", "nytimes.com")
                _needs_translation = (
                    (category == "Teknoloji" and any(d in rss_url for d in _en_domains))
                    or (category == "Dünya" and ("bbc.co.uk" in rss_url or "nytimes.com" in rss_url))
                    or (category == "Genel" and ("hl=en" in rss_url or "gl=US" in rss_url))
                )
                if _needs_translation and pending_items:
                    SEP = " ||| "
                    raw_titles = [p[0] for p in pending_items]
                    try:
                        from deep_translator import GoogleTranslator
                        joined = SEP.join(raw_titles)
                        translated_joined = GoogleTranslator(source="auto", target="tr").translate(joined)
                        translated_parts = [t.strip() for t in (translated_joined or "").split(SEP.strip())]
                        if len(translated_parts) == len(raw_titles):
                            pending_items = [
                                (translated_parts[i],) + pending_items[i][1:]
                                for i in range(len(pending_items))
                            ]
                    except Exception as te:
                        logger.warning("Toplu çeviri başarısız (%s), orijinal başlıklar kullanılacak: %s", rss_url[:40], te)

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
                logger.warning("RSS sync hatası (%s / %s): %s", category, rss_url[:60], e)
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
