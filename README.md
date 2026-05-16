# SEO Agent — Kapsamlı İzleme Panosu

Birden fazla siteye ait SEO sinyallerini, gerçek zamanlı trafik verilerini, hata loglarını ve meta tag denetimlerini tek panoda toplayan, otomatik mail uyarıları gönderen monolitik bir **SEO izleme sistemi**.

**Teknoloji yığını:** FastAPI · Jinja2 · HTMX · Tailwind CSS CDN · SQLAlchemy 2.0 · PostgreSQL (Railway) · APScheduler · Google Analytics Data API · Google Search Console API

---

## İçindekiler

1. [Siteler ve Profiller](#1-siteler-ve-profiller)
2. [Sekme / Sayfa Yapısı](#2-sekme--sayfa-yapısı)
3. [Veri Toplama Mimarisi](#3-veri-toplama-mimarisi)
4. [Otomatik Zamanlanmış İşler](#4-otomatik-zamanlanmış-işler)
5. [Mail Sistemi ve Preheader](#5-mail-sistemi-ve-preheader)
6. [GA4 Realtime Motoru](#6-ga4-realtime-motoru)
7. [SEO Denetim Sistemi](#7-seo-denetim-sistemi)
8. [Hata İzleme 404 / 5xx](#8-hata-izleme-404--5xx)
9. [Veritabanı Modelleri](#9-veritabanı-modelleri)
10. [API Kota Yönetimi](#10-api-kota-yönetimi)
11. [Ortam Değişkenleri](#11-ortam-değişkenleri)
12. [Dağıtım Railway](#12-dağıtım-railway)

---

## 1. Siteler ve Profiller

Sistem birden fazla siteyi eş zamanlı yönetir. Her sitenin birden fazla GA4 profili olabilir:

| Profil | Açıklama |
|--------|----------|
| `web` | Masaüstü / genel web property |
| `mweb` | Mobil web property |
| `android` | Android uygulama property |
| `ios` | iOS uygulama property |

**Harici siteler** (`ExternalSite` tablosu): Sisteme bağlı ancak tam izleme yapılmayan siteler. 404 raporları ve SEO denetiminden hariç tutulur, sadece bazı widget'larda gösterilir.

Aktif siteler: **d...** (ana site, birden fazla subdomain) ve **s...** (film/dizi platformu).

---

## 2. Sekme / Sayfa Yapısı

| Sekme | URL | Açıklama |
|-------|-----|----------|
| **Realtime** | `/realtime` | Canlı GA4 kullanıcı sayısı, haber alarmları, 404 spike |
| **GA4** | `/ga4` | Günlük/haftalık GA4 raporları, trend grafikleri |
| **Console** | `/search-console` | Google Search Console tıklama/gösterim verileri |
| **Alerts** | `/alerts` | SEO alarm kuralları ve tetiklenme geçmişi |
| **Speed** | `/pagespeed` | Lighthouse skorları (mobile/desktop) |
| **App** | `/app-intel` | Android/iOS uygulama metrikleri |
| **AI** | `/ai` | Yapay zeka destekli SEO önerileri |
| **News** | `/intelligence` | Çok kanallı haber takibi (RSS + Yahoo Finance API) |
| **Errors** | `/errors` | GA4 kaynaklı 404 URL listesi, periyot filtresi |
| **SEO** | `/seo-audit` | Meta tag denetimi, H1/H2/schema kontrolü |
| **Inbox** | `/inbox` | Gelen kutusu özeti, mail thread takibi |
| **Movie** | `/tmdb-upcoming` | TMDB film takvimi, ülke/kategori filtresi |
| **Settings** | `/settings` | Site, credential, alarm kural yönetimi |

---

## 3. Veri Toplama Mimarisi

### 3.1 GA4 Analytics Data API (Tarihsel)

- **Kütüphane:** `google-analytics-data` (`BetaAnalyticsDataClient`)
- **Auth:** Global service account JSON (Railway env) → `GA4_SERVICE_ACCOUNT_JSON`
- **Kullanım yerleri:**
  - GA4 sayfa raporları (90 günlük pencere)
  - 404 hata tespiti (`pagePathPlusQueryString` + `pageTitle` filtreleri)
  - SEO audit URL keşfi (`hostname` + `pagePath` → top 250 sayfa/profil)
  - Haber alarmı KPI özeti
- **Kota:** 200.000 token/gün; her `run_report()` çağrısı 1–10 token tüketir

### 3.2 GA4 Realtime API

- **Kütüphane:** `BetaAnalyticsDataClient.run_realtime_report()`
- **Kullanım yerleri:**
  - Canlı aktif kullanıcı sayısı (web/mweb/android/ios)
  - Haber trafik spike tespiti
  - 404 spike izleme (15 dakikalık pencere)
- **Kota:** Property başına yaklaşık 250 istek/gün; her 5 dk'da bir çalışır
- **Özel kural:** `pagePath` boyutu bazı property'lerde çalışmaz; `include_page_path=False` ve `compare_previous=False` ile güvenli mod uygulanır

### 3.3 Google Search Console API

- **Auth:** Site başına OAuth token (refresh token DB'de şifreli saklanır)
- **Veri:** Tıklama, gösterim, CTR, ortalama pozisyon (sorgu/sayfa bazlı)
- **Kota:** 100 istek/100 saniye, 1.200 istek/dakika (paylaşımlı)

### 3.4 PageSpeed Insights API

- **Auth:** API key (`PAGESPEED_API_KEY`)
- **Strateji:** `mobile` + `desktop` ayrı ayrı
- **Kota:** 400 istek/gün; `bypass_quota` flag ile manuel tetiklemede aşılabilir

### 3.5 RSS / Web Crawl

- **Haber kaynakları:** CNN Türk, AA, Sabah, Milliyet, Bloomberg HT, Dünya, Ekonomim, Google News, Yahoo Finance
- **Yahoo Finance:** RSS (`feeds.finance.yahoo.com`) önce denenir; başarısız olursa JSON API (`query1/query2.finance.yahoo.com/v1/finance/search`)
- **SEO Crawler:** `requests` ile doğrudan HTTP fetch; regex ile title/H1/H2/canonical/meta/OG/schema çıkarımı
- **TMDB:** `api.themoviedb.org/3` REST API; 6 saatlik stale-while-revalidate önbelleği, `_refresh_lock` ile çift-kontrol kilitleme

---

## 4. Otomatik Zamanlanmış İşler

Tüm işler **APScheduler** (`BackgroundScheduler`) ile yönetilir, tek process içinde çalışır. Timezone: `Europe/Istanbul`.

### Gece Toplu İşler

| Saat | Job ID | Açıklama |
|------|--------|----------|
| **01:00** | `news-intelligence-sync` | Haber kaynakları taranır, DB güncellenir |
| **01:30** | `daily-error-detection` | Tüm siteler için 404 sayfaları GA4'ten çekilir (1g/7g/14g/30g periyot) |
| **02:15** | `daily-meta-audit-snapshot` | UrlAuditRecord → MetaTagSnapshot; kritik değişiklik varsa alarm maili |
| **02:30** | `tmdb-cache-refresh` | TMDB film takvimi güncellenir (5 ay ilerisi) |
| **03:00** | `daily-seo-audit` | GA4 top 250 web + 250 mweb sayfası crawl edilir, UrlAuditRecord güncellenir |
| **03:30** | `daily-db-retention-cleanup` | 90+ günlük eski snapshot'lar ve loglar temizlenir |

### Gün İçi Periyodik İşler

| Saat | Job ID | Açıklama |
|------|--------|----------|
| **06:00, 09:00, 12:00, 15:00, 18:00, 20:00, 22:00** | `inbox-summary-on-hour` | Inbox özet maili |
| **07:30, 10:30, 13:30, 16:30** | `inbox-summary-on-half` | Inbox özet maili |
| **13:15, 23:15** | `error-report-email` | 404 hata özet maili (günlük 2 kez) |
| **Her ~5 dk** (07:01–23:51) | `ga4-realtime-check` | Realtime KPI + haber alarm + 404 spike kontrolü |
| **Her 10 dk** (07:01–23:51) | `news-intelligence-sync` | Haber senkronizasyonu |

### Özel Senaryolar

**Gece modu (00:00–07:00):** Realtime job çalışır ancak KPI alarmları kayıt edilmez ve mail gönderilmez. Sadece trend verisi toplanır.

**Coalesce=True:** Bir job kaçırılırsa (restart, overload) bir sonraki çalışmada sadece 1 kez tetiklenir.

**max_instances=1:** Aynı job eş zamanlı çalışamaz. Uzun süren SEO crawl veya GA4 çekimi bir sonraki zamanlanmış çalışmayı bloklayabilir.

---

## 5. Mail Sistemi ve Preheader

### Mail Tipleri

| Tip | Tetikleyici | Gmail Thread | Preheader Formatı |
|-----|-------------|--------------|-------------------|
| **Haber alarmı** | Yeni haber entry + trafik eşiği | Evet | `6.824 kul. +84% · Haber: 154` |
| **Sayfa alarmı** | Sayfa trafik değişimi | Evet | URL + kullanıcı sayısı |
| **KPI alarmı** | Site geneli eşik aşımı | Evet | Metrik adı + değer |
| **404 spike** | 10+ (uyarı) / 25+ (kritik) kul. 15 dk'da | Evet | `35 kul. 404'te (UYARI) · /url: 12` |
| **404 günlük rapor** | 13:15 ve 23:15 | Hayır | `200 URL · /emtia/...: 847` |
| **Meta tag regresyon** | noindex eklenmesi, canonical değişimi | Hayır | `3 kritik değişiklik · site+url` |
| **Inbox özeti** | 1.5 saatte bir (06:00–23:59) | Hayır | Thread başlıkları + özet |

### Preheader Tekniği

Tüm HTML mailler, email client preview alanında (Apple Watch, iPhone lock screen) görünmesi için **görünmez preheader span** içerir:

```html
<span style="display:none;font-size:1px;color:#fafafa;max-height:0;overflow:hidden;">
  6,824 kul. +84% · Kripto: 154 · Altın: 58      (80 karakter dolgu)
</span>
```

Bu sayede konu + preheader birlikte sayısal veriyi doğrudan ekranda gösterir, mail açılmadan bilgi iletilir.

### Mail Cooldown

Realtime alarmlar `RealtimeAlarmLog` tablosu üzerinden cooldown yönetir. Aynı kural için varsayılan 30 dakika bekleme süresi (`GA4_REALTIME_ALARM_EMAIL_COOLDOWN_MINUTES`).

### Toplu Mail (Batch)

Bir realtime job döngüsünde oluşan tüm alarmlar (site/sayfa/haber/404) tek bir mail olarak birleştirilir:

```
realtime_email_batch_begin()
    → KPI alarmları
    → Sayfa alarmları
    → Haber alarmları
    → 404 spike alarmları
realtime_email_batch_flush()  →  tek mail gönderilir
```

---

## 6. GA4 Realtime Motoru

### Kontrol Döngüsü (her ~5 dk)

```
1. run_all_sites_realtime_check()     → KPI alarmları (trafik eşikleri)
2. run_page_alarm_check_all_sites()   → Sayfa bazlı trafik alarmları
3. run_news_alarm_check_all_sites()   → Haber trafik alarmları
4. run_404_spike_check_all_sites()    → 404 spike tespiti
5. realtime_email_batch_flush()       → Toplu tek mail
```

### 404 Spike Tespiti

- Realtime API'den aktif kullanıcı çekilir (15 dk pencere)
- Başlık bazlı filtre: `"404"`, `"bulunamadı"`, `"not found"`, `"sayfa bulunamadı"` içeren sayfalar
- **Uyarı eşiği:** 10 kullanıcı (config: `GA4_REALTIME_404_WARNING_THRESHOLD`)
- **Kritik eşiği:** 25 kullanıcı (config: `GA4_REALTIME_404_CRITICAL_THRESHOLD`)
- Cooldown: 30 dk — aynı site için tekrar mail gönderilmez
- Log: `RealtimeAlarmLog` tablosu, `rule_id="rt_404_spike"` veya `"rt_404_critical"`

### Haber Alarm Mantığı

- En çok trafik alan sayfalar `_is_news_page()` ile filtrelenir
- `news_new_entry`: son pencerede yokken şimdi var → anlık mail
- `news_disappeared`: vardı, artık yok → mail
- `news_traffic_spike`: kullanıcı sayısı eşik aşımı
- Alarm body'sine genel trafik KPI banner'ı eklenir

### d... Özel Senaryosu

d... birden fazla subdomain'e sahip olduğundan mweb profili özellikle takip edilir. Haber alarmları `mweb` property'den gelir. `pagePath` boyutu bazı property'lerde `(not set)` döndürdüğünden bu property'lerde başlık bazlı eşleştirme kullanılır.

---

## 7. SEO Denetim Sistemi

### URL Kaynağı: GA4 Top Sayfalar

Her gece 03:00'da ve manuel "Tara" ile:

1. `web` property → son 7 gün, session'a göre top 250 sayfa
2. `mweb` property → son 7 gün, session'a göre top 250 sayfa
3. Boyutlar: `hostname` + `pagePath` → tam URL oluşturulur
4. Subdomain sayfaları doğru URL'e map edilir (örn. `kur.d.../serbest-piyasa/dolar`)
5. Deduplicate → max 500 benzersiz URL

**Akaryakıt fallback:** GA4 listesinde `akaryakit-fiyatlari` yoksa 8 şehir sayfası otomatik eklenir:
istanbul-avrupa, istanbul-anadolu, ankara, izmir, adana, bursa, antalya, gaziantep.

### Crawl Süreci

```
Her URL için:
requests.get(url, timeout=8s)
├── title, title_length
├── meta description, meta_description_length
├── H1 (varlık + sayı)
├── H2 (sayı)
├── canonical URL + final URL eşleşmesi
├── JSON-LD schema varlığı
├── noindex / robots meta
├── og:title + og:description
└── HTTP status code, content-type, final_url
```

Kaydedilir → anında DB'ye (progress gerçek zamanlıdır)
Tarama biter → `collected_at < tarama_başlangıcı` olan eski kayıtlar silinir

### Kontrol Edilen SEO Sinyalleri

| Sorun | Badge Rengi | Kural |
|-------|-------------|-------|
| Başlık yok | Kırmızı | `has_title = False` |
| Başlık kısa | Sarı | `title_length < 20` |
| Başlık uzun | Sarı | `title_length > 65` |
| Desc yok | Kırmızı | `has_meta_description = False` |
| Desc kısa | Sarı | `meta_description_length < 70` |
| Desc uzun | Sarı | `meta_description_length > 170` |
| Canonical yok | Kırmızı | `has_canonical = False` |
| Canonical hata | Kırmızı | `canonical_matches_final = False` |
| H1 yok | Kırmızı | `has_h1 = False` |
| Çoklu H1 | Sarı | `h1_count > 1` |
| H2 yok | Sarı | `h2_count = 0` |
| Schema yok | Gri | `has_schema = False` |
| OG eksik | Gri | `has_og_title = False OR has_og_description = False` |
| Noindex | Kırmızı | `is_noindex = True` |

### Değişiklik Takibi (MetaTagSnapshot)

Her gece 02:15'te `MetaTagSnapshot` tablosuna anlık görüntü alınır. Ardışık iki snapshot karşılaştırılır:

- `noindex False → True` → **Kritik alarm maili**
- `canonical_url` değişimi → **Kritik alarm maili**
- `title` değişimi → Değişiklikler sekmesinde gösterilir

Retention: 90 gün.

---

## 8. Hata İzleme 404 / 5xx

### Veri Kaynağı

GA4 Analytics Data API — iki ayrı sorgu:

1. **Ana sorgu:** `pagePathPlusQueryString` + `pageTitle` + `screenPageViews` + `totalUsers`
2. **Referrer sorgusu:** Aynı filtre + `pageReferrer` (referrer listesi max 20 saklanır)

Filtreler:
- Path'te `/404` geçen
- Title'da `"404"`, `"bulunamadı"`, `"not found"`, `"sayfa bulunamadı"` geçen

### Periyot Sistemi

| source değeri | Periyot | Çekilme saati |
|---------------|---------|---------------|
| `ga4_1d` | Son 1 gün | 01:30 |
| `ga4_7d` | Son 7 gün | 01:30 |
| `ga4_14d` | Son 14 gün | 01:30 |
| `ga4_30d` | Son 30 gün | 01:30 |

Filtre chip'leri sadece DB'den okur, GA4 çağrısı yapmaz. Çekim yapılmadan periyot değiştirilebilir.

### 404 Günlük Maili

13:15 ve 23:15'te tüm siteler için tek konsolide mail:
- Her site başına en fazla 15 URL
- URL başına max 3 referrer gösterilir
- Preheader: `200 URL · /en-cok-gorulen-url: 847`

---

## 9. Veritabanı Modelleri

| Tablo | Açıklama | Retention |
|-------|----------|-----------|
| `sites` | Site tanımları | Kalıcı |
| `site_credentials` | API credential'lar (Fernet şifreli) | Kalıcı |
| `external_sites` | Harici/partner site tanımları | Kalıcı |
| `collector_runs` | Veri toplama çalışma geçmişi | 90 gün |
| `url_audit_records` | SEO crawl sonuçları (title/desc/h1/h2/canonical/...) | Her taramada yenilenir |
| `meta_tag_snapshots` | Günlük meta tag anlık görüntüsü | 90 gün |
| `site_error_logs` | GA4 kaynaklı 404/5xx logları | Her çekimde yenilenir |
| `realtime_snapshots` | GA4 realtime KPI geçmişi | 90 gün |
| `realtime_alarm_logs` | Alarm tetiklenme geçmişi (cooldown için) | 90 gün |
| `alert_rules` | Kullanıcı tanımlı alarm kuralları | Kalıcı |
| `alert_events` | Alarm olayları | 90 gün |
| `ga4_report_snapshots` | GA4 tarihsel raporlar | 90 gün |
| `search_console_query_snapshots` | SC sorgu verileri | 90 gün |
| `crux_history_snapshots` | Chrome UX Report verileri | 90 gün |
| `url_inspection_snapshots` | Google URL Inspection sonuçları | 90 gün |
| `news_intelligence_items` | Haber akışı | 30 gün |
| `news_alarm_logs` | Haber alarm geçmişi | 30 gün |

---

## 10. API Kota Yönetimi

### GA4 Analytics Data API

- **Limit:** 200.000 token/gün (tüm property'ler toplamı)
- **Tahmini günlük tüketim:**
  - 404 detection: 4 periyot × 2 site = 8 sorgu
  - SEO audit: 2 sorgu (web + mweb)
  - GA4 raporları: ~10 sorgu
  - Toplam: ~25 sorgu/gün
- **Risk:** Çok düşük

### GA4 Realtime API

- **Limit:** Property başına yaklaşık 250 istek/gün (doğrulanmamış, Google tarafından belgelenmemiş)
- **Tahmini tüketim:** Her 5 dk × 2 site × 2-4 profil = 576+ istek/gün
- **Risk:** Yüksek — `GA4_REALTIME_INTERVAL_MINUTES` artırılarak azaltılabilir
- **Not:** 404 spike + haber alarmı aynı döngüde ek sorgu yapar

### PageSpeed Insights

- **Limit:** 400 istek/gün (API key ile)
- **Tüketim:** Mobile + desktop = 2 istek/site; cooldown sistemi tekrarı önler
- **Risk:** Orta — manuel tetikleme sıklaşırsa dikkat

### TMDB

- **Limit:** 50 istek/sn
- **Tüketim:** Sadece gece 02:30 refresh
- **Risk:** Yok

### SMTP

- **Tüketim:** Inbox × 18/gün + 404 × 2 + realtime alarm (değişken) = günde 25–60 mail
- **Risk:** Sağlayıcı limitine ve mail frekansına göre değerlendir

---

## 11. Ortam Değişkenleri

```env
# Veritabanı
DATABASE_URL=postgresql://...

# Google APIs
GA4_SERVICE_ACCOUNT_JSON={"type":"service_account",...}
SEARCH_CONSOLE_CLIENT_ID=...
SEARCH_CONSOLE_CLIENT_SECRET=...
PAGESPEED_API_KEY=...

# Mail (SMTP)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=...
SMTP_PASSWORD=...
MAIL_TO=...
GA4_REALTIME_EMAIL_ENABLED=true
GA4_REALTIME_NEWS_ALERT_EMAIL=...

# Şifreleme (Fernet)
ENCRYPTION_KEY=...

# Realtime Alarm Eşikleri
GA4_REALTIME_404_WARNING_THRESHOLD=10
GA4_REALTIME_404_CRITICAL_THRESHOLD=25
GA4_REALTIME_404_WINDOW_MINUTES=15
GA4_REALTIME_ALARM_EMAIL_COOLDOWN_MINUTES=30
GA4_REALTIME_INTERVAL_MINUTES=5

# TMDB
TMDB_API_KEY=...

# Özellik Bayrakları
LIVE_REFRESH_ENABLED=true
GA4_REALTIME_ENABLED=true
GA4_REALTIME_NEWS_ALERTS_ENABLED=true
GA4_REALTIME_404_ENABLED=true
AI_DAILY_BRIEF_ENABLED=false
```

---

## 12. Dağıtım Railway

**Platform:** Railway (tek dyno, tek process)
**Komut:** `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`

### Mimari Notlar

- Railway **tek process** çalıştırdığından APScheduler in-process çalışır
- `_seo_audit_progress` gibi in-memory state Railway'de güvendedir (tek replica)
- Birden fazla replica açılırsa scheduler çift çalışır — önlem: Railway scaling kapalı tutulmalı
- PostgreSQL bağlantı havuzu: SQLAlchemy connection pool (varsayılan 5 bağlantı)

### Nightly Job Zaman Çizelgesi

```
01:00  ── haber senkronizasyonu (RSS + Yahoo Finance)
01:30  ── GA4 404 çekimi (1g/7g/14g/30g × tüm siteler)
02:15  ── meta tag snapshot + regresyon alarm kontrolü
02:30  ── TMDB film takvimi yenileme (5 ay ilerisi)
03:00  ── SEO audit crawl (GA4 top 250+250 + akaryakıt fallback)
03:30  ── DB retention cleanup (90 gün üstü sil)
```

### Deployment Sonrası Kontrol

1. `/seo-audit` sayfası açılıyor mu? (200 OK)
2. `Tara` butonuyla manuel crawl çalışıyor mu?
3. Railway loglarında `SEO audit job tamamlandı` satırı var mı?
4. `/errors` sayfasında GA4 verisi geliyor mu?
5. Realtime sayfasında aktif kullanıcı görünüyor mu?
