# SEO Agent — Kapsamlı İzleme Panosu

> ⚠️ **Bu pano şifre ile korunmaktadır.** Tüm erişim yetkili kullanıcılarla sınırlıdır. Yazma/silme tetikleyen aksiyonlar için ikinci bir şifre adımı (action-auth) devrededir.

Birden fazla web sitesi ve mobil uygulamadan SEO sinyallerini, gerçek zamanlı trafik verilerini, hata loglarını, meta tag denetimlerini, mağaza analitiklerini ve crash raporlarını tek bir panoda toplayan; otomatik mail uyarıları gönderen monolitik bir **SEO + uygulama izleme sistemi**.

**Teknoloji yığını:**
FastAPI · Jinja2 · HTMX · Tailwind CSS (CDN) · SQLAlchemy 2.0 · PostgreSQL (Railway) · APScheduler · Google Analytics Data API · Google Search Console API · Google PageSpeed Insights · CrUX History API · BigQuery (Firebase Crashlytics export) · Gmail API (OAuth, send) · iTunes/Google Play scrapers · TMDB · Çoklu LLM (Groq · Gemini · OpenAI) · Plotly

---

## İçindekiler

1. [Şifre ve Erişim Sistemleri](#1-şifre-ve-erişim-sistemleri)
2. [Siteler, Profiller ve Mağaza Hesapları](#2-siteler-profiller-ve-mağaza-hesapları)
3. [Sekme / Sayfa Yapısı](#3-sekme--sayfa-yapısı)
4. [Anasayfa (Dashboard) Mimarisi](#4-anasayfa-dashboard-mimarisi)
5. [Veri Toplama Mimarisi](#5-veri-toplama-mimarisi)
6. [Otomatik Zamanlanmış İşler (Cron)](#6-otomatik-zamanlanmış-i̇şler-cron)
7. [Manuel Tetiklenen Süreçler](#7-manuel-tetiklenen-süreçler)
8. [Mail Sistemi ve Preheader](#8-mail-sistemi-ve-preheader)
9. [GA4 Realtime Motoru](#9-ga4-realtime-motoru)
10. [Search Console Alarm Sistemi](#10-search-console-alarm-sistemi)
11. [Threshold Alarm Sistemi](#11-threshold-alarm-sistemi)
12. [SEO Denetim Sistemi](#12-seo-denetim-sistemi)
13. [404 / 5xx Hata İzleme](#13-404--5xx-hata-i̇zleme)
14. [Firebase Crashlytics Entegrasyonu](#14-firebase-crashlytics-entegrasyonu)
15. [App Intelligence (Mağaza Analitiği)](#15-app-intelligence-mağaza-analitiği)
16. [Gmail Inbox + İki Aşamalı Auth](#16-gmail-inbox--i̇ki-aşamalı-auth)
17. [News Intelligence (Çok Kanallı Haber)](#17-news-intelligence-çok-kanallı-haber)
18. [AI Günlük Brief (LLM Destekli)](#18-ai-günlük-brief-llm-destekli)
19. [Data Explorer (PSI + CrUX)](#19-data-explorer-psi--crux)
20. [Veritabanı Modelleri](#20-veritabanı-modelleri)
21. [API Kota Yönetimi + Circuit Breaker](#21-api-kota-yönetimi--circuit-breaker)
22. [Tema Sistemi (Light / Dark / Midnight)](#22-tema-sistemi-light--dark--midnight)
23. [Mobil ve Responsive Davranış](#23-mobil-ve-responsive-davranış)
24. [Ortam Değişkenleri](#24-ortam-değişkenleri)
25. [Dağıtım (Railway)](#25-dağıtım-railway)
26. [Diagnostic / Debug Uçları](#26-diagnostic--debug-uçları)

---

## 1. Şifre ve Erişim Sistemleri

İki ayrı koruma katmanı vardır:

### 1.1 Admin Login (Pano Girişi)

- Tüm `/` rotaları admin login arkasındadır
- Login formu: `templates/admin_login.html` → POST `/admin-login`
- Session cookie (`HttpOnly`, `Secure`, `SameSite=Lax`)
- Şifre env değişkeninde tutulur (`ADMIN_PASSWORD`), bcrypt ile karşılaştırılır
- Logout: `/admin-logout` → session cookie siler

### 1.2 Inbox Action Auth (İkinci Şifre)

Inbox sayfasında mesaj listesi ve detay erişimi serbesttir, ancak **yazma tetikleyen tüm aksiyonlar** ek bir şifre adımı ister:

- `/api/inbox/action-auth` POST endpoint'i
- `/api/inbox/action-auth/status` GET (cookie geçerli mi kontrolü)
- Cookie: `seo_inbox_action_auth` (kısa TTL)
- Korunan endpoint'ler: read/unread, answered, delete, summarize, draft, send, template-send, sync-stream, oauth disconnect
- Serbest endpoint'ler: thread list, thread detail, refresh-bodies, reply-templates view, status
- Frontend gating: `_ensureInboxActionAuth()` her aksiyon butonuna eklenmiş; 403 (`inbox_action_auth_required`) dönerse modal açar

### 1.3 Rate Limiting

`slowapi` ile request başına IP rate limit:
- `/api/inbox/sync-stream`: 30/dakika
- `/api/inbox/threads/{id}/read`: 60/dakika
- `/api/inbox/threads/{id}/send`: 15/dakika
- Diğer endpoint'ler için varsayılan limit yok (admin login arkasında)

---

## 2. Siteler, Profiller ve Mağaza Hesapları

Sistem birden fazla siteyi eş zamanlı yönetir. Her sitenin birden fazla GA4 profili olabilir:

| Profil Anahtarı | Açıklama |
|----------------|----------|
| `web` | Masaüstü / genel web property |
| `mweb` | Mobil web property |
| `android` | Android uygulama property |
| `ios` | iOS uygulama property |

**Aktif siteler (dahili):** Tam izleme yapılan iki ana site — biri finansal veri/döviz platformu, diğeri görsel medya/sinema platformu. İsimler ve domain'ler env tarafında konfigure edilir (`sites` DB tablosunda saklanır).

**External Sites (`external_sites` tablosu):** Sisteme bağlı ancak tam izleme yapılmayan partner siteler. 404 raporları, SEO denetimi ve threshold alarmlarından **hariç tutulur**; yalnızca bazı widget'larda referans olarak gösterilir.

**Mağaza hesapları:** `APP_PRODUCTS` sözlüğü (`backend/services/app_intel.py`) iki mobil ürün için bundle id / package name / app store id / play store id eşleştirmesi tutar. Her ürünün ayrı bir Firebase projesi ve BigQuery export'u vardır.

---

## 3. Sekme / Sayfa Yapısı

Header navigation sıralı (soldan sağa):

| Sekme | URL | Açıklama |
|-------|-----|----------|
| **Logo** | `/` | Anasayfa (Günün Özeti) |
| **Tema pill** | — | Light ↔ Dark anahtarı |
| **alerts** | `/alerts` | İki sekmeli alarm paneli (Search Console + Threshold) |
| **speed** | `/data-explorer/{domain}` | PSI Lighthouse + CrUX 28 gün lab/saha |
| **app** | `/app?product=...&period=...` | Mağaza sıralama, puan, indirme istatistikleri |
| **firebase** | `/firebase` | Crashlytics özet, crash listesi, ANR, versiyonlar |
| **errors** | `/errors` | GA4 kaynaklı 404/5xx URL listesi (1g/7g/14g/30g filtre) |
| **seo** | `/seo-audit` | Meta tag denetimi (title/desc/H1/H2/canonical/schema/OG) |
| **gitlab** | `/boards` | GitLab issue/sprint takibi (vurgulu mavi pill) |
| **inbox** | `/inbox` | Gmail thread listesi, AI yanıt şablonları |
| **news** | `/intelligence` | Çok kanallı haber paneli (kategori sekmeli) |
| **movie** | `/tmdb-upcoming` | TMDB film takvimi (ülke/kategori filtre) |
| **ai** | `/ai` | AI günlük brief sayfası |
| **settings** | `/settings` | Site / credential / alarm kural yönetimi |

Header sıralaması mobile/desktop arasında değişmez; header pill'leri yatay kaydırmalıdır (`overflow-x-auto`, `scrollbar-hide`).

Logo ile sekmeler arasında **tema pill** (light/dark butonları) konumlanır; tema seçimi `localStorage` (`seo-theme`) ile kalıcıdır.

---

## 4. Anasayfa (Dashboard) Mimarisi

Anasayfa iki sütunlu **split-screen** layout kullanır — sol komple finansal/döviz platformuna, sağ komple görsel/sinema platformuna ayrılmıştır. Aralarında lg breakpoint ve üstünde dikey gradient divider çizgi vardır.

### 4.1 Sütun Başlıkları

- Sol: mavi noktalı `finansal site adı` etiketi (ring-1 blue-tonlu çerçeve)
- Sağ: pembe noktalı `görsel site adı` etiketi (ring-1 bordo-tonlu çerçeve)
- Her iki pill'in dolgusu aynı (`bg-blue-50/60` light, `dark:bg-blue-950/30` dark) — sadece çerçeve ve noktanın rengi siteyi ayırt eder

### 4.2 Widget Satırları (Her satır pair: sol = site A, sağ = site B)

| Satır | Widget | Kaynak Endpoint | Polling |
|-------|--------|------------------|---------|
| 1 | Anlık Aktif Kullanıcı | `/api/home/realtime?site=...` | her 60s |
| 2 | GA4 · 7 Gün Session Değişimi | `/api/home/ga4-sessions?site=...` | load |
| 3 | Search Console · 7 Gün | `/api/home/sc-summary?site=...` | load |
| 4 | En Kritik Pozisyon Düşüşleri · 7g | `/api/home/position-drops?site=...` | load |
| 5 | Data Explorer · Core Web Vitals | `/api/home/data-explorer?site=...` | load |
| 6 | Dün · Kritik 404 URL'leri | `/api/home/top-404s?site=...` | load |
| 7 | SEO · Kritik Hatalar | `/api/home/seo-errors?site=...` | load |
| 8 (asimetrik) | iOS & Android Release (sol) / boş placeholder (sağ) | `/api/home/app-release` | load |

### 4.3 Site Filtresi (Backend)

Her `/api/home/*` endpoint'i `?site=doviz` veya `?site=sinemalar` query param'ı kabul eder. Backend'de:

- `_home_site_filter_ids(site)` → string'i set'e çevirir (örn. `doviz` → `{1}`)
- `_build_dashboard_data_explorer_summary(db, only_site_ids=...)` → tek site filtrelenebilir

Bu sayede tek endpoint hem komple liste hem tek-site filtreli yanıt döner; dashboard 16 ayrı çağrı yapar ama her biri kompakt veri döner.

### 4.4 Container Renkli Çerçeve

Her widget container'ına ince renkli ring uygulanır (Tailwind ring-1 utility'sinin CSS değişkeni override edilir):

```css
section[hx-get*="site=doviz"]      { --tw-ring-color: rgba(59, 130, 246, 0.55); }  /* mavi */
section[hx-get*="site=sinemalar"]  { --tw-ring-color: rgba(159, 18, 57, 0.60); }   /* bordo */
section[hx-get="/api/home/app-release"] { --tw-ring-color: rgba(59, 130, 246, 0.55); }
```

Dark modda daha sönük varyantlar kullanılır (`blue-400/40`, `rose-700/45`).

### 4.5 Header Bar

En üstte hero card:
- Sol: "Günün Özeti" başlığı + altyazı (Sol · site A | Sağ · site B)
- Sağ: "Uyarılar" ve "Realtime" hızlı erişim pill butonları

---

## 5. Veri Toplama Mimarisi

### 5.1 GA4 Analytics Data API (Tarihsel)

- **Kütüphane:** `google-analytics-data` (`BetaAnalyticsDataClient`)
- **Auth:** Global service account JSON (`GA4_SERVICE_ACCOUNT_JSON` env)
- **Kullanım yerleri:**
  - GA4 raporları (90 günlük pencere)
  - 404 hata tespiti (`pagePathPlusQueryString` + `pageTitle` filtreleri)
  - SEO audit URL keşfi (`hostname` + `pagePath` → top 250 sayfa/profil)
  - Haber alarmı KPI özeti
- **Kota:** Property başına 200.000 token/gün; her `run_report()` çağrısı 1–10 token tüketir
- **Format:** `Ga4ReportSnapshot` tablosuna JSON olarak saklanır (period_days bazlı)

### 5.2 GA4 Realtime API

- **Kütüphane:** `BetaAnalyticsDataClient.run_realtime_report()`
- **Kullanım yerleri:**
  - Canlı aktif kullanıcı sayısı (web/mweb/android/ios)
  - Haber trafik spike + peak-drop tespiti
  - 404 spike izleme (15 dakikalık pencere)
  - Page-level trafik alarmı
- **Kota:** Property başına ~250 istek/gün (Google tarafından dokümante edilmemiş tahmini)
- **Özel kural:** `pagePath` boyutu bazı property'lerde çalışmaz; `include_page_path=False` ve `compare_previous=False` ile güvenli mod uygulanır
- **Floor-safety:** Top-N listesinden düşen sayfalar için "0 kullanıcı" varsaymak yerine listenin alt sınırı (floor) baz alınır; false-positive "zirveden düştü" alarmlarını engeller

### 5.3 Google Search Console API

- **Auth:** Site başına OAuth token (refresh token DB'de Fernet ile şifreli saklanır)
- **Veri:** Tıklama, gösterim, CTR, ortalama pozisyon (sorgu / sayfa / cihaz bazlı)
- **Kota:** 100 istek/100 saniye, 1.200 istek/dakika (paylaşımlı)
- **Optimizasyon:**
  - Bulk insert (`db.execute(insert(Model), mappings)`) — row-by-row 12.500 INSERT yerine tek transaction
  - `batch_size = max_rows` — API pagination bypass (tek API çağrısı)
  - Parallel multi-site (`ThreadPoolExecutor(max_workers=4)`) — gece nightly job ve manuel refresh-all'da
- **Saklanan dönemler:** 1d, 7d, 30d, 90d (her birinin current + previous karşılaştırması)

### 5.4 PageSpeed Insights API

- **Auth:** API key (`PAGESPEED_API_KEY`)
- **Strateji:** `mobile` + `desktop` ayrı ayrı
- **Kota:** 25.000 istek/gün (API key ile, free tier)
- **Saklanan veri:** Lighthouse audit JSON tam dump (`pagespeed_payload_snapshots`) + parsed snapshot (`lighthouse_audit_records`)

### 5.5 CrUX History API

- **Kullanım:** Chrome User Experience Report — 28 günlük p75 verileri
- **Metrikler:** LCP, INP, CLS, FCP, TTFB (mobile + desktop ayrı)
- **Saklanan:** `crux_history_snapshots` — chart serileri (40+ haftalık tarih)

### 5.6 BigQuery (Firebase Crashlytics Export)

- **Dataset:** `firebase_crashlytics` (Firebase tarafından otomatik oluşturulur)
- **Tablo şablonu:** `{bundle_id_underscored}_{PLATFORM}` (örn. `com_X_ANDROID`, `com_X_IOS`)
- **Auth:** Platform başına ayrı service account JSON
  - `CRASHLYTICS_IOS_SERVICE_ACCOUNT_JSON`
  - `CRASHLYTICS_ANDROID_SERVICE_ACCOUNT_JSON`
- **Auto-discovery:** `_discover_table_id()` farklı naming convention'larda tablo arar (`{bundle}_PLATFORM`, `{bundle}_REALTIME_PLATFORM`, eski `{bundle}`)
- **Location detection:** `_get_dataset_location()` ile dataset bölgesi (US/EU) tespit edilir, tüm query'ler doğru location'a gönderilir
- **Circuit breaker:** Dataset boşsa 1 saat boyunca yeni query gönderilmez (BigQuery kotasını korur)
- **Query budget:** 200 MB/sorgu (dry-run ile tahmin); aşılırsa hata döner
- **Concurrency:** Eş zamanlı max 2 sorgu (`threading.Semaphore`)
- **Cache:** 4 saat in-memory (product + days + platform anahtar)

### 5.7 RSS / Web Crawl

- **Haber kaynakları:** ~30 RSS feed (yerel ekonomi, dünya haberleri, finans, teknoloji, sinema)
- **Yahoo Finance:** RSS önce denenir; başarısız olursa JSON API (`query1/query2.finance.yahoo.com/v1/finance/search`)
- **SEO Crawler:** `requests` ile direkt HTTP fetch; regex ile title/H1/H2/canonical/meta/OG/schema çıkarımı
- **Çeşitlilik algoritması:** News intelligence API'sinde greedy interleave — aynı kaynaktan iki haber ardışık gelmez

### 5.8 TMDB (Movie API)

- **Endpoint:** `api.themoviedb.org/3` REST
- **Auth:** API key (`TMDB_API_KEY`)
- **Cache:** 6 saatlik stale-while-revalidate, `_refresh_lock` ile çift-kontrol kilitleme
- **Veri:** 5 ay ileri film takvimi (ülke + kategori filtre)

### 5.9 iTunes / Google Play Scraper

- **iTunes:** `itunes.apple.com/lookup` resmi REST API
- **Google Play:** `google_play_scraper` Python kütüphanesi (web parse)
- **Veri:** Puan, yorum sayısı, son versiyon, son güncelleme tarihi, sıra
- **Saklanan:** `AppIntelRawCache` (DB JSON) + disk cache

### 5.10 GitLab / Boards

- **API:** GitLab REST v4 (`/projects/{id}/issues`, `/groups/{id}/issues`)
- **Auth:** Personal access token (`GITLAB_TOKEN`)
- **Veri:** Sprint, milestone, label, assignee, state — kanban board halinde render
- **Auth proxy:** Tüm calls backend'den geçer (token frontend'e sızmaz)

---

## 6. Otomatik Zamanlanmış İşler (Cron)

Tüm işler **APScheduler** (`BackgroundScheduler`) ile yönetilir, tek process içinde çalışır. Timezone: `Europe/Istanbul`.

Ortak parametreler:
- `coalesce=True` — kaçırılan job bir sonraki çalışmada tek sefer tetiklenir
- `max_instances=1` — aynı job eş zamanlı çalışamaz
- `replace_existing=True` — startup'ta eski tanımlar üzerine yazılır
- `misfire_grace_time` — job'a göre 600-3600 saniye arası

### 6.1 Gece Toplu İşler

| Saat (TSİ) | Job ID | Açıklama |
|-----------|--------|----------|
| **01:00** | `news-intelligence-sync` | Tüm RSS kaynakları taranır, news_intelligence_items güncellenir |
| **01:30** | `daily-error-detection` | GA4 üzerinden 404 sayfaları tüm sitelerde 4 periyot (1g/7g/14g/30g) çekilir |
| **02:15** | `daily-meta-audit-snapshot` | UrlAuditRecord → MetaTagSnapshot; noindex/canonical değişimi varsa kritik alarm maili |
| **02:30** | `tmdb-cache-refresh` | TMDB film takvimi yenilenir (5 ay ilerisi) |
| **03:00** | `daily-seo-audit` | GA4 top 250 web + 250 mweb sayfası crawl edilir, UrlAuditRecord güncellenir |
| **03:30** | `daily-db-retention-cleanup` | 90 günü aşan snapshot ve loglar silinir |
| **05:00** | `daily-data-explorer-refresh` | PSI + CrUX otomatik yenileme (her aktif site için mobile + desktop) |
| **06:15** | `daily-crashlytics-refresh` | Firebase Crashlytics BigQuery'den tam veri çekimi (her ürün için) |

### 6.2 Sabah Search Console Refresh

| Saat | Job ID | Açıklama |
|------|--------|----------|
| **07:30** | `daily-sc-refresh-all` | Tüm aktif sitelerin SC verileri parallel (ThreadPoolExecutor) çekilir; SearchConsoleQuerySnapshot bulk-insert ile güncellenir |
| **08:00** | `alerts-scheduled-refresh` | Search Console alarm metrikleri yeniden hesaplanır + tetiklenenler mail edilir |

### 6.3 Gün İçi Periyodik İşler

| Saat | Job ID | Açıklama |
|------|--------|----------|
| **06:00, 09:00, 12:00, 15:00, 18:00, 20:00, 22:00** | `inbox-summary-on-hour` | Inbox özet maili (kapatılabilir: `INBOX_SUMMARY_EMAIL_ENABLED=true` ile aktifleşir) |
| **07:30, 10:30, 13:30, 16:30** | `inbox-summary-on-half` | Inbox özet maili (aynı flag) |
| **13:15, 23:15** | `error-report-email` | 404 hata özet maili (günde 2 kez tüm siteler için tek konsolide mail) |
| **Her ~5 dk** (07:01–23:51) | `ga4-realtime-check` | Realtime KPI + sayfa alarm + 404 spike kontrolü |
| **Her ~5 dk** (07:01–23:51) | `ga4-realtime-news-check` | Haber realtime alarm kontrolü (peak-drop, traffic-spike, new-entry, disappeared) |
| **Her 10 dk** (07:01–23:51) | `news-intelligence-sync` | Haber senkronizasyonu (gün içi mini-batch) |
| **Her 3 saat** | `rank-refresh-3h` | Mağaza kategori sıralaması güncellenir |

### 6.4 Scheduled Refresh Monitor

`scheduled-refresh-monitor` job'u her saat başı (`hour='*'`) çalışır:
- Beklenen zaman pencerelerinde başarılı çalışma kaydı (`CollectorRun.status='success'`) yoksa alarm maili
- `notification_key` ile aynı gün için tek mail (duplikasyon önleme)
- Kontrol edilen sistemler: PageSpeed, Crawler, SC, GA4, CrUX, URL Inspection, Alerts

### 6.5 Özel Senaryolar

**Gece modu (00:00–07:00):** Realtime job çalışır ancak KPI alarmları kayıt edilmez ve mail gönderilmez. Sadece trend verisi (`RealtimeSnapshot`) toplanır.

**Email manual triggers only:** `EMAIL_MANUAL_TRIGGERS_ONLY=true` set edilirse tüm scheduled mail'ler bastırılır, sadece manuel tetikleme mail gönderir.

**AI Daily Brief:** Varsayılan `False`. Açılırsa `ai-daily-brief-scheduled` job'u 09:00 (TR) çalışır; LLM ile günlük SEO brief oluşturur ve mail eder (`ai_daily_brief_send_email=true` ise).

---

## 7. Manuel Tetiklenen Süreçler

### 7.1 Search Console

- `/search-console` → tek site refresh: site kartında "Yenile" butonu (ileri optimize edilmiş bulk-insert path)
- "Tümünü Yenile": üst toolbar'dan tüm aktif sitelerin parallel refresh + mail

### 7.2 Alerts

- `/alerts` → "Yenile" butonu: tüm sitelerin SC alarm metriklerini yeniden hesaplar + threshold cooldown'larını sıfırlar
- Search Console sekmesinde: tarih dönem filtresi (1g/7g) + tip chip filtresi (CTR, Pozisyon, Impression, Schema, vb.)
- Threshold sekmesinde: tip filtresi (Tümü / GA4 Realtime / 404 Hata)

### 7.3 Data Explorer

- `/data-explorer/{domain}` → "Verileri yenile" butonu: PSI + CrUX paralel refresh
- Progress bar ile gerçek zamanlı durum

### 7.4 Firebase Crashlytics

- `/firebase` → "Manuel Yenile" butonu
- Circuit breaker'ı sıfırlar (önceki "dataset boş" cache'i temizler)
- Background worker'da `ThreadPoolExecutor` ile 8 ayrı query (2 platform × 4 sorgu tipi: summary, top issues, versions, ANR)
- 1s interval polling ile UI'a progress bilgisi geçer

### 7.5 SEO Audit

- `/seo-audit` → "Tara" butonu: GA4 top 250 + mweb 250 sayfası crawl başlatır
- Anlık progress: kaç URL tarandı / toplam, geçen süre, tamamlanan/sorunlu sayısı
- DB'ye anlık yazılır (kullanıcı kapanırsa ilerleme korunur)

### 7.6 News Intelligence

- `/intelligence` → "Güncelle" butonu: tüm RSS kaynaklarını anlık sync (~12 sn) + sayfa otomatik yenilenir
- Tab değişimi otomatik fetch eder (kategori filtresi)
- Auto-refresh: aktif sekmede her 5 dakikada bir yeni haberler kontrol edilir (sadece since parametresi ile)

### 7.7 Inbox

- "Senkronize et" (NDJSON streaming) → action-auth ister; Gmail API'den gelen son thread'leri çekip DB ile birleştirir
- Mesaj detayı serbest, aksiyon butonları (Sil, Cevaplandı, AI özet/taslak, Gmail ile gönder) action-auth ister
- AI yanıt şablonları: Groq → Gemini → OpenAI sıralı fallback, 3 farklı tonda Türkçe taslak

### 7.8 App Intelligence

- `/app?product=...&period=...` → ürün ve dönem seçimi
- Manual refresh: cache invalidation ile yeni iTunes/Play scrape

### 7.9 AI Brief

- `/ai` → "Oluştur" butonu: seçilen LLM ile günlük brief üretir (Gemini varsayılan)
- Saklanır: `AiDailyBrief` tablosunda

---

## 8. Mail Sistemi ve Preheader

### 8.1 Mail Tipleri

| Tip | Tetikleyici | Gmail Thread | Preheader Formatı |
|-----|-------------|--------------|-------------------|
| **Haber alarmı** | Yeni haber entry + trafik eşiği veya peak-drop | Evet (site bazlı thread) | `N haber · başlık · değişim` |
| **Sayfa alarmı** | Sayfa trafik değişimi (page_traffic_drop/spike/new_entry/disappeared) | Evet | URL + kullanıcı sayısı |
| **KPI alarmı** | Site geneli eşik aşımı (genel realtime) | Evet | Metrik adı + değer |
| **404 spike** | 10+ (uyarı) / 25+ (kritik) kul. 15 dk'da | Evet | `35 kul. 404'te · /url: 12` |
| **404 günlük rapor** | 13:15 ve 23:15 | Hayır | `200 URL · /en-cok-gorulen: 847` |
| **Meta tag regresyon** | noindex eklenmesi, canonical değişimi | Hayır | `3 kritik değişiklik · site+url` |
| **Inbox özeti** | 1.5 saatte bir (yalnızca env aktifse) | Hayır | Thread başlıkları + özet |
| **SC alarm refresh** | 08:00 cron veya manuel "Yenile" | Hayır | Konsolide site listesi |
| **Crawler raporu** | Manual veya scheduled crawler run | Hayır | Tek mail tüm siteler için |
| **Scheduled missed-run** | Beklenen zaman penceresinde başarılı run yoksa | Hayır | Sistem adı + etkilenen site sayısı |
| **AI Daily Brief** | 09:00 (eğer aktif) | Hayır | Günlük öne çıkan metrikler |

### 8.2 Preheader Tekniği

Tüm HTML mailler email client preview alanında (Apple Watch, iPhone lock screen, Gmail liste önizlemesi) görünmesi için **görünmez preheader span** içerir:

```html
<span style="display:none;font-size:1px;color:#fafafa;max-height:0;overflow:hidden;">
  6.824 kul. +84% · Kripto: 154 · Altın: 58      (80 karakter dolgu)
</span>
```

Bu sayede konu + preheader birlikte sayısal veriyi doğrudan ekranda gösterir, mail açılmadan bilgi iletilir.

### 8.3 Mail Cooldown

Realtime alarmlar `RealtimeAlarmLog` tablosu üzerinden cooldown yönetir. Aynı kural için varsayılan 30 dakika bekleme süresi (`GA4_REALTIME_ALARM_EMAIL_COOLDOWN_MINUTES`).

`_split_alarms_by_sentiment()` negatifleri (drop/disappeared) ve pozitifleri (spike/new_entry) ayrı ayrı cooldown'lar — negatif alarmlar pozitiflerle bastırılmaz.

### 8.4 Toplu Mail (Batch)

Bir realtime job döngüsünde oluşan tüm alarmlar tek bir mail olarak birleştirilir:

```
realtime_email_batch_begin()
    → KPI alarmları
    → Sayfa alarmları
    → Haber alarmları
    → 404 spike alarmları
realtime_email_batch_flush()  →  tek mail gönderilir
```

### 8.5 Hyperlink Sürdürülebilirliği

Mail body'sindeki tüm başlıklar (`_html_news_alarm_body`, `_html_page_alarm_body`) **tıklanabilir `<a>` tag** ile sarılır. Başlığın altında ince ground çizgi (text-decoration border-bottom) ile vurgu. URL `_alarm_row_public_url(domain, "news:" + page)` veya `("page:" + page)` ile inşa edilir.

### 8.6 Email Allow List

`email_allows_trigger_source(trigger_source)` — manuel ve system kaynaklarını ayırt eder; `EMAIL_MANUAL_TRIGGERS_ONLY=true` ile sadece manuel tetiklemeler mail gönderir.

---

## 9. GA4 Realtime Motoru

### 9.1 Kontrol Döngüsü (her ~5 dk)

```
1. run_all_sites_realtime_check()     → KPI alarmları (trafik eşikleri)
2. run_page_alarm_check_all_sites()   → Sayfa bazlı trafik alarmları
3. run_news_alarm_check_all_sites()   → Haber trafik alarmları (peak-drop, spike, vb.)
4. run_404_spike_check_all_sites()    → 404 spike tespiti
5. realtime_email_batch_flush()       → Toplu tek mail
```

### 9.2 404 Spike Tespiti

- Realtime API'den aktif kullanıcı çekilir (15 dk pencere)
- Başlık bazlı filtre: `"404"`, `"bulunamadı"`, `"not found"`, `"sayfa bulunamadı"` içeren sayfalar
- **Uyarı eşiği:** 10 kullanıcı (`GA4_REALTIME_404_WARNING_THRESHOLD`)
- **Kritik eşiği:** 25 kullanıcı (`GA4_REALTIME_404_CRITICAL_THRESHOLD`)
- Cooldown: 30 dk
- Log: `RealtimeAlarmLog` tablosu, `rule_id="rt_404_spike"` veya `"rt_404_critical"`

### 9.3 Haber Alarm Kuralları

| Kural | Tetik | Mail Tonu |
|-------|-------|-----------|
| `news_new_entry` | Yeni haber listede yokken şimdi var, eşiği aşmış | Pozitif (yeşil) |
| `news_traffic_spike` | Mevcut haberin kullanıcısı eşik üstü arttı | Pozitif |
| `news_traffic_drop` | Trafik %X üstü düştü (her iki snapshot'ta da var) | Negatif (kırmızı) |
| `news_peak_drop` | Son 90 dk içindeki peak'ten ciddi düştü | Negatif |
| `news_disappeared` | Önceki listede vardı, şimdi yok | Negatif |

### 9.4 Floor-Safety (False Positive Önleme)

Top-N listesi (varsayılan 100) ile çalışıldığından, liste dışına düşen sayfalar için "0 kullanıcı" varsaymak yanıltıcı olabilir. `evaluate_news_alarms` ve `evaluate_page_alarms`'da:

```
floor_users = min(activeUsers for sayfalar in curr_map)

Sayfa prev_map'te var ama curr_map'te yoksa:
  - floor <= 2 → güvenle 0 varsay (top listenin altı zaten düşük) → alarm ver
  - floor > 2 ve prev/peak >= floor * 4 → floor-1 varsay → drop hâlâ belirgin → alarm
  - aksi halde → sayfa floor düzeyinde kullanıcı sahibi olabilir → ALARM YOK
```

Bu mantık sayesinde "23 → 0 (-100%)" gibi yanıltıcı mailler durduruldu; sadece gerçek/sağlam düşüşlerde alarm gönderiliyor.

### 9.5 Page Alarm Kuralları

| Kural | Tetik |
|-------|-------|
| `page_traffic_drop` | Sayfa trafiği %X üstü düştü |
| `page_traffic_spike` | Sayfa trafiği %Y üstü arttı |
| `page_new_entry` | Listeye yeni giren sayfa |
| `page_disappeared` | Listeden çıkan sayfa (floor-safety ile) |

### 9.6 Domain-Bazlı Threshold Override

`_realtime_rules_threshold_pct_for_domain()` — site domain'ine göre özel eşik değerleri uygulanır. Yüksek hacimli sitelerde eşikler daha sıkı tutulur.

---

## 10. Search Console Alarm Sistemi

`/alerts` sayfasında **Search Console** sekmesi `alert_engine.py`'dan beslenir.

### 10.1 Alarm Kategorileri

- CTR
- Pozisyon
- Impression
- Schema (markup bulunamadı)
- Sitemap (sitemap.xml bulunamadı)
- PageSpeed Mobile/Desktop
- Robots.txt erişilemiyor
- Canonical
- Kırık link
- Redirect zinciri

### 10.2 Tetik Mekanizması

- Site başına `AlertRule` kayıtları (kullanıcı tanımlı)
- Cron (08:00) + manuel refresh ile değerlendirilir
- Tetiklenenler `AlertEvent` tablosuna logla; sıralı 1g/7g/aylık karşılaştırma yapılır

### 10.3 Alert Card Görsel

Her alarm kartında:
- Sol şerit (doviz mavi / sinema bordo / partner gri)
- Tip badge (renkli: CTR rose, Pozisyon sky, Impression amber, vb.)
- Sorgu metni + delta metin
- Sağ: domain + zaman + mail-gönderildi ✓ tick

### 10.4 Alert Refresh Progress

`/alerts/refresh` endpoint'i SSE benzeri progress yayını yapar; `#alerts-progress-panel` UI'da gerçek zamanlı yüzde + adım açıklaması.

---

## 11. Threshold Alarm Sistemi

`/alerts` sayfasında **Threshold** sekmesi cemevecen@... adresine giden eşik bazlı uyarıları paneller.

### 11.1 Veri Kaynakları

| Tip | Tablo | Veri |
|-----|-------|------|
| **GA4 Realtime** | `realtime_alarm_logs` | rule_id, metric, severity, current/previous, change_pct, message, triggered_at |
| **404 Hata** | `site_error_logs` (status_code=404) | url, hit_count, source, error_type, last_seen |

### 11.2 Kart Görseli

GA4 Realtime kartı:
- Tip badge: "Realtime" (yeşil)
- Sol şerit: domain rengine göre (doviz mavi / sinema bordo)
- Başlık: message'dan ayıklanmış title — em-dash öncesi kısım (örn. "Top Gun")
- Tıklanabilir hyperlink → `_alarm_row_public_url(domain, metric)` ile inşa edilir
- Altı: `prev → cur` formatlı tabular sayı + change % chip + metric path mono font

404 kartı:
- Tip badge: "404" (rose)
- URL clickable (font-mono, decoration underline)
- Hit count chip: ≥20 rose, ≥5 orange, altı slate

### 11.3 Üst Sayaç Badge

Her sekme adının yanında toplam kayıt sayısı (Search Console: `recent_alerts|length`, Threshold: `realtime + 404 toplamı`).

### 11.4 Tip Filtresi

Threshold içinde alt-tip chip'leri:
- Tümü
- GA4 Realtime
- 404 Hata

JS ile client-side filtering (`data-threshold-kind` attribute).

---

## 12. SEO Denetim Sistemi

### 12.1 URL Kaynağı: GA4 Top Sayfalar

Her gece 03:00'da ve manuel "Tara" ile:

1. `web` property → son 7 gün, session'a göre top 250 sayfa
2. `mweb` property → son 7 gün, top 250 sayfa
3. Boyutlar: `hostname` + `pagePath` → tam URL oluşturulur
4. Subdomain sayfaları doğru URL'e map edilir (örn. `kur.X.com/serbest-piyasa/dolar`)
5. Deduplicate → max 500 benzersiz URL

**Akaryakıt fallback:** GA4 listesinde `akaryakit-fiyatlari` yoksa 8 şehir sayfası otomatik eklenir (istanbul-avrupa, istanbul-anadolu, ankara, izmir, adana, bursa, antalya, gaziantep).

### 12.2 Crawl Süreci

```
Her URL için:
requests.get(url, timeout=8s, headers={User-Agent:...})
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

### 12.3 Kontrol Edilen SEO Sinyalleri

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
| OG eksik | Gri | OG başlığı veya açıklaması yoksa |
| Noindex | Kırmızı | `is_noindex = True` |

### 12.4 Değişiklik Takibi (MetaTagSnapshot)

Her gece 02:15'te `MetaTagSnapshot` tablosuna anlık görüntü alınır. Ardışık iki snapshot karşılaştırılır:

- `noindex False → True` → **Kritik alarm maili**
- `canonical_url` değişimi → **Kritik alarm maili**
- `title` değişimi → Değişiklikler sekmesinde gösterilir

Retention: 90 gün.

---

## 13. 404 / 5xx Hata İzleme

### 13.1 Veri Kaynağı

GA4 Analytics Data API — iki ayrı sorgu:

1. **Ana sorgu:** `pagePathPlusQueryString` + `pageTitle` + `screenPageViews` + `totalUsers`
2. **Referrer sorgusu:** Aynı filtre + `pageReferrer` (referrer listesi max 20 saklanır)

Filtreler:
- Path'te `/404` geçen
- Title'da `"404"`, `"bulunamadı"`, `"not found"`, `"sayfa bulunamadı"` geçen

### 13.2 Periyot Sistemi

| source değeri | Periyot | Çekilme saati |
|---------------|---------|---------------|
| `ga4_1d` | Son 1 gün | 01:30 |
| `ga4_7d` | Son 7 gün | 01:30 |
| `ga4_14d` | Son 14 gün | 01:30 |
| `ga4_30d` | Son 30 gün | 01:30 |

Filtre chip'leri sadece DB'den okur, GA4 çağrısı yapmaz. Periyot değiştirmek için yeni çekim gerekmez.

### 13.3 404 Günlük Mail

13:15 ve 23:15'te tüm siteler için tek konsolide mail:
- Her site başına en fazla 15 URL
- URL başına max 3 referrer gösterilir
- Preheader: `200 URL · /en-cok-gorulen-url: 847`
- Tüm URL'ler tıklanabilir (https://domain/path)

### 13.4 Dedupe (Anasayfa)

`/api/home/top-404s` endpoint'i shortened URL bazında dedup yapar; aynı kısaltmaya denk gelen birden fazla URL tek satırda gösterilir.

---

## 14. Firebase Crashlytics Entegrasyonu

### 14.1 Genel Yapı

- Her mobil ürün için ayrı Firebase project ve BigQuery dataset
- Service account JSON env değişkenlerinde tutulur
- Code GCP project ID'sini service account JSON'undan okur (`_effective_project`)
- Dataset location otomatik tespit edilir (US/EU) — query'ler doğru bölgeye gönderilir

### 14.2 Tablo Discovery

`_discover_table_id()` farklı naming convention'ları dener:

```
{bundle}_PLATFORM            (standart, örn. com_X_ANDROID)
{bundle}_REALTIME_PLATFORM    (realtime export)
{bundle}                     (legacy)
{bundle.lower()}_PLATFORM     (case varyasyonu)
```

Exact match bulunamazsa substring fuzzy match. Sonuç process-içi cache'lenir.

### 14.3 Query Tipleri

| Sekme | SQL Özeti | Kaynak Fonksiyon |
|-------|-----------|------------------|
| Özet | error_type bazlı COUNT + DISTINCT users | `query_summary` |
| Crash'ler | issue_id + issue_title + event_count + affected_users | `query_top_issues` |
| ANR | error_type='ANR' ile aynı | `query_top_anr` |
| Versiyonlar | app_version bazlı dağılım | `query_versions` |

### 14.4 Güvenlik ve Optimizasyon

- **Query budget:** 200 MB/sorgu (dry-run ile tahmin); aşılırsa hata mesajı
- **Concurrency:** Eş zamanlı max 2 sorgu (`threading.Semaphore(2)`)
- **Cache:** 4 saat in-memory
- **Circuit breaker:** Dataset boşsa 1 saat boyunca yeni query gönderilmez (`_circuit_trip()` / `_circuit_open()`)
- **Manuel refresh:** Circuit breaker + location cache'i sıfırlar

### 14.5 Diagnose Endpoint

`/api/app/crashlytics/diagnose?product=X` — production troubleshooting için JSON yanıt:

```json
{
  "product": "...",
  "platforms": {
    "android": {
      "service_account_email": "...",
      "effective_project_id": "...",
      "dataset_location": "US",
      "all_datasets_in_project": [...],
      "all_datasets_status": {
        "firebase_crashlytics": {
          "location": "US",
          "created": "...",
          "modified": "...",
          "table_count": 0,
          "tables": []
        },
        "firebase_performance": {...},
        "firebase_sessions": {...}
      },
      "dataset_exists": true,
      "dataset_tables": [...],
      "discovered_table": "com_X_ANDROID"
    },
    "ios": {...}
  }
}
```

Her adımda hata varsa ayrı alanlarda raporlanır (`list_datasets_error`, `dataset_check_error`, `list_tables_error`).

---

## 15. App Intelligence (Mağaza Analitiği)

### 15.1 Veri Kaynakları

- **iOS:** `itunes.apple.com/lookup` (resmi REST, API key gerekmez)
- **Android:** `google_play_scraper` (Python lib, web parse)

### 15.2 Saklanan Veri

- Versiyon, son güncelleme tarihi
- Yıldız puanı, toplam yorum sayısı
- Kategori sırası (her 3 saatte bir refresh)
- Yorumların son N'i (örn. son 50)
- Sıralama trend grafiği (`AppStoreRankSnapshot` historic)

### 15.3 Cache Mimarisi

3 katmanlı:
1. **In-memory cache** (process içi, dict)
2. **Disk cache** (JSON dosya, `_DISK_RAW_DIR`)
3. **DB cache** (`AppIntelRawCache` tablosu)

Okuma sırası: memory → disk → DB → fresh fetch
Yazma: tüm 3 katmana atomik

### 15.4 Anasayfa Widget

`/api/home/app-release` — yalnızca finansal/döviz ürünü için (görsel platform mobil uygulamaya sahip değil):
- iOS + Android iki kart
- Yeni versiyon güncellemesi varsa "YENİ" rozet
- Son 5 günden yeni release ise amber dot

---

## 16. Gmail Inbox + İki Aşamalı Auth

### 16.1 OAuth Akışı

- `/api/inbox/oauth/start?next=/inbox` → Google OAuth consent ekranı
- `/api/inbox/oauth/callback` → token alır, DB'ye Fernet ile şifreli yazar
- `/api/inbox/oauth` DELETE → bağlantıyı keser (action-auth ister)

### 16.2 Sync Stream

- `/api/inbox/sync-stream` POST → NDJSON streaming response
- Gmail API'den son thread'leri çeker (varsayılan 35 thread)
- Her thread için event yayını: pct, current, total, subject, snippet, messages_written
- Frontend `consumeInboxSyncStream()` ile parse edip progress bar günceller

### 16.3 Thread Yapısı

`SupportInboxThread`:
- `gmail_thread_id`
- `subject`
- `route_tag` (info/sinemalar/feedback/mixed — gönderen adresine göre)
- `gmail_unread`, `answered_flag`
- `ai_summary`, `ai_draft_reply`
- `snippet`, `message_preview`

`SupportInboxMessage`:
- `gmail_message_id`
- `from_addr`, `to_addr`, `subject`
- `body_text`, `body_preview`
- `is_outbound`
- `internal_ms`

### 16.4 AI Yanıt Şablonları

`/api/inbox/threads/{id}/reply-templates` POST:
- Sıralı LLM denemesi: Groq → Gemini → OpenAI
- `?provider=groq|gemini|openai` ile force seçim
- 3 farklı tonda Türkçe taslak (resmi/samimi/kısa)
- Focus message ID ile o iletiye özel yanıt
- LLM cevabı parse edilir (`label`, `body`)

### 16.5 Aksiyon Auth Gate

Görüntüleme serbest (list + detail + templates view + sync read), aksiyon korunan:
- read/unread → `_require_inbox_action_auth(request)`
- answered → `_require_inbox_action_auth(request)`
- delete → `_require_inbox_action_auth(request)`
- summarize (AI özet) → `_require_inbox_action_auth(request)`
- draft (AI taslak) → `_require_inbox_action_auth(request)`
- send (Gmail) → `_require_inbox_action_auth(request)`
- sync-stream → `_require_inbox_action_auth(request)`
- oauth disconnect → `_require_inbox_action_auth(request)`

Frontend her aksiyon butonuna `await _ensureInboxActionAuth()` eklenmiş; 403 dönerse modal açar, şifre alır, başarılıysa retry yapar.

### 16.6 Reply-To Detection

`getReplyRecipient()`: sistem mailleri (noreply, info, feedback, doviz.com) için mail body'sinden gerçek kullanıcı email'ini çıkartır (regex ile "Email: kullanici@..."). Doviz.com feedback formundan gelen mailler için tasarlanmış.

### 16.7 Inbox Özet Mail

`run_inbox_summary_job()` — varsayılan **kapalı**. `INBOX_SUMMARY_EMAIL_ENABLED=true` env ile aktifleşir. 06:00–23:59 arası 1.5 saatte bir okunmamış thread özetini mail eder.

---

## 17. News Intelligence (Çok Kanallı Haber)

### 17.1 Kaynak Kategorileri

| Kategori | RSS Sayısı | Örnek Kaynaklar |
|----------|-----------|-----------------|
| Türkiye | 8 | CNN Türk, AA, Sabah, Milliyet, Hürriyet, Habertürk, Google News |
| Genel | 8 | Ekonomim, Dünya, Bloomberg HT, NTV, CNN Türk, AA, Google News |
| İş Dünyası | 7 | Dünya, Ekonomim, Bloomberg HT, paraanaliz, Google News topic+search |
| Finans & Borsa | 8 | Bloomberg HT, Foreks, Ekonomim, paraanaliz, Google News (borsa, faiz, kur, kripto) |
| Dünya | 7 | AA, CNN Türk, Google News topic, BBC World, NYT World, search queries |
| Yahoo Finance | 5 | RSS for EUR/USD, Gold, S&P 500, BTC, NASDAQ + JSON API fallback |
| Bilim ve Teknoloji | 4 | Google News (Tech, Science topics + AI + space searches) |

### 17.2 Çeşitlilik Algoritması

Backend `/api/admin/news-intelligence/list` endpoint'inde:

```python
# Greedy picker: aynı kaynaktan iki haber ardışık olamaz
last_source = None
while True:
    # last_source dışındaki kaynakları al
    pickable = [k for k, v in buckets.items() if v and k != last_source]
    if not pickable:
        # Tüm diğer kaynaklar tükenmiş — kalan tek kaynaktan devam
        pickable = [k for k, v in buckets.items() if v]
        if not pickable: break
    best = max(pickable, key=lambda k: len(buckets[k]))  # en çok kalan
    interleaved.append(buckets[best].pop(0))
    last_source = best
```

Bu sayede `Bloomberg, Bloomberg, Bloomberg` gibi tek-kaynak kümeleri engellenir.

### 17.3 Filtre Sistemi

- **FILTER_KEYWORDS:** Etiketleme için (engelleme yapmaz)
- **EXCLUDE_KEYWORDS:** Negatif filtre — "canlı grafik", "teknik analiz", "günlük bülten", "ekonomi takvimi" gibi başlıkları eler
- **Title minimum length:** 50 karakter (çok kısa başlıklar atlanır)
- **Source name normalize:** Google News suffix'leri ("- Google News", "- En yeni") temizlenir

### 17.4 UI Davranışı

- Tab değişimi → offset reset → fetch
- Infinite scroll: 500px aşağı kalınca otomatik next page
- Auto-refresh: aktif tab'da her 5 dakikada bir `since` parametresi ile sadece yenileri çeker
- Manual sync button: backend job tetikler → 12 saniye bekler → listeyi sıfırdan yükler

---

## 18. AI Günlük Brief (LLM Destekli)

### 18.1 Genel Yapı

- Varsayılan **kapalı** (`AI_DAILY_BRIEF_ENABLED=false`, `AI_DAILY_BRIEF_SEND_EMAIL=false`)
- Aktifse 09:00 (TR) `ai-daily-brief-scheduled` job'u çalışır
- Her sitenin son 24 saat GA4 + SC + 404 + alert verisini toplar
- LLM ile günlük brief üretir (Gemini varsayılan, Groq ve OpenAI fallback)
- `AiDailyBrief` tablosuna kaydeder
- `ai_daily_brief_send_email=true` ise mail gönderir

### 18.2 Manual Üretim

`/ai` sayfasında "Oluştur" butonu → seçilen LLM ile anlık brief üretir.

### 18.3 Render

`parse_stored_brief_section_for_ui()` — markdown benzeri brief'i frontend için bölümlere ayırır.

---

## 19. Data Explorer (PSI + CrUX)

### 19.1 Yapı

- `/data-explorer/{domain}` → her domain için ayrı detay sayfa
- Üst kısımda Mobil ↔ Masaüstü saha (CrUX) ve lab (PSI) karşılaştırma
- Sağlık özeti kartı (Mobil↔Masaüstü delta + 7 günlük 404 hata özeti)
- CrUX 28 gün metrik kartları (LCP/INP/CLS/FCP/TTFB) — verdict + good_pct + chart
- PSI Mobile + Desktop tek-koşum lab sonuçları (skor halkaları, metrik tile'lar, audit bölümleri)
- Warehouse Özeti (collector run counts, snapshot sayıları) — dropdown içinde
- Crawler Denetimi (broken/redirect link audit) — dropdown içinde

### 19.2 Otomatik Yenileme

`daily-data-explorer-refresh` job'u her gün 05:00'da çalışır; tüm aktif siteler için PSI + CrUX fresh çekim yapar.

### 19.3 Dark Mode Matlaştırma

Sayfada `dex-mat-dark` CSS class'ı altında bright dark utility'ler (text-emerald-300/400, text-rose-300/400, text-violet-400, bg-emerald-500, vb.) muted hex değerlerine override edilir. Skor halkalarındaki inline SVG stroke renkleri `filter: saturate(0.55) brightness(0.82)` ile desature edilir. Light mode'a hiç dokunulmaz.

### 19.4 Anasayfa Özet Widget

`/api/home/data-explorer?site=X` — tek-site filtreli kompakt görünüm. Her metrik (LCP, INP, CLS, FCP, TTFB) için:
- Brand renkli (LCP violet, INP rose, CLS sky, FCP emerald, TTFB amber)
- Verdict chip (İYİ / ORTA / KÖTÜ)
- Büyük tabular sayı
- "iyi oran" yüzdesi + colored progress bar
- Period range (örn. 03-22→04-18)

Aynı dark mode matlaştırma `home-de-card` scope'unda uygulanır.

---

## 20. Veritabanı Modelleri

| Tablo | Açıklama | Retention |
|-------|----------|-----------|
| `sites` | Site tanımları | Kalıcı |
| `site_credentials` | API credential'lar (Fernet şifreli) | Kalıcı |
| `external_sites` | Harici/partner site tanımları | Kalıcı |
| `collector_runs` | Veri toplama çalışma geçmişi | 90 gün |
| `url_audit_records` | SEO crawl sonuçları | Her taramada yenilenir |
| `meta_tag_snapshots` | Günlük meta tag anlık görüntüsü | 90 gün |
| `site_error_logs` | GA4 kaynaklı 404/5xx logları | Her çekimde yenilenir |
| `realtime_snapshots` | GA4 realtime KPI geçmişi | 90 gün |
| `realtime_alarm_logs` | Alarm tetiklenme geçmişi (cooldown için) | 90 gün |
| `realtime_page_snapshots` | Sayfa bazlı realtime snapshot | 30 gün |
| `realtime_news_snapshots` | Haber bazlı realtime snapshot | 30 gün |
| `realtime_app_event_snapshots` | App event realtime snapshot | 30 gün |
| `alert_rules` | Kullanıcı tanımlı alarm kuralları | Kalıcı |
| `alert_events` | Alarm olayları | 90 gün |
| `ga4_report_snapshots` | GA4 tarihsel raporlar | 90 gün |
| `search_console_query_snapshots` | SC sorgu verileri | 90 gün |
| `crux_history_snapshots` | Chrome UX Report verileri | 90 gün |
| `url_inspection_snapshots` | Google URL Inspection sonuçları | 90 gün |
| `pagespeed_payload_snapshots` | Ham PSI cevapları | 90 gün |
| `lighthouse_audit_records` | Parsed Lighthouse audit | 90 gün |
| `app_intel_raw_cache` | App store/play store cache | Kalıcı |
| `app_store_rank_snapshots` | Mağaza kategori sırası | 30 gün |
| `news_intelligence_items` | Haber akışı | 30 gün |
| `news_alarm_logs` | Haber alarm geçmişi | 30 gün |
| `notification_delivery_log` | Mail gönderim audit | 90 gün |
| `support_inbox_threads` | Gmail thread cache | Kalıcı |
| `support_inbox_messages` | Gmail mesaj cache | Kalıcı |
| `ai_daily_briefs` | Üretilmiş AI brief'ler | 90 gün |
| `inbox_credentials` | Gmail OAuth tokens (Fernet şifreli) | Kalıcı |

---

## 21. API Kota Yönetimi + Circuit Breaker

### 21.1 GA4 Analytics Data API

- **Limit:** 200.000 token/gün (property başına)
- **Tahmini günlük tüketim:** ~25 sorgu/gün (4 periyot × 2 site = 8 + SEO 2 + GA4 raporları 10)
- **Risk:** Çok düşük

### 21.2 GA4 Realtime API

- **Limit:** ~250 istek/gün/property (resmi olmayan)
- **Tüketim:** 5 dk × 2 site × 2-4 profil = 576+/gün
- **Risk:** Yüksek
- **Kontrol:** `GA4_REALTIME_INTERVAL_MINUTES` artırılarak azaltılabilir

### 21.3 Search Console API

- **Limit:** 1.200 istek/dakika (paylaşımlı)
- **Tüketim:** Tek site refresh: 4 scope (1d, 7d, 30d, 90d) × 1 sorgu = 4 (parallel)
- **Risk:** Düşük

### 21.4 PageSpeed Insights

- **Limit:** 25.000 istek/gün (API key ile)
- **Tüketim:** Mobile + desktop = 2/site/refresh; cooldown sistemi tekrarı önler
- **Risk:** Düşük

### 21.5 BigQuery (Crashlytics)

- **Query budget:** 200 MB/sorgu (dry-run kontrolü)
- **Concurrency:** 2 eş zamanlı sorgu max
- **Circuit breaker:** Dataset boşsa 1 saat dondurma
- **Cache:** 4 saat in-memory

### 21.6 TMDB

- **Limit:** 50 istek/sn
- **Tüketim:** Gece 02:30 tek refresh + manuel anlık
- **Risk:** Yok

### 21.7 Gmail API

- **Limit:** 1 milyar quota unit/gün (paylaşımlı)
- **Tüketim:** Inbox sync stream: thread başına ~10 unit, send başına 100 unit
- **Risk:** Çok düşük

### 21.8 SMTP / Gmail Send

- **Tüketim:** Otomatik mailler (404 günlük 2, scheduled refresh, realtime alarmlar, kaçırılmış run, vb.) günde 25–60 mail
- **Risk:** Sağlayıcı limitine bağlı

### 21.9 LLM API'leri

- **Groq:** Yüksek ücretsiz tier
- **Gemini:** 60 istek/dakika ücretsiz tier
- **OpenAI:** Pay-as-you-go
- **Sıralı fallback:** Groq → Gemini → OpenAI

---

## 22. Tema Sistemi (Light / Dark / Midnight)

### 22.1 Üç Mod

- **Light:** `data-theme-value="light"` — Varsayılan, slate-based palette, açık zemin
- **Dim:** `data-theme-value="dim"` → `html.dark` (yalnız) — Yumuşak koyu mod, zinc-800 ağırlıklı
- **Midnight:** `data-theme-value="midnight"` → `html.dark` + `html.midnight` — True black, header'da extra koyu zemin

### 22.2 UI Switcher (3 Buton)

Header pill'inde 3 buton var:
- ☀ Light (güneş ikonu, amber)
- ◐ Dim (yarım daire, slate)
- ☾ Midnight (ay ikonu, indigo)

Pill animasyonu CSS transition ile slide, butonlar mobile'da 32px touch target (sm+ ekranda 28px). Toplam switcher genişliği: mobile 100px, desktop 88px.

**Pill rengi mod'a göre değişir:**
- Light → beyaz (`bg-white`)
- Dim → zinc-600 (`bg-zinc-600`)
- Midnight → zinc-900 + zinc-700 ring (`bg-zinc-900 ring-1 ring-zinc-700`)

### 22.3 LocalStorage + Geriye Uyumluluk

- Anahtar: `seo-theme` (`light` | `dim` | `midnight`)
- **Legacy migration:** Eski `'dark'` değeri varsa otomatik `'midnight'` olarak okunur (eski kullanıcıların deneyimi değişmez)
- Sistem tercihi (`prefers-color-scheme: dark`) varsayılan olarak **midnight** modunu uygular

### 22.4 Override Stilleri

Dark mode'da parlak Tailwind utility'lerini bastırmak için global override blokları:

- **`seo-dark-muted-accents`:** Dark mode'da `dark:text-violet/rose/sky/...-300/400/500` → `zinc-300/400/500` çevirir; `dark:bg-*` solid renkleri zinc-900 yapar (hariç tutulanlar: bg-*-9*, delta tonları)
- **`seo-dark-delta-tones`:** Delta/değişim göstergeleri için zinc bastırmasını geri alır, +/- okunabilir tutar (`dark:text-emerald-* → #10b981`)
- **`seo-midnight-overrides`:** Midnight modunda theme-switcher border + box-shadow kaldırır
- **`seo-inbox-dark`:** Inbox-spesifik override'lar

Sayfa-spesifik scope'lar (`dex-mat-dark` data explorer, `app-mat-dark` /app sayfası, `home-de-card` anasayfa CWV widget'ı): kendi sayfaları içinde parlak renkleri muted hex'lere çevirir.

### 22.5 SVG Filter

Plotly grafiklerinde + inline SVG stroke renklerinde dark mode için `filter: saturate(0.55) brightness(0.82)` uygulanır (sayfa-spesifik scope altında). Chart renkleri kodda kalmaya devam eder, sadece görsel render'da matlaşır.

---

## 23. Mobil ve Responsive Davranış

### 23.1 Breakpoint Stratejisi

Tailwind default'ları kullanılır:
- `sm`: 640px+
- `md`: 768px+
- `lg`: 1024px+
- `xl`: 1280px+
- `2xl`: 1536px+

Anasayfa dashboard split: `grid grid-cols-1 lg:grid-cols-2` — phone/tablet'te tek sütun (stacked), lg+ ekranda iki sütun.

### 23.2 Header

- Mobile: tek satır = `logo + tema pill`, nav alt satıra yatay scroll
- Desktop: tek satır = `logo · tema ─── nav (sağ yaslı)`
- CSS Grid + flex hybrid (`@media (min-width: 1024px) { display: grid }`)
- Nav pill'leri `flex-shrink-0` ile küçülmez, gerekirse yatay scroll

### 23.3 Tablolar

Geniş tablolar (`site_list`, `settings`, `alert_thresholds`) `overflow-x-auto` wrapper içinde — mobile'da yatay scroll. Sticky thead ile başlık görünür kalır.

### 23.4 Touch Target'lar (WCAG/Material)

Mobil ekranda tüm interaktif elemanlar **minimum 32px yükseklik**:

- Tema switcher butonları: mobilde 32px (`h-6 w-8`), sm+ ekranda 28px (`sm:w-7`)
- Header nav pill'leri (`.header-nav-link`): mobilde CSS media query ile `min-height: 32px` zorlanır (`@media (max-width: 639px)`)
- Alert thresholds tablo "Kaydet" butonu: `py-1.5` + `min-h-[32px]`
- Icon boyutları: mobile 16px (`h-4 w-4`), sm+ 14px (`sm:h-3.5 sm:w-3.5`)

### 23.5 Sayfa Bazlı Mobile İyileştirmeler

- `/data-explorer`: metric grid `grid-cols-2 sm:grid-cols-3 lg:grid-cols-5` — küçük ekranda 2 sütun, lg'de 5 sütun
- `/intelligence`: news cards `grid-cols-1 md:grid-cols-2 lg:grid-cols-3` — responsive grid
- `/alerts`: filter chip'leri `overflow-x-auto pb-1` ile yatay scrollable
- `/inbox`: list/detail panes `grid-cols-1 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]` — küçük ekranda stack

---

## 24. Ortam Değişkenleri

```env
# Veritabanı
DATABASE_URL=postgresql://...

# Şifreleme
ENCRYPTION_KEY=...
ADMIN_PASSWORD=...
INBOX_ACTION_PASSWORD=...

# Google APIs
GA4_SERVICE_ACCOUNT_JSON={"type":"service_account",...}
SEARCH_CONSOLE_CLIENT_ID=...
SEARCH_CONSOLE_CLIENT_SECRET=...
PAGESPEED_API_KEY=...

# Firebase Crashlytics (BigQuery)
CRASHLYTICS_IOS_SERVICE_ACCOUNT_JSON=...
CRASHLYTICS_ANDROID_SERVICE_ACCOUNT_JSON=...

# Mail (SMTP veya Gmail OAuth)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=...
SMTP_PASSWORD=...
MAIL_TO=...
OPERATIONS_EMAIL=...

# Realtime Eşikleri
GA4_REALTIME_404_WARNING_THRESHOLD=10
GA4_REALTIME_404_CRITICAL_THRESHOLD=25
GA4_REALTIME_404_WINDOW_MINUTES=15
GA4_REALTIME_ALARM_EMAIL_COOLDOWN_MINUTES=30
GA4_REALTIME_INTERVAL_MINUTES=5

# Mail/Feature Bayrakları
GA4_REALTIME_EMAIL_ENABLED=true
GA4_REALTIME_NEWS_ALERTS_ENABLED=true
GA4_REALTIME_NEWS_ALERT_EMAIL=true
GA4_REALTIME_404_ENABLED=true
INBOX_SUMMARY_EMAIL_ENABLED=false       # varsayılan KAPALI
AI_DAILY_BRIEF_ENABLED=false
AI_DAILY_BRIEF_SEND_EMAIL=false
EMAIL_MANUAL_TRIGGERS_ONLY=false

# Scheduler
SCHEDULED_REFRESH_ENABLED=true
ALERTS_SCHEDULED_REFRESH_ENABLED=true
SEARCH_CONSOLE_SCHEDULED_REFRESH_ENABLED=true
SCHEDULED_REFRESH_MONITOR_ENABLED=true
SCHEDULED_REFRESH_MONITOR_GRACE_MINUTES=30

# Search Console Optimizasyon
SEARCH_CONSOLE_ROW_BATCH_SIZE=2500
SEARCH_CONSOLE_MAX_ROWS=2500

# 3rd Party
TMDB_API_KEY=...
GITLAB_TOKEN=...
GROQ_API_KEY=...
GEMINI_API_KEY=...
OPENAI_API_KEY=...

# Gmail OAuth (Inbox)
GMAIL_CLIENT_ID=...
GMAIL_CLIENT_SECRET=...
GMAIL_REDIRECT_URI=...
```

---

## 25. Dağıtım (Railway)

**Platform:** Railway (tek dyno, tek process)
**Komut:** `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`

### 25.1 Mimari Notlar

- Railway **tek process** çalıştırdığından APScheduler in-process çalışır
- In-memory state (cache, circuit breaker, progress dict) Railway'de güvendedir (tek replica)
- Birden fazla replica açılırsa scheduler çift çalışır — Railway scaling kapalı tutulmalı
- PostgreSQL bağlantı havuzu: SQLAlchemy connection pool (varsayılan 5 bağlantı)

### 25.2 Nightly Job Zaman Çizelgesi

```
01:00  ── haber senkronizasyonu (RSS + Yahoo Finance)
01:30  ── GA4 404 çekimi (1g/7g/14g/30g × tüm siteler)
02:15  ── meta tag snapshot + regresyon alarm kontrolü
02:30  ── TMDB film takvimi yenileme
03:00  ── SEO audit crawl
03:30  ── DB retention cleanup
05:00  ── Data Explorer (PSI + CrUX) refresh
06:15  ── Firebase Crashlytics BigQuery refresh
07:30  ── Search Console refresh-all
08:00  ── Alerts scheduled refresh
09:00  ── AI Daily Brief (eğer aktif)
```

### 25.3 Deployment Sonrası Kontrol

1. `/` anasayfa açılıyor mu? (200 OK, dashboard widget'ları yükleniyor mu?)
2. `/alerts` sayfası iki sekmesiyle (Search Console + Threshold) açılıyor mu?
3. `/seo-audit` sayfası açılıyor mu? "Tara" çalışıyor mu?
4. Railway loglarında `Scheduler started with N jobs` satırı var mı?
5. `/errors` sayfasında GA4 verisi geliyor mu?
6. `/realtime` sayfasında aktif kullanıcı görünüyor mu?
7. `/firebase` sayfasında crash verisi var mı? (Yoksa diagnose endpoint kontrol)
8. Tema switcher çalışıyor mu? (light/dark arasında geçiş)

### 25.4 Health Check

`/health` endpoint'i basit JSON döner:
```json
{"status": "ok", "scheduler_jobs": 24}
```

---

## 26. Diagnostic / Debug Uçları

### 26.1 Firebase Crashlytics

```
GET /api/app/crashlytics/diagnose?product=X
```

Tam dump: SA email, project ID, dataset location, mevcut tablolar, discovered table.

### 26.2 Scheduler Status

`/admin/scheduler` (planlı) — aktif job'lar, son çalışma zamanı, next-run.

### 26.3 BigQuery Test

`/admin/test-bigquery-crashlytics?product=X&platform=android` — manuel BQ erişim testi (yetki + tablo kontrolü).

### 26.4 Mail Test

`/admin/test-mail` — SMTP veya Gmail API üzerinden test maili gönderir.

### 26.5 Cache Invalidation

`/admin/cache/clear?key=...` — process-içi cache'leri temizler (app_intel, crashlytics, news_intelligence).

### 26.6 Manuel Job Tetikleme

```
GET /api/admin/run-error-detection-now
GET /api/admin/run-seo-audit-now
GET /api/admin/run-news-intelligence-now
GET /api/admin/run-meta-audit-now
GET /api/admin/run-error-report-mail-now
GET /api/admin/run-inbox-summary-now
```

Her biri ilgili background job'u anlık tetikler; admin login arkasında.

---

## Lisans ve Notlar

Bu pano kapalı kaynak ve özel bir kurumsal projedir. Tüm hesap, API anahtarı, domain ve kullanıcı bilgileri env değişkenleri / DB üzerinden yönetilir; repository'de hardcoded hassas veri **YOKTUR**.

İletişim ve geliştirme süreci: shared GitLab board üzerinden yürütülür.

---

*Son güncelleme: 2026-05-17 · APScheduler 24 aktif job · Railway tek replica · PostgreSQL + Fernet şifreli credential*
