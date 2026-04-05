# SEO Agent

Google API'ları ve özel crawler ile sitelerin SEO performansını izleyen, uyarı üreten, raporlayan monolitik bir **SEO izleme panosu**.

**Teknoloji:** FastAPI · Jinja2 · HTMX · Tailwind CSS · SQLAlchemy · PostgreSQL/SQLite · Redis · APScheduler

---

## Sayfalar (Tab'lar)

### 1. Dashboard (`/`)
Ana sayfa. Tüm sitelerin özet kartlarını gösterir:
- Her site için PageSpeed skorları (mobile/desktop)
- Crawler bulguları (robots.txt, sitemap, schema, broken links)
- Son veri toplama zamanları
- Canlı Lighthouse ölçümü tetikleme

### 2. Search Console (`/search-console`)
Google Search Console entegrasyonu (OAuth ile bağlanır):
- Site kartları: tıklama, gösterim, CTR, ortalama pozisyon
- Dönem karşılaştırma: 1g / 7g / 30g
- Cihaz ayrımı: Mobil / Desktop
- Top 50 query tablosu: anahtar kelime bazlı değişim
- Trend grafikleri (Plotly.js)
- Toplu yenileme + gerçek zamanlı progress bar

### 3. GA4 (`/ga4`)
Google Analytics 4 entegrasyonu (Service Account):
- Oturum, kullanıcı, etkileşim oranı, sayfa görüntüleme
- Trafik kaynakları: Organic, Direct, Paid, Social, Referral, Email
- Sayfa bazlı trafik tablosu
- Dönem karşılaştırma

### 4. PageSpeed (`/data-explorer/{domain}`)
PageSpeed Insights + Lighthouse detaylı analizi:
- LCP, FCP, TTFB, CLS, INP metrikleri
- Lighthouse audit kayıtları (Performance, Accessibility, Best Practices, SEO)
- Mobile vs Desktop karşılaştırma
- Detaylı öneriler: sorun, etki, çözüm (TR+EN)
- Site Audit: sitemap URL taraması — title, H1, canonical, schema, meta robots
- URL Inspection: Google indexleme durumu

### 5. External (`/external`)
Search Console bağlantısı olmadan crawler ile harici site izleme:
- Domain girerek site ekleme
- Arka planda otomatik veri toplama
- Crawler profili: robots.txt, sitemap, schema, canonical, broken links

### 6. Alerts (`/alerts`)
13 hazır kurallı uyarı sistemi:
- PageSpeed skor düşüşü (mobile/desktop)
- robots.txt / sitemap / schema / canonical eksikliği
- Kırık link ve redirect zincirleri
- Search Console: pozisyon düşüşü, gösterim düşüşü, CTR düşüşü, düşen query sayısı
- E-posta bildirimi (SMTP) + 24 saat deduplikasyon
- Site bazlı eşik ayarları

### 7. Settings (`/settings`)
- Site yönetimi (ekle/sil/düzenle)
- API anahtarları ve OAuth yapılandırması
- Zamanlama ayarları
- Uyarı eşikleri
- SMTP yapılandırması

---

## Veri Toplayıcılar

| Toplayıcı | Kaynak | Toplanan Veri |
|-----------|--------|---------------|
| Search Console | Google SC API | Query, tıklama, gösterim, pozisyon |
| PageSpeed | PSI API | Lighthouse skorları, Web Vitals |
| GA4 | Analytics Data API | Oturum, kanal, sayfa |
| CrUX | CrUX History API | Core Web Vitals (gerçek kullanıcı) |
| Crawler | HTTP crawl | robots, sitemap, schema, link |
| Site Audit | Sitemap crawl | URL bazlı SEO sinyalleri |
| URL Inspection | SC Inspection API | İndeksleme durumu |

---

## Zamanlanmış Görevler

| Görev | Varsayılan Saat | İşlev |
|-------|----------------|-------|
| Alert yenileme | 01:00 | Tüm uyarıları değerlendir |
| Site yenileme | 02:00 | PageSpeed, Crawler, CrUX |
| SC yenileme | 03:00 | Search Console verileri |
| DB temizlik | 03:30 | Eski verileri otomatik sil |
| GA4 yenileme | 04:00 | Analytics verileri |
| Monitor | Her 5 dk | Kaçan job kontrolü |

Tüm saatler yapılandırılabilir (varsayılan: Europe/Istanbul).

---

## Veritabanı ve Retention

17 tablo, SQLAlchemy 2.0 ORM. Otomatik retention (her gece 03:30):

- **Snapshot tabloları** (SC, PageSpeed, GA4, CrUX, Lighthouse, URL Audit, URL Inspection) → her site için sadece son çekim kalır
- **CollectorRun** → 30 gün
- **AlertLog** → 60 gün
- **Metric** → 90 gün
- **NotificationDeliveryLog** → 30 gün

Admin endpoint'leri: `GET /admin/db-size`, `POST /admin/cleanup-sc-snapshots`

---

## Kota Yönetimi

| API | Günlük | Aylık | Cooldown |
|-----|--------|-------|----------|
| PageSpeed | 80 | 1.500 | 30 dk |
| Search Console | 80 | 1.500 | 6 saat |
| CrUX | — | — | 6 saat |
| URL Inspection | — | — | 6 saat |

---

## Kurulum

### Önkoşullar
- Python 3.10+ (3.12 önerilir)
- PostgreSQL veya SQLite
- Redis (opsiyonel, önbellekleme için)
- Git

### 1. Depoyu klonla

```bash
git clone https://github.com/<kullanici>/<repo>.git
cd seo-agent
```

### 2. Sanal ortam ve bağımlılıklar

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Ortam değişkenleri

```bash
cp .env.example .env
```

`.env` dosyasını düzenleyerek kendi değerlerinizi girin. **Asla** gerçek anahtarları repoya commit etmeyin.

Gerekli değişkenler:
- `DATABASE_URL` — Veritabanı bağlantı adresi
- `SECRET_KEY` — Oturum şifreleme anahtarı
- `GOOGLE_API_KEY` — PageSpeed/CrUX API anahtarı
- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` — Search Console OAuth
- `GA4_SERVICE_ACCOUNT_FILE` — GA4 service account JSON dosya yolu

### 4. Sunucuyu çalıştır

```bash
source .venv/bin/activate
python run_server.py
```

Varsayılan adres: `http://127.0.0.1:8012`

### Docker ile çalıştırma

```bash
cp .env.example .env
# .env dosyasını düzenle
docker compose up -d
```

PostgreSQL + Redis + App otomatik ayağa kalkar.

---

## Deploy (Railway)

Proje [Railway](https://railway.app) üzerinde deploy edilebilir:

1. GitHub reposunu Railway'e bağla
2. PostgreSQL ve Redis eklentilerini ekle
3. Ortam değişkenlerini Railway Variables'a gir
4. Her push'ta otomatik deploy

`railway.toml` yapılandırması hazırdır (Nixpacks builder).

---

## Güvenlik

| Konu | Uygulama |
|------|----------|
| Sırlar | `.env`, `keys/` repoya girmez (`.gitignore`) |
| Credential'lar | AES ile şifrelenerek veritabanında saklanır |
| OAuth | Redirect URI'ler uygulamanın dinlediği adresle eşleşmeli |
| IP kısıtlama | `ALLOWED_CLIENT_IPS` ile opsiyonel whitelist |
| Bağımlılıklar | Düzenli güvenlik güncellemesi önerilir |

---

## Proje Yapısı

```
seo-agent/
├── backend/
│   ├── main.py              # FastAPI uygulama (~6.000 satır)
│   ├── config.py             # Pydantic ayarlar
│   ├── models.py             # SQLAlchemy modelleri (17 tablo)
│   ├── database.py           # DB bağlantısı
│   ├── collectors/           # 7 veri toplayıcı
│   │   ├── search_console.py
│   │   ├── pagespeed.py
│   │   ├── ga4.py
│   │   ├── crawler.py
│   │   ├── site_audit.py
│   │   ├── crux_history.py
│   │   └── url_inspection.py
│   └── services/             # İş mantığı (~20 servis)
│       ├── alert_engine.py
│       ├── warehouse.py
│       ├── mailer.py
│       ├── quota_guard.py
│       ├── search_console_auth.py
│       └── ...
├── templates/                # Jinja2 şablonlar
│   ├── base.html
│   ├── dashboard.html
│   ├── search_console.html
│   ├── ga4.html
│   └── partials/             # 16 partial şablon
├── static/                   # Favicon, JS
├── docker-compose.yml
├── railway.toml
├── requirements.txt
└── .env.example
```

---

*Bu depo işletim sırları içermez; tüm hassas bilgiler `.env` ve `keys/` klasöründe tutulur.*
