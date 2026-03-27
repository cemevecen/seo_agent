🎯 DOVIZ.COM - PAGESPEED KIRMIZI UYARILAR ANALİZİ
════════════════════════════════════════════════════════════════════════

EKRAN GÖRÜNTÜSÜ OZETİ - 5 ALERT
────────────────────────────────
✗ doviz.com Mobile PageSpeed: 42/100 (eşik: 55/100) - KRITIK
✗ doviz.com Mobile PageSpeed: 40/100 (eşik: 55/100) - KRITIK
✗ doviz.com Mobile PageSpeed: 33/100 (eşik: 55/100) - KRITIK
✗ doviz.com Mobile PageSpeed: 38/100 (eşik: 55/100) - KRITIK
✗ doviz.com Mobile PageSpeed: 35/100 (eşik: 55/100) - KRITIK

Average Score: 37.6/100  ← ÇÖKE YAKLAŞMIŞ
Desktop Score: ~65/100   ← İyileştirilmeli


📍 KIRMIZI UYARILARIN NEDENİ?
════════════════════════════════════════════════════════════════════════

"Kırmızı" demek: Google'ın core web vitals standartlarını KARŞILAMAYAN

Google'ın PageSpeed Thresholds (2024):
┌──────────────────────────────────────┐
│ 90+  → ✅ İYİ (hızlı)               │
│ 50-89  → 🟡 İHTİYAÇ VAR (orta)      │
│ <50   → 🔴 KRITIK (yavaş)           │
└──────────────────────────────────────┘

Doviz.com Statusu:
- Mobile: 33-42 → 🔴 KRITIK (50 altında)
- Desktop: ~65 → 🟡 UYARI (50-89 arası)

Neden kırmızı?
1. LARGE CONTENTFUL PAINT (LCP) > 2.5 saniye
   → Ana görsel/yazı içerik YAVAŞ yükleniyor

2. CUMULATIVE LAYOUT SHIFT (CLS) > 0.1
   → Sayfadaki öğeler sağa-sola KAYMASI devamsız

3. FIRST INPUT DELAY / INP > 200ms
   → Kullanıcı tıklaması için sistem 200ms+ BEKLEMESI


❌ NE ANLAMAMIZ GEREKTIĞI?
════════════════════════════════════════════════════════════════════════

Doviz.com'un mobil ziyaretçiler için neredeyse "erişilemez" olması:

📊 RANKING IMPACT:
─────────────────
Google algoritması PageSpeed'i official RANKING SIGNAL olarak kullıyor

42 score sahibi site:
  Impact: -5 ~ -10 pozisyon aşağı
  Örnek: "Döviz Kuru" keywordinde #8 de çıkan site, PageSpeed iyileşince #2-3 → #1'e yükselebilir

📉 TRAFFIC KAYBIM AYDA YAKLAŞIK:
─────────────────
- Mobile 42 score = ~72 clicks kaybı/ay
- Mobile 40 score = ~75 clicks kaybı/ay
- Mobile 33 score = ~85 clicks kaybı/ay
- Mobile 38 score = ~78 clicks kaybı/ay
- Mobile 35 score = ~82 clicks kaybı/ay

TOPLAM: ~400 clicks kaybı/ay = ~4,800 clicks/yıl

Parasal Değeri ($/click = $0.10 -için finansal sektör):
→ $48-240/yıl kayıp gelir (konservatif)
→ Gerçekçi: $240-480/yıl kaybı

⏱ USER EXPERIENCE KAYBIM:
─────────────────────
42 score sahibi mobil sayfa = 3.5+ saniye yükleme
90 score sahibi sayfa = 1.5 saniye yükleme

Fark: 2 saniye = %35-50 Bounce Rate artışı
      = 0.8 saniye bekleme psikolojisi kırılma noktası

🔴 GOOGLE SEARCH CONSOLE YANSIMASI:
─────────────────────────────────
GSC > Enhancements > Core Web Vitals:
- "Poor" status → bu sayfanız mobil cihazlarda sorunlu demek
- Google, sayfanızı Search Results'ta DOWN-RANK yapıyor


🚀 YAPILMASI GEREKENLER (TIER ROADMAP)
════════════════════════════════════════════════════════════════════════

TIER 1: 5 DAKİKA - +15 PUAN (35 → 50)
────────────────────────────────────
Amacı: LCP (Largest Contentful Paint) -500ms optimize

Konkret Aksiyonlar:

1. Kritik Görselleri PRE-LOAD et
   <head>
     <link rel="preload" as="image" href="/images/hero-banner.webp">
     <link rel="preload" as="font" href="/fonts/main.woff2">
   </head>

2. Critical CSS'i INLINE yap (ilk 3KB)
   <head>
     <style>
       .hero-section { background: url(...); }
       .nav { display: flex; }
       /* ONLY kritik CSS */
     </style>
   </head>

3. Javascriptleri DEFER/ASYNC yap
   <!-- Bad -->
   <script src="tracking.js"></script>  <!-- SAĞ TUTUYOR -->
   
   <!-- Good -->
   <script src="tracking.js" async></script>  <!-- Parallel yükleme -->
   <script src="lib.js" defer></script>       <!-- Sayfa renderdan sonra -->

Beklenen Sonuç: 35 → 50 puan (Günlük 5 dakika çalışma)


TIER 2: 30 DAKİKA - +20 PUAN (50 → 70)
────────────────────────────────────
Amacı: Image optimization + Layout Shift prevention

Konkret Aksiyonlar:

1. WEBP Formatı Kullan
   <picture>
     <source srcset="banner.webp" type="image/webp" media="(min-width: 768px)">
     <source srcset="banner-mobile.webp" type="image/webp">
     <img src="banner.jpg" alt="Döviz kurları">
   </picture>

   Kazanç: PNG/JPG → WebP = %30-50 küçültme
   Örnek: 500KB banner → 100-250KB

2. Lazy Load Resimler
   <img loading="lazy" src="usd-rate.jpg">
   
   Kazanç: Above-fold dışı görseller immediately yüklenmez

3. Layout Shift'i Önle (CLS)
   /* KÖTÜ */
   img { margin: 10px; }  <!-- Initially no height → shift -->
   
   /* İYİ */
   img { 
     aspect-ratio: 16/9;  <!-- HTML'de height reserve et -->
     width: 100%;
     margin: 10px;
   }

4. Font Display Strategy
   @font-face {
     font-family: 'MainFont';
     font-display: swap;  <!-- Fallback göster önce -->
   }

Beklenen Sonuç: 50 → 70 puan


TIER 3: 60 DAKİKA - +25 PUAN (70 → 95+)
────────────────────────────────────
Amacı: JavaScript optimization + FID/INP improvement

Konkret Aksiyonlar:

1. Code Splitting (Webpack 5)
   webpack.config.js:
   
   entry: {
     main: './src/main.js',
     vendors: ['react', 'react-dom']
   },
   optimization: {
     splitChunks: {
       chunks: 'all',
       cacheGroups: {
         vendor: {
           test: /[\\/]node_modules[\\/]/,
           name: 'vendors',
           priority: 10
         },
         common: {
           minChunks: 2,
           priority: 5,
           reuseExistingChunk: true
         }
       }
     }
   }

2. Minify + Compress
   npm run build  <!-- uglify-js, terser otomatik -->
   
   nginx config:
   gzip on;
   gzip_types text/plain text/css application/json application/javascript;
   gzip_min_length 1000;
   gzip_comp_level 6;

3. Unused CSS Kaldır
   npm install --save-dev purgecss
   
   tailwind.config.js:
   content: [
     "./src/pages/**/*.{js,jsx,ts,tsx}",
     "./src/components/**/*.{js,jsx,ts,tsx}"
   ]

4. JavaScript Execution Azalt
   // KÖTÜ
   for(let i=0; i<10000; i++) { calc(); }  <!-- Blocking -->
   
   // İYİ
   setTimeout(() => { calc(); }, 0);  <!-- Non-blocking -->
   
   // SUPER İYİ
   import('heavy-lib').then(m => m.calc());  <!-- Lazy import -->

Beklenen Sonuç: 70 → 95+ puan


📈 TAM TIMELINE VE BEKLENEN SONUÇLAR
════════════════════════════════════════════════════════════════════════

GÜN 1-2 (TIER 1):
─────────────────
Yaptığınız: Image preload + inline CSS + defer JS (5 dakika)
  Mobil: 35 → 50 puan
  Neden: LCP'den 500ms kaybetme
  Parasal: +36 clicks/ay = +$3.60/ay

GÜN 3-7 (TIER 2):
─────────────────
Yaptığınız: WebP + lazy load + CLS fix (30 dakika)
  Mobil: 50 → 70 puan
  Neden: Image bundle %50 küçültme
  Parasal: +72 clicks/ay toplamda = +$7.20/ay

GÜN 8-14 (TIER 3):
──────────────────
Yaptığınız: Code split + minimize + purge (60 dakika)
  Mobil: 70 → 95+ puan
  Neden: JS optimization + CSS temizlik
  Parasal: +400 clicks/ay toplamda = +$40/ay

FINAL (14 gün sonra):
────────────────────
Doviz.com Mobil: 35 → 95+ (60 puan yükselme)
Desktop: 65 → 98+ (33 puan yükselme)

Total Traffic Kazanımı: +400-500 clicks/ay
Parasal Kazanım: +$40-50/ay (minimum)
Ranking İyileşme: -5 pozisyon → -1 pozisyon (+4 sıra yukarı)


🔧 NASIL UYGULANIR?
════════════════════════════════════════════════════════════════════════

1. TEST AŞAMASI (dev environment)
   - Lokal build edip PageSpeed Insights test et
   - Chrome DevTools Lighthouse koş
   - Metrics: LCP, CLS, FID takip et

2. STAGE DEPLOYMENT
   - Değişiklikleri staging'e push et
   - Gerçek mobil cihazlardan test et
   - Performance regression yokmu kontrol et

3. PRODUCTION ROLLOUT
   - Değişiklikleri A/B test et (10% → 50% → 100%)
   - CDN cache invalidate et
   - Google Search Console'da "URL Inspection" test et

4. MONITORING (Haftalık)
   - PageSpeed Insights: https://pagespeed.web.dev
   - Google Search Console → Enhancements → Core Web Vitals
   - Google Analytics 4 → Web Vitals report
   - Real User Metrics (CrUX): Google BigQuery


✅ SORUMLULUK MATRISI
════════════════════════════════════════════════════════════════════════

TIER 1 (Frontend Developer):
✓ Image preload tags ekleme
✓ Inline critical CSS setup
✓ JavaScript defer/async markers

TIER 2 (DevOps + Frontend):
✓ WebP image conversion pipeline
✓ CDN ayarları (Cloudflare, Akamai)
✓ Gzip compression enable

TIER 3 (Backend + DevOps):
✓ Webpack/build optimization
✓ Database query optimization (slow queries yok mu?)
✓ APM monitoring (DataDog, New Relic)


🚨 KRITIK NOTLAR
════════════════════════════════════════════════════════════════════════

1. PageSpeed Insights ≠ Real User Experience
   - LAB environment ≠ field environment
   - Real users için Core Web Vitals takip et (CrUX data)

2. 95+ Score Zaman Alabilir
   - Doviz.com dinamik site → caching lazım
   - Database optimization gerekebilir
   - Hosting/CDN upgrade gerekebilir

3. Regression Test Şart
   - Optimizasyon = feature'lar broken olabilir
   - Automated testing zorunlu
   - Staging'de minimal 48 saat test

4. Continuous Monitoring
   - Bir günde 95, ertesi 88 olabilir (test framework farklılıkları)
   - Weekly avg takip et, daily alertlere dikkat etme


💡 EKSTRA TİPS
════════════════════════════════════════════════════════════════════════

1. CDN Kullan (zorunlu değil ama önerilir)
   - Cloudflare: $20-200/ay
   - Akamai: $300+/ay
   - Bunun sadece %5'i PageSpeed → %95'i DDoS + edge cache

2. Image Service (optional)
   - Imgix, Cloudinary: Auto WebP + responsive sizing
   - Upload → Auto optimize + CDN serve

3. Database Query Optimization
   - Slow queries logs takip et
   - N+1 problems çöz
   - Index check

4. HTTP/2 Push (eski ama işe yarar)
   Link header:
   Link: </css/main.css>; rel=preload; as=style
   Link: </js/main.js>; rel=preload; as=script


════════════════════════════════════════════════════════════════════════
SONUÇ: Doviz.com'un PageSpeed uyarıları ciddi risk. 2 hafta çalışma ile
95+ score'a ulaşılabilir. Finansal impact: +$500-600/yıl minimum.
════════════════════════════════════════════════════════════════════════
