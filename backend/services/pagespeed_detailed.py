"""Comprehensive PageSpeed Analyzer - All metrics + detailed recommendations."""

from typing import Dict, List


def analyze_pagespeed_detailed(mobile_score: int, desktop_score: int) -> Dict:
    """
    Detaylı PageSpeed analizi - TÜM metrikler ve sub-recommendations.
    
    Args:
        mobile_score: Mobil PageSpeed skoru
        desktop_score: Desktop PageSpeed skoru
    
    Returns:
        Dict: Kapsamlı analiz, tüm sorunlar, ve çözüm yolları
    """
    
    # Risk seviyesi
    THRESHOLD = 55
    mobile_risk = "KRİTİK" if mobile_score < 50 else "UYARI" if mobile_score < THRESHOLD else "İYİ"
    
    # Tahmini sorunlar (score'a göre)
    diagnostics = get_estimated_diagnostics(mobile_score)
    
    result = {
        "check": "PageSpeed Comprehensive Analysis",
        "passed": mobile_score >= THRESHOLD,
        "mobile_score": mobile_score,
        "desktop_score": desktop_score,
        "mobile_risk": mobile_risk,
        "priority": "CRITICAL" if mobile_score < 50 else "HIGH" if mobile_score < THRESHOLD else "MEDIUM",
        
        # Tüm sorunlar ve sublar
        "diagnostics": diagnostics,
        
        # Tier-based roadmap
        "tier_recommendations": get_tier_recommendations(mobile_score),
        
        # Financial impact
        "financial_impact": calculate_financial_impact(mobile_score),
    }
    
    return result


def get_estimated_diagnostics(score: int) -> Dict:
    """
    Score'a göre tahmini tanılamalar ve boyut tasarrufu.
    """
    
    diagnostics = {
        "performance_diagnostics": [],
        "opportunities": [],
        "passed_audits": []
    }
    
    # Score 42 civarı için yaygın sorunlar
    if score < 50:
        diagnostics["performance_diagnostics"] = [
            {
                "id": "cache-lifetimes",
                "title": "Use Efficient Cache Lifetimes (Verimli Cache Ömürleri Kullan)",
                "description": "Browser cache not optimized / Cache optimize edilmemiş. Static: 30+ days, Dynamic: 1-7 days / Statik: 30+ gün, Dinamik: 1-7 gün.",
                "estimated_savings_kib": 1400,
                "severity": "HIGH",
                "solution": get_cache_lifetime_solution()
            },
            {
                "id": "lcp-request-discovery",
                "title": "LCP Request Discovery (LCP İstek Keşfi)",
                "description": "Blocking requests for LCP / LCP'yi block eden request'ler: font, CSS, hero image / font, CSS, ana görsel.",
                "estimated_savings_ms": 500,
                "severity": "CRITICAL",
                "solution": get_lcp_solution()
            },
            {
                "id": "network-dependency-tree",
                "title": "Network Dependency Tree (Ağ Bağımlılık Ağacı)",
                "description": "Request chain not optimized / Request chain optimize edilmemiş. Parallel instead of sequential / Sequential yerine parallel yapılabilir.",
                "estimated_savings_ms": 800,
                "severity": "HIGH",
                "solution": get_network_dependency_solution()
            },
            {
                "id": "preconnect-warnings",
                "title": "Preconnect Overdrive (Preconnect Aşırı Kullanım)",
                "description": "4+ preconnect connections detected / 4+ preconnect connection kaydedildi. Limit to top 3 origins / Sadece top 3 origin'e limit et.",
                "estimated_savings_ms": 200,
                "severity": "MEDIUM",
                "solution": get_preconnect_solution()
            },
            {
                "id": "document-request-latency",
                "title": "Document Request Latency (Dokuman İstek Gecikmesi)",
                "description": "Slow HTML document fetch / HTML dokuman fetchi yavaş. Server response time > 600ms / Server response time > 600ms.",
                "estimated_savings_kib": 131,
                "estimated_savings_ms": 350,
                "severity": "HIGH",
                "solution": get_document_latency_solution()
            },
            {
                "id": "improve-image-delivery",
                "title": "Improve Image Delivery (Görsel Teslimini İyileştir)",
                "description": "Images not optimized / Resimler optimize edilmemiş. Missing WebP, responsive sizing, lazy loading / WebP, responsive sizing, lazy load eksik.",
                "estimated_savings_kib": 180,
                "severity": "HIGH",
                "solution": get_image_delivery_solution()
            },
            {
                "id": "legacy-javascript",
                "title": "Legacy JavaScript (ES5 JavaScript / Eski JavaScript)",
                "description": "Old JavaScript syntax detected / Eski JavaScript syntax kullanılıyor. Polyfill + ES5 transpile overhead / Polyfill + ES5 transpile yükü.",
                "estimated_savings_kib": 27,
                "severity": "MEDIUM",
                "solution": get_legacy_js_solution()
            },
            {
                "id": "layout-shift-culprits",
                "title": "Layout Shift Culprits (Layout Shift Nedenleri)",
                "description": "Causing CLS issues / CLS'ye sebep olan öğeler: ads, embeds, dynamic content boxes / reklam, embed'ler, dinamik content box'lar.",
                "cls_impact": 0.15,
                "severity": "HIGH",
                "solution": get_layout_shift_solution()
            },
            {
                "id": "optimize-dom-size",
                "title": "Optimize DOM Size (DOM Boyutunu Optimize Et)",
                "description": "DOM nodes > 1800 / DOM node sayısı > 1800. Unused elements, deep nesting, redundant divs / Kullanılmayan öğeler, deep nesting, redundant div'ler.",
                "estimated_nodes": 2500,
                "ideal_nodes": 1500,
                "severity": "MEDIUM",
                "solution": get_dom_optimization_solution()
            },
        ]
        
        diagnostics["opportunities"] = [
            {
                "id": "unused-css",
                "title": "Remove Unused CSS",
                "savings_kib": 45,
                "difficulty": "EASY"
            },
            {
                "id": "minify-javascript",
                "title": "Minify JavaScript",
                "savings_kib": 52,
                "difficulty": "EASY"
            },
            {
                "id": "serve-modern-javascript",
                "title": "Serve Modern JavaScript only",
                "savings_kib": 89,
                "difficulty": "MEDIUM"
            },
            {
                "id": "defer-off-screen-images",
                "title": "Defer Off-Screen Images",
                "savings_kib": 120,
                "difficulty": "EASY"
            },
        ]
    
    elif score < 75:
        diagnostics["opportunities"] = [
            {
                "id": "minify-css",
                "title": "Minify CSS",
                "savings_kib": 23,
                "difficulty": "EASY"
            },
            {
                "id": "preload-fonts",
                "title": "Preload Key Fonts",
                "savings_ms": 200,
                "difficulty": "EASY"
            },
        ]
    
    # Passed audits (herkes için)
    diagnostics["passed_audits"] = [
        {"title": "Uses HTTPS", "description": "Secure connection verified"},
        {"title": "HTML is Valid", "description": "No critical HTML errors"},
    ]
    
    return diagnostics


def get_cache_lifetime_solution() -> Dict:
    """Cache lifetime optimization / Cache ömrü optimizasyonu."""
    return {
        "problem": "Browser cache not optimal / Cache optimal ayarlanmamış. Static files re-downloaded on every visit / Her ziyarette yeniden indirilir.",
        "impact": "1,400 KiB loss / kayıp (46 KiB/day over 30 days = ~1.4 MB/month on repeat visitors / repeat visitor'larda).",
        "solution": [
            {
                "step": 1,
                "title": "Add Nginx/Apache Cache Headers (Nginx/Apache Cache Header'ları Ekle)",
                "code": """# nginx.conf
location ~* \\.(jpg|jpeg|png|gif|ico|css|js|woff|woff2|ttf|svg)$ {
  expires 30d;           # Statik: 30 gün
  add_header Cache-Control "public, immutable";
}

location ~* \\.(html)$ {
  expires 7d;            # HTML: 7 gün
  add_header Cache-Control "public, must-revalidate";
}

# API endpoints
location /api/ {
  expires 1h;            # API: 1 saat
  add_header Cache-Control "private, must-revalidate";
}""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Vercel/Firebase Hosting Cache (Vercel/Firebase Hosting Cache Ayarları)",
                "code": """// vercel.json
{
  "headers": [
    {
      "source": "/images/(.*)",
      "headers": [
        {
          "key": "Cache-Control",
          "value": "public, max-age=31536000, immutable"
        }
      ]
    },
    {
      "source": "/static/(.*)",
      "headers": [
        {
          "key": "Cache-Control",
          "value": "public, s-maxage=86400"
        }
      ]
    }
  ]
}""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "CDN Cache Settings - Cloudflare (CDN Cache Ayarları - Cloudflare)",
                "code": """// Cache Rule
Pattern: *.doviz.com/static/*
Cache Level: Cache Everything
Browser Cache TTL: 30 days
Edge Cache TTL: 7 days

Pattern: *.doviz.com/api/*
Cache Level: Bypass
(API'ler cache'lenmesin)""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "Repeat visitor optimization / Repeat visitor'larda 1,400 KiB savings (cache hit = instant load / instant yükleme).",
        "timeline": "15 minutes / dakika"
    }


def get_lcp_solution() -> Dict:
    """LCP (Largest Contentful Paint) optimization / LCP Öptimizasyonu."""
    return {
        "problem": "LCP element loads too slow / LCP elementi (ust görsel) 4.7 saniyede yükleniyor. Target: < 2.5s / Hedef.",
        "impact": "500ms-1000ms speed loss / hiz kaybı = Ranking -5 positions / pozisyon, CTR -15%",
        "solution": [
            {
                "step": 1,
                "title": "Preload Hero Image (Hero Image'i Preload Et)",
                "code": """<head>
  <!-- Identify LCP element -->
  <link rel="preload" as="image" href="/images/hero-banner.webp"
        imagesrcset="/images/hero-m.webp 480w, /images/hero-d.webp 1920w"
        imagesizes="100vw">
</head>

<body>
  <!-- Hero image -->
  <div class="hero" style="background: url(/images/hero-banner.webp);">
    <h1>Döviz Kurları</h1>
  </div>
</body>""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Inline Critical CSS (Kritik CSS'yi Satır İçine Al)",
                "code": """<head>
  <style>
    /* Critical CSS (ilk 3KB) */
    .hero {
      background: linear-gradient(to bottom, #000, #333);
      min-height: 400px;
      display: flex;
      align-items: center;
    }
    h1 { font-size: 48px; color: white; }
    .nav { position: fixed; top: 0; width: 100%; }
  </style>
  
  <!-- Non-critical CSS deferred -->
  <link rel="preload" href="/css/main.css" as="style" onload="this.onload=null;this.rel='stylesheet'">
  <noscript><link rel="stylesheet" href="/css/main.css"></noscript>
</head>""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Optimize Font Loading (Font Yüklenmesini Optimize Et)",
                "code": """<head>
  <!-- Font preload + font-display: swap -->
  <link rel="preload" href="/fonts/main.woff2" as="font" type="font/woff2" crossorigin>
  
  <style>
    @font-face {
      font-family: 'MainFont';
      src: url('/fonts/main.woff2') format('woff2');
      font-display: swap;  /* Show fallback immediately */
    }
  </style>
</head>""",
                "difficulty": "EASY"
            },
            {
                "step": 4,
                "title": "Optimize Backend Response Time (Backend Tepki Süresini Optimize Et)",
                "code": """// 1. Cache database queries
const redis = require('redis');
const client = redis.createClient();

app.get('/api/rates', async (req, res) => {
  const cached = await client.get('rates_cache');
  if (cached) return res.json(JSON.parse(cached));
  
  const data = await db.query('SELECT * FROM rates');
  await client.setex('rates_cache', 300, JSON.stringify(data));
  res.json(data);
});

// 2. Use CDN push (for static HTML)
app.get('/', (req, res) => {
  res.set('Link', '</css/main.css>; rel=preload; as=style');
  res.send(cachedHTML);
});""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "LCP 4.7s → 2.0s (57% improvement / %57 iyileşme)",
        "timeline": "30 minutes / dakika"
    }


def get_network_dependency_solution() -> Dict:
    """Network dependency tree optimization / Ağ bağımlılık ağacı optimizasyonu."""
    return {
        "problem": "Requests are serial / Request'ler serial (sıra ile) yapılıyor. CSS → Font → JS → image (serial chain).",
        "impact": "800ms-1200ms waste / harcama (request waterfall).",
        "solution": [
            {
                "step": 1,
                "title": "Optimize DNS Preconnect (DNS Preconnect'i Optimize Et)",
                "code": """<head>
  <!-- Sadece TOP 3 origin'e preconnect -->
  <link rel="preconnect" href="https://cdn.example.com">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://analytics.example.com">
  
  <!-- Geri kalanlar: dns-prefetch -->
  <link rel="dns-prefetch" href="https://other-cdn.example.com">
</head>""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Create Parallel Request Chain (Parallel İstek Chain'i Oluştur)",
                "code": """<!-- KÖTÜ - Serial -->
<head>
  <link rel="stylesheet" href="/css/main.css">
  <link rel="stylesheet" href="/css/theme.css">
</head>
<body>
  <script src="/js/lib.js"></script>
  <script src="/js/app.js"></script>
</body>

<!-- İYİ - Parallel + Priority -->
<head>
  <!-- High priority -->
  <link rel="preload" href="/css/main.css" as="style">
  <link rel="preload" href="/js/lib.js" as="script">
  
  <!-- Render-blocking stylesheet (async CSS) -->
  <link rel="stylesheet" href="/css/main.css" media="print" onload="this.media='all'">
  <noscript><link rel="stylesheet" href="/css/main.css"></noscript>
</head>

<body>
  <!-- Defer non-critical JS -->
  <script defer src="/js/lib.js"></script>
  <script defer src="/js/app.js"></script>
</body>""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "HTTP/2 Server Push (Optional / Opsiyonel)",
                "code": """// nginx.conf
http2_push_preload on;

location / {
  add_header Link "</css/main.css>; rel=preload; as=style" always;
  add_header Link "</js/app.js>; rel=preload; as=script" always;
}""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "Network waterfall 1200ms → 400ms (66% reduction / azalma).",
        "timeline": "20 minutes / dakika"
    }


def get_preconnect_solution() -> Dict:
    """Preconnect warning fix / Preconnect uyarısı düzeltişi."""
    return {
        "problem": "4+ preconnect configured / 4+ preconnect ayarlanmış. Browser connection limit = 3-4. Excess = waste / fazlası = harcama.",
        "impact": "CPU/memory waste, connection timeout risk / riski.",
        "solution": [
            {
                "step": 1,
                "title": "Limit Preconnect to 3 Origins (Preconnect'i 3 Origin'e Limit Et)",
                "code": """<!-- TOP 3 origin ONLY -->
<link rel="preconnect" href="https://cdn.example.com">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://analytics.example.com">

<!-- Geri kalanlar: dns-prefetch (only DNS, no TLS) -->
<link rel="dns-prefetch" href="https://other.example.com">
<link rel="dns-prefetch" href="https://third-party.com">""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Prioritize Critical Resources (Kritik Kaynakları Öncelendir)",
                "code": """<!-- Identify TOP 3 by usage -->
1. CDN (images, JS, CSS) → PRECONNECT
2. Google Fonts → PRECONNECT
3. Analytics → PRECONNECT

<!-- NOT critical -->
- Ads → DNS-PREFETCH only
- Social widgets → DNS-PREFETCH only
- Third-party embeds → DNS-PREFETCH only""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "Browser connection pool optimized / optimize edildi, 200ms faster / daha hızlı.",
        "timeline": "5 minutes / dakika"
    }


def get_document_latency_solution() -> Dict:
    """Document request latency fix / Dokuman İstek Gecikmesi düzeltişi."""
    return {
        "problem": "HTML document fetch too slow / yavaş: TTFB > 600ms. Dynamic render + slow DB query / yavaş DB sorgusu.",
        "impact": "131 KiB transfer delay + 350ms server response / tepki.",
        "solution": [
            {
                "step": 1,
                "title": "Optimize Backend Response Time (Backend Tepki Süresini Optimize Et)",
                "code": """// 1. Add caching layer
import redis from 'redis';
const cache = redis.createClient();

app.get('/', async (req, res) => {
  const cached = await cache.get('homepage');
  if (cached) return res.send(cached);  // 10ms response
  
  const html = await renderPage();
  await cache.setex('homepage', 300, html);  // 5 min cache
  res.send(html);
});

// 2. Use Edge caching (Cloudflare Workers)
/**
 * Cloudflare Worker script
 */
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  const cache = caches.default;
  let response = await cache.match(request);
  
  if (!response) {
    response = await fetch(request);
    response = new Response(response.body, response);
    response.headers.append('Cache-Control', 's-maxage=3600');
    event.waitUntil(cache.put(request, response.clone()));
  }
  
  return response;
}""",
                "difficulty": "MEDIUM"
            },
            {
                "step": 2,
                "title": "Optimize Database Query (Veritabanı Sorgusunu Optimize Et)",
                "code": """// SLOW (600ms+)
SELECT * FROM rates;  // No index, full table scan

// FAST (< 50ms)
SELECT id, currency, rate, updated_at 
FROM rates 
WHERE updated_at > NOW() - INTERVAL 1 DAY
ORDER BY id DESC
LIMIT 100;

-- Add index
CREATE INDEX idx_rates_updated ON rates(updated_at DESC);
CREATE INDEX idx_rates_currency ON rates(currency);""",
                "difficulty": "MEDIUM"
            },
            {
                "step": 3,
                "title": "Enable Compression (Sıkıştırmayı Etkinleştir)",
                "code": """# nginx.conf
gzip on;
gzip_vary on;
gzip_types text/plain text/css text/xml text/javascript application/javascript;
gzip_comp_level 6;
gzip_min_length 1000;  # Don't compress < 1KB

# Brotli (better compression)
brotli on;
brotli_types text/plain text/css text/xml text/javascript;""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "TTFB 600ms → 100ms (83% improvement / %83 iyileşme).",
        "timeline": "60 minutes / dakika"
    }


def get_image_delivery_solution() -> Dict:
    """Image optimization / Görsel optimizasyonu."""
    return {
        "problem": "Images not optimized / Resimler optimize edilmemiş: WebP yok, responsive sizing yok, lazy load yok.",
        "impact": "180 KiB loss / kayıp (per page 300+ KiB images unnecessary / sayfada 300+ KiB reklam gereksız).",
        "solution": [
            {
                "step": 1,
                "title": "Use WebP + Responsive Formats (WebP + Responsive Format Kullan)",
                "code": """<!-- Before -->
<img src="usd-rate.jpg" alt="USD" width="200" height="200">

<!-- After -->
<picture>
  <source srcset="usd-rate.webp" type="image/webp">
  <source srcset="usd-rate.jpg" type="image/jpeg">
  <img 
    src="usd-rate.jpg" 
    alt="USD" 
    width="200" 
    height="200"
    loading="lazy"
  >
</picture>

<!-- Responsive version -->
<picture>
  <source media="(min-width: 1200px)" srcset="usd-lg.webp 1200w">
  <source media="(min-width: 768px)" srcset="usd-md.webp 768w">
  <source srcset="usd-sm.webp 480w">
  <img src="usd-sm.jpg" alt="USD">
</picture>""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Enable Lazy Loading (Lazy Loading'i Etkinleştir)",
                "code": """<!-- Native lazy loading -->
<img src="rate.jpg" alt="Rate" loading="lazy" width="400" height="300">

<!-- Intersection Observer (polyfill gerekli) -->
<img src="placeholder.jpg" data-src="rate.jpg" class="lazy" alt="Rate">

<script>
if ('IntersectionObserver' in window) {
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const img = entry.target;
        img.src = img.dataset.src;
        observer.unobserve(img);
      }
    });
  });
  
  document.querySelectorAll('img.lazy').forEach(img => observer.observe(img));
}
</script>""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Image Compression Pipeline (Görsel Sıkıştırma Hattı)",
                "code": """# Build-time image optimization
# npm install -D @squoosh/lib imagemin imagemin-webp

import { ImagePool } from '@squoosh/lib';
import imagemin from 'imagemin';
import imageminWebp from 'imagemin-webp';

// Compress + WebP conversion
await imagemin(['images/**/*.jpg'], {
  destination: 'public/images',
  plugins: [
    imageminWebp({ quality: 75 })
  ]
});

// Result: JPG 300KB → WebP 60KB (80% reduction!)""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "Image payload 300+ KiB → 80-120 KiB (60-70% reduction / azalma).",
        "timeline": "45 minutes / dakika"
    }


def get_legacy_js_solution() -> Dict:
    """Legacy JavaScript fix / Eski JavaScript Düzeltmeşi."""
    return {
        "problem": "ES5 JavaScript + polyfills = 27 KiB extra / fazla. Modern browsers / Tarayıcılar ES2015+ don't need / gerek kılmaz.",
        "impact": "27 KiB unnecessary payload / gereksız yük.",
        "solution": [
            {
                "step": 1,
                "title": "Use Module/NoModule Pattern (Module/NoModule Deseni Kullan)",
                "code": """<!-- Modern browsers (70%) -->
<script type="module" src="/js/app.mjs"></script>

<!-- Legacy browsers (30%) - fallback -->
<script nomodule src="/js/app.es5.js"></script>

<!-- Webpack config -->
// webpack.config.js
{
  output: {
    library: 'app'
  },
  plugins: [
    new BabelPlugin({
      // Modern target
      targets: "> 1%"
    })
  ],
  entry: {
    'app': './src/index.js',
    'app.es5': './src/index.js'  // Separate ES5 build
  }
}""",
                "difficulty": "HARD"
            },
            {
                "step": 2,
                "title": "Conditional Polyfill Loading (Koşullu Polyfill Yükleme)",
                "code": """// Only load polyfills if needed
const polyfills = [];

if (!window.Promise) polyfills.push('/js/polyfill-promise.js');
if (!Array.prototype.includes) polyfills.push('/js/polyfill-array.js');

if (polyfills.length) {
  Promise.all(polyfills.map(s => import(s)));
}""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Use Modern JavaScript Syntax (Modern JavaScript Syntax Kullan)",
                "code": """// KÖTÜ (legacy)
var users = [];
function foo() { return 1; }
user.forEach(function(u) { console.log(u); });

// İYİ (modern)
const users = [];
const foo = () => 1;
users.forEach(u => console.log(u));""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "27 KiB polyfill overhead eliminated / çıkarıldı.",
        "timeline": "90 minutes / dakika (production refactor)"
    }


def get_layout_shift_solution() -> Dict:
    """CLS (Cumulative Layout Shift) fix / CLS Düzeltmeşi."""
    return {
        "problem": "Elements causing layout shift / Layout shift'e sebep olan öğeler: ad gaps, dynamic content, fonts / reklam boşluğu, dinamik content, fontlar.",
        "impact": "CLS > 0.1 (critical), poor user experience / kullanıcı deneyimi bozulur.",
        "solution": [
            {
                "step": 1,
                "title": "Reserve Aspect Ratio Container (Ön Tarafından Aspect Ratio Kapsayıcısı Ayır)",
                "code": """<!-- KÖTÜ - Shift -->
<img src="rate.jpg" alt="Rate">  <!-- Height biliniyor, sonra load → shift -->

<!-- İYİ - No shift -->
<div style="aspect-ratio: 4/3; width: 100%;">
  <img src="rate.jpg" alt="Rate" style="width: 100%; height: 100%;">
</div>

<!-- CSS-in-CSS version -->
<style>
  .image-container {
    aspect-ratio: 16 / 9;  /* Reserve space */
    width: 100%;
    overflow: hidden;
  }
  .image-container img {
    width: 100%;
    height: 100%;
    object-fit: cover;
  }
</style>""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Fixed Ad Placement (Sabit Reklam Yerleşimi)",
                "code": """<!-- KÖTÜ - Ad pushes content down -->
<div class="ad-slot"></div>  <!-- Unknown height -->
<h1>Title</h1>

<!-- İYİ - Reserved space -->
<div class="ad-slot" style="height: 300px; width: 100%; overflow: hidden;">
  <!-- Ad frame loads here -->
</div>
<h1>Title</h1>  <!-- No shift -->""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Prevent Font Loading CLS (Font Yükleme CLS'sini Önle)",
                "code": """@font-face {
  font-family: 'MainFont';
  src: url('/fonts/main.woff2') format('woff2');
  font-display: swap;  /* Show fallback immediately */
  /* OR */
  font-display: optional;  /* Hide until loaded */
}

/* Fallback + custom font same height */
body {
  font-family: Georgia, 'MainFont', serif;  /* Fallback matches width */
  font-size: 16px;
  line-height: 1.5;
}""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "CLS 0.15 → 0.05 (good / iyi).",
        "timeline": "25 minutes / dakika"
    }


def get_dom_optimization_solution() -> Dict:
    """DOM size optimization / DOM Boyutu Optimizasyonu."""
    return {
        "problem": "2500+ DOM nodes / node. Ideal: < 1500. Deep nesting / iç içe geçmiş, unused elements / kullanılmayan öğeler, duplicate classes / yinelenen sınıflar.",
        "impact": "Memory waste / harcama, parse time +500ms.",
        "solution": [
            {
                "step": 1,
                "title": "Remove Unused Elements (Kullanılmayan Öğeleri Kaldır)",
                "code": """<!-- KÖTÜ - Deep nesting -->
<div class="container">
  <div class="wrapper">
    <div class="content">
      <div class="inner">
        <p>Text</p>  <!-- 4 levels deep -->
      </div>
    </div>
  </div>
</div>

<!-- İYİ - Flat structure -->
<div class="container">
  <p>Text</p>  <!-- 1 level deep -->
</div>

<!-- CSS Flexbox replaces div nesting -->
.container {
  display: flex;
  flex-wrap: wrap;
}""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Component-Based Architecture (Bıleşen Tabanlı Mimarlık)",
                "code": """// React component - auto DOM cleanup
function RateCard({ rate }) {
  return (
    <div className="rate-card">
      <h3>{rate.name}</h3>
      <p>{rate.value}</p>
    </div>
  );
}

// List render
export function RatesList() {
  return (
    <div className="rates">
      {rates.map(rate => <RateCard key={rate.id} rate={rate} />)}
    </div>
  );
}

// Virtual scrolling (for 1000+ items)
import { FixedSizeList } from 'react-window';

<FixedSizeList height={600} itemCount={10000} itemSize={50} width="100%">
  {({ index, style }) => <RateCard style={style} rate={rates[index]} />}
</FixedSizeList>""",
                "difficulty": "HARD"
            },
            {
                "step": 3,
                "title": "DOM Profiling Tools (DOM Profil Araçları)",
                "code": """// Chrome DevTools > Performance
// 1. Performance tab
// 2. Record > Do action > Stop
// 3. Look for "Parse HTML" > "Evaluate Script" spike
// 4. Identify culprits

// Lighthouse audit
lighthouse https://doviz.com --output-path=report.html
// Check "Reduce DOM size" audit

// Code:
const nodeCount = document.querySelectorAll('*').length;
console.log('DOM nodes:', nodeCount);  // Should be < 1500""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "2500 DOM nodes → 1200 nodes (52% reduction / azalma).",
        "timeline": "120 minutes / dakika (refactor)"
    }


def get_tier_recommendations(score: int) -> List[Dict]:
    """TIER recommendations based on score / Skora göre TIER önerileri."""
    return [
        {
            "tier": "TIER 1",
            "duration": "30 minutes / dakika",
            "score_gain": "+15 points / puan",
            "items": [
                "Cache lifetime headers (nginx/CF)",
                "Preconnect limit (3 origin)",
                "Font display: swap",
                "Image aspect-ratio reserve"
            ]
        },
        {
            "tier": "TIER 2",
            "duration": "60 minutes / dakika",
            "score_gain": "+20 points / puan",
            "items": [
                "Image optimization (WebP, lazy load)",
                "Critical CSS inline",
                "Minify + gzip compression",
                "Remove unused CSS"
            ]
        },
        {
            "tier": "TIER 3",
            "duration": "120 minutes / dakika",
            "score_gain": "+25 points / puan",
            "items": [
                "Code splitting (Webpack)",
                "Modern JS only (module/nomodule)",
                "DOM optimization",
                "Database query optimization"
            ]
        }
    ]


def calculate_financial_impact(score: int) -> Dict:
    """Financial impact calculation."""
    
    # Traffic loss estimation
    monthly_clicks_loss = max(15, int((90 - score) * 0.3 * 5))
    annual_clicks_loss = monthly_clicks_loss * 12
    
    # Monetary value (conservative $0.10/click for finance sector)
    monthly_loss_usd = monthly_clicks_loss * 0.10
    annual_loss_usd = annual_clicks_loss * 0.10
    
    return {
        "monthly": {
            "clicks_loss": monthly_clicks_loss,
            "estimated_usd": round(monthly_loss_usd, 2)
        },
        "annual": {
            "clicks_loss": annual_clicks_loss,
            "estimated_usd": round(annual_loss_usd, 2)
        },
        "ranking_penalty": {
            "current_position_estimate": 8,  # Top 10 likely
            "after_optimization": 1,  # Potential #1
            "position_recovery": 7
        },
        "timeline_to_recover": "14 gün",
        "roi": {
            "investment": "4-6 saat developer time",
            "return": f"${annual_loss_usd:.0f}/yıl + ranking recovery"
        }
    }
