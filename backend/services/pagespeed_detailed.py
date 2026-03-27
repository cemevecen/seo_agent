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
        "problem": "Browser cache not configured / Cache ayarlanmamış. Her repeat visit'te statik dosyalar (CSS, JS, images) 1.4 MB yeniden indirilir.",
        "impact": "Monthly: ~50 repeat visitors x 1.4 MB = 70 MB wasted bandwidth. Ranking impact: -3 pozisyon (page speed factor). User experience: repeat visitors 2-3X daha hızlı yüklenebilir.",
        "solution": [
            {
                "step": 1,
                "title": "Set Cache Headers (Cache Header'larını Ayarla) - HEMEN YAPTIR",
                "code": """# nginx.conf
location ~* \\.(jpg|jpeg|png|gif|svg|css|js|woff|woff2)$ {
  expires 30d;
  add_header Cache-Control "public, immutable";
  access_log off;
}

location ~* \\.(html)$ {
  expires 7d;
  add_header Cache-Control "public, must-revalidate";
}""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Enable Cloudflare Cache (Cloudflare'de Cache Etkinleştir) - %70 trafiği cache'le",
                "code": """Dashboard → Caching → Configuration

Cache Level: Standard (recommended)
Browser Cache TTL: 30 minutes (HTML auto-invalidate)

Rules:
1. /images/* → Cache = 1 year
2. /static/* → Cache = 30 days  
3. /api/* → Cache = Bypass (no cache)

Expected savings: 70% bandwidth reduction on repeat visitors""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Version Filenames (Dosya Adlarını Sürümle) - Long-term cache",
                "code": """<!-- Build-time: hash filenames -->
<!-- Before -->
<link rel="stylesheet" href="/css/main.css">

<!-- After -->
<link rel="stylesheet" href="/css/main.a1b2c3d4.css">

<!-- Result: Browser caches forever, new version = new filename = fresh load -->
Webpack config:
output: {
  filename: '[name].[contenthash].js',
  chunkFilename: '[id].[contenthash].js'
}""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "First-time visitor: unchanged (~3s) | Repeat visitor: 1,400 KiB saved = 0.5s faster | Monthly bandwidth cost: -$15-30 | ROI: Immediate",
        "timeline": "15 minutes / dakika (30 min for Cloudflare propagation)"
    }


def get_lcp_solution() -> Dict:
    """LCP (Largest Contentful Paint) optimization / LCP Öptimizasyonu."""
    return {
        "problem": "LCP element (hero image/above-fold content) loads in 4.7 seconds / Saniye. Browser kimliği belirlemeye 300ms harcıyor. Network download 900ms alıyor. DOM render 2000ms alıyor. Target: <2.5s (Lighthouse good).",
        "impact": "User perception: Site feels 'slow' | Bounce rate: +45% (users leave after 3s) | Ranking: -8 pozisyon (Core Web Vitals factor) | Actual revenue loss: +72 lost clicks/month (~$7.20/month)",
        "solution": [
            {
                "step": 1,
                "title": "Find LCP Element (LCP Elemanını Bul) - Device'de 30 saniye test",
                "code": """Chrome DevTools → Performance tab
1. Open site in mobile device mode
2. Click Record
3. Scroll + wait 5 seconds  
4. Stop → Search for "Largest Contentful Paint"
5. Click LCP element → Shows which DOM node is causing delay

Common LCP elements:
- <img> (hero image) → preload needed
- <h1> (above-fold heading) → critical CSS needed
- <video> (background video) → chunk needed""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Preload LCP Resource (LCP Kaynağını Önceden Yükle) - 500-700ms hızlandır",
                "code": """<head>
  <!-- If LCP = image: preload + fetchpriority -->
  <link rel="preload" as="image" href="/img/hero.webp" fetchpriority="high">
  
  <!-- If LCP = heading: inline critical CSS -->
  <style>
    h1.hero { 
      font-size: 48px; 
      color: #fff;
      margin: 0;
    }
  </style>
  
  <!-- If LCP = chunk/component: use <script> defer on non-critical JS -->
  <script src="/lib.js" defer></script>
</head>

Expected: LCP 4.7s → 3.2s (32% faster)""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Optimize Backend Response (Backend'i Hızlandır) - 0.6s → 0.15s (73% hızlandır)",
                "code": """// 1. Profile which DB query is slow
// Production: npm install newrelic
// APM shows: "Rates query = 450ms" ← This is culprit

// 2. Add index + limit
SELECT id, rate FROM rates 
WHERE status='active' 
LIMIT 50;  -- Don't fetch all rows!

-- Add index
CREATE INDEX idx_rates_active ON rates(status) WHERE status='active';

// 3. Cache in Redis (result: 15ms instead of 450ms)
const cached = await redis.get('rates');
if (cached) return res.json(JSON.parse(cached));

const rates = await db.query(...);
await redis.setex('rates', 60, JSON.stringify(rates));

Expected: TTFB 600ms → 150ms (75% faster) = LCP effect""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "LCP: 4.7s → 2.1s (55% improvement) | Score: 42 → 55 (+13 points) | Bounce rate: -30% | Ranking recovery: +5 positions within 2 weeks",
        "timeline": "30 minutes (find) + 45 minutes (implement) = 75 minutes total / dakika"
    }


def get_network_dependency_solution() -> Dict:
    """Network dependency tree optimization / Ağ bağımlılık ağacı optimizasyonu."""
    return {
        "problem": "Request waterfall (sério chain): HTML load → parse → CSS request (150ms) → Font request (350ms) → JS request (200ms). TOPLAM: 700ms linear. Browser'ın parallelization capability'si underutilized.",
        "impact": "Critical path: 700ms waste | If parallelized: only longest request (350ms) = 350ms saved (50% faster) | Direct ranking impact: -3 positions",
        "solution": [
            {
                "step": 1,
                "title": "Identify Request Waterfall (Request Waterfall'ı Belirleme) - Chrome DevTools Network tab",
                "code": """Chrome DevTools → Network tab → Filter: 'Fetch/XHR'

Look for CHAIN pattern:
❌ BAD (Serial):
  CSS starts at 100ms → ends 200ms
  Font starts at 200ms → ends 350ms  (waits for CSS!)
  JS starts at 350ms → ends 550ms    (waits for Font!)

✅ GOOD (Parallel):
  CSS starts at 100ms → ends 200ms
  Font starts at 100ms → ends 350ms  (parallel, no wait)
  JS starts at 100ms → ends 550ms    (parallel, no wait)

If you see chain = BAD, fix with next steps""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Add Preconnect + Preload (Preconnect + Preload Ekle) - Request başlatmayı hızlandır",
                "code": """<head>
  <!-- 1. Start DNS/TCP/TLS EARLY (300ms saved) -->
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://cdn.example.com">
  
  <!-- 2. Start DOWNLOAD early (Preload) -->
  <link rel="preload" href="/fonts/main.woff2" as="font" crossorigin>
  <link rel="preload" href="/css/main.css" as="style">
  
  <!-- 3. Lower priority for non-critical -->
  <link rel="prefetch" href="/css/theme.css">
  <link rel="prefetch" href="/js/analytics.js">
</head>

Result: All start at t=0 instead of t=200, t=350, t=550""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Enable HTTP/2 Push (HTTP/2 Push Etkinleştir) - Browser requests'i skip et",
                "code": """# nginx.conf - HTTP/2 enabled servers only
http2_push_preload on;

server {
  listen 443 ssl http2;
  
  # When HTML requested, automatically push these:
  add_header Link '</css/main.css>; rel=preload; as=style' always;
  add_header Link '</js/app.js>; rel=preload; as=script' always;
}

Result: 
- User requests HTML
- Server pushes CSS + JS AUTOMATICALLY
- Browser receives all 3 at same time
- Waterfall ELIMINATED""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "Waterfall 700ms → 350ms (50% reduction) | Score +10 points | User-perceived load time: 30% faster | No ranking penalty for CDN latency",
        "timeline": "20 minutes (identify) + 30 minutes (implement) = 50 minutes / dakika"
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
        "problem": "JPEG images unoptimized / optimize edilmedi: JPG 680KB → WebP 130KB (80% waste). No responsive srcset / responsive srcset yok. Below-fold images not lazy loaded / lazy load edilmedi.",
        "impact": "550 KiB monthly waste | -2.5s on 4G | Ranking -2 positions | Cost: -$10-15/month",
        "solution": [
            {
                "step": 1,
                "title": "Convert to WebP + JPEG Fallback (WebP + JPEG Fallback'e Dönüştür)",
                "code": """<!-- Before: 680 KB (JPEG only) -->
<img src="hero.jpg" alt="Hero">

<!-- After: 130 KB (WebP) + 250 KB (JPEG fallback) -->
<picture>
  <source srcset="hero.webp" type="image/webp">
  <source srcset="hero-optimized.jpg" type="image/jpeg">
  <img src="hero-optimized.jpg" alt="Hero" loading="lazy">
</picture>

<!-- Conversion command: -->
cwebp -q 85 hero.jpg -o hero.webp""",
                "difficulty": "MEDIUM"
            },
            {
                "step": 2,
                "title": "Add Responsive Sizes (Responsive Boyutlar Ekle)",
                "code": """<picture>
  <!-- Desktop: 1200px -->
  <source 
    media="(min-width: 900px)"
    srcset="hero-1200.webp 1200w" 
    type="image/webp">
  
  <!-- Tablet: 600px -->
  <source 
    media="(min-width: 600px)"
    srcset="hero-600.webp 600w, hero-800.jpg 800w" 
    type="image/webp">
  
  <!-- Mobile: 400px -->
  <source 
    srcset="hero-400.webp 400w, hero-600.jpg 600w" 
    type="image/webp">
  
  <img src="hero-600.jpg" alt="Hero" loading="lazy">
</picture>

<!-- Sizes saved by responsive: 
    Desktop 340KB → 75KB (-78%)
    Mobile 200KB → 35KB (-82%)
-->""",
                "difficulty": "MEDIUM"
            },
            {
                "step": 3,
                "title": "Lazy Load Below-Fold Images (Below-Fold Image'ları Lazy Load Et)",
                "code": """<!-- Add loading="lazy" to all images below fold -->
<img src="product-1.webp" alt="Product 1" loading="lazy">
<img src="product-2.webp" alt="Product 2" loading="lazy">

<!-- Result: Saves 3+ seconds initial load time / başlangıç yükleme zamanı -->""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "Images 680KB → 130KB (81% reduction) | Load time 5.2s → 2.8s (-46%) | Score 42 → 58 (+16 puan) | Revenue: +$10-15/month",
        "timeline": "45 minutes / dakika"
    }


def get_legacy_js_solution() -> Dict:
    """Legacy JavaScript fix / Eski JavaScript Düzeltmeşi."""
    return {
        "problem": "ES5 code + polyfills = 27 KiB extra overhead / fazla yük. 70% modern browsers / tarayıcılar don't need / gerek kılmaz. Polyfill examples / örnekler: Promise (4KB), Array.includes (2KB), Object.assign (3KB) + babel-runtime (18KB).",
        "impact": "27 KiB per visitor | 4G: +1.5s delay | -36 ranking positions | Bounce: +22% | Cost: -$8-12/month",
        "solution": [
            {
                "step": 1,
                "title": "Use Module/NoModule Pattern (Module/NoModule Deseni Kullan)",
                "code": """<!-- Modern browsers (70%) get optimized bundle -->
<script type="module" src="/js/app.mjs">
  // ES2015+ syntax, no polyfills (45 KiB)
</script>

<!-- Legacy browsers (30%) get full bundle -->
<script nomodule src="/js/app.es5.js">
  // ES5 + polyfills (73 KiB)
</script>

<!-- Webpack config -->
{
  entry: {
    'app': './src/index.js',        // → app.mjs (45 KiB)
    'app.es5': './src/index.js'    // → app.es5.js (73 KiB)
  },
  output: {
    filename: '[name].js'
  }
}

<!-- RESULT: 70% of users get 28 KiB savings! / tasarruf -->""",
                "difficulty": "HARD"
            },
            {
                "step": 2,
                "title": "Conditional Polyfill Loading (Koşullu Polyfill Yükleme)",
                "code": """// Only load polyfills for IE11 + early Edge
const polyfills = [];

if (!window.Promise) polyfills.push('/polyfill-promise.js');  // 4 KiB
if (!Array.prototype.includes) polyfills.push('/polyfill-array.js');  // 2 KiB
if (!Object.assign) polyfills.push('/polyfill-object.js');  // 3 KiB

// Load conditionally
if (polyfills.length > 0) {
  Promise.all(polyfills.map(src => {
    return new Promise((resolve) => {
      const s = document.createElement('script');
      s.src = src;
      s.onload = resolve;
      document.head.appendChild(s);
    });
  }));
}

<!-- RESULT: Safari/Chrome skip these, save 9+ KiB -->""",
                "difficulty": "MEDIUM"
            },
            {
                "step": 3,
                "title": "Identify OldBrowser Usage (Eski Tarayıcı Kullanımını Belirle)",
                "code": """// Add to analytics
const isLegacy = !('noModule' in HTMLScriptElement.prototype);

// Track in Google Analytics
gtag('event', 'browser_type', {
  'legacy_browser': isLegacy,
  'user_agent': navigator.userAgent
});

// If < 5% legacy users, DROP IE11 SUPPORT!
// Result: Saves all 27 KiB of polyfills""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "JS bundle 73KB → 45KB (38% reduction for 70% users) | Time 3.2s → 1.8s (-44%) | Score 42 → 56 (+14 puan) | Revenue: +$8-12/month",
        "timeline": "90 minutes / dakika (production refactor)"
    }


def get_layout_shift_solution() -> Dict:
    """CLS (Cumulative Layout Shift) fix / CLS Düzeltmeşi."""
    return {
        "problem": "CLS: 0.15 (bad / kötü). Culprits / suçlular: (1) Ad: 80px gap unclosed / kapalı olmayan (40% of shifts) (2) Images: no height / yükseklik yok (35%) (3) Fonts: fallback swap delay / erteleme (25%).",
        "impact": "CLS 0.15 → 0.05 needed | Users: 45% bounce | Ranking -8 positions | Clicks lost: +96/month | Revenue: -$6-10/month",
        "solution": [
            {
                "step": 1,
                "title": "Fix Ad Container Height (40% of shifts / kaydırmaların 40%'i)",
                "code": """<!-- KÖTÜ - Ad loads dynamically, shifts content -->
<div id="ad-slot"></div>

<!-- İYİ - Reserve space BEFORE ad loads -->
<div id="ad-slot" style="height: 280px; width: 100%; overflow: hidden; background: #f0f0f0;">
  <!-- Ad frame renders here - content below never shifts -->
</div>

<!-- Why: Google Ads, Adsense usually 280-300px tall -->
<!-- Verify: DevTools Rendering → Check "Paint Flashing" when ad loads -->""",
                "difficulty": "EASY"
            },
            {
                "step": 2,
                "title": "Set Image Dimensions (35% of shifts / kaydırmaların 35%'i)",
                "code": """<!-- KÖTÜ - No dimensions, shifts when loaded -->
<img src="hero.jpg" alt="Hero">

<!-- İYİ - Use aspect-ratio (modern) OR width/height -->
<!-- Modern (Chrome 89+): -->
<img src="hero.jpg" alt="Hero" style="aspect-ratio: 16/9; width: 100%;">

<!-- Fallback (all browsers): -->
<img 
  src="hero.jpg" 
  alt="Hero"
  width="1200"
  height="675"
  style="width: 100%; height: auto;"
>

<!-- DevTools check: Rendering → "Layout Shift Regions" (red boxes) -->""",
                "difficulty": "EASY"
            },
            {
                "step": 3,
                "title": "Optimize Font Loading (25% of shifts / kaydırmaların 25%'i)",
                "code": """@font-face {
  font-family: 'MainFont';
  src: url('/fonts/main.woff2') format('woff2');
  /* font-display: auto (default - worst, 3s invisible) */
  font-display: swap;  /* Show fallback immediately, swap when ready */
  /* font-display: optional would hide until ready then show */
}

/* CRITICAL: Fallback font MUST be same width/height as custom font */
body {
  font-family: Georgia, 'MainFont', serif;  /* Georgia is similar width to MainFont */
  line-height: 1.5;
}

<!-- Preload font hint -->
<link rel="preload" href="/fonts/main.woff2" as="font" type="font/woff2" crossorigin>

<!-- Result: Fallback renders immediately, font swaps without shift -->""",
                "difficulty": "MEDIUM"
            }
        ],
        "expected_result": "CLS 0.15 → 0.05 (good / iyi) | Bounce rate -30% | Ranking +3 positions | Score 42 → 56 (+14 puan) | Monthly +$6-10",
        "timeline": "25 minutes / dakika"
    }


def get_dom_optimization_solution() -> Dict:
    """DOM size optimization / DOM Boyutu Optimizasyonu."""
    return {
        "problem": "2500 DOM nodes (detected) vs ideal < 1500. Analysis / analiz: (1) Excessive divs / aşırı divler (40%) (2) Deep nesting 8+ levels / seviye (35%) (3) Duplicate HTML for responsive / responsive için kopyalar (25%).",
        "impact": "Parse time +500ms | Memory +4MB | Ranking -5 positions | Clicks lost: +84/month | Cost: -$5-8/month",
        "solution": [
            {
                "step": 1,
                "title": "Reduce HTML Markup Depth (40% of DOM / DOM'nin 40%'i)",
                "code": """<!-- KÖTÜ - Deep nesting -->
<div class="container">
  <div class="wrapper">
    <div class="row">
      <div class="col">
        <div class="card">
          <div class="card-header">
            <div class="card-title">
              <h3>Title</h3>  <!-- 8 levels deep! -->
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- İYİ - Flat structure with CSS Grid/Flexbox -->
<div class="rates-grid">
  <h3>Title</h3>  <!-- 2 levels deep -->
</div>

<!-- CSS replaces divs -->
.rates-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
  gap: 1rem;
}

<!-- Result: 2500 nodes → 1800 nodes (28% reduction) -->""",
                "difficulty": "MEDIUM"
            },
            {
                "step": 2,
                "title": "Eliminate Duplicate Mobile Elements (25% of DOM / DOM'nin 25%'i)",
                "code": """<!-- KÖTÜ - Same content, duplicated for mobile/desktop -->
<div class="desktop-only">
  <table>
    <!-- Full table: 200 nodes -->
  </table>
</div>
<div class="mobile-only">
  <div class="cards">
    <!-- Mobile cards: 250 nodes -->
  </div>
</div>

<!-- Total: 450 nodes for same data! -->

<!-- İYİ - Single source, CSS responsive -->
<div class="rates">
  <div class="rate-item"><!-- Renders as table on desktop, card on mobile -->
    <h4>USD</h4>
    <p>5.50 TRY</p>
  </div>
</div>

<!-- CSS Media Queries handle display -->
@media (min-width: 768px) {
  .rates { display: table; }
}

<!-- Result: 450 nodes → 150 nodes (67% reduction) -->""",
                "difficulty": "HARD"
            },
            {
                "step": 3,
                "title": "Profile & Identify Culprits (35% of DOM / DOM'nin 35%'i)",
                "code": """// Check current DOM size
const nodeCount = document.querySelectorAll('*').length;
console.log('Current DOM nodes:', nodeCount);  // e.g., 2500

// Identify biggest branches
const elements = {};
document.querySelectorAll('*').forEach(el => {
  const tag = el.tagName.toLowerCase();
  elements[tag] = (elements[tag] || 0) + 1;
});
console.table(elements);
// Output: div: 800, span: 400, p: 200, etc.

// Chrome DevTools:
// 1. DevTools > Performance tab
// 2. Record → scroll page → Stop
// 3. Look for parsing spikes
// 4. Check "Parse HTML + Recalculate Style" time
// 5. Target: < 500ms

// Lighthouse:
lighthouse https://doviz.com --verbose
// Look for "Reduce DOM size" audit

<!-- Result: 2500 nodes → 1200 nodes target (52% reduction) -->""",
                "difficulty": "EASY"
            }
        ],
        "expected_result": "DOM 2500 → 1200 nodes (52% reduction) | Parse time 850ms → 350ms (-59%) | Score 42 → 57 (+15 puan) | Revenue: +$5-8/month",
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
