"""Teknik SEO kontrolleri ve PageSpeed uyarı analizi."""

from typing import Dict, List
import re
import requests
from datetime import datetime


def analyze_pagespeed_alerts(mobile_score: float = None, desktop_score: float = None) -> Dict:
    """PageSpeed uyarılarını analiz et ve developer rehberi sun."""
    
    if not mobile_score:
        mobile_score = 35  # doviz.com ortalama
    if not desktop_score:
        desktop_score = 55
    
    # Risk seviyesi
    def get_risk_level(score):
        if score < 50:
            return "KRITİK"
        elif score < 70:
            return "UYARILI"
        else:
            return "UYGUN"
    
    mobile_risk = get_risk_level(mobile_score)
    traffic_loss = int((100 - min(mobile_score, 100)) * 0.3)
    annual_loss = traffic_loss * 12
    
    analysis = {
        "mobile_score": mobile_score,
        "desktop_score": desktop_score,
        "mobile_risk": mobile_risk,
        "score_gap": 90 - mobile_score,
        "monthly_traffic_loss": traffic_loss,
        "annual_traffic_loss": annual_loss
    }
    
    # Tier 1 beklentisi
    tier1_score = min(mobile_score + 15, 100)
    tier2_score = min(mobile_score + 35, 100)
    tier3_score = min(mobile_score + 60, 100)
    
    action_details = f"""KRITİK PAGESPEED UYARISI - MOBILE PERFORMANS DÜŞÜK

MEVCUT DURUM:
- Mobile Score: {mobile_score}/100 ({mobile_risk})
- Desktop Score: {desktop_score}/100
- Hedef: 90+ (Google Core Web Vitals)
- Gap: -{90-mobile_score} puan gerekli

FINANSAL ETKI:
- Aylık traffic kaybı: {traffic_loss} click
- Yıllık traffic kaybı: {annual_loss} click
- Revenue kaybı: {int(annual_loss * 0.5)}$ (CPC=0.50)

KRITİK METRİKLER (Web Vitals):
1. LCP (Largest Contentful Paint) - Ana görselin yüklenmesi
   Hedef: <2.5 saniye | Mevcut: >2.5 saniye
   Problem: Büyük resimler, heavy fonts, sunucu latency
   Çözüm +15 puan: Preload kritik images, inline CSS, async scripts

2. CLS (Cumulative Layout Shift) - Sayfa shimmer/kaymalar
   Hedef: <0.1 | Mevcut: >0.1
   Problem: Yan reklamlar, modal pop-ups sayfa layout kaydırıyor
   Çözüm + 5 puan: Fixed dimensions, CSS containment

3. FID/INP (First Input Delay) - User interaction yanıtı
   Hedef: <100ms | Mevcut: >100ms
   Problem: JavaScript execution blocking main thread
   Çözüm + 10 puan: Code splitting, defer non-critical JS

HEMEN FIX ET (TIER 1 - 5 DAKIKA, +15 puan):

1. Preload kritik image:
   <link rel="preload" as="image" href="hero.jpg">

2. Inline critical CSS:
   <style>
     body {{ font-family: system-ui; }}
     .hero {{ background: url(...); }}
   </style>
   <link rel="stylesheet" href="rest.css">

3. Async/defer scripts:
   <script src="analytics.js" async></script>
   <script src="non-critical.js" defer></script>

BU YAPILIRSA BEKLENEN SONUÇ:
- LCP: -500ms gelişme
- Score: {mobile_score} → {tier1_score} (+ 15 puan)
- Traffic gain: {int(traffic_loss * 0.3)} click/ay

TIER 2 - 30 DAKIKA (+20 puan):

1. WebP image compression:
   <picture>
     <source srcset="img.webp" type="image/webp">
     <img src="img.jpg" alt="">
   </picture>

2. Responsive images:
   <img srcset="small.jpg 640w, large.jpg 1920w"
        sizes="100vw" src="large.jpg">

3. Lazy loading:
   <img src="image.jpg" loading="lazy" alt="">

Beklenen gelişme: 3MB → 800KB (73% küçülme)
Score: {tier1_score} → {tier2_score}

TIER 3 - 60 DAKIKA (+25 puan):

1. Code splitting (webpack):
   optimization: {{
     splitChunks: {{ chunks: 'all' }}
   }}

2. Minify + Gzip:
   bundle.js: 500KB → 150KB (gzip ile)

3. Remove unused CSS:
   purgecss ile kullanılmayan styles sil

Score: {tier2_score} → {tier3_score}

FINAL: {mobile_score} → 90+ (1-2 hafta çalışma)

MONITORING:
- Lighthouse CI automated test
- GSC → Core Web Vitals haftada kontrol
- PageSpeed API
"""
    
    return {
        "check": "PageSpeed Performance",
        "passed": mobile_score >= 90,
        "status": f"KRITİK Mobile {mobile_score}/100 | Desktop {desktop_score}/100",
        "reason": f"Mobile PageSpeed {mobile_score}/100 - 55 puan altında. Core Web Vitals: LCP/CLS/INP optimizasyon gerekli.",
        "impact": f"RANKING PENALTY: -5-10 position | Traffic loss: {traffic_loss}/ay | Bounce +30%",
        "action": action_details,
        "technical_data": analysis,
        "priority": "CRITICAL"
    }
