"""PageSpeed Performance Analyzer - TIER-based optimization roadmap."""

from typing import Dict


def analyze_pagespeed_alerts(mobile_score: int, desktop_score: int) -> Dict:
    """
    PageSpeed uyarılarını analiz et ve TIER-based fix roadmap oluştur.
    
    Args:
        mobile_score: Mobil PageSpeed skoru (0-100)
        desktop_score: Desktop PageSpeed skoru (0-100)
    
    Returns:
        Dict: Analiz sonuçları, impact, ve TIER roadmap
    """
    
    # Risk seviyesi belirle
    THRESHOLD = 55
    GOOD_SCORE = 90
    
    mobile_risk = "KRITİK" if mobile_score < 50 else "UYARI" if mobile_score < THRESHOLD else "İYİ"
    
    # Score gap hesapla
    score_gap = GOOD_SCORE - mobile_score
    
    # Traffic impact tahmin et
    ctr_loss_percent = (GOOD_SCORE - mobile_score) * 0.3
    
    # Ayda tahmini click kaybı
    monthly_clicks_loss = max(15, int((ctr_loss_percent / 100) * 500))
    annual_clicks_loss = monthly_clicks_loss * 12
    
    # Status mesajı
    status = "KRİTİK" if mobile_score < 50 else "UYARI" if mobile_score < THRESHOLD else "İYİ"
    status_msg = f"{status} Mobile {mobile_score}/100 | Desktop {desktop_score}/100"
    
    # TIER roadmap oluştur
    tier_roadmap = create_tier_roadmap(mobile_score, desktop_score)
    
    # Impact analizi
    impact_msg = f"🔴 RANKING PENALTY -5-10 position | Monthly traffic loss ~{monthly_clicks_loss} clicks" if mobile_score < THRESHOLD else "✅ Good performance"
    
    result = {
        "check": "PageSpeed Performance",
        "passed": mobile_score >= THRESHOLD and desktop_score >= THRESHOLD,
        "status": status_msg,
        "reason": f"Mobile PageSpeed {mobile_score}/100, Core Web Vitals optimization needed",
        "impact": impact_msg,
        "action": tier_roadmap,
        "technical_data": {
            "mobile_score": mobile_score,
            "desktop_score": desktop_score,
            "mobile_risk": mobile_risk,
            "score_gap": score_gap,
            "monthly_traffic_loss": monthly_clicks_loss,
            "annual_traffic_loss": annual_clicks_loss,
            "ctr_loss_percent": round(ctr_loss_percent, 1)
        },
        "priority": "CRITICAL" if mobile_score < 50 else "HIGH" if mobile_score < THRESHOLD else "MEDIUM"
    }
    
    return result


def create_tier_roadmap(mobile_score: int, desktop_score: int) -> str:
    """TIER-based optimization roadmap oluştur."""
    
    tier1_score = mobile_score + 15
    tier2_score = mobile_score + 35
    monthly_to_gain = max(15, int((90 - mobile_score) * 0.3 * 5))
    
    roadmap = (
        "\n📋 TIER-BASED FIX ROADMAP:\n"
        "\n"
        "TIER 1 (5 dakika, +15 puan):\n"
        "─────────────────────────────\n"
        "Hedef: LCP (Largest Contentful Paint) -500ms\n"
        "\n"
        "1. Image Preload:\n"
        "   <link rel=\"preload\" as=\"image\" href=\"critical-image.webp\">\n"
        "\n"
        "2. Inline Critical CSS:\n"
        "   <style> .hero { padding: 20px; } </style>\n"
        "\n"
        "3. Async/Defer JavaScript:\n"
        "   <script src=\"lib.js\" defer></script>\n"
        "\n"
        "Beklenen Sonuc: " + str(mobile_score) + " → " + str(tier1_score) + " puan\n"
        "─────────────────────────────\n"
        "\n"
        "TIER 2 (30 dakika, +20 puan):\n"
        "─────────────────────────────\n"
        "Hedef: Image optimization + CLS prevention\n"
        "\n"
        "1. WebP Format:\n"
        "   <picture>\n"
        "     <source srcset=\"img.webp\" type=\"image/webp\">\n"
        "     <img src=\"img.jpg\">\n"
        "   </picture>\n"
        "\n"
        "2. Lazy Load:\n"
        "   <img loading=\"lazy\" src=\"...\">\n"
        "\n"
        "3. CSS Layout Shift Prevention:\n"
        "   img { aspect-ratio: 16/9; width: 100%; }\n"
        "\n"
        "Beklenen Sonuc: " + str(tier1_score) + " → " + str(tier2_score) + " puan\n"
        "─────────────────────────────\n"
        "\n"
        "TIER 3 (60 dakika, +25 puan):\n"
        "─────────────────────────────\n"
        "Hedef: JavaScript optimization\n"
        "\n"
        "1. Code Splitting (Webpack):\n"
        "   optimization: { splitChunks: { chunks: 'all' } }\n"
        "\n"
        "2. Minify + Gzip:\n"
        "   npm run build\n"
        "   gzip on;\n"
        "\n"
        "3. Unused CSS Removal:\n"
        "   npm install purgecss\n"
        "\n"
        "Beklenen Sonuc: " + str(tier2_score) + " → 95+ puan\n"
        "─────────────────────────────\n"
        "\n"
        "🎯 TIMELINE:\n"
        "\n"
        "Gun 1-2: TIER 1 → Mobile " + str(mobile_score) + " + 15 = " + str(tier1_score) + "\n"
        "Gun 3-7: TIER 2 → Mobile " + str(tier1_score) + " + 20 = " + str(tier2_score) + "\n"
        "Gun 8-14: TIER 3 → Mobile " + str(tier2_score) + " + 25 = 95+\n"
        "\n"
        "Final: Mobile " + str(mobile_score) + " → 95+ (2 hafta)\n"
        "Expected: +" + str(monthly_to_gain) + " clicks/month kurtarimi\n"
        "\n"
        "✅ MONITORING:\n"
        "1. https://pagespeed.web.dev\n"
        "2. Google Search Console > Core Web Vitals\n"
        "3. Analytics > Real User Metrics\n"
    )
    
    return roadmap
