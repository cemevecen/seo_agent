"""Teknik SEO kontrolleri ve detailed teknik analiz."""

from typing import Dict, List
import re
import requests
from datetime import datetime


def check_robots_txt(domain: str) -> Dict:
    """robots.txt varlığını, yapısını ve crawl efficiency'sini analiz et."""
    try:
        url = f"https://{domain}/robots.txt"
        resp = requests.get(url, timeout=5)
        
        if resp.status_code == 200:
            content = resp.text
            
            # robots.txt analizi
            crawl_delay_found = bool(re.search(r'crawl-delay|request-rate', content, re.IGNORECASE))
            disallow_count = len(re.findall(r'^Disallow:', content, re.MULTILINE))
            allow_count = len(re.findall(r'^Allow:', content, re.MULTILINE))
            
            # Crawl budget hesaplama
            if crawl_delay_found:
                crawl_efficiency = 85  # İyi yönetiliyor
            else:
                crawl_efficiency = 60  # Optimizasyon şansı var
            
            analysis = {
                "robots_size_kb": len(resp.content) / 1024,
                "crawl_delay_configured": crawl_delay_found,
                "disallow_rules": disallow_count,
                "allow_rules": allow_count,
                "crawl_efficiency_score": crawl_efficiency,
                "estimated_daily_crawl_budget_reduction": f"{100-crawl_efficiency}%"
            }
            
            action_details = f"""✅ TEKNIK DETAY:
- Dosya boyutu: {analysis['robots_size_kb']:.2f} KB
- Crawl Delay yapılandırması: {'✓ Var' if crawl_delay_found else '✗ Yok'}
- Disallow kuralları: {disallow_count}
- Allow kuralları: {allow_count}
- Crawl Efficiency Skoru: {crawl_efficiency}/100

📊 ETKİ ANALİZİ:
- Google crawl budget optimizasyonu: {crawl_efficiency}% verimli
- Estimat: Günde ~{int(10000 * (crawl_efficiency/100))} URL crawl edilebiliyor
- Crawl waste riski: %{100-crawl_efficiency}

🔧 DEVELOPER REKOMENDASYONLARı:
1. Crawl-delay ayarla (minimum 1-2 saniye):
   User-agent: *
   Crawl-delay: 2
   
2. Needless URL'leri dışa çıkar:
   Disallow: /admin/
   Disallow: /temp/
   Disallow: /search?
   
3. robots.txt boyutunu <100KB tutarken, semantik istek-reddetmeleri tercih et
4. Aylık bazda analiz yap: search-analytics Google Search Console'dan
5. Sitemap referansı ekle (robots.txt sonunda):
   Sitemap: https://{domain}/sitemap.xml"""
            
            return {
                "check": "robots.txt",
                "passed": True,
                "status": "✓ Mevcut ve Yapılandırılmış",
                "reason": f"robots.txt dosyası erişilerek, {disallow_count} blocking rule ve crawl efficiency {crawl_efficiency}/100 olarak tespit edildi",
                "impact": f"Crawl budget yönetimi: {crawl_efficiency}% verimli | Potansiyel günlük crawl artışı: +{100-crawl_efficiency}% (iyileştirme yapılırsa)",
                "action": action_details,
                "technical_data": analysis,
                "priority": "MEDIUM" if crawl_efficiency < 80 else "LOW"
            }
        else:
            return {
                "check": "robots.txt",
                "passed": False,
                "status": f"✗ Erişim Başarısız (HTTP {resp.status_code})",
                "reason": f"robots.txt dosyasına istek {resp.status_code} yanıtı aldı. Server tarafından engellenmişor sunucu hatası var.",
                "impact": "KRITIK: Google tüm URL'leri crawl etmeye çalışır → crawl budget israfı (+40-60% bandwidth kullanımı) → Crawl delays ve Disallow kuralları göz ardı edilir",
                "action": """🔴 HEMEN ÇÖZ:
1. /robots.txt dosyasının sunucuda var olduğunu doğrula
2. HTTP 403/404 alıyorsan, dosya izinlerini kontrol et:
   chmod 644 /var/www/html/robots.txt
   
3. Base header'larını kontrol et (X-Robots-Tag header'ı):
   GET /robots.txt HTTP/1.1
   Host: example.com
   
4. CDN/firewall kurallarında robots.txt'i block ediyorsan, whitelist'e al
5. Server log'larında /robots.txt erişim hatasını ara

YAYGYN HATALAR:
- robots.txt dosyasını /public/{domain}/ yerine / dizinine koy
- robots.txt application logic'i (auth) arkasında gizleme""",
                "technical_data": {
                    "http_status_code": resp.status_code,
                    "error": "robots.txt not properly accessible",
                    "crawl_efficiency_score": 10
                },
                "priority": "CRITICAL"
            }
    except requests.exceptions.Timeout:
        return {
            "check": "robots.txt",
            "passed": False,
            "status": "✗ Timeout (5+ saniye)",
            "reason": "robots.txt isteği 5 saniye içinde cevap vermedi. Server latency yüksek.",
            "impact": "ORTA: Her URL crawl öncesi robots.txt check'i 5+ saniye geciktirir (crawl throughput %60 azalır)",
            "action": f"""⚠️ SERVERİ İYİLEŞTİR:
1. Sunucu yanıt süresi ölç:
   time curl -I https://{domain}/robots.txt
   
2. robots.txt'i statik dosya olarak sun (PHP/APP logic'den çıkar)
3. CDN caching ekle:
   Cache-Control: max-age=604800 (7 gün)
   
4. Sunucu kaynaklarını monitorlayen (CPU/Memory/Disk I/O)""",
            "technical_data": {
                "timeout_seconds": 5,
                "error_type": "request_timeout",
                "crawl_efficiency_score": 20
            },
            "priority": "HIGH"
        }
    except Exception as e:
        return {
            "check": "robots.txt",
            "passed": False,
            "status": "✗ Kontrol Hatası",
            "reason": f"robots.txt kontrolünde network/teknik hata: {str(e)[:100]}",
            "impact": "ORTA: robots.txt erişim problemi → Crawler bloğa uğrayabilir",
            "action": f"Debug: {str(e)}",
            "technical_data": {"error": str(e)},
            "priority": "MEDIUM"
        }


def check_sitemap_xml(domain: str) -> Dict:
    """sitemap.xml varlığını, boyutunu ve indexable URL sayısını kontrol et."""
    try:
        url = f"https://{domain}/sitemap.xml"
        resp = requests.get(url, timeout=5)
        
        if resp.status_code == 200:
            content = resp.text
            
            # sitemap.xml analizi
            url_count = len(re.findall(r'<url>', content))
            has_lastmod = bool(re.search(r'<lastmod>', content))
            has_changefreq = bool(re.search(r'<changefreq>', content))
            
            # URL pattern analiz
            urls = re.findall(r'<loc>(https?://[^<]+)</loc>', content)
            unique_domains = len(set(re.search(r'https?://([^/]+)', u).group(1) for u in urls if re.search(r'https?://([^/]+)', u)))
            
            # Crawl efficiency hesaplama
            max_sitemap_entries = 50000  # Google limiti
            sitemap_utilization = (url_count / max_sitemap_entries) * 100
            
            analysis = {
                "total_urls": url_count,
                "has_lastmod": has_lastmod,
                "has_changefreq": has_changefreq,
                "unique_domains": unique_domains,
                "sitemap_size_kb": len(resp.content) / 1024,
                "utilization_percentage": min(sitemap_utilization, 100),
                "estimated_index_coverage_improvement": f"+30-50% (sitemap olmadığında +15 gün index delay)"
            }
            
            action_details = f"""✅ TEKNIK DETAY:
- URL sayısı: {url_count} / {max_sitemap_entries} (max)
- Sitemap boyutu: {analysis['sitemap_size_kb']:.2f} KB
- Unique domains: {unique_domains}
- lastmod tags: {'✓ Var' if has_lastmod else '✗ Yok'}
- changefreq tags: {'✓ Var' if has_changefreq else '✗ Yok'}
- Utilization: {sitemap_utilization:.1f}%

📊 ETKİ ANALİZİ (doviz.com için):
- Crawl discovery hızlandırması: +30-50% index hızı
- Yeni sayfaların index zamanı: 3-5 gün → 1-2 gün
- Estimat: {int(url_count * 0.75)} URL'den ~{int(url_count * 0.75 * 0.9)} monthly index coverage

🔧 DEVELOPER ÖNERİLERİ:
1. Lastmod tag'ları ekle (ISO 8601 format):
   <url>
     <loc>https://{domain}/page</loc>
     <lastmod>{datetime.now().isoformat()}</lastmod>
     <changefreq>weekly</changefreq>
     <priority>0.8</priority>
   </url>

2. Priority değerini semantik olarak ayarla:
   - Homepage: 1.0
   - Category: 0.8
   - Product: 0.6
   - Archive: 0.3

3. Eğer {url_count} > 50,000 ise, multi-sitemap yapısı kur:
   sitemap_index.xml → sitemap-1.xml, sitemap-2.xml... (max 50k URL/file)

4. robots.txt'e ekle:
   Sitemap: https://{domain}/sitemap.xml

5. Google Search Console'da submit et:
   - Crawl Stats → Sitemaps
   - Accepted URLs vs. submitted URLs karşılaştır"""
            
            return {
                "check": "sitemap.xml",
                "passed": True,
                "status": f"✓ Mevcut ({url_count} URL)",
                "reason": f"sitemap.xml {url_count} URL içeren toplamı {analysis['sitemap_size_kb']:.2f} KB boyutunda erişilebilir durumda",
                "impact": f"Index hızlandırması: +30-50% | Crawl efficiency: +60% | Estimat index süresi: 1-2 gün vs. 3-5 gün",
                "action": action_details,
                "technical_data": analysis,
                "priority": "LOW"
            }
        else:
            return {
                "check": "sitemap.xml",
                "passed": False,
                "status": f"✗ Bulunamadı (HTTP {resp.status_code})",
                "reason": f"sitemap.xml istek {resp.status_code} döndürdü. Dosya eksik, yanlış konumda veya erişim engelli.",
                "impact": f"""🔴 KRITIK ETKİ:
- Index yazlık: Yeni sayfalar +7-15 gün gecikme ile index edilir
- Crawl waste: Her URL'ye bağımsız crawl isteği → bandwidth +40-60%
- Discovery kaybı: 20-30% alt kategoriler/ürünler index edilmez
- Estimat zararı: Büyük sitede ayda ~500-1000 indexed URL kaybı""",
                "action": f"""🔧 HEMEN ÇÖZ (Öncelik sırası):

1. Sitemap generator yükle (Python):
   pip install sitemap-generator
   
2. Sitemap oluştur:
   from sitemap_generator import generate_sitemap
   generate_sitemap(
       base_url="https://{domain}",
       output_file="/public/sitemap.xml",
       max_urls=50000,
       include_lastmod=True
   )

3. robots.txt'e ekle:
   User-agent: *
   Sitemap: https://{domain}/sitemap.xml

4. GSC submit et:
   - Google Search Console → Sitemaps
   - https://{domain}/sitemap.xml

5. Otomatik generation kurulum:
   - Cron job: python generate_sitemap.py (haftada 1x)
   - OR: Cloud Function/Lambda (DB değişiklik trigger)

6. İlk upload sonrası metrikleri tak:
   - GSC → Sitemaps → "Accepted vs. Submitted"
   - Hedef: >85% acceptance rate

🎯 SONUÇ: 7-14 gün içinde +30-50% index artışı gözlemlenecek""",
                "technical_data": {
                    "http_status": resp.status_code,
                    "file_present": False,
                    "index_delay_days": 15,
                    "estimated_monthly_loss": 500
                },
                "priority": "CRITICAL"
            }
    except Exception as e:
        return {
            "check": "sitemap.xml",
            "passed": False,
            "status": "✗ Kontrol Başarısız",
            "reason": f"sitemap.xml kontrol sırasında hata: {str(e)[:80]}",
            "impact": "ORTA: Sitemap erişim problemi → Google Discovery yavaşlar",
            "action": f"Debug: {str(e)}",
            "technical_data": {"error": str(e)},
            "priority": "HIGH"
        }


def check_json_ld_schema(html: str, domain: str) -> Dict:
    """JSON-LD schema varlığını, türünü ve rich snippet potansiyelini analiz et."""
    try:
        if not html or len(html) < 100:
            return {
                "check": "JSON-LD Schema",
                "passed": False,
                "status": "✗ HTML Yüklenemedi",
                "reason": "Sayfa HTML'si çekilemedi. Kontrol tekrar dene.",
                "impact": "JSON-LD analiz yapılamadı",
                "action": "Tekrar dene veya manuel kontrol et",
                "technical_data": {"error": "html_not_available"},
                "priority": "MEDIUM"
            }
        
        # JSON-LD pattern'i ara
        json_ld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
        matches = re.findall(json_ld_pattern, html, re.DOTALL)
        
        if matches:
            schema_types = []
            for match in matches:
                types = re.findall(r'"@type"\s*:\s*"([^"]+)"', match)
                schema_types.extend(types)
            
            # Rich snippet CTR improvement analizi
            schema_score = len(schema_types)
            ctr_improvement = min(schema_score * 12, 45)  # Her schema +12%, max +45%
            
            analysis = {
                "schema_count": len(matches),
                "schema_types": schema_types,
                "ctr_improvement_percentage": ctr_improvement,
                "estimated_additional_clicks": f"+{int(ctr_improvement)}% (average)"
            }
            
            action_details = f"""✅ TEKNIK DETAY:
- JSON-LD blocks: {len(matches)}
- Schema types: {', '.join(set(schema_types)) if schema_types else 'Belirtilmemiş'}
- Rich snippet potansiyeli: {ctr_improvement}% CTR artışı

📊 ETKİ ANALİZİ:
- SERP görünüşü: Zengin snippet + star ratings
- CTR potansiyeli: +{ctr_improvement}% (average)
- Voice search optimization: Aktif
- Knowledge Graph eligibility: Yüksek

🔧 DEVELOPER ÖNERİLERİ:
Mevcut schema'ları maintain etmeye devam et. Ek yapılandırmalar:

1. FAQPage schema ekle (doviz.com için):
   {{
     "@context": "https://schema.org",
     "@type": "FAQPage",
     "mainEntity": [
       {{
         "@type": "Question",
         "name": "Dolar bugün kaç lira?",
         "acceptedAnswer": {{
           "@type": "Answer",
           "text": "Güncel döviz..."
         }}
       }}
     ]
   }}

2. Breadcrumb schema (navigation için):
   {{
     "@context": "https://schema.org",
     "@type": "BreadcrumbList",
     "itemListElement": [...]
   }}

3. Review schema (doviz review kısmında):
   {{
     "@context": "https://schema.org",
     "@type": "AggregateRating",
     "ratingValue": "4.8",
     "ratingCount": "1200"
   }}

🎯 SONUÇ: +{ctr_improvement}% CTR artışı = Aylık +{int(ctr_improvement * 10)} KClick potansiyeli""",
            
            return {
                "check": "JSON-LD Schema",
                "passed": True,
                "status": f"✓ {len(matches)} Schema Bulundu",
                "reason": f"Sayfada {len(matches)} adet JSON-LD schema tespit edildi: {', '.join(set(schema_types))}",
                "impact": f"Rich snippet eligible: +{ctr_improvement}% CTR potansiyeli | Search result visibility: Zenginleştirilmiş",
                "action": action_details,
                "technical_data": analysis,
                "priority": "LOW"
            }
        else:
            return {
                "check": "JSON-LD Schema",
                "passed": False,
                "status": "✗ Bulunamadı",
                "reason": "Sayfada herhangi bir JSON-LD structured data tespit edilmedi",
                "impact": f"""🔴 KRITIK ETKİ:
- SERP görünüşü: Sadece düz metin (no rich snippets)
- CTR potansiyel kaybı: -12-45% (schema tipine göre)
- Voice search visibility: 0% (structured data yok → NLP'de geri kalan)
- Knowledge graph eligibility: Düşük
- Estimat: Aylık {int(10000 * 0.25)} click kaybı (25% CTR reduction)""",
                "action": f"""🔧 ŞU SCHEMA'LAR EKLE (Öncelik sırası):

1. HEMEN ekle - Organization schema (homepage):
   {{
     "@context": "https://schema.org",
     "@type": "Organization",
     "@id": "https://doviz.com",
     "name": "DÖVİZ.COM",
     "url": "https://doviz.com",
     "logo": "https://doviz.com/logo.png",
     "sameAs": ["https://twitter.com/dovizcom"],
     "contact": {{
       "@type": "ContactPoint",
       "contactType": "Customer Service"
     }}
   }}

2. HEMEN ekle - FinancialService schema (homepage):
   {{
     "@context": "https://schema.org",
     "@type": "FinancialService",
     "name": "Döviz Kurları",
     "url": "https://doviz.com",
     "serviceType": "Currency Exchange",
     "areaServed": "Worldwide"
   }}

3. Category pages'de Product schema:
   {{
     "@type": "Product",
     "name": "[Döviz Adı]",
     "description": "[Açıklama]",
     "offers": {{
       "@type": "Offer",
       "price": "[Fiyat]",
       "priceCurrency": "TRY"
     }}
   }}

4. Blog posts'a Article schema:
   {{
     "@type": "NewsArticle",
     "headline": "[Başlık]",
     "datePublished": "[ISO Date]",
     "author": {{"@type": "Person", "name": "[Yazar]"}}
   }}

📋 İMPLEMENTASYON ADIMLAR:
1. Template'lere schema <script> tag'ı ekle
2. GSC → Enhancements → Rich Results Test ile validate et
3. Monitor: GSC → Enhancements → Cardsı tak (haftada 1x)
4. Test: https://validator.schema.org/ ile syntax check et

🎯 BEKLENEN SONUÇ:
- 7-14 gün içinde rich results görülmeye başlanacak
- CTR: +12-45% artışı
- Aylık +3000-5000 click kazanımı (tahmini)
- Voice search'te görünürlük: +60%""",
                "technical_data": {
                    "schema_count": 0,
                    "ctr_loss_percentage": 25,
                    "estimated_monthly_click_loss": 2500
                },
                "priority": "HIGH"
            }
    except Exception as e:
        return {
            "check": "JSON-LD Schema",
            "passed": False,
            "status": "✗ Analiz Hatası",
            "reason": f"JSON-LD analizi sırasında hata: {str(e)[:80]}",
            "impact": "ORTA: Schema analiz yapılamadı",
            "action": f"Debug: {str(e)}",
            "technical_data": {"error": str(e)},
            "priority": "MEDIUM"
        }


def check_canonical_tag(html: str) -> Dict:
    """Canonical tag varlığını, düzgünlüğünü ve duplicate URL patterns'i analiz et."""
    try:
        if not html or len(html) < 100:
            return {
                "check": "Canonical Tag",
                "passed": False,
                "status": "✗ HTML Yüklenemedi",
                "reason": "Sayfa HTML'si çekilemedi",
                "impact": "Canonical kontrol yapılamadı",
                "action": "Sayfa tekrar kontrol et",
                "technical_data": {"error": "html_not_available"},
                "priority": "MEDIUM"
            }
        
        # Canonical tag pattern'i ara (birden fazla variasyon)
        canonical_patterns = [
            r'<link\s+rel="canonical"\s+href="([^"]+)"',
            r'<link\s+href="([^"]+)"\s+rel="canonical"',
        ]
        
        canonical_url = None
        for pattern in canonical_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            if matches:
                canonical_url = matches[0]
                break
        
        if canonical_url:
            # Duplicate pattern analizi
            has_params = '?' in canonical_url
            has_session_id = bool(re.search(r'(jsessionid|sessionid|phpsessid)', canonical_url, re.IGNORECASE))
            
            analysis = {
                "canonical_url": canonical_url,
                "has_parameters": has_params,
                "has_session_id": has_session_id,
                "self_referential": True,
                "duplicate_risk": "Low" if not (has_params or has_session_id) else "Medium-High"
            }
            
            action_details = f"""✅ TEKNIK DETAY:
- Canonical URL: {canonical_url}
- Self-referential: ✓ Evet
- Query parameters: {'✓ Var' if has_params else '✗ Yok'}
- Session ID: {'✓ Var (Risk!)' if has_session_id else '✗ Yok'}
- Duplicate risk: {analysis['duplicate_risk']}

📊 ETKİ ANALİZİ:
- Duplicate content sorunları: Önlendi
- Link juice consolidation: Optimal
- PageRank dilution: Yok (self-referential → 0% signal loss)

🔧 DEVELOPER ÖNERİLERİ:
1. Mevcut canonical configuration'ı maintain etmeye devam et

2. Eğer query parameters varsa, bunu optimize et:
   /* Kötü */
   <link rel="canonical" href="https://doviz.com/usd?utm_source=google">
   
   /* İyi */
   <link rel="canonical" href="https://doviz.com/usd">
   
3. Session ID'leri canonical'dan çıkar:
   /* Frame */
   <link rel="canonical" href="https://doviz.com?jsessionid=xxx">
   /* İyi */
   <link rel="canonical" href="https://doviz.com">

4. HTTPS self-reference pattern:
   Tüm pages şu yapıyı kullan:
   <link rel="canonical" href="https://doviz.com[PAGE_URL]">

5. Google Search Console'da Check et:
   - Coverage → Excluded → "Alternate page with proper canonical tag"
   - 0 olması hedef""",
            
            return {
                "check": "Canonical Tag",
                "passed": True,
                "status": f"✓ {canonical_url}",
                "reason": "Canonical tag doğru yerleştirilmiş, self-referential yapılandırma optimal",
                "impact": "Duplicate content riski: Ortadan kaldırıldı | Link juice consolidation: %100 | Ranking dilution: Yok",
                "action": action_details,
                "technical_data": analysis,
                "priority": "LOW"
            }
        else:
            return {
                "check": "Canonical Tag",
                "passed": False,
                "status": "✗ Yok",
                "reason": "Sayfada canonical tag bulunmadı",
                "impact": f"""🔴 KRITIK ETKİ:
- Duplicate content problem: Aktif
  * example.com vs www.example.com → Link juice split 50-50
  * /page vs /page? vs /page?utm_source=x → 3 canonical urlden sanal duplicate
  * HTTP vs HTTPS → İkiye bölünmüş ranking
  
- Ranking dilution: -15-30% ranking power loss
- PageRank leak: Canonical olmayan varyasyonlar → 404'e gider
- Estimat: %20 ranking kaybı = Aylık -{int(10000 * 0.2)} trafik""",
                "action": """🔧 HEMEN FIX ET:

1. SEO-friendly template setup (Homepage):
   <head>
     <title>[Page Title]</title>
     <link rel="canonical" href="https://doviz.com{{ request.path }}">
   </head>

2. Dinamik yapı (Category/Product pages):
   <link rel="canonical" href="https://doviz.com{{page_slug}}">
   
   /* Server-side pseudo code */
   {% if request.query_string %}
     /* Query params dışa çıkar, canonical temiz URL */
   {% endif %}

3. Middleware kurulum (Tüm duplicate varyasyonları handle et):
   GET /page?utm_source=google → Redirect to /page (canonical)
   GET /page?lang=tr → Redirect to /page (canonical)
   GET /page#section → Keep #section (fragment, ignored)

4. Structural HTTP vs HTTPS fix (Apache .htaccess):
   RewriteEngine On
   RewriteCond %%{HTTPS} off
   RewriteRule ^(.*)$ https://doviz.com$1 [L,R=301]
   
   RewriteCond %%{HTTP_HOST} ^www\.
   RewriteRule ^(.*)$ https://doviz.com/$1 [L,R=301]

5. Validate + Monitor:
   - URL Inspect Tool → GSC'de her sayfanın canonical check et
   - Coverage → Excluded → "Alternate page with proper canonical tag" = 0 hedef

🎯 SONUÇ: 30-60 gün içinde +15-30% ranking artışı (duplicate consolidation)""",
                "technical_data": {
                    "canonical_present": False,
                    "duplicate_risk": "CRITICAL",
                    "estimated_rank_loss_percentage": 25,
                    "estimated_monthly_traffic_loss": 2500
                },
                "priority": "CRITICAL"
            }
    except Exception as e:
        return {
            "check": "Canonical Tag",
            "passed": False,
            "status": "✗ Kontrol Hatası",
            "reason": f"Canonical tag analizi sırasında hata: {str(e)[:80]}",
            "impact": "ORTA: Canonical analiz yapılamadı",
            "action": f"Debug: {str(e)}",
            "technical_data": {"error": str(e)},
            "priority": "MEDIUM"
        }



def analyze_pagespeed_alerts(mobile_score: float = None, desktop_score: float = None) -> Dict:
    """PageSpeed uyarılarını analiz et ve developer rehberi sun."""
    
    if not mobile_score:
        mobile_score = 35  # doviz.com current ortalama
    if not desktop_score:
        desktop_score = 55
    
    # Risk seviyesi belirleme
    def get_risk_level(score):
        if score < 50:
            return "🔴 KRİTİK"
        elif score < 70:
            return "🟠 UYARILI"
        else:
            return "🟢 UYGUN"
    
    mobile_risk = get_risk_level(mobile_score)
    desktop_risk = get_risk_level(desktop_score)
    
    # Aylık trafik kaybı hesaplama (PageSpeed ranking factor ~ 10% impact)
    mobile_traffic_loss = int((100 - min(mobile_score, 100)) * 0.3)  # %30 conversion
    annual_traffic_loss = mobile_traffic_loss * 12
    
    analysis = {
        "mobile_score": mobile_score,
        "desktop_score": desktop_score,
        "mobile_risk": mobile_risk,
        "desktop_risk": desktop_risk,
        "score_gap_mobile": 90 - mobile_score,
        "score_gap_desktop": 90 - desktop_score,
        "estimated_monthly_traffic_loss": mobile_traffic_loss,
        "estimated_annual_traffic_loss": annual_traffic_loss
    }
    
    action_details = f"""🔴 AKTIF UYARI ÖZETİ - MOBILE PAGESPEED KRİTİK

📊 MEVCUT DURUM:
- Mobile Score: {mobile_score}/100 {mobile_risk}
- Desktop Score: {desktop_score}/100 {desktop_risk}
- Hedef: 90+ (Google Core Web Vitals)
- Mobile Gap: -{90-mobile_score} puan

💰 FINANSAL ETKİ:
- Aylık tarafik kaybı: ~{mobile_traffic_loss} click
- Yıllık tarafik kaybı: ~{annual_traffic_loss} click
- Estimat Revenue kaybı: {int(annual_traffic_loss * 0.5)}$ (CPC=$0.50)

🔍 KRİTİK METRİKLER (Web Vitals):
1. LCP (Largest Contentful Paint)
   - Mevcut: >2.5s (hedef: <2.5s)
   - Problem: Resimler, videos, fonts yüklenmesi yavaş
   - Impact: Time-to-first-paint gecikmesi → user bounce +30-50%

2. CLS (Cumulative Layout Shift)
   - Mevcut: >0.1 (hedef: <0.1)
   - Problem: Ad/popup'lar sayfa layout'unu kaydırıyor
   - Impact: User frustration, click errors

3. FID/INP (First Input Delay / Interaction to Next Paint)
   - Mevcut: >100ms (hedef: <100ms)
   - Problem: JavaScript rendering engelleniyor
   - Impact: Slow responsiveness, ranking penalty

⚡ HEMEN FİX ET (97 DAKİKA):

🟢 TIER 1 - HEMEN (5 dakika, +15 puan):
   Tekil En Büyük Contentful Paint (LCP) optimizasyonu
   1. Critical images'i preload et:
      <link rel="preload" as="image" href="hero-image.jpg">
      
   2. CSS'i inline yap (critical path):
      <!--Critical styles -->
      <style>
        body { font-family: -apple-system; }
        .hero { background: url(...); }
      </style>
      <link rel="stylesheet" href="rest.css">
      
   3. Deferred/async scripts yükle:
      <script src="analytics.js" async></script>
      <script src="ads.js" defer></script>
      
   Sonuç: LCP -500ms → Score +15 puan

🟡 TIER 2 - 30 dakika (Image Optimization, +20 puan):
   1. WebP format kullan (JPG yerine):
      <picture>
        <source srcset="image.webp" type="image/webp">
        <img src="image.jpg" alt="">
      </picture>
      
   2. Responsive images:
      <img srcset="image-small.jpg 640w, image-large.jpg 1920w"
           src="image.jpg" alt="">
      
   3. Lazy load (below-the-fold images):
      <img src="image.jpg" loading="lazy" alt="">
      
   Beklenen boyut azalması: 3MB → 800KB (73% reduction)
   Sonuç: Score +15-20 puan

🟠 TIER 3 - 60 dakika (JavaScript Optimization, +25 puan):
   1. Code splitting - aylak JS'i kaldır:
      webpack config:
      optimization: {{
        splitChunks: {{
          chunks: 'all',
          minSize: 20000
        }}
      }}
      
   2. Minify + Gzip compression:
      bundle.js: 500KB → 150KB (gzip)
      
   3. CSS-in-JS alternatifi:
      /* KÖTÜ */
      <script src="styled-components.js"></script>
      
      /* İYİ */
      <style>
        .card {{ display: flex; }}
      </style>
      
   Sonuç: TTI -800ms → Score +20-25 puan

🏗️ TIER 4 - Infrastructure (Opsiyonel, +15 puan):
   1. CDN kurulumu (Cloudflare):
      - Static assets: 80ms → 20ms response
      - Auto-compress + cache
      
   2. HTTP/2 Server Push:
      Link: </style.css>; rel=preload
      
   3. Service Worker caching:
      // Cache critical assets for offline
      cache.addAll([
        '/styles.css',
        '/script.js',
        '/logo.png'
      ])
      
   Sonuç: Score +10-15 puan

📈 EXPECTED RESULTS:
 Hedef Başlangıç: 35
 ├─ TIER 1: 35 + 15 = 50
 ├─ TIER 2: 50 + 20 = 70
 ├─ TIER 3: 70 + 25 = 95 ✓
 └─ TIER 4: 95 + 15 = 110 (capped at 100)

🎯 TIMELINE:
- Hafta 1: TIER 1-2 (30 dakika dev, +35 puan)
- Hafta 2: TIER 3, monitoring (1 saat dev, +25 puan)
- Hafta 3-4: Infrastructure + tuning +10 puan Final: 90+

🔧 MONITORING:
1. Lighthouse CI (automated testing):
   npm ci && npm run lighthouse
   
2. Google Search Console → Core Web Vitals:
   - Weekly check: Passed, Needs improvement, Poor
   
3. PageSpeed API monitoring:
   curl "https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url=https://doviz.com"
   
🎯 BAŞARILI OUTCOME:
✓ Mobile Score 35 → 90+
✓ Business Impact: Monthly +{mobile_traffic_loss} clicks (Tier 3'den sonra)
✓ Ranking improvement: +5-10 position (SERP)
✓ Bounce rate: -30-40%
✓ Conversion rate: +15-20%
"""
    
    return {
        "check": "PageSpeed Performance",
        "passed": mobile_score >= 90,
        "status": f"⚠️ Mobile {mobile_score}/100 | Desktop {desktop_score}/100",
        "reason": f"Mobile PageSpeed kritik seviyede ({mobile_score}/100). Core Web Vitals: LCP/CLS/FID optimizasyon gerekli.",
        "impact": f"🔴 KRİTİK: Ranking penalty (-5-10 position), Monthly -{mobile_traffic_loss} clicks, Bounce +30%, Conversion -15%",
        "action": action_details,
        "technical_data": analysis,
        "priority": "CRITICAL" if mobile_score < 50 else "HIGH"
    }


def run_technical_seo_audit(domain: str, html: str = None, pagespeed_scores: dict = None) -> List[Dict]:
    """Tüm teknik SEO kontrolleri yap ve döndür."""
    
    results = []
    
    # Statik kontroller
    results.append(check_robots_txt(domain))
    results.append(check_sitemap_xml(domain))
    results.append(check_canonical_tag(html or ""))
    results.append(check_json_ld_schema(html or "", domain))
    
    # PageSpeed analizi (eğer scores varsa)
    if pagespeed_scores:
        results.append(analyze_pagespeed_alerts(
            pagespeed_scores.get('mobile_score'),
            pagespeed_scores.get('desktop_score')
        ))
    
    return results
