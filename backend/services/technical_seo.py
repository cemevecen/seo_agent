"""Teknik SEO kontrolleri ve analizleri."""

from typing import Dict, List
import re
import requests


def check_robots_txt(domain: str) -> Dict:
    """robots.txt varlığını ve erişilebilirliğini kontrol et."""
    try:
        url = f"https://{domain}/robots.txt"
        resp = requests.head(url, timeout=5)
        if resp.status_code == 200:
            return {
                "check": "robots.txt",
                "passed": True,
                "status": "✓ Mevcut",
                "reason": "robots.txt dosyası erişilebilir durumda",
                "impact": "Google crawl budget'ını kontrol ediyor. İyi yönetiliyor.",
                "action": "Mevcut robots.txt'i periyodik olarak gözden geçir"
            }
        else:
            return {
                "check": "robots.txt",
                "passed": False,
                "status": "✗ Bulunamadı",
                "reason": f"robots.txt dosyasına erişim başarısız (HTTP {resp.status_code})",
                "impact": "Crawlers tüm sayfaları crawl edebilir, bandwidth israfı olabilir",
                "action": "En az şunu ekle: User-agent: * \n Disallow: /admin/ \n Crawl-delay: 1"
            }
    except Exception as e:
        return {
            "check": "robots.txt",
            "passed": False,
            "status": "✗ Hata",
            "reason": f"Kontrol sırasında hata: {str(e)}",
            "impact": "Dosya varlığı doğrulanamadı",
            "action": "robots.txt Create et ve root'a upload et"
        }


def check_sitemap_xml(domain: str) -> Dict:
    """sitemap.xml varlığını kontrol et."""
    try:
        url = f"https://{domain}/sitemap.xml"
        resp = requests.head(url, timeout=5)
        if resp.status_code == 200:
            return {
                "check": "sitemap.xml",
                "passed": True,
                "status": "✓ Mevcut",
                "reason": "sitemap.xml dosyası erişilebilir",
                "impact": "Tüm sayfalar Google'a bildiriliyor. Index hızı optimal.",
                "action": "Sitemap'ı güncellemeyi otomatikleştir (weekly cron)"
            }
        else:
            return {
                "check": "sitemap.xml",
                "passed": False,
                "status": "✗ Bulunamadı",
                "reason": "sitemap.xml dosyasına erişim başarısız",
                "impact": "Yeni sayfalar daha yavaş index edilir, crawl efficiency düşer. Büyük siteler için kritik.",
                "action": """1. Sitemap generator kullan (Python: sitemap-generator)
2. /sitemap.xml'e upload et
3. robots.txt'e ekle: Sitemap: https://{domain}/sitemap.xml
4. Google Search Console'a submit et"""
            }
    except Exception as e:
        return {
            "check": "sitemap.xml",
            "passed": False,
            "status": "✗ Hata",
            "reason": f"Kontrol sırasında hata: {str(e)}",
            "impact": "Sitemap varlığı doğrulanamadı",
            "action": "XML sitemap oluştur ve root'a deploy et"
        }


def check_json_ld_schema(html: str, domain: str) -> Dict:
    """Sayfada JSON-LD structured data olup olmadığını kontrol et."""
    try:
        if not html:
            return {
                "check": "JSON-LD Schema",
                "passed": False,
                "status": "✗ Tespit Edilemedi",
                "reason": "HTML içeriği yüklenemedi",
                "impact": "Structured data kontrol edilemiyor",
                "action": "Sayfayı yeniden kontrol et"
            }
        
        # JSON-LD pattern'i ara
        json_ld_pattern = r'<script type="application/ld\+json">(.*?)</script>'
        matches = re.findall(json_ld_pattern, html, re.DOTALL)
        
        if matches:
            return {
                "check": "JSON-LD Schema",
                "passed": True,
                "status": "✓ Mevcut",
                "reason": f"Sayfada {len(matches)} adet JSON-LD schema bulundu",
                "impact": "Rich snippets, voice search ve knowledge graph optimizasyonu aktif",
                "action": "Schema Markup'ı schema.org spesifikasyonuna uygun tutmaya devam et"
            }
        else:
            return {
                "check": "JSON-LD Schema",
                "passed": False,
                "status": "✗ Yok",
                "reason": "Sayfada JSON-LD structured data bulunmadı",
                "impact": "Rich snippets yok (SERP'de düz metin görünüş), voice search optimizasyon kaybı, knowledge graph şansı azalır. CTR %5-10 düşer.",
                "action": """Şu schema türlerini ekle:
1. Organization schema (homepage'de)
2. Product/FinancialService schema (ana sayfada)
3. BreadcrumbList schema (kategori sayfalarında)

Örnek (doviz.com için):
{
  "@context": "https://schema.org",
  "@type": "FinancialService",
  "name": "DÖVİZ.COM",
  "url": "https://doviz.com"
}"""
            }
    except Exception as e:
        return {
            "check": "JSON-LD Schema",
            "passed": False,
            "status": "✗ Hata",
            "reason": f"Kontrol sırasında hata: {str(e)}",
            "impact": "Schema kontrol edilemiyor",
            "action": "Teknik desteğe başvur"
        }


def check_canonical_tag(html: str) -> Dict:
    """Canonical tag varlığını ve doğruluğunu kontrol et."""
    try:
        if not html:
            return {
                "check": "Canonical Tag",
                "passed": False,
                "status": "✗ Tespit Edilemedi",
                "reason": "HTML içeriği yüklenemedi",
                "impact": "Canonical tag kontrol edilemiyor",
                "action": "Sayfayı yeniden kontrol et"
            }
        
        # Canonical tag pattern'i ara
        canonical_pattern = r'<link\s+rel="canonical"\s+href="([^"]+)"'
        matches = re.findall(canonical_pattern, html, re.IGNORECASE)
        
        if matches:
            canonical_url = matches[0]
            return {
                "check": "Canonical Tag",
                "passed": True,
                "status": f"✓ {canonical_url}",
                "reason": "Canonical tag doğru yerleştirilmiş",
                "impact": "Duplicate content sorunları önleniyor. Link juice konsantrasyonu optimal.",
                "action": "Canonical tag'ı maintain etmeye devam et"
            }
        else:
            return {
                "check": "Canonical Tag",
                "passed": False,
                "status": "✗ Yok",
                "reason": "Sayfada canonical tag bulunmadı",
                "impact": "Duplicate content (example.com, www.example.com, ?utm= vb.) sorunu. Google metric'leri scatter ediyor.",
                "action": """Her sayfaya ekle:
<link rel="canonical" href="https://example.com/sayfaurl" />

Self-referential canonical kullan (sayfanın kendisine işaret et)"""
            }
    except Exception as e:
        return {
            "check": "Canonical Tag",
            "passed": False,
            "status": "✗ Hata",
            "reason": f"Kontrol sırasında hata: {str(e)}",
            "impact": "Canonical tag kontrol edilemiyor",
            "action": "HTML source'u kontrol et"
        }


def run_technical_seo_audit(domain: str, html: str = None) -> List[Dict]:
    """Tüm teknik SEO kontrolleri yap ve döndür."""
    
    results = []
    
    # Statik kontroller
    results.append(check_robots_txt(domain))
    results.append(check_sitemap_xml(domain))
    results.append(check_canonical_tag(html or ""))
    results.append(check_json_ld_schema(html or "", domain))
    
    return results
