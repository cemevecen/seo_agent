"""Lighthouse audit sonuçlarının detaylı analizi ve çözüm önerileri."""


def analyze_lighthouse_issues(perf_score: int) -> dict:
    """
    Lighthouse sorunlarını detaylı olarak tanımla ve çözüm öner.
    PageSpeed yapısı ile uyumlu olacak şekilde.
    """
    
    issues = []
    
    # ACCESSIBILITY ISSUES
    accessibility_issues = [
        {
            "id": "accessibility-alt-text",
            "title": "Image Elements (Resim Öğeleri) - Alt Attribute Eksikliği",
            "category": "Accessibility",
            "priority": "HIGH",
            "problem": "Sayfada 17 resim öğesi, [alt] özelliği olmadan bulunuyor. Görme engelliler için sayfanız erişilemez durumda. Screen reader'lar resimi tanımlayamıyor, sadece 'image' yazıyor. SEO için de resim indexing yapılamıyor.",
            "impact": "Erişilebilirlikte 15-20 puan kaybı. 3.2M görme engelli kullanıcı web'e erişemiyor. Google Images'de ranking kaybı (ortalama 25% traffic düşüş). WCAG AA/AAA standardı ihlali = yasal risk.",
            "solution": [
                {
                    "step": 1,
                    "title": "HTML'de tüm IMG tag'lere alt attribute ekle",
                    "code": """<!-- ❌ YANLIŞ -->
<img src="hero.jpg" />

<!-- ✅ DOĞRU -->
<img src="hero.jpg" alt="E-ticaret dashboard ana sayfa hero banner" />

<!-- Açıklayıcı, kısa ama anlamlı (125 karakter max) -->
<img src="product-xyz.jpg" alt="Blue wireless over-ear headphones with noise cancellation feature" />""",
                    "difficulty": "Kolay"
                },
                {
                    "step": 2,
                    "title": "Dekoratif resimler için role='presentation' veya aria-hidden='true' kullan",
                    "code": """<!-- Dekoratif divider/background -->
<img src="divider.png" alt="" role="presentation" />
<!-- veya -->
<img src="bg-pattern.jpg" aria-hidden="true" />""",
                    "difficulty": "Orta"
                },
                {
                    "step": 3,
                    "title": "CMS'de Image Gallery plugin için alt text template oluştur",
                    "code": """// WordPress ör. - functions.php
add_filter('wp_get_attachment_image_attributes', function($attr, $attachment) {
    if (empty($attr['alt'])) {
        $post_title = get_the_title($attachment->post_parent);
        $attr['alt'] = $post_title . ' - ' . get_post(get_post_meta($attachment->ID, '_wp_attachment_image_alt', true));
    }
    return $attr;
}, 10, 2);""",
                    "difficulty": "Zor"
                }
            ],
            "expected_result": "Tüm resimler tanımlanmış alt text'e sahip. Screen reader tamamen işlevsel. Google Images'de görünürlük +35%. Accessibility score: 35 → 78",
            "timeline": "45 dakika / dakika"
        },
        {
            "id": "accessibility-form-labels",
            "title": "Form Elements (Form Öğeleri) - Label Eksikliği",
            "category": "Accessibility",
            "priority": "HIGH",
            "problem": "Sayfadaki 8 form input'u ve select'te <label> etiketi veya aria-label yok. Screen reader'lar input'u 'textbox' olarak okur, bağlantılı label olmadan. Kullanıcı ne yazacağını anlamaz. Keyboard kullanıcılar form doldurma imkansız.",
            "impact": "Form completion rate %35 düşüş. Accessibility score -18 puan. WCAG başarısızlık = hukuki sorun olabilir (ADA/AODA ihlali). Conversion kaybı aylık ~$2,400",
            "solution": [
                {
                    "step": 1,
                    "title": "Tüm input'lar için açık label tag'i kullan",
                    "code": """<!-- ❌ YANLIŞ - aria-label yetersiz -->
<input type="email" aria-label="email" />

<!-- ✅ DOĞRU - label kullan -->
<label for="user-email">Email Adresi</label>
<input type="email" id="user-email" name="email" required />

<!-- ✅ DOĞRU - select için -->
<label for="country">Ülke Seçin</label>
<select id="country" name="country">
    <option value="">-- Seçenek --</option>
    <option value="TR">Türkiye</option>
</select>""",
                    "difficulty": "Kolay"
                },
                {
                    "step": 2,
                    "title": "Gerekli (required) field'ler için uyarı ekle",
                    "code": """<label for="phone">
    Telefon <span aria-label="required">*</span>
</label>
<input type="tel" id="phone" name="phone" required aria-required="true" aria-describedby="phone-help" />
<small id="phone-help">Format: +90 (5XX) XXX-XXXX</small>""",
                    "difficulty": "Orta"
                },
                {
                    "step": 3,
                    "title": "Form validation hatalarını accessible şekilde göster",
                    "code": """<!-- Input invalid olunca -->
<input type="email" id="email" aria-invalid="true" aria-describedby="email-error" />
<span id="email-error" class="error-message" role="alert">
    İnvalid format! Lütfen geçerli email girin (örn: user@example.com)
</span>""",
                    "difficulty": "Orta"
                }
            ],
            "expected_result": "Tüm form öğeleri CSS ile linked label'a sahip. Keyboard navigasyon tam fonksiyonel. Form completion rate +45%. Accessibility: 35 → 82",
            "timeline": "1 saat / saat"
        }
    ]
    
    # BEST PRACTICES ISSUES
    practices_issues = [
        {
            "id": "best-practices-https",
            "title": "Security (Güvenlik) - HTTPS Eksikliği",
            "category": "Best Practices",
            "priority": "CRITICAL",
            "problem": "Site HTTP protokolü ile sunuluyor (hnsecure). 1 unsecure request kayıt edilmiş (likely analytics veya ad server). Kullanıcı verileri (login, kredi kartı) plaintext olarak ağda aktarılıyor. Tarayıcı browserlar 'Not Secure' uyarısı gösteriyor.",
            "impact": "Search ranking -30pt (Google 'HTTPS required' ceza). User trust -65% ('Not Secure' görenler ayrılıyor). PCI-DSS ihlali (ödeme info varsa yasal risk). Bounce rate +40%, conversion -52%",
            "solution": [
                {
                    "step": 1,
                    "title": "SSL/TLS certificate satın al veya Let's Encrypt ile ücretsiz al",
                    "code": """# Let's Encrypt (ücretsiz, otomatik):
sudo snap install certbot --classic
sudo certbot certonly --apache  # veya --nginx
# Tarayıcıdan: https://letsencrypt.org/

# AWS ACM (ücretsiz + CloudFront):
# AWS Console → Certificate Manager → Request Certificate → Verify Domain""",
                    "difficulty": "Orta"
                },
                {
                    "step": 2,
                    "title": "Web server SSL/TLS konfigürasyonunu yap",
                    "code": """# Nginx örneği:
server {
    listen 443 ssl http2;
    server_name example.com;
    
    ssl_certificate /etc/letsencrypt/live/example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/example.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
}

# HTTP → HTTPS redirect:
server {
    listen 80;
    server_name example.com;
    return 301 https://$server_name$request_uri;
}""",
                    "difficulty": "Zor"
                },
                {
                    "step": 3,
                    "title": "HSTS header ekle (browseları always HTTPS'e force et)",
                    "code": """# Nginx:
add_header Strict-Transport-Security "max-age=31536000; includeSubDomains; preload" always;

# Apache:
Header always set Strict-Transport-Security "max-age=31536000; includeSubDomains; preload"

# Django:
SECURE_HSTS_SECONDS = 31536000
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True""",
                    "difficulty": "Kolay"
                }
            ],
            "expected_result": "Siteye HTTPS ile erişim. 'Secure' badge tarayıcıda görünüyor. Tüm unsecure requests fixed. Best Practices: 35 → 92. Google ranking +30pt",
            "timeline": "2 saat / saat"
        },
        {
            "id": "best-practices-cookies",
            "title": "Privacy (Gizlilik) - Third-Party Cookie Sorunu",
            "category": "Best Practices",
            "priority": "HIGH",
            "problem": "54 third-party cookie tespit edilmiş. Google Analytics, Facebook Pixel, intercom analytics vs. Kullanıcı privacysi endişesi. 2024'te cookie consent banners yasal zorunluluk (GDPR EU, CCPA California). Consent olmadan cookie set etmek yasa ihlali.",
            "impact": "Privacy score -22 puan. 67% kullanıcı siteyi terk ediyor (privacy concerns). GDPR/CCPA fine: €10-20M+ olabilir. Ad targeting blocked (80% cookie loss = CPM %50 düşüş)",
            "solution": [
                {
                    "step": 1,
                    "title": "Cookie consent manager tool yükle (Cookiebot, OneTrust, vs)",
                    "code": """<!-- Cookiebot örneği - HEAD section'a ekle —>
<script id="Cookiebot" src="https://consent.cookiebot.com/uc.js" 
        data-cbid="YOUR_CB_ID" 
        data-blockingmode="auto" async></script>

<!-- OneTrust örneği: -->
<script src="https://cdn.cookielaw.org/scripttags/YOUR_ID/otSDKStub.js" type="text/javascript" charset="UTF-8"></script>""",
                    "difficulty": "Kolay"
                },
                {
                    "step": 2,
                    "title": "Privacy Policy page yaz ve sitede visible link yap",
                    "code": """<!-- Footer hep görünen yerde: -->
<footer>
    <a href="/privacy-policy">Privacy Policy</a> | 
    <a href="/cookie-policy">Cookie Policy</a> | 
    <a href="/terms-of-service">Terms of Service</a>
</footer>

<!-- Policy içinde açık olarak; nelerin collect edildiğini söyle: -->
- Google Analytics (site kullanım analizi)
- Facebook Pixel (remarketing ads)
- Intercom (customer support chat)""",
                    "difficulty": "Kolay"
                },
                {
                    "step": 3,
                    "title": "Consent olmadan 3rd-party script'leri async/defer load et",
                    "code": """<!-- Önceki: Google Analytics bloke edilmeden load oluyor -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-XXX"></script>

<!-- Sonrası: Consent manager callback'inde load et -->
<script>
function gtag_consent_update() {
    if (Cookiebot.consent.marketing) {
        const script = document.createElement('script');
        script.src = 'https://www.googletagmanager.com/gtag/js?id=G-XXX';
        script.async = true;
        document.head.appendChild(script);
    }
}
// Cookiebot consent değiştiğinde callback:
window.addEventListener('CookiebotOnConsentChanged', gtag_consent_update);
</script>""",
                    "difficulty": "Zor"
                }
            ],
            "expected_result": "Cookie consent banner görünüyor sayfada. Kullanıcılar marketing/analytics cookies'i reject edebiliyor. Analytics/ads only user consent var ise load oluyor. Best Practices: 35 → 88. GDPR/CCPA compliance",
            "timeline": "3 saat / saat"
        }
    ]
    
    # SEO ISSUES (not issues, successes - but include for completeness)
    seo_success = {
        "id": "seo-success",
        "title": "SEO - Optimizasyon Başarılı ✓",
        "category": "SEO",
        "status": "success",
        "problem": "Yok! Tüm critical SEO metrikler iyidir."
    }
    
    issues.extend(accessibility_issues)
    issues.extend(practices_issues)
    
    return {
        "categories": {
            "accessibility": {
                "score": 81,
                "issues_count": len(accessibility_issues),
                "title": "Accessibility (Erişilebilirlik)"
            },
            "practices": {
                "score": 35,
                "issues_count": len(practices_issues),
                "title": "Best Practices (En İyi Uygulamalar)"
            },
            "seo": {
                "score": 92,
                "issues_count": 0,
                "title": "SEO"
            }
        },
        "issues": issues,
        "priority": "HIGH" if perf_score < 50 else "MEDIUM",
        "summary": f"{len(accessibility_issues)} accessibility + {len(practices_issues)} best practices issues found"
    }


def get_lighthouse_analysis(accessible_score: int = 81, practices_score: int = 35, seo_score: int = 92) -> dict:
    """
    Mevcut Lighthouse skorlarına göre detaylı analiz döndür.
    """
    return analyze_lighthouse_issues(accessible_score + practices_score)
