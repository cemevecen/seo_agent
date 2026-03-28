"""PageSpeed Insights collector'ı."""

from __future__ import annotations

import json
import logging
import re
import socket
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import PageSpeedAuditSnapshot, Site
from backend.services.alert_engine import emit_custom_alert, evaluate_site_alerts
from backend.services.metric_store import get_latest_metrics, save_metrics
from backend.services.quota_guard import consume_api_quota
from backend.services.warehouse import (
    finish_collector_run,
    save_lighthouse_audit_records,
    save_pagespeed_payload_snapshot,
    start_collector_run,
)

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
LOGGER = logging.getLogger(__name__)

STRATEGY_METRIC_MAP = {
    "mobile": {
        "performance_score": "pagespeed_mobile_score",
        "accessibility_score": "pagespeed_mobile_accessibility_score",
        "best_practices_score": "pagespeed_mobile_best_practices_score",
        "seo_score": "pagespeed_mobile_seo_score",
        "lcp": "pagespeed_mobile_lcp",
        "fcp": "pagespeed_mobile_fcp",
        "ttfb": "pagespeed_mobile_ttfb",
        "cls": "pagespeed_mobile_cls",
        "inp": "pagespeed_mobile_inp",
    },
    "desktop": {
        "performance_score": "pagespeed_desktop_score",
        "accessibility_score": "pagespeed_desktop_accessibility_score",
        "best_practices_score": "pagespeed_desktop_best_practices_score",
        "seo_score": "pagespeed_desktop_seo_score",
        "lcp": "pagespeed_desktop_lcp",
        "fcp": "pagespeed_desktop_fcp",
        "ttfb": "pagespeed_desktop_ttfb",
        "cls": "pagespeed_desktop_cls",
        "inp": "pagespeed_desktop_inp",
    },
}


def _normalize_url(domain: str) -> str:
    # API çağrıları için çıplak domain değerini HTTPS URL'ye çevirir.
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain
    return f"https://{domain}"


def _extract_lighthouse_metrics(payload: dict) -> dict[str, float]:
    # Lighthouse category score'lari ile field/lab metrikleri tek ciktiya cevirir.
    lighthouse = payload.get("lighthouseResult", {})
    categories = lighthouse.get("categories", {})
    audits = lighthouse.get("audits", {})
    performance_score = categories.get("performance", {}).get("score") or 0
    accessibility_score = categories.get("accessibility", {}).get("score") or 0
    best_practices_score = categories.get("best-practices", {}).get("score") or 0
    seo_score = categories.get("seo", {}).get("score") or 0
    lcp = (audits.get("largest-contentful-paint") or {}).get("numericValue") or 0
    fcp = (audits.get("first-contentful-paint") or {}).get("numericValue") or 0
    cls = (audits.get("cumulative-layout-shift") or {}).get("numericValue") or 0
    ttfb = 0
    for audit_name in ["server-response-time", "network-server-latency"]:
        if audit_name in audits:
            audit = audits.get(audit_name) or {}
            numeric_value = audit.get("numericValue")
            if numeric_value is not None and numeric_value >= 0:
                ttfb = numeric_value
                break
            display_value = audit.get("displayValue") or ""
            match = re.search(r"([\d.]+)\s*(ms|s)", display_value)
            if match:
                amount = float(match.group(1))
                unit = match.group(2)
                ttfb = amount * 1000 if unit == "s" else amount
                break
    
    # INP: Interaction to Next Paint - Check multiple possible audit names
    # Google's PageSpeed API may use different names depending on version
    inp = 0
    for audit_name in ["interaction-to-next-paint", "experimental-interaction-to-next-paint",
                       "experimental-interaction-to-next-paint-v2", "first-input-delay"]:
        if audit_name in audits:
            numeric_value = (audits.get(audit_name) or {}).get("numericValue")
            if numeric_value is not None and numeric_value > 0:
                inp = numeric_value
                LOGGER.debug(f"Found INP value {inp} from audit '{audit_name}'")
                break
    
    if inp == 0:
        metrics_items = ((audits.get("metrics") or {}).get("details") or {}).get("items") or []
        if metrics_items:
            item = metrics_items[0] or {}
            inp = float(item.get("observedInteractionToNextPaint") or item.get("observedMaxPotentialFID") or 0)

    if ttfb == 0:
        metrics_items = ((audits.get("metrics") or {}).get("details") or {}).get("items") or []
        if metrics_items:
            item = metrics_items[0] or {}
            ttfb = float(item.get("observedTimeToFirstByte") or item.get("observedTtfb") or 0)

    if inp == 0:
        # If still zero, log all audit names for debugging
        available_audits = list(audits.keys())
        LOGGER.warning(f"INP value is 0. Available audits: {available_audits}")

    # Field data takes precedence for the user-facing CWV values because
    # PageSpeed's main panel shows CrUX 28-day percentiles, not lab numbers.
    field_metrics = (payload.get("loadingExperience") or {}).get("metrics") or {}
    if not field_metrics:
        field_metrics = (payload.get("originLoadingExperience") or {}).get("metrics") or {}

    field_lcp = ((field_metrics.get("LARGEST_CONTENTFUL_PAINT_MS") or {}).get("percentile") or 0)
    field_fcp = ((field_metrics.get("FIRST_CONTENTFUL_PAINT_MS") or {}).get("percentile") or 0)
    field_ttfb = ((field_metrics.get("EXPERIMENTAL_TIME_TO_FIRST_BYTE") or {}).get("percentile") or 0)
    field_inp = ((field_metrics.get("INTERACTION_TO_NEXT_PAINT") or {}).get("percentile") or 0)
    field_cls_raw = ((field_metrics.get("CUMULATIVE_LAYOUT_SHIFT_SCORE") or {}).get("percentile"))
    field_cls = (float(field_cls_raw) / 100.0) if field_cls_raw is not None else 0

    return {
        "performance_score": float(performance_score) * 100,
        "accessibility_score": float(accessibility_score) * 100,
        "best_practices_score": float(best_practices_score) * 100,
        "seo_score": float(seo_score) * 100,
        "lcp": float(field_lcp or lcp),
        "fcp": float(field_fcp or fcp),
        "ttfb": float(field_ttfb or ttfb),
        "cls": float(field_cls or cls),
        "inp": float(field_inp or inp),
    }


def _audit_failed(audit: dict) -> bool:
    mode = str(audit.get("scoreDisplayMode") or "")
    if mode in {"notApplicable", "manual", "informative", "error"}:
        return False
    score = audit.get("score")
    if score is None:
        return False
    if mode == "binary":
        return float(score) < 1.0
    return float(score) < 0.9


def _audit_priority(audit: dict) -> str:
    score = audit.get("score")
    if score is None:
        return "MEDIUM"
    value = float(score)
    if value < 0.3:
        return "CRITICAL"
    if value < 0.6:
        return "HIGH"
    if value < 0.9:
        return "MEDIUM"
    return "LOW"


def _audit_state(audit: dict) -> str:
    mode = str(audit.get("scoreDisplayMode") or "")
    if mode == "manual":
        return "manual"
    if mode == "notApplicable":
        return "not_applicable"
    if mode == "informative":
        return "informative"
    if mode == "error":
        return "error"
    if _audit_failed(audit):
        return "failed"
    return "passed"


def _audit_examples(audit: dict) -> list[str]:
    details = audit.get("details") or {}
    items = details.get("items") or []
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        return []
    examples: list[str] = []
    for item in items[:3]:
        if not isinstance(item, dict):
            continue
        for key in ("node", "url", "source", "selector", "snippet"):
            value = item.get(key)
            if isinstance(value, dict):
                value = value.get("snippet") or value.get("selector") or value.get("nodeLabel")
            if value:
                examples.append(str(value)[:180])
                break
    return examples


def _solution_step(
    step: int,
    title_en: str,
    title_tr: str,
    details_en: str,
    details_tr: str,
    difficulty: str = "Medium",
    code: str | None = None,
) -> dict:
    return {
        "step": step,
        "title": title_en,
        "title_en": title_en,
        "title_tr": title_tr,
        "details_en": details_en,
        "details_tr": details_tr,
        "difficulty": difficulty,
        "code": code,
    }


def _localized_section_content(section_key: str, title_en: str, description_en: str = "") -> dict[str, str]:
    title_map = {
        "metrics": "Metrikler",
        "insights": "Icgoruler",
        "diagnostics": "Tani Bulgulari",
        "passed": "Gecen Denetimler",
        "manual": "Manuel Kontrol Gereken Ogeler",
        "not_applicable": "Uygulanamaz Ogeler",
        "opportunities": "Iyilestirme Firsatlari",
        "a11y-aria": "ARIA",
        "a11y-audio-video": "Ses ve video",
        "a11y-best-practices": "En iyi uygulamalar",
        "a11y-color-contrast": "Kontrast",
        "a11y-language": "Uluslararasilastirma ve yerellestirme",
        "a11y-names-labels": "Isimler ve etiketler",
        "a11y-navigation": "Gezinme",
        "a11y-tables-lists": "Tablolar ve listeler",
        "best-practices-browser-compat": "Tarayici uyumlulugu",
        "best-practices-general": "Genel",
        "best-practices-trust-safety": "Guven ve Guvenlik",
        "best-practices-ux": "Kullanici deneyimi",
        "seo-content": "Icerik en iyi uygulamalari",
        "seo-crawl": "Tarama ve dizine ekleme",
        "seo-mobile": "Mobil uyumluluk",
    }
    description_map = {
        "metrics": "Kategori skorunu hesaplayan ana metrikler.",
        "insights": "Performansi ve kaynak kullanimini iyilestirmek icin oncelikli canli bulgular.",
        "diagnostics": "Skoru dogrudan etkilemeyen ama koken nedeni anlamaya yardim eden teknik tani maddeleri.",
        "passed": "Bu kontroller mevcut olcumde basarili gecti.",
        "manual": "Lighthouse otomatik dogrulayamadi; manuel inceleme gerekiyor.",
        "not_applicable": "Bu kontroller mevcut sayfa yapisina uygulanmiyor.",
        "opportunities": "Bu kategori altinda dikkat isteyen iyilestirme alanlari.",
    }
    return {
        "title_tr": title_map.get(section_key, title_en),
        "description_tr": description_map.get(section_key, ""),
    }


def _localized_audit_content(audit_id: str, title_en: str, category: str, strategy: str) -> dict[str, object]:
    token = f"{audit_id} {title_en}".lower()

    if "cache" in token or "ttl" in token:
        return {
            "title_tr": "Verimli cache süreleri kullan",
            "problem_tr": "Statik dosyalar kisa sureli veya hic cache edilmedigi icin tarayici ayni kaynaklari tekrar indiriyor.",
            "impact_tr": "Tekrar ziyaretlerde sayfa daha yavas acilir, bant genisligi tuketimi artar.",
            "solution": [
                _solution_step(1, "Set long Cache-Control headers", "Uzun Cache-Control basliklari tanimla", "Serve images, JavaScript and CSS with a long max-age and immutable when the filename is versioned.", "Gorsel, JavaScript ve CSS dosyalarina versiyonlu dosya adi kullaniyorsan uzun max-age ve immutable tanimla.", "Easy"),
                _solution_step(2, "Version static assets", "Statik dosyalari versiyonla", "Use hashed filenames so you can cache aggressively without serving stale files.", "Dosya adlarini hash veya surum bilgisiyle yayinla; boylece bayat dosya riski olmadan cache suresini uzatabilirsin.", "Medium"),
                _solution_step(3, "Review third-party resources", "3. parti kaynaklari gozden gecir", "Move large third-party assets behind a CDN or remove non-critical ones from the first view.", "Buyuk 3. parti kaynaklari CDN arkasina al veya ilk gorunum icin kritik olmayanlari kaldir.", "Medium"),
            ],
            "expected_result_tr": "Tekrar ziyaretlerde daha hizli yukleme ve daha dusuk veri transferi elde edilir.",
        }

    if "render-blocking" in token:
        return {
            "title_tr": "Render engelleyici kaynaklari azalt",
            "problem_tr": "Kritik CSS veya JavaScript dosyalari ilk boyamayi geciktiriyor.",
            "impact_tr": "Kullanici ilk icerigi daha gec gorur; FCP ve LCP kotulesebilir.",
            "solution": [
                _solution_step(1, "Inline critical CSS", "Kritik CSS'i inline et", "Inline only above-the-fold styles required for the first paint.", "Ilk gorunum icin gerekli CSS parcacigini inline et.", "Medium"),
                _solution_step(2, "Defer non-critical scripts", "Kritik olmayan scriptleri ertele", "Use defer or async for scripts that are not required before the first render.", "Ilk render oncesi gerekmeyen scriptler icin defer veya async kullan.", "Easy"),
                _solution_step(3, "Split large bundles", "Buyuk paketleri bol", "Ship route-based or component-based chunks instead of a single large bundle.", "Tek buyuk bundle yerine rota veya bilesen bazli parcali bundle kullan.", "Medium"),
            ],
            "expected_result_tr": "Ilk boyama daha hizli olur ve ana icerik daha erken gorunur.",
        }

    if "bootup-time" in token or "javascript execution" in title_en.lower():
        return {
            "title_tr": "JavaScript calistirma suresini azalt",
            "problem_tr": "Ana thread fazla JavaScript parse ve execution yukune maruz kaliyor; bu da ilk etkileşimi geciktiriyor.",
            "impact_tr": "Ozellikle mobile cihazlarda ana thread bloke olur, LCP ve etkileşim kalitesi duser.",
            "solution": [
                _solution_step(1, "Split large bundles", "Buyuk bundle'lari parcala", "Code-splitting or route-based loading can keep non-critical JavaScript out of the initial path.", "Code-splitting veya route bazli yukleme ile kritik olmayan JavaScript'i ilk yukleme yolundan cikar.", "Medium"),
                _solution_step(2, "Delay non-critical scripts", "Kritik olmayan script'leri ertele", "Defer analytics, widgets and secondary modules until after the first render or user interaction.", "Analytics, widget ve ikincil modulleri ilk render veya kullanici etkilesimi sonrasina ertele.", "Medium"),
                _solution_step(3, "Trim heavy execution paths", "Agir calisma yollarini hafiflet", "Remove unnecessary loops, large hydration blocks or duplicate script work on page load.", "Sayfa acilisinda calisan gereksiz dongu, agir hydration bloklari veya tekrarli script islerini azalt.", "Hard"),
            ],
            "expected_result_tr": "Main thread daha az mesgul olur ve Performance skoru yukselir.",
        }

    if "unused-css" in token:
        return {
            "title_tr": "Kullanilmayan CSS'i azalt",
            "problem_tr": "Sayfada hic kullanilmayan veya ilk gorunumde gerekmeyen CSS kurallari indiriliyor.",
            "impact_tr": "CSS boyutu buyur, indirme ve parse suresi artar.",
            "solution": [
                _solution_step(1, "Audit loaded stylesheets", "Yuklenen stilleri analiz et", "Find selectors that are never used on the page or only used in later interactions.", "Sayfada hic kullanilmayan veya yalnizca gec etkileşimde kullanilan secicileri belirle.", "Medium"),
                _solution_step(2, "Purge dead CSS", "Olup de kullanilmayan CSS'i temizle", "Use a purge step or remove obsolete component styles from the build.", "Build asamasinda purge uygula veya artik kullanilmayan bilesen stillerini cikar.", "Medium"),
                _solution_step(3, "Load page-specific CSS", "Sayfa bazli CSS yukle", "Split styles by page or feature so only required CSS ships initially.", "Stilleri sayfa veya ozellik bazinda bolerek ilk acilista sadece gerekli CSS'i yukle.", "Medium"),
            ],
            "expected_result_tr": "CSS boyutu azalir ve ilk boyama daha hizli hale gelir.",
        }

    if "unused-javascript" in token or "duplicated-javascript" in token:
        return {
            "title_tr": "Kullanilmayan JavaScript'i azalt",
            "problem_tr": "Sayfa ilk acilista gerekmeyen JavaScript kodu indiriliyor veya parse ediliyor.",
            "impact_tr": "Ana thread meşgul olur, etkileşim gecikir ve bundle agirlasir.",
            "solution": [
                _solution_step(1, "Identify dead code and large dependencies", "Olup de kullanilmayan kodu ve buyuk bagimliliklari belirle", "Check bundle analysis to find modules loaded but not needed on the page.", "Bundle analizinde sayfada gerekmeyen modulleri ve buyuk bagimliliklari tespit et.", "Medium"),
                _solution_step(2, "Lazy-load secondary features", "Ikincil ozellikleri lazy-load et", "Load widgets, carousels and rarely used modules only after interaction.", "Widget, carousel ve nadir kullanilan modulleri ancak etkileşimden sonra yukle.", "Medium"),
                _solution_step(3, "Remove duplicate libraries", "Tekrarlanan kutuphaneleri kaldir", "Ensure the same library is not bundled multiple times in different chunks.", "Ayni kutuphanenin farkli chunk'larda birden fazla kez paketlenmediginden emin ol.", "Hard"),
            ],
            "expected_result_tr": "Daha hafif JavaScript ve daha iyi etkileşim hizi elde edilir.",
        }

    if "image" in token:
        return {
            "title_tr": "Gorsel teslimatini optimize et",
            "problem_tr": "Gorseller boyut, format veya responsive teslimat acisindan optimize edilmemis olabilir.",
            "impact_tr": "LCP/FCP gecikir ve gereksiz veri transferi olusur.",
            "solution": [
                _solution_step(1, "Resize and compress images", "Gorselleri yeniden boyutlandir ve sikistir", "Export images close to their rendered size and compress them before publishing.", "Gorselleri ekranda kullanildigi boyuta yakin export et ve yayin oncesi sikistir.", "Easy"),
                _solution_step(2, "Use modern formats and srcset", "Modern format ve srcset kullan", "Serve AVIF/WebP where possible and provide srcset sizes for responsive layouts.", "Mumkunse AVIF/WebP kullan ve responsive duzenler icin srcset boyutlari sun.", "Medium"),
                _solution_step(3, "Preload the LCP image", "LCP gorselini preload et", "If the main hero image is the LCP element, preload it and avoid lazy-loading it.", "Ana hero gorseli LCP ise preload et ve lazy-load uygulama.", "Medium"),
            ],
            "expected_result_tr": "LCP ve FCP iyilesir, veri transferi azalir.",
        }

    if "server-response" in token or "latency" in token or "ttfb" in token:
        return {
            "title_tr": "Sunucu yanit suresini azalt",
            "problem_tr": "Backend isleme suresi veya cache eksikligi ilk yaniti geciktiriyor.",
            "impact_tr": "TTFB ve buna bagli olarak FCP/LCP kotulesebilir.",
            "solution": [
                _solution_step(1, "Profile the slow request path", "Yavas istek yolunu profille", "Measure application time, database time and external API time for the affected page.", "Ilgili sayfa icin uygulama, veritabani ve dis API surelerini ayri ayri olc.", "Medium"),
                _solution_step(2, "Add caching at the right layer", "Dogru katmanda cache ekle", "Cache rendered fragments, expensive queries or full responses when safe.", "Guvenliyse render parcasi, pahali sorgu veya tum response icin cache kullan.", "Medium"),
                _solution_step(3, "Trim synchronous work", "Senkron isleri azalt", "Move non-critical work out of the request path and parallelize remote calls where possible.", "Kritik olmayan isleri request akisinin disina tas ve uygun yerlerde uzak cagrilari paralellestir.", "Hard"),
            ],
            "expected_result_tr": "Sunucu daha hizli cevap verir ve ana metrikler daha dengeli olur.",
        }

    if "third-parties" in token:
        return {
            "title_tr": "3. parti kod etkisini azalt",
            "problem_tr": "3. parti script veya widget'lar ana thread'i ve ag yukunu arttiriyor.",
            "impact_tr": "Performans skoru ve etkileşim kalitesi dusuyor.",
            "solution": [
                _solution_step(1, "Keep only critical vendors", "Yalnizca kritik vendor'lari birak", "Remove tags and widgets that do not support a clear business need.", "Net is degeri olmayan etiket ve widget'lari kaldir.", "Easy"),
                _solution_step(2, "Delay non-critical tags", "Kritik olmayan etiketleri gec yukle", "Load analytics, embeds and recommendation scripts after the first render.", "Analitik, embed ve oneriler gibi scriptleri ilk render sonrasinda yukle.", "Medium"),
                _solution_step(3, "Monitor vendor cost", "Vendor maliyetini izle", "Track transfer size and main-thread cost per third-party script in every release.", "Her yayinda 3. parti scriptlerin transfer boyutu ve main-thread maliyetini izle.", "Medium"),
            ],
            "expected_result_tr": "Daha temiz bir ilk yukleme ve daha az main-thread maliyeti elde edilir.",
        }

    if "font-display" in token or "font" in token:
        return {
            "title_tr": "Web font yuklemesini iyilestir",
            "problem_tr": "Font yukleme stratejisi gec metin boyamasi veya layout kaymasi uretebilir.",
            "impact_tr": "Metin gec gorunur veya gorunum degisimi yasanir.",
            "solution": [
                _solution_step(1, "Use font-display: swap", "font-display: swap kullan", "Allow text to render immediately with a fallback font.", "Metnin once fallback font ile hemen gorunmesine izin ver.", "Easy"),
                _solution_step(2, "Subset and preload key fonts", "Temel fontlari subset et ve preload et", "Ship only required glyph sets and preload the fonts used above the fold.", "Yalnizca gerekli karakter setlerini gonder ve ilk gorunumde kullanilan fontlari preload et.", "Medium"),
                _solution_step(3, "Reduce font variants", "Font varyantlarini azalt", "Keep the number of families, weights and styles as small as possible.", "Aile, agirlik ve stil sayisini minimumda tut.", "Easy"),
            ],
            "expected_result_tr": "Metin daha erken gorunur ve layout degisimi azalir.",
        }

    if "dom-size" in token:
        return {
            "title_tr": "DOM boyutunu kucult",
            "problem_tr": "Sayfada gerektiginden fazla DOM dugumu bulunuyor.",
            "impact_tr": "Tarayici hesaplama maliyeti artar; style/layout suresi uzar.",
            "solution": [
                _solution_step(1, "Remove redundant wrappers", "Gereksiz wrapper'lari kaldir", "Flatten deeply nested markup where possible.", "Mumkun olan yerlerde derin nested yapilari sadeleştir.", "Medium"),
                _solution_step(2, "Virtualize long lists", "Uzun listeleri virtualize et", "Render only visible rows for large tables or result lists.", "Buyuk tablo ve listelerde yalnizca gorunen satirlari render et.", "Hard"),
                _solution_step(3, "Defer hidden sections", "Gizli bolumleri gec render et", "Mount heavy accordions or tabs only when the user opens them.", "Agir accordion veya tab iceriklerini kullanici actiginda yukle.", "Medium"),
            ],
            "expected_result_tr": "Layout ve style hesaplamalari hizlanir.",
        }

    if "image-alt" in token:
        return {
            "title_tr": "Resimlerde alt metin eksik",
            "problem_tr": "Bazi img ogelerinde anlamli alt aciklamasi bulunmuyor.",
            "impact_tr": "Erisilebilirlik ve image SEO zayiflar.",
            "solution": [
                _solution_step(1, "Add meaningful alt text", "Anlamli alt metin ekle", "Describe the content or purpose of informative images.", "Bilgi tasiyan gorsellerin icerigini veya amacini alt metinle acikla.", "Easy"),
                _solution_step(2, "Mark decorative images correctly", "Dekoratif gorselleri dogru isaretle", "Use empty alt text for decorative images that do not convey information.", "Bilgi tasimayan dekoratif gorsellerde bos alt kullan.", "Easy"),
                _solution_step(3, "Enforce alt text in CMS", "CMS tarafinda alt metni zorunlu kil", "Require alt text for uploaded editorial images in your admin flow.", "Yonetim panelinde yuklenen editoryal gorseller icin alt metni zorunlu yap.", "Medium"),
            ],
            "expected_result_tr": "Screen reader deneyimi ve image SEO iyilesir.",
        }

    if "label" in token or "name" in token:
        return {
            "title_tr": "Form ve etkileşim ogelerinde erisilebilir isim eksik",
            "problem_tr": "Buton, input veya link ogeleri screen reader tarafinda ayirt edici bir isim tasimiyor olabilir.",
            "impact_tr": "Form kullanimi ve gezilebilirlik zorlasir.",
            "solution": [
                _solution_step(1, "Associate labels with controls", "Label'lari alanlarla eslestir", "Ensure inputs have a visible label or aria-label/aria-labelledby.", "Input alanlarinda gorunur label veya aria-label/aria-labelledby kullan.", "Easy"),
                _solution_step(2, "Name icon-only buttons", "Sadece ikon iceren butonlari adlandir", "Provide accessible names for icon-only buttons and links.", "Yalnizca ikon iceren buton ve linklere erisilebilir isim ver.", "Easy"),
                _solution_step(3, "Test with screen readers", "Screen reader ile test et", "Verify that controls are announced with the intended names.", "Kontrollerin beklenen isimlerle okundugunu screen reader ile dogrula.", "Medium"),
            ],
            "expected_result_tr": "Formlar ve etkileşim ogeleri daha kolay kullanilir.",
        }

    if "contrast" in token:
        return {
            "title_tr": "Renk kontrastini iyilestir",
            "problem_tr": "Metin ile arka plan arasindaki kontrast yetersiz.",
            "impact_tr": "Okunabilirlik ozellikle dusuk gorus kosullarinda azalir.",
            "solution": [
                _solution_step(1, "Increase text contrast", "Metin kontrastini arttir", "Adjust text or background colors to meet WCAG contrast ratios.", "Metin veya arka plan renklerini WCAG kontrast oranlarini saglayacak sekilde guncelle.", "Easy"),
                _solution_step(2, "Check hover and disabled states", "Hover ve disabled durumlarini kontrol et", "Review all states, not just the default one.", "Yalnizca varsayilan degil hover ve disabled gibi tum durumlari kontrol et.", "Medium"),
                _solution_step(3, "Tokenize accessible colors", "Erisilebilir renk tokenlari tanimla", "Create design tokens for approved contrast-safe color pairs.", "Kontrast guvenli renk eslesmeleri icin tasarim tokenlari olustur.", "Medium"),
            ],
            "expected_result_tr": "Okunabilirlik artar ve erisilebilirlik skoru iyilesir.",
        }

    if "title" in token or "meta-description" in token or "canonical" in token or "robots-txt" in token or "hreflang" in token or "structured-data" in token or "crawlable" in token:
        return {
            "title_tr": "SEO sinyallerini duzelt",
            "problem_tr": "Teknik SEO denetimlerinden biri eksik, zayif veya hatali durumda.",
            "impact_tr": "Arama motoru taramasi, indeksleme veya snippet kalitesi etkilenebilir.",
            "solution": [
                _solution_step(1, "Fix the failing SEO signal", "Basarisiz SEO sinyalini duzelt", "Update the page title, meta description, canonical, robots or structured data depending on the audit.", "Denetime gore title, meta description, canonical, robots veya structured data alanini duzelt.", "Medium"),
                _solution_step(2, "Verify in rendered HTML", "Render edilen HTML'de dogrula", "Confirm the signal exists in the final rendered HTML, not only in templates.", "Sinyalin yalnizca template'te degil son render edilen HTML'de de geldigini dogrula.", "Easy"),
                _solution_step(3, "Re-test with Search Console and Lighthouse", "Search Console ve Lighthouse ile yeniden test et", "Validate the page again after deployment.", "Yayin sonrasi sayfayi yeniden dogrula.", "Easy"),
            ],
            "expected_result_tr": "Tarama ve snippet kalitesi daha tutarli hale gelir.",
        }

    generic_title_tr = {
        "Performance": "Performans bulgusunu iyilestir",
        "Accessibility": "Erisilebilirlik bulgusunu iyilestir",
        "Best Practices": "En iyi uygulama bulgusunu iyilestir",
        "SEO": "SEO bulgusunu iyilestir",
    }.get(category, "Lighthouse bulgusunu iyilestir")

    return {
        "title_tr": generic_title_tr,
        "problem_tr": "Bu Lighthouse denetimi basarisiz oldu; ilgili kaynak veya isaretleme optimize edilmeli.",
        "impact_tr": f"Bu sorun {strategy} tarafinda {category} skorunu dusuruyor.",
        "solution": [
            _solution_step(1, "Inspect the failing audit in context", "Basarisiz denetimi baglaminda incele", "Review the failing resources, DOM nodes or requests attached to this audit.", "Bu denetime bagli kaynak, DOM dugumu veya request'leri incele.", "Medium"),
            _solution_step(2, "Apply the targeted fix", "Hedefe yonelik duzeltmeyi uygula", "Adjust code, assets or server configuration according to the audit signal.", "Denetimin gosterdigi sinyale gore kodu, asset'i veya sunucu ayarini duzelt.", "Medium"),
            _solution_step(3, "Measure again", "Yeniden olc", "Run Lighthouse again and verify the score and examples improved.", "Lighthouse'i yeniden calistir ve skorla birlikte orneklerin iyilestigini dogrula.", "Easy"),
        ],
        "expected_result_tr": f"{category} skorunda gozle gorulur bir iyilesme beklenir.",
    }


def _build_lighthouse_analysis(payload: dict, strategy: str) -> dict:
    lighthouse = payload.get("lighthouseResult", {})
    categories = lighthouse.get("categories", {})
    audits = lighthouse.get("audits", {})
    category_groups = lighthouse.get("categoryGroups", {})
    category_defs = [
        ("performance", "Performance"),
        ("accessibility", "Accessibility"),
        ("best-practices", "Best Practices"),
        ("seo", "SEO"),
    ]
    issues: list[dict] = []
    category_summary: dict[str, dict] = {}
    analysis_sections: dict[str, list[dict]] = {}

    for key, label in category_defs:
        category = categories.get(key) or {}
        refs = category.get("auditRefs") or []
        category_issues: list[dict] = []
        sections_map: dict[str, dict] = {}
        for ref in refs:
            audit_id = ref.get("id")
            audit = audits.get(audit_id) or {}
            if not audit:
                continue
            state = _audit_state(audit)
            examples = _audit_examples(audit)
            title_en = str(audit.get("title") or audit_id or "Audit")
            problem_en = str(audit.get("description") or "This audit check failed.")
            display_value = str(audit.get("displayValue") or "").strip()
            impact_en = display_value or (
                "This item is lowering the Lighthouse category score."
                if state == "failed"
                else "This Lighthouse check is included in the current report."
            )
            localized = _localized_audit_content(audit_id or "", title_en, label, strategy)
            item = {
                "id": audit_id,
                "title": title_en,
                "title_en": title_en,
                "title_tr": str(localized.get("title_tr") or title_en),
                "category": label,
                "priority": _audit_priority(audit),
                "score": float(audit.get("score") or 0.0),
                "score_display_mode": str(audit.get("scoreDisplayMode") or ""),
                "state": state,
                "state_label_en": {
                    "failed": "Needs Fix",
                    "passed": "Passed",
                    "manual": "Manual Check",
                    "not_applicable": "Not Applicable",
                    "informative": "Informative",
                    "error": "Error",
                }.get(state, "Included"),
                "state_label_tr": {
                    "failed": "Duzeltme Gerekli",
                    "passed": "Gecti",
                    "manual": "Manuel Kontrol",
                    "not_applicable": "Uygulanamaz",
                    "informative": "Bilgilendirici",
                    "error": "Hata",
                }.get(state, "Dahil"),
                "problem": problem_en,
                "problem_en": problem_en,
                "problem_tr": str(
                    localized.get("problem_tr")
                    or (
                        "Bu denetim mevcut olcumde basarili gorunuyor."
                        if state == "passed"
                        else "Bu denetim manuel olarak kontrol edilmeli."
                        if state == "manual"
                        else "Bu denetim bu sayfa icin uygulanamaz durumda."
                        if state == "not_applicable"
                        else "Bu denetim rapora bilgi amacli dahil edildi."
                    )
                ),
                "impact": impact_en,
                "impact_en": impact_en,
                "impact_tr": str(
                    localized.get("impact_tr")
                    or (
                        "Bu alan ilgili Lighthouse skorunu dusuruyor."
                        if state == "failed"
                        else "Bu madde raporda referans olarak tutuluyor."
                    )
                ),
                "display_value": display_value,
                "solution": localized.get("solution") if state == "failed" else [],
                "expected_result": "This audit should improve after the fix is deployed." if state == "failed" else "No action is required for this audit in the current report.",
                "expected_result_en": "This audit should improve after the fix is deployed." if state == "failed" else "No action is required for this audit in the current report.",
                "expected_result_tr": str(
                    localized.get("expected_result_tr")
                    or (
                        "Bu audit duzeldikten sonra ilgili kategori skoru yukselir."
                        if state == "failed"
                        else "Bu denetim icin mevcut raporda ek aksiyon gerekmiyor."
                    )
                ),
                "timeline": None,
                "examples": examples,
                "source_strategy": strategy,
                "group_id": ref.get("group"),
            }
            if state == "failed":
                category_issues.append(item)

            group_id = str(ref.get("group") or "")
            if group_id in {"metrics", "insights", "diagnostics"}:
                section_key = group_id
                section_title = str((category_groups.get(group_id) or {}).get("title") or group_id.replace("-", " ").title())
                section_desc = str((category_groups.get(group_id) or {}).get("description") or "")
            elif state == "manual":
                section_key = "manual"
                section_title = "Additional items to manually check"
                section_desc = "These items address areas which an automated testing tool cannot cover."
            elif state == "not_applicable":
                section_key = "not_applicable"
                section_title = "Not applicable"
                section_desc = "These audits are not applicable to the current page."
            elif state == "passed":
                section_key = "passed"
                section_title = "Passed audits"
                section_desc = "These audits passed in the current Lighthouse report."
            else:
                section_key = group_id or "opportunities"
                section_info = category_groups.get(group_id) or {}
                section_title = str(section_info.get("title") or "Opportunities")
                section_desc = str(section_info.get("description") or "These items need attention in the current category.")

            localized_section = _localized_section_content(section_key, section_title, section_desc)
            section = sections_map.setdefault(
                section_key,
                {
                    "key": section_key,
                    "title_en": section_title,
                    "title_tr": localized_section.get("title_tr") or section_title,
                    "description_en": section_desc,
                    "description_tr": localized_section.get("description_tr") or "",
                    "items": [],
                },
            )
            section["items"].append(item)
        def _issue_sort_key(issue: dict) -> tuple[int, int, str]:
            issue_id = str(issue.get("id") or "")
            title = str(issue.get("title_en") or issue.get("title") or "").lower()
            priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
            insight_like = (
                "insight" in issue_id
                or any(
                    token in issue_id
                    for token in (
                        "unused",
                        "render-blocking",
                        "cache",
                        "image",
                        "document-latency",
                        "server-response",
                        "network",
                        "redirect",
                        "font-display",
                    )
                )
                or any(
                    phrase in title
                    for phrase in (
                        "use ",
                        "reduce ",
                        "eliminate ",
                        "improve ",
                        "avoid ",
                        "serve ",
                        "defer ",
                    )
                )
            )
            metric_like = issue_id in {
                "largest-contentful-paint",
                "first-contentful-paint",
                "speed-index",
                "interactive",
                "max-potential-fid",
                "total-blocking-time",
                "cumulative-layout-shift",
            }
            bucket = 0 if insight_like else 2 if metric_like else 1
            return (bucket, priority_order.get(str(issue.get("priority") or "").upper(), 9), issue_id)

        category_issues.sort(key=_issue_sort_key)
        for section in sections_map.values():
            section["items"] = sorted(list(section.get("items") or []), key=_issue_sort_key)

        section_order_map = {
            "metrics": 0,
            "insights": 1,
            "diagnostics": 2,
            "opportunities": 3,
            "a11y-names-labels": 4,
            "a11y-color-contrast": 5,
            "a11y-navigation": 6,
            "a11y-aria": 7,
            "a11y-audio-video": 8,
            "a11y-language": 9,
            "a11y-tables-lists": 10,
            "a11y-best-practices": 11,
            "best-practices-general": 4,
            "best-practices-trust-safety": 5,
            "best-practices-browser-compat": 6,
            "best-practices-ux": 7,
            "seo-content": 4,
            "seo-crawl": 5,
            "seo-mobile": 6,
            "manual": 90,
            "passed": 91,
            "not_applicable": 92,
        }
        ordered_sections = sorted(
            sections_map.values(),
            key=lambda section: (
                section_order_map.get(str(section.get("key") or ""), 50),
                str(section.get("title_en") or ""),
            ),
        )
        score = float(category.get("score") or 0) * 100
        issues.extend(category_issues)
        category_summary[key.replace("-", "_")] = {
            "score": round(score),
            "issues_count": len(category_issues),
            "title": label,
        }
        analysis_sections[key.replace("-", "_")] = ordered_sections

    return {
        "strategy": strategy,
        "categories": category_summary,
        "issues": issues,
        "sections": analysis_sections,
        "summary": f"{strategy} icin {len(issues)} audit bulgusu bulundu",
    }


def _mock_lighthouse_analysis(strategy: str) -> dict:
    if strategy == "mobile":
        return {
            "strategy": strategy,
            "categories": {
                "performance": {"score": 72, "issues_count": 2, "title": "Performance"},
                "accessibility": {"score": 84, "issues_count": 2, "title": "Accessibility"},
                "best_practices": {"score": 68, "issues_count": 1, "title": "Best Practices"},
                "seo": {"score": 90, "issues_count": 1, "title": "SEO"},
            },
            "issues": [
                {
                    "id": "mobile-touch-targets",
                    "title": "Tap targets çok yakın konumlanmış",
                    "category": "Accessibility",
                    "priority": "HIGH",
                    "problem": "Mobil ekranda buton ve linkler birbirine cok yakin. Dokunmatik kullanimda yanlis tiklama riski artiyor.",
                    "impact": "Ozellikle telefon kullanan ziyaretciler icin etkileşim hatasi ve form terk orani artabilir.",
                    "solution": [],
                    "expected_result": "Mobilde buton alanlari daha rahat tiklanir.",
                    "timeline": None,
                    "examples": [".header-nav a", ".filter-chip"],
                    "source_strategy": strategy,
                },
                {
                    "id": "mobile-alt-text",
                    "title": "Gorsellerin bir kisminda alt metin eksik",
                    "category": "Accessibility",
                    "priority": "MEDIUM",
                    "problem": "Mobil audit bazi img ogelerinde anlamsiz veya eksik alt text tespit etti.",
                    "impact": "Erisilebilirlik ve image SEO kalitesi dusebilir.",
                    "solution": [],
                    "expected_result": "Screen reader uyumu guclenir.",
                    "timeline": None,
                    "examples": ["Hero banner image", "Campaign card thumbnail"],
                    "source_strategy": strategy,
                },
                {
                    "id": "mobile-image-sizing",
                    "title": "Responsive image boyutlari optimize degil",
                    "category": "Best Practices",
                    "priority": "MEDIUM",
                    "problem": "Mobilde gerektiginden buyuk gorseller indiriliyor.",
                    "impact": "Bant genisligi tuketimi ve ilk yukleme suresi artar.",
                    "solution": [],
                    "expected_result": "Mobil performans daha tutarli olur.",
                    "timeline": None,
                    "examples": ["/hero-mobile.webp", "/campaign-1200.jpg"],
                    "source_strategy": strategy,
                },
                {
                    "id": "mobile-seo-titles",
                    "title": "Mobil snippet uzunlugu icin title kisaltilmali",
                    "category": "SEO",
                    "priority": "LOW",
                    "problem": "Baslik etiketi mobil SERP gorunumunde kesilebilir.",
                    "impact": "Mobil CTR etkilenebilir.",
                    "solution": [],
                    "expected_result": "Mobil arama sonucunda daha net gorunum elde edilir.",
                    "timeline": None,
                    "examples": ["Title length: 68 characters"],
                    "source_strategy": strategy,
                },
            ],
            "summary": "mobile icin 4 audit bulgusu bulundu",
        }
    return {
        "strategy": strategy,
        "categories": {
            "performance": {"score": 89, "issues_count": 1, "title": "Performance"},
            "accessibility": {"score": 91, "issues_count": 1, "title": "Accessibility"},
            "best_practices": {"score": 82, "issues_count": 2, "title": "Best Practices"},
            "seo": {"score": 94, "issues_count": 0, "title": "SEO"},
        },
        "issues": [
            {
                "id": "desktop-contrast",
                "title": "Masaustu tabloda kontrast dusuk",
                "category": "Accessibility",
                "priority": "MEDIUM",
                "problem": "Desktop audit bazi tablo satirlarinda metin-kontrast oranini dusuk buldu.",
                "impact": "Buyuk ekranlarda okunabilirlik azalir.",
                "solution": [],
                "expected_result": "Tablo okunabilirligi artar.",
                "timeline": None,
                "examples": [".markets-table .muted", ".widget-subtitle"],
                "source_strategy": strategy,
            },
            {
                "id": "desktop-console-errors",
                "title": "Tarayici console hatalari tespit edildi",
                "category": "Best Practices",
                "priority": "HIGH",
                "problem": "Desktop audit, sayfa yuklenirken JavaScript console hatalari yakaladi.",
                "impact": "Bazi widgetlar gec yuklenebilir veya beklendigi gibi calismayabilir.",
                "solution": [],
                "expected_result": "Desktop deneyimi daha stabil olur.",
                "timeline": None,
                "examples": ["TypeError in widget.js:142"],
                "source_strategy": strategy,
            },
            {
                "id": "desktop-legacy-js",
                "title": "Gereksiz legacy JavaScript gonderiliyor",
                "category": "Best Practices",
                "priority": "MEDIUM",
                "problem": "Modern desktop tarayicilar icin gereksiz polyfill ve eski bundle parcasi tespit edildi.",
                "impact": "Desktop bundle boyutu ve parse suresi buyur.",
                "solution": [],
                "expected_result": "Desktop bundle daha hafif olur.",
                "timeline": None,
                "examples": ["legacy.bundle.js"],
                "source_strategy": strategy,
            },
        ],
        "summary": "desktop icin 3 audit bulgusu bulundu",
    }


def _fetch_pagespeed(url: str, strategy: str) -> tuple[dict[str, float], dict, dict]:
    # API key yoksa deterministic mock veri döndürür, varsa gerçek API çağrısı yapar.
    api_key = settings.google_api_key.strip()
    if not api_key or api_key.startswith("local-"):
        payload = {
            "mock": True,
            "strategy": strategy,
            "generated_at": datetime.utcnow().isoformat(),
        }
        if strategy == "mobile":
            return ({
                "performance_score": 72.0,
                "accessibility_score": 84.0,
                "best_practices_score": 68.0,
                "seo_score": 90.0,
                "lcp": 2850.0,
                "fcp": 1700.0,
                "ttfb": 420.0,
                "cls": 0.08,
                "inp": 180.0,
            }, _mock_lighthouse_analysis(strategy), payload)
        return ({
            "performance_score": 89.0,
            "accessibility_score": 91.0,
            "best_practices_score": 82.0,
            "seo_score": 94.0,
            "lcp": 1650.0,
            "fcp": 900.0,
            "ttfb": 260.0,
            "cls": 0.03,
            "inp": 110.0,
        }, _mock_lighthouse_analysis(strategy), payload)

    query = urlencode(
        [
            ("url", url),
            ("strategy", strategy),
            ("key", api_key),
            ("category", "performance"),
            ("category", "accessibility"),
            ("category", "best-practices"),
            ("category", "seo"),
            ("fields", "loadingExperience,originLoadingExperience,lighthouseResult(categories,categoryGroups,audits)"),
        ]
    )
    with urlopen(f"{PAGESPEED_ENDPOINT}?{query}", timeout=settings.pagespeed_request_timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _extract_lighthouse_metrics(payload), _build_lighthouse_analysis(payload, strategy), payload


def _fetch_pagespeed_with_retries(url: str, strategy: str) -> tuple[dict[str, float], dict, dict]:
    # Geçici ağ hatalarında yeniden deneyip kalıcı hataları açıklayıcı şekilde döndürür.
    attempts = max(1, settings.pagespeed_max_retries + 1)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return _fetch_pagespeed(url, strategy)
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="ignore")[:300]
            if exc.code not in {408, 429, 500, 502, 503, 504}:
                raise RuntimeError(f"{strategy} istegi reddedildi ({exc.code}). {details}".strip()) from exc
            last_error = RuntimeError(f"{strategy} istegi gecici olarak basarisiz oldu ({exc.code}). {details}".strip())
        except (TimeoutError, socket.timeout, URLError) as exc:
            last_error = exc

        LOGGER.warning("PageSpeed %s denemesi %s/%s basarisiz oldu: %s", strategy, attempt, attempts, last_error)
        if attempt < attempts:
            time.sleep(max(0.0, settings.pagespeed_retry_backoff_seconds) * attempt)

    raise RuntimeError(f"{strategy} PageSpeed verisi alinamadi: {last_error}") from last_error


def _load_latest_strategy_metrics(db: Session, site_id: int, strategy: str) -> dict[str, float] | None:
    # Strateji icin daha once kaydedilmis son metrikleri fallback olarak yukler.
    latest = {metric.metric_type: metric for metric in get_latest_metrics(db, site_id)}
    metric_names = STRATEGY_METRIC_MAP[strategy]
    if any(latest.get(metric_name) is None for metric_name in metric_names.values()):
        return None
    return {
        key: float(latest[metric_name].value)
        for key, metric_name in metric_names.items()
    }


def get_latest_pagespeed_audit_snapshot(db: Session, site_id: int, strategy: str) -> dict | None:
    snapshot = (
        db.query(PageSpeedAuditSnapshot)
        .filter(PageSpeedAuditSnapshot.site_id == site_id, PageSpeedAuditSnapshot.strategy == strategy)
        .order_by(PageSpeedAuditSnapshot.collected_at.desc(), PageSpeedAuditSnapshot.id.desc())
        .first()
    )
    if snapshot is None:
        return None
    try:
        analysis = json.loads(snapshot.analysis_json)
    except json.JSONDecodeError:
        return None
    issues = analysis.get("issues") or []
    generic_titles = {
        "Performans bulgusunu iyilestir",
        "Erisilebilirlik bulgusunu iyilestir",
        "En iyi uygulama bulgusunu iyilestir",
        "SEO bulgusunu iyilestir",
        "Lighthouse bulgusunu iyilestir",
    }
    for issue in issues:
        localized = _localized_audit_content(
            str(issue.get("id") or ""),
            str(issue.get("title_en") or issue.get("title") or ""),
            str(issue.get("category") or ""),
            strategy,
        )
        if not issue.get("title_tr") or str(issue.get("title_tr")) in generic_titles:
            issue["title_tr"] = str(localized.get("title_tr") or issue.get("title_tr") or "")
        if not issue.get("problem_tr"):
            issue["problem_tr"] = str(localized.get("problem_tr") or "")
        if not issue.get("impact_tr"):
            issue["impact_tr"] = str(localized.get("impact_tr") or "")
        if not issue.get("solution"):
            issue["solution"] = localized.get("solution") or []
        if not issue.get("expected_result_tr"):
            issue["expected_result_tr"] = str(localized.get("expected_result_tr") or "")
    return analysis


def _save_pagespeed_audit_snapshot(
    db: Session,
    site_id: int,
    strategy: str,
    analysis: dict,
    collected_at: datetime,
) -> None:
    db.add(
        PageSpeedAuditSnapshot(
            site_id=site_id,
            strategy=strategy,
            analysis_json=json.dumps(analysis, ensure_ascii=True),
            collected_at=collected_at,
        )
    )


def _flatten_strategy_metrics(strategy: str, payload: dict[str, float]) -> dict[str, float]:
    return {
        STRATEGY_METRIC_MAP[strategy][key]: value
        for key, value in payload.items()
    }


def collect_pagespeed_metrics(db: Session, site: Site) -> dict:
    """Mobile ve desktop performans verilerini toplayıp Metric tablosuna kaydeder."""
    decision = consume_api_quota(db, site, provider="pagespeed", units=2)
    if not decision.allowed:
        return {
            "site_id": site.id,
            "blocked": True,
            "reason": decision.reason,
        }

    target_url = _normalize_url(site.domain)
    collected_at = datetime.utcnow()
    metrics: dict[str, float] = {}
    strategy_payloads: dict[str, dict[str, float] | None] = {"mobile": None, "desktop": None}
    strategy_analyses: dict[str, dict | None] = {"mobile": None, "desktop": None}
    strategy_status: dict[str, dict[str, object]] = {}
    errors: dict[str, str] = {}

    for strategy in ("mobile", "desktop"):
        collector_run = start_collector_run(
            db,
            site_id=site.id,
            provider="pagespeed",
            strategy=strategy,
            target_url=target_url,
            requested_at=collected_at,
        )
        try:
            payload, analysis, raw_payload = _fetch_pagespeed_with_retries(target_url, strategy)
            strategy_payloads[strategy] = payload
            strategy_analyses[strategy] = analysis
            strategy_status[strategy] = {"state": "fresh", "message": "Canli veri guncellendi."}
            metrics.update(_flatten_strategy_metrics(strategy, payload))
            if analysis:
                _save_pagespeed_audit_snapshot(db, site.id, strategy, analysis, collected_at)
                audit_row_count = save_lighthouse_audit_records(
                    db,
                    site_id=site.id,
                    strategy=strategy,
                    analysis=analysis,
                    collected_at=collected_at,
                    collector_run_id=collector_run.id,
                )
            else:
                audit_row_count = 0
            save_pagespeed_payload_snapshot(
                db,
                site_id=site.id,
                strategy=strategy,
                payload=raw_payload,
                collected_at=collected_at,
                collector_run_id=collector_run.id,
            )
            finish_collector_run(
                db,
                collector_run,
                status="success",
                finished_at=collected_at,
                summary={
                    "source": "mock" if raw_payload.get("mock") else "live",
                    "saved_metric_keys": sorted(_flatten_strategy_metrics(strategy, payload).keys()),
                    "audit_rows": audit_row_count,
                },
                row_count=audit_row_count,
            )
        except RuntimeError as exc:
            fallback = _load_latest_strategy_metrics(db, site.id, strategy)
            strategy_analyses[strategy] = get_latest_pagespeed_audit_snapshot(db, site.id, strategy)
            errors[strategy] = str(exc)
            if fallback is not None:
                strategy_payloads[strategy] = fallback
                strategy_status[strategy] = {
                    "state": "stale",
                    "message": "Canli istek basarisiz oldu, son basarili olcum gosteriliyor.",
                }
                emit_custom_alert(
                    db,
                    site,
                    f"pagespeed_{strategy}_fetch_error",
                    f"{site.domain} icin {strategy} PageSpeed istegi basarisiz oldu. Son basarili olcum korunuyor. Hata: {exc}",
                    dedupe_hours=3,
                )
            else:
                strategy_status[strategy] = {
                    "state": "failed",
                    "message": "Canli veri alinamadi ve gosterilecek onceki olcum bulunmuyor.",
                }
                emit_custom_alert(
                    db,
                    site,
                    f"pagespeed_{strategy}_fetch_error",
                    f"{site.domain} icin {strategy} PageSpeed istegi basarisiz oldu ve onceki olcum bulunmuyor. Hata: {exc}",
                    dedupe_hours=3,
                )
            LOGGER.warning("PageSpeed %s fallback durumuna gecti for %s: %s", strategy, site.domain, exc)
            finish_collector_run(
                db,
                collector_run,
                status="failed" if fallback is None else "stale",
                finished_at=datetime.utcnow(),
                error_message=str(exc),
                summary={
                    "fallback_used": fallback is not None,
                },
                row_count=0,
            )

    if metrics:
        save_metrics(db, site.id, metrics, collected_at)
        evaluate_site_alerts(db, site)

    return {
        "site_id": site.id,
        "url": target_url,
        "mobile": strategy_payloads["mobile"],
        "desktop": strategy_payloads["desktop"],
        "mobile_analysis": strategy_analyses["mobile"],
        "desktop_analysis": strategy_analyses["desktop"],
        "status": strategy_status,
        "errors": errors,
        "saved_metric_count": len(metrics),
    }
