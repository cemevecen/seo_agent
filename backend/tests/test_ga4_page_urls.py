from backend.services.ga4_page_urls import (
    ga4_canonical_page_url,
    is_m_doviz_flat_product_url,
    is_m_doviz_phantom_breadcrumb_url,
    repair_seo_audit_url,
    seo_audit_url_from_ga4,
)


def test_mweb_keeps_altin_prefix():
    url = ga4_canonical_page_url("m.doviz.com", "/altin/22-ayar-bilezik")
    assert url == "https://m.doviz.com/altin/22-ayar-bilezik"


def test_mweb_root_slug_gets_altin_prefix():
    url = ga4_canonical_page_url("m.doviz.com", "/22-ayar-bilezik")
    assert url == "https://m.doviz.com/altin/22-ayar-bilezik"


def test_mweb_bank_slug_gets_altin_prefix():
    url = ga4_canonical_page_url("m.doviz.com", "/fibabanka")
    assert url == "https://m.doviz.com/altin/fibabanka"


def test_mweb_does_not_rewrite_to_altin_host():
    url = ga4_canonical_page_url("m.doviz.com", "/gram-altin")
    assert url.startswith("https://m.doviz.com/")
    assert "altin.doviz.com" not in url


def test_altin_subdomain_still_strips_altin_prefix():
    url = ga4_canonical_page_url("altin.doviz.com", "/altin/22-ayar-bilezik")
    assert url == "https://altin.doviz.com/22-ayar-bilezik"


def test_seo_audit_url_from_ga4_mweb():
    u = seo_audit_url_from_ga4("m.doviz.com", "/besli-altin", ga4_profile="mweb")
    assert u == "https://m.doviz.com/altin/besli-altin"


def test_seo_audit_mweb_profile_forces_mobile_host():
    u = seo_audit_url_from_ga4("www.doviz.com", "/22-ayar-bilezik", ga4_profile="mweb")
    assert u == "https://m.doviz.com/altin/22-ayar-bilezik"


def test_flat_m_doviz_detected_and_repaired():
    bad = "https://m.doviz.com/akbank"
    assert is_m_doviz_flat_product_url(bad)
    assert repair_seo_audit_url(bad) == "https://m.doviz.com/altin/akbank"
    assert not is_m_doviz_flat_product_url(repair_seo_audit_url(bad))


def test_mweb_haber_root_unchanged():
    url = ga4_canonical_page_url("m.doviz.com", "/haberler/dunya")
    assert url == "https://m.doviz.com/haberler/dunya"


def test_mweb_phantom_breadcrumb_haber_stripped():
    bad = (
        "https://m.doviz.com/altin/haber/altin-ve-degerli-metal-haberleri/"
        "altin-fiyatlarina-trump-destegi/878112"
    )
    assert is_m_doviz_phantom_breadcrumb_url(bad)
    fixed = repair_seo_audit_url(bad)
    assert fixed.startswith("https://m.doviz.com/haber/")
    assert "/altin/haber/" not in fixed
    assert not is_m_doviz_phantom_breadcrumb_url(fixed)


def test_mweb_phantom_breadcrumb_kur_stripped():
    bad = "https://m.doviz.com/altin/kur/altinkaynak/amerikan-dolari"
    assert is_m_doviz_phantom_breadcrumb_url(bad)
    assert repair_seo_audit_url(bad) == "https://m.doviz.com/kur/altinkaynak/amerikan-dolari"


def test_mweb_harem_under_altin_unchanged():
    url = "https://m.doviz.com/altin/harem/ons"
    assert not is_m_doviz_phantom_breadcrumb_url(url)
    assert repair_seo_audit_url(url) == url


def test_seo_audit_from_ga4_mweb_phantom_path():
    raw = (
        "/altin/haber/altin-ve-degerli-metal-haberleri/"
        "altin-fiyatlarina-trump-destegi/878112"
    )
    u = seo_audit_url_from_ga4("m.doviz.com", raw, ga4_profile="mweb")
    assert u.startswith("https://m.doviz.com/haber/")
    assert "/altin/haber" not in u
