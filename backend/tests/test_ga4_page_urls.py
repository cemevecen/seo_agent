from backend.services.ga4_page_urls import (
    ga4_canonical_page_url,
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
    u = seo_audit_url_from_ga4("m.doviz.com", "/besli-altin")
    assert u == "https://m.doviz.com/altin/besli-altin"


def test_mweb_haber_root_unchanged():
    url = ga4_canonical_page_url("m.doviz.com", "/haberler/dunya")
    assert url == "https://m.doviz.com/haberler/dunya"
