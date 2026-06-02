"""SEO audit URL filtresi — GA4 (other) yer tutucuları ve özel hariç sayfalar."""

from backend.services.ga4_page_urls import (
    is_seo_audit_crawl_url,
    is_seo_audit_excluded_url,
    seo_audit_url_from_ga4,
)


def test_seo_audit_rejects_other_urls():
    assert not is_seo_audit_crawl_url("https://(other)(other)/")
    assert not is_seo_audit_crawl_url("https://(other)/path")
    assert not is_seo_audit_crawl_url("https://www.doviz.com/(other)")
    assert not seo_audit_url_from_ga4("(other)", "/")
    assert not seo_audit_url_from_ga4("www.doviz.com", "(other)")


def test_seo_audit_accepts_real_urls():
    u = seo_audit_url_from_ga4("www.doviz.com", "/dolar")
    assert u == "https://www.doviz.com/dolar"
    assert is_seo_audit_crawl_url(u)


def test_seo_audit_excludes_yorum_listing_only():
    assert is_seo_audit_excluded_url("https://www.doviz.com/yorum")
    assert is_seo_audit_excluded_url("https://www.doviz.com/yorum/")
    assert is_seo_audit_excluded_url("https://www.doviz.com/yorum?tab=1")
    assert not is_seo_audit_excluded_url("https://www.doviz.com/yorum/123")
    assert not is_seo_audit_excluded_url("https://www.doviz.com/yorum/baslik-slug")
    assert not seo_audit_url_from_ga4("www.doviz.com", "/yorum")
    assert seo_audit_url_from_ga4("www.doviz.com", "/yorum/123") == "https://www.doviz.com/yorum/123"
    assert not is_seo_audit_crawl_url("https://www.doviz.com/yorum")
    assert is_seo_audit_crawl_url("https://www.doviz.com/yorum/456")
