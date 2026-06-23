from backend.services.ga4_page_urls import (
    ga4_canonical_page_url,
    ga4_url_match_keys,
    is_m_doviz_flat_product_url,
    is_m_doviz_phantom_breadcrumb_url,
    normalize_seo_audit_doviz_fuel_url,
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


def test_ga4_url_match_keys_sc_page_full_url():
    keys = set(ga4_url_match_keys("https://www.doviz.com/kur", "www.doviz.com"))
    assert "doviz.com/kur" in keys
    assert "/kur" in keys


def test_ga4_url_match_keys_ga4_href_and_sc_label_align():
    sc_label = "https://m.doviz.com/altin/gram-altin"
    ga4_href = ga4_canonical_page_url("m.doviz.com", "/gram-altin")
    sc_keys = set(ga4_url_match_keys(sc_label, "www.doviz.com"))
    ga4_keys = set(ga4_url_match_keys(ga4_href, "www.doviz.com"))
    assert sc_keys & ga4_keys


def test_seo_audit_from_ga4_mweb_phantom_path():
    raw = (
        "/altin/haber/altin-ve-degerli-metal-haberleri/"
        "altin-fiyatlarina-trump-destegi/878112"
    )
    u = seo_audit_url_from_ga4("m.doviz.com", raw, ga4_profile="mweb")
    assert u.startswith("https://m.doviz.com/haber/")
    assert "/altin/haber" not in u


# Canlı m.doviz.com kategori, haber ve büro yolları — breadcrumb/GA4 düzeltmesi dokunmamalı
_MWEB_VALID_CATEGORY_PATHS = (
    "/kur",
    "/altin",
    "/kripto-paralar",
    "/borsa",
    "/borsa/halka-arz",
    "/emtia",
    "/akaryakit-fiyatlari",
    "/ev-sarj-fiyatlari",
    "/yakit-sarj",
    "/haberler",
    "/borsa/temettu-ve-sermaye-artirimi-takvimi",
    "/kredi",
    "/ekonomik-takvim",
    "/borsa/borsa-yatirim-fonlari",
    "/tahvil",
    "/pariteler",
    "/altin-cevirici",
    "/doviz-cevirici",
)

_MWEB_VALID_HABER_PATHS = (
    "/haberler",
    "/haber/dunya-haberleri",
    "/haber/yerel-ve-sektorel-haberleri",
    "/haber/gundem-haberleri",
    "/haber/borsa-haberleri",
    "/haber/doviz-haberleri",
    "/haber/altin-ve-degerli-metal-haberleri",
    "/haber/emtia-haberleri",
)

_MWEB_VALID_BUREAU_PATHS = (
    "/altin/altinkaynak/gram-altin",
    "/altin/kapalicarsi",
    "/altin/ziraat-dinamik",
    "/altin/turkiye-finans",
    "/kur/kapalicarsi/amerikan-dolari",
    "/kur/altinkaynak",
    "/kur/papara",
    "/kur/harem/amerikan-dolari",
    "/kur/ziraat-bankasi/sterlin",
)

_MWEB_PHANTOM_FIXES = (
    ("/altin/kur", "/kur"),
    ("/altin/haber/gundem-haberleri", "/haber/gundem-haberleri"),
    ("/altin/borsa/halka-arz", "/borsa/halka-arz"),
    ("/altin/tahvil", "/tahvil"),
    ("/altin/ekonomik-takvim", "/ekonomik-takvim"),
    ("/altin/kur/kapalicarsi/amerikan-dolari", "/kur/kapalicarsi/amerikan-dolari"),
    ("/altin/altin-cevirici", "/altin-cevirici"),
    ("/altin/ev-sarj-fiyatlari", "/ev-sarj-fiyatlari"),
    ("/altin/yakit-sarj", "/yakit-sarj"),
)


def _mweb_url(path: str) -> str:
    return f"https://m.doviz.com{path}"


def test_mweb_valid_category_paths_unchanged():
    for path in _MWEB_VALID_CATEGORY_PATHS:
        url = _mweb_url(path)
        assert not is_m_doviz_phantom_breadcrumb_url(url)
        assert not is_m_doviz_flat_product_url(url)
        assert repair_seo_audit_url(url) == url
        assert ga4_canonical_page_url("m.doviz.com", path) == url


def test_mweb_valid_haber_paths_unchanged():
    for path in _MWEB_VALID_HABER_PATHS:
        url = _mweb_url(path)
        assert not is_m_doviz_phantom_breadcrumb_url(url)
        assert repair_seo_audit_url(url) == url


def test_mweb_valid_bureau_paths_unchanged():
    for path in _MWEB_VALID_BUREAU_PATHS:
        url = _mweb_url(path)
        assert not is_m_doviz_phantom_breadcrumb_url(url)
        assert repair_seo_audit_url(url) == url


def test_mweb_altin_cevirici_not_under_altin_prefix():
    url = ga4_canonical_page_url("m.doviz.com", "/altin-cevirici")
    assert url == "https://m.doviz.com/altin-cevirici"
    u = seo_audit_url_from_ga4("m.doviz.com", "/altin-cevirici", ga4_profile="mweb")
    assert u == "https://m.doviz.com/altin-cevirici"
    bad = "https://m.doviz.com/altin/altin-cevirici"
    assert is_m_doviz_phantom_breadcrumb_url(bad)
    assert repair_seo_audit_url(bad) == "https://m.doviz.com/altin-cevirici"


def test_mweb_phantom_category_prefix_stripped():
    for bad_suffix, good_suffix in _MWEB_PHANTOM_FIXES:
        bad = _mweb_url(bad_suffix)
        good = _mweb_url(good_suffix)
        assert is_m_doviz_phantom_breadcrumb_url(bad)
        assert repair_seo_audit_url(bad) == good
        assert not is_m_doviz_phantom_breadcrumb_url(good)


def test_absolute_audit_href_full_url_unchanged():
    from backend.services.ga4_page_urls import absolute_audit_href

    u = "https://m.doviz.com/haber/borsa-haberleri/foo/880554"
    assert absolute_audit_href("www.doviz.com", u) == u


def test_absolute_audit_href_relative_path():
    from backend.services.ga4_page_urls import absolute_audit_href

    assert absolute_audit_href("www.doviz.com", "/kripto-paralar/avalanche") == (
        "https://www.doviz.com/kripto-paralar/avalanche"
    )


def test_absolute_audit_href_no_double_host():
    from backend.services.ga4_page_urls import absolute_audit_href

    bad = "https://www.doviz.comhttps://www.doviz.com/foo"
    # Tam URL ise olduğu gibi (bozuk kayıt ayrı temizlenir); göreli path birleşmez
    full = "https://www.doviz.com/kripto-paralar/avalanche"
    assert absolute_audit_href("www.doviz.com", full) == full
    assert "www.doviz.comhttps" not in absolute_audit_href("www.doviz.com", full)


def test_mweb_ev_sarj_no_altin_prefix():
    url = ga4_canonical_page_url("m.doviz.com", "/ev-sarj-fiyatlari")
    assert url == "https://m.doviz.com/ev-sarj-fiyatlari"
    assert "/altin/" not in url


def test_mweb_yakit_sarj_no_altin_prefix():
    url = ga4_canonical_page_url("m.doviz.com", "/yakit-sarj")
    assert url == "https://m.doviz.com/yakit-sarj"


def test_seo_audit_mweb_ev_sarj_maps_to_www():
    u = seo_audit_url_from_ga4("www.doviz.com", "/ev-sarj-fiyatlari", ga4_profile="mweb")
    assert u == "https://www.doviz.com/ev-sarj-fiyatlari"


def test_seo_audit_mweb_yakit_sarj_maps_to_www():
    u = seo_audit_url_from_ga4("m.doviz.com", "/yakit-sarj", ga4_profile="mweb")
    assert u == "https://www.doviz.com/yakit-sarj"


def test_phantom_breadcrumb_ev_sarj_repaired_to_www():
    bad = "https://m.doviz.com/altin/ev-sarj-fiyatlari"
    assert is_m_doviz_phantom_breadcrumb_url(bad)
    assert normalize_seo_audit_doviz_fuel_url(repair_seo_audit_url(bad)) == (
        "https://www.doviz.com/ev-sarj-fiyatlari"
    )


def test_phantom_breadcrumb_yakit_sarj_repaired_to_www():
    bad = "https://m.doviz.com/altin/yakit-sarj"
    assert is_m_doviz_phantom_breadcrumb_url(bad)
    assert normalize_seo_audit_doviz_fuel_url(repair_seo_audit_url(bad)) == (
        "https://www.doviz.com/yakit-sarj"
    )
