"""Realtime Haberler path filtresi."""

from backend.collectors.ga4 import (
    is_doviz_realtime_haber_row,
    is_realtime_haber_path,
    realtime_haber_row_allowed,
)


def test_haber_detail_path():
    assert is_realtime_haber_path("/gundem-haberleri/baslik/837872")
    assert is_realtime_haber_path("/haber/ekonomi/12345/")


def test_haber_category_path():
    assert is_realtime_haber_path("/gundem-haberleri")
    assert is_realtime_haber_path("/dunya-haberleri")


def test_non_haber_rejected():
    assert not is_realtime_haber_path("/altin-fiyatlari")
    assert not is_realtime_haber_path("/canli-doviz")
    assert not is_realtime_haber_path("Güncel altın fiyatları")


def test_doviz_haber_host_path():
    assert is_doviz_realtime_haber_row(
        "haber.doviz.com",
        "/gundem-haberleri/mayis-ayi-aclik-ve-yoksulluk-siniri-rakamlari-aciklandi/877640",
    )
    assert is_doviz_realtime_haber_row("haber.doviz.com", "/")
    assert not is_doviz_realtime_haber_row("www.doviz.com", "/")
    assert realtime_haber_row_allowed(
        "www.doviz.com",
        None,
        "/gundem-haberleri/google-dan-dogaya-mudahale/877616",
    )
    assert realtime_haber_row_allowed("www.doviz.com", None, "/dunya-haberleri")
    assert not realtime_haber_row_allowed("www.doviz.com", None, "/altin-fiyatlari")


def test_doviz_haber_url_builder():
    from backend.collectors.ga4 import doviz_haber_url

    assert (
        doviz_haber_url("/dunya-haberleri", "www.doviz.com")
        == "https://haber.doviz.com/dunya-haberleri"
    )
    assert (
        doviz_haber_url("/gundem-haberleri/x/877640", None)
        == "https://haber.doviz.com/gundem-haberleri/x/877640"
    )


def test_normalize_news_title_key():
    from backend.collectors.ga4 import _normalize_news_title_key

    assert _normalize_news_title_key(
        "Mayıs ayı açlık ve yoksulluk sınırı rakamları açıklandı - Döviz.com"
    ) == _normalize_news_title_key("mayıs ayı açlık ve yoksulluk sınırı rakamları açıklandı")
