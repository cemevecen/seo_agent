"""Realtime haber path filtresi."""

from backend.services.realtime_news_paths import (
    is_realtime_news_path,
    realtime_news_page_link,
    unified_screen_news_candidate,
)


def test_haber_doviz_category_and_article():
    assert is_realtime_news_path("/ekonomi-haberleri", site_domain="www.doviz.com")
    assert is_realtime_news_path("/ekonomi-haberleri/", site_domain="www.doviz.com")
    assert is_realtime_news_path(
        "/ekonomi-haberleri/dolar-rekor-kirdi",
        site_domain="www.doviz.com",
    )
    assert is_realtime_news_path(
        "/ekonomi-haberleri/dolar-rekor-kirdi/837872",
        site_domain="www.doviz.com",
    )
    assert is_realtime_news_path("/", site_domain="haber.doviz.com")


def test_haber_doviz_rejects_market_paths():
    assert not is_realtime_news_path("/gram-altin", site_domain="www.doviz.com")
    assert not is_realtime_news_path("/ons", site_domain="haber.doviz.com")
    assert not is_realtime_news_path("/doviz-cevirici", site_domain="haber.doviz.com")
    assert not is_realtime_news_path("/kripto-paralar", site_domain="haber.doviz.com")


def test_unified_title_rejects_live_rates():
    assert not unified_screen_news_candidate("Canlı Dolar — Güncel Kur", site_domain="www.doviz.com")
    assert not unified_screen_news_candidate("Güncel Altın Fiyatları", site_domain="www.doviz.com")
    assert unified_screen_news_candidate("Ekonomi Haberleri", site_domain="www.doviz.com")


def test_realtime_news_link_haber_host():
    url = realtime_news_page_link("/gundem-haberleri/ornek-baslik", site_domain="www.doviz.com")
    assert url
    assert "haber.doviz.com" in url
    assert "gundem-haberleri" in url
