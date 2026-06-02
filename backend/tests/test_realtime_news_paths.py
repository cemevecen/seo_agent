"""Realtime haber path filtresi."""

from backend.services.realtime_news_paths import (
    is_realtime_news_path,
    realtime_news_page_link,
    unified_screen_news_article_title,
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
    assert is_realtime_news_path(
        "/merkez-bankasi-faiz-karari/837872",
        site_domain="haber.doviz.com",
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
    assert not unified_screen_news_candidate("Borsa Endeksleri", site_domain="www.doviz.com")
    assert not unified_screen_news_candidate("Gümüş Ons fiyatları", site_domain="www.doviz.com")
    assert not unified_screen_news_candidate("Geçmiş Halka Arzlar", site_domain="www.doviz.com")
    assert unified_screen_news_candidate("Ekonomi Haberleri", site_domain="www.doviz.com")
    assert unified_screen_news_candidate(
        "Merkez Bankası faiz kararı açıklandı",
        site_domain="www.doviz.com",
    )


def test_news_article_title_includes_trump_headline():
    assert unified_screen_news_article_title(
        "Altın fiyatlarına Trump desteği",
        site_domain="www.doviz.com",
    )
    assert unified_screen_news_article_title(
        "Havalimanlarında ücretsiz içme suyu dönemi",
        site_domain="www.doviz.com",
    )
    assert not unified_screen_news_article_title(
        "Canlı Gram Altın Fiyatı - Anlık Gram Altın Ne Kadar?",
        site_domain="www.doviz.com",
    )
    assert not unified_screen_news_article_title(
        "Canlı Emtia Fiyatları ve Emtia Piyasası",
        site_domain="www.doviz.com",
    )


def test_realtime_news_link_haber_host():
    url = realtime_news_page_link("/gundem-haberleri/ornek-baslik", site_domain="www.doviz.com")
    assert url
    assert "haber.doviz.com" in url
    assert "gundem-haberleri" in url
