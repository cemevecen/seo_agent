"""Realtime Haberler path filtresi."""

from backend.collectors.ga4 import is_realtime_haber_path


def test_haber_detail_path():
    assert is_realtime_haber_path("/gundem-haberleri/baslik/837872")
    assert is_realtime_haber_path("/haber/ekonomi/12345/")


def test_haber_category_path():
    assert is_realtime_haber_path("/gundem-haberleri")
    assert is_realtime_haber_path("/haber/")


def test_non_haber_rejected():
    assert not is_realtime_haber_path("/altin-fiyatlari")
    assert not is_realtime_haber_path("/canli-doviz")
    assert not is_realtime_haber_path("Güncel altın fiyatları")
    assert not is_realtime_haber_path("/vizyondaki-filmler")
