"""Hedef sayfa iç/dış sınıflandırması."""

from backend.services.backlink_csv import referrer_belongs_to_site, target_url_belongs_to_site


def test_referrer_internal_external():
    site = "www.doviz.com"
    assert referrer_belongs_to_site("https://www.doviz.com/", site)
    assert referrer_belongs_to_site("https://kur.doviz.com/serbest-piyasa/euro", site)
    assert not referrer_belongs_to_site("https://example.com/page", site)
    assert target_url_belongs_to_site("https://haber.doviz.com/", site)
