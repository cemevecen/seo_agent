"""Top backlink ranking helpers (hedef URL, IP domain)."""

from backend.services.backlink_csv import (
    target_url_belongs_to_site,
    _canonical_target_key,
    _referrer_excluded_from_top_rankings,
)
from backend.services.backlink_risk import domain_is_ip_host


def test_domain_is_ip_host():
    assert domain_is_ip_host("192.168.1.1")
    assert domain_is_ip_host("10.0.0.5")
    assert not domain_is_ip_host("example.com")
    assert not domain_is_ip_host("doviz.com")


def test_target_url_belongs_to_site():
    site = "doviz.com"
    assert target_url_belongs_to_site("https://www.doviz.com/dolar", site)
    assert target_url_belongs_to_site("https://haber.doviz.com/ekonomi", site)
    assert target_url_belongs_to_site("/altin/fiyatlari", site)
    assert not target_url_belongs_to_site("https://evil-porn.example/page", site)


def test_canonical_target_key_groups_paths():
    site = "doviz.com"
    a = _canonical_target_key("https://www.doviz.com/dolar/", site)
    b = _canonical_target_key("https://doviz.com/dolar", site)
    assert a != b  # www vs bare host — acceptable split
    assert _canonical_target_key("/dolar", site) == "doviz.com/dolar"


def test_referrer_excluded_adult():
    assert _referrer_excluded_from_top_rankings('["adult"]')
    assert not _referrer_excluded_from_top_rankings('["editorial_path"]')
