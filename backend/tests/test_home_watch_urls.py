from backend.services.home_watch_urls import (
    WATCH_URLS,
    _hosts_for,
    _match_key,
    _parse_url,
    _path_variants,
    _pct_delta,
    _watch_targets,
)


def test_watch_url_count_and_order():
    assert len(WATCH_URLS) == 10
    assert WATCH_URLS[0]["label"] == "Kur"
    assert WATCH_URLS[-1]["label"] == "Kredi karşılaştırma"


def test_parse_and_hosts():
    host, path = _parse_url("https://www.doviz.com/emtia")
    assert host == "www.doviz.com"
    assert path == "/emtia"
    assert _hosts_for(host) == ["www.doviz.com", "m.doviz.com"]
    assert _path_variants(path) == ["/emtia", "/emtia/"]

    host2, path2 = _parse_url("https://kur.doviz.com/")
    assert host2 == "kur.doviz.com"
    assert path2 == "/"
    assert _hosts_for(host2) == ["kur.doviz.com"]


def test_match_key_sums_mweb_host():
    targets = _watch_targets()
    assert _match_key("m.doviz.com", "/emtia", targets) == "https://www.doviz.com/emtia"
    assert _match_key("www.doviz.com", "/emtia/", targets) == "https://www.doviz.com/emtia"
    assert _match_key("kur.doviz.com", "/", targets) == "https://kur.doviz.com/"
    assert _match_key("www.doviz.com", "/", targets) is None


def test_pct_delta_tones():
    fmt, tone, pct = _pct_delta(120, 100)
    assert fmt == "+20.0%"
    assert tone == "up-strong"
    assert pct == 20.0
    fmt2, tone2, _ = _pct_delta(0, 0)
    assert fmt2 == "0%"
    assert tone2 == "flat"
