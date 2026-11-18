from backend.services.home_watch_urls import (
    WATCH_URLS,
    _hosts_for,
    _match_exact_key,
    _match_tree_keys,
    _parse_url,
    _path_belongs_tree,
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


def test_exact_vs_tree_matching():
    targets = _watch_targets()
    assert _match_exact_key("kur.doviz.com", "/", targets) == "https://kur.doviz.com/"
    assert _match_exact_key("kur.doviz.com", "/amerikan-dolari", targets) is None

    tree_keys = _match_tree_keys("kur.doviz.com", "/amerikan-dolari", targets)
    assert "https://kur.doviz.com/" in tree_keys

    assert _match_exact_key("www.doviz.com", "/emtia", targets) == "https://www.doviz.com/emtia"
    assert _match_tree_keys("m.doviz.com", "/emtia/altin", targets) == [
        "https://www.doviz.com/emtia"
    ]
    # /emtialar should NOT match /emtia tree
    emtia = next(t for t in targets if t["path"] == "/emtia")
    assert not _path_belongs_tree(emtia, "www.doviz.com", "/emtialar")

    # borsa root tree includes halka-arz; halka-arz exact is separate
    assert "https://borsa.doviz.com/" in _match_tree_keys(
        "borsa.doviz.com", "/halka-arz", targets
    )
    assert _match_exact_key("borsa.doviz.com", "/halka-arz", targets) == (
        "https://borsa.doviz.com/halka-arz"
    )


def test_pct_delta_tones():
    fmt, tone, pct = _pct_delta(120, 100)
    assert fmt == "+20.0%"
    assert tone == "up-strong"
    assert pct == 20.0
    fmt2, tone2, _ = _pct_delta(0, 0)
    assert fmt2 == "0%"
    assert tone2 == "flat"
