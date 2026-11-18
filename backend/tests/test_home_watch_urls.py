from backend.services.home_watch_urls import (
    WATCH_CATEGORIES,
    WATCH_DETAILS,
    _hosts_for,
    _match_exact_key,
    _match_tree_keys,
    _parse_url,
    _path_belongs_tree,
    _path_variants,
    _pct_delta,
    _watch_targets,
)


def test_watch_lists():
    assert len(WATCH_CATEGORIES) == 10
    assert WATCH_CATEGORIES[0]["label"] == "Kur"
    assert any(d["url"].endswith("/serbest-piyasa/amerikan-dolari") for d in WATCH_DETAILS)
    assert any(d["url"].endswith("/akaryakit-fiyatlari") for d in WATCH_DETAILS)
    assert any(d["url"].endswith("/ev-sarj-fiyatlari") for d in WATCH_DETAILS)


def test_parse_and_hosts():
    host, path = _parse_url("https://www.doviz.com/emtia")
    assert host == "www.doviz.com"
    assert path == "/emtia"
    assert _hosts_for(host) == ["www.doviz.com", "m.doviz.com"]
    assert _path_variants(path) == ["/emtia", "/emtia/"]


def test_targets_include_details():
    targets = _watch_targets()
    urls = {t["url"] for t in targets}
    assert "https://kur.doviz.com/" in urls
    assert "https://kur.doviz.com/serbest-piyasa/amerikan-dolari" in urls
    assert "https://www.doviz.com/akaryakit-fiyatlari" in urls
    detail = next(t for t in targets if t["url"].endswith("/amerikan-dolari") and "serbest" in t["url"])
    assert detail["kind"] == "detail"
    assert detail["scope"] == "exact"


def test_exact_vs_tree_matching():
    targets = _watch_targets()
    assert _match_exact_key("kur.doviz.com", "/", targets) == "https://kur.doviz.com/"
    assert _match_exact_key("kur.doviz.com", "/serbest-piyasa/amerikan-dolari", targets) == (
        "https://kur.doviz.com/serbest-piyasa/amerikan-dolari"
    )
    tree_keys = _match_tree_keys("kur.doviz.com", "/serbest-piyasa/amerikan-dolari", targets)
    assert "https://kur.doviz.com/" in tree_keys
    assert "https://kur.doviz.com/serbest-piyasa/amerikan-dolari" in tree_keys

    hisseler = next(t for t in targets if t["url"].endswith("/hisseler") and t["kind"] == "detail")
    assert _path_belongs_tree(hisseler, "borsa.doviz.com", "/hisseler/ktlev-katilimevim-tas-fin")
    usd = next(t for t in targets if t["url"].endswith("/serbest-piyasa/amerikan-dolari"))
    assert not _path_belongs_tree(usd, "kur.doviz.com", "/serbest-piyasa/amerikan-dolari/extra")


def test_pct_delta_tones():
    fmt, tone, pct = _pct_delta(120, 100)
    assert fmt == "+20.0%"
    assert tone == "up-strong"
    assert pct == 20.0
