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


def test_mweb_path_aliases():
    """m.doviz.com/kur|altin|borsa ↔ desktop subdomain."""
    from backend.services.home_watch_urls import _mweb_alias_paths

    assert _mweb_alias_paths("kur.doviz.com", "/") == ["/kur"]
    assert _mweb_alias_paths("kur.doviz.com", "/serbest-piyasa/amerikan-dolari") == [
        "/kur/serbest-piyasa/amerikan-dolari"
    ]
    assert _mweb_alias_paths("altin.doviz.com", "/gram-altin") == ["/altin/gram-altin"]
    assert _mweb_alias_paths("www.doviz.com", "/emtia") == []

    targets = _watch_targets()
    assert _match_exact_key("m.doviz.com", "/kur", targets) == "https://kur.doviz.com/"
    assert _match_exact_key("m.doviz.com", "/kur/serbest-piyasa/amerikan-dolari", targets) == (
        "https://kur.doviz.com/serbest-piyasa/amerikan-dolari"
    )
    assert "https://kur.doviz.com/" in _match_tree_keys(
        "m.doviz.com", "/kur/harem/amerikan-dolari", targets
    )
    assert _match_exact_key("m.doviz.com", "/altin/gram-altin", targets) == (
        "https://altin.doviz.com/gram-altin"
    )
    assert "https://borsa.doviz.com/" in _match_tree_keys(
        "m.doviz.com", "/borsa/hisseler", targets
    )


def test_www_paths_still_match_on_mweb_host():
    """www.doviz.com/emtia ↔ m.doviz.com/emtia (prefix yok)."""
    targets = _watch_targets()
    assert _match_exact_key("m.doviz.com", "/emtia", targets) == "https://www.doviz.com/emtia"
    assert _match_exact_key("www.doviz.com", "/emtia", targets) == "https://www.doviz.com/emtia"
    assert "https://www.doviz.com/emtia" in _match_tree_keys(
        "m.doviz.com", "/emtia/brent-petrol", targets
    )
    assert _match_exact_key("m.doviz.com", "/pariteler", targets) == (
        "https://www.doviz.com/pariteler"
    )
    assert _match_exact_key("m.doviz.com", "/kripto-paralar", targets) == (
        "https://www.doviz.com/kripto-paralar"
    )
    assert _match_exact_key("m.doviz.com", "/yakit-sarj", targets) == (
        "https://www.doviz.com/yakit-sarj"
    )
    assert _match_exact_key("m.doviz.com", "/kredi-karsilastirma", targets) == (
        "https://www.doviz.com/kredi-karsilastirma"
    )


def test_pct_delta_tones():
    fmt, tone, pct = _pct_delta(120, 100)
    assert fmt == "+20.0%"
    assert tone == "up-strong"
    assert pct == 20.0


def test_period_windows_1g_is_same_weekday_wow():
    """1g: dün vs 7 gün önceki aynı hafta günü; 7g ardışık kalır."""
    from datetime import date, timedelta

    from backend.collectors.ga4 import report_calendar_yesterday
    from backend.services.home_watch_urls import _period_windows

    yesterday = report_calendar_yesterday()
    (ls, le), (ps, pe) = _period_windows(1)
    assert ls == le == yesterday.isoformat()
    assert ps == pe == (yesterday - timedelta(days=7)).isoformat()
    assert date.fromisoformat(ls).weekday() == date.fromisoformat(ps).weekday()

    (ls7, le7), (ps7, pe7) = _period_windows(7)
    assert ls7 != ps7
    assert (
        date.fromisoformat(le7) - date.fromisoformat(ls7)
    ).days == 6
    assert date.fromisoformat(ps7) == date.fromisoformat(ls7) - timedelta(days=7)
