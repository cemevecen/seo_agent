"""TMDB Türk yapımı / dizi filtre yardımcıları."""

from backend.services.tmdb import _is_turkish_origin, _tv_keep_after_on_air_filter


def test_is_turkish_origin_language():
    assert _is_turkish_origin({"original_language": "tr", "origin_country": []})


def test_is_turkish_origin_country():
    assert _is_turkish_origin({"original_language": "en", "origin_country": ["TR", "US"]})


def test_is_turkish_origin_false():
    assert not _is_turkish_origin({"original_language": "en", "origin_country": ["US"]})


def test_tv_keep_old_ended_out():
    assert not _tv_keep_after_on_air_filter({
        "first_air_date": "2005-01-01",
        "status": "Bitti",
    })


def test_tv_keep_returning_old_format():
    assert _tv_keep_after_on_air_filter({
        "first_air_date": "2010-01-01",
        "status": "Devam Ediyor",
    })
