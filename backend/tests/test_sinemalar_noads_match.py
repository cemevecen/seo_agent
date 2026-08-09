"""noAds ↔ policy URL eşleştirme birim testleri."""

from backend.services.sinemalar_noads import (
    build_noads_keyset,
    entity_keys_from_text,
    normalize_url,
    violation_matches,
)


def test_normalize_strips_www_and_m():
    assert normalize_url("https://www.sinemalar.com/film/foo/123") == "sinemalar.com/film/foo/123"
    assert normalize_url("https://m.sinemalar.com/mobileweb/movieInfo/123") == "sinemalar.com/mobileweb/movieinfo/123"


def test_entity_keys_movie():
    keys = entity_keys_from_text("https://m.sinemalar.com/mobileweb/movieInfo/998877")
    assert "id:998877" in keys
    assert "movie:998877" in keys


def test_match_by_id_from_noads_list():
    keys = build_noads_keyset(
        [
            {"entity_id": "998877", "label": "Some Film"},
            {"url": "https://www.sinemalar.com/film/x/112233"},
        ]
    )
    assert violation_matches("https://m.sinemalar.com/mobileweb/movieInfo/998877", keys)
    assert violation_matches("https://www.sinemalar.com/film/slug/112233", keys)
    assert not violation_matches("https://m.sinemalar.com/mobileweb/movieInfo/1", keys)
