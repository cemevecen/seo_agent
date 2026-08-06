"""SC pozisyon: güncel gösterim + ID eşlemesi."""

from backend.main import (
    _attach_sc_position_to_ga4_rows,
    _build_search_console_top_entities,
    _home_sc_ga4_match_keys,
    _home_sc_path_id_keys,
)


def test_path_id_keys_match_film_and_movieinfo():
    assert "id:264007" in _home_sc_path_id_keys("/movieInfo/264007")
    assert "id:264007" in _home_sc_path_id_keys("/film/264007/orumcek-adam-yepyeni-bir-gun")
    keys_ga4 = set(_home_sc_ga4_match_keys("/movieInfo/264007", "sinemalar.com"))
    keys_sc = set(
        _home_sc_ga4_match_keys(
            "https://www.sinemalar.com/film/264007/orumcek-adam-yepyeni-bir-gun",
            "sinemalar.com",
        )
    )
    assert keys_ga4 & keys_sc


def test_entities_keep_current_position_without_previous():
    current = [
        {
            "query": "https://www.sinemalar.com/film/264007/x",
            "clicks": 10,
            "impressions": 100,
            "position": 7.5,
            "device": "MOBILE",
        }
    ]
    ents = _build_search_console_top_entities(current, [], label_key="query", limit=10)
    assert len(ents) == 1
    assert ents[0]["position_current"] == 7.5
    assert ents[0]["position_previous"] == 0.0
    assert ents[0]["position_has_previous"] is False
    assert ents[0]["position_diff"] == 0.0


def test_entities_fallback_previous_when_no_current_position():
    previous = [
        {
            "query": "https://www.doviz.com/altin",
            "clicks": 5,
            "impressions": 50,
            "position": 3.2,
            "device": "DESKTOP",
        }
    ]
    ents = _build_search_console_top_entities([], previous, label_key="query", limit=10)
    assert ents[0]["position_current"] == 0.0
    assert ents[0]["position_previous"] == 3.2


def test_attach_uses_id_match_and_skips_fake_delta():
    rows = [{"page": "/movieInfo/264007", "page_host": "m.sinemalar.com", "last_total": 100}]
    current = {"id:264007": 7.76}
    diff = {"id:264007": 1.01}
    has_prev = {"id:264007": False}
    out = _attach_sc_position_to_ga4_rows(
        rows, diff, current, "sinemalar.com", has_prev_lookup=has_prev
    )
    assert out[0]["sc_position_current"] == 7.76
    assert out[0]["sc_position_has_previous"] is False
    assert out[0]["sc_position_diff"] is None
