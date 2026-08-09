"""Top 20 chip — Search Console Top queries kaynağı."""
from backend.services.sc_top_queries import build_search_console_top_queries, sc_position_delta


def test_build_search_console_top_queries_sorts_by_clicks_and_deltas():
    current = [
        {"query": "döviz", "device": "MOBILE", "clicks": 100, "impressions": 1000, "position": 1.2},
        {"query": "döviz", "device": "DESKTOP", "clicks": 50, "impressions": 400, "position": 1.0},
        {"query": "altın", "device": "MOBILE", "clicks": 80, "impressions": 800, "position": 3.0},
    ]
    previous = [
        {"query": "döviz", "device": "MOBILE", "clicks": 90, "impressions": 900, "position": 1.0},
        {"query": "döviz", "device": "DESKTOP", "clicks": 40, "impressions": 350, "position": 0.9},
        {"query": "altın", "device": "MOBILE", "clicks": 70, "impressions": 700, "position": 4.0},
    ]
    top = build_search_console_top_queries(current, previous, limit=20)
    assert [r["query"] for r in top] == ["döviz", "altın"]
    assert top[0]["clicks_current"] == 150.0
    # previous − current: döviz weighted pos worsened slightly → negative SC delta
    assert top[0]["position_diff"] == sc_position_delta(
        top[0]["position_current"], top[0]["position_previous"]
    )
    # altın improved 4 → 3 → positive SC delta
    assert top[1]["position_diff"] > 0


def test_top20_matches_sc_mobile_builder_row_shape():
    from backend.services.sc_top_queries import build_search_console_top_queries

    current = [{"query": "döviz", "device": "MOBILE", "clicks": 100, "impressions": 1000, "position": 1.5}]
    previous = [{"query": "döviz", "device": "MOBILE", "clicks": 90, "impressions": 900, "position": 1.2}]
    top = build_search_console_top_queries(current, previous, limit=20)
    assert top[0]["query"] == "döviz"
    assert top[0]["clicks_current"] == 100.0
    assert top[0]["position_current"] == 1.5
    assert top[0]["position_previous"] == 1.2
