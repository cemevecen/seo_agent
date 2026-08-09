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


def test_top20_sc_source_classifies_drop_and_rise():
    from backend.services.alert_engine import _position_drop_from_row, _position_rise_from_row

    top = build_search_console_top_queries(
        [
            {"query": "down q", "clicks": 200, "impressions": 1000, "position": 5.0},
            {"query": "up q", "clicks": 150, "impressions": 800, "position": 2.0},
            {"query": "flat q", "clicks": 100, "impressions": 500, "position": 3.0},
        ],
        [
            {"query": "down q", "clicks": 180, "impressions": 900, "position": 4.0},
            {"query": "up q", "clicks": 140, "impressions": 750, "position": 3.0},
            {"query": "flat q", "clicks": 100, "impressions": 500, "position": 3.0},
        ],
        limit=20,
    )
    assert len(top) == 3
    rows = [
        {
            "query": r["query"],
            "position": r["position_current"],
            "previous_position": r["position_previous"],
            "clicks": r["clicks_current"],
            "impressions": r.get("impressions_current") or 0,
        }
        for r in top
    ]
    drops = [d for d in (_position_drop_from_row(r, min_diff=0.1) for r in rows) if d]
    rises = [u for u in (_position_rise_from_row(r, min_diff=0.1) for r in rows) if u]
    assert [d["query"] for d in drops] == ["down q"]
    assert [u["query"] for u in rises] == ["up q"]
