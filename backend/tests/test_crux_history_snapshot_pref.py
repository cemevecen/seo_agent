"""CrUX history snapshot: scrape tek nokta vs çok haftalık History tercihi."""

from backend.services.warehouse import (
    _crux_history_period_count,
    _crux_payload_is_scrape,
)


def test_scrape_payload_detected():
    assert _crux_payload_is_scrape({"source": "pagespeed_web_scrape"}, {})
    assert _crux_payload_is_scrape({}, {"source": "pagespeed_web_scrape"})
    assert not _crux_payload_is_scrape({"history": {}}, {"series": {}})


def test_period_count_from_collection_periods():
    payload = {
        "history": {
            "record": {
                "collectionPeriods": [{"lastDate": {"year": 2026, "month": 1, "day": i}} for i in range(1, 26)]
            }
        }
    }
    assert _crux_history_period_count(payload, {}) == 25


def test_period_count_from_series_points():
    summary = {
        "series": {
            "largest_contentful_paint": {
                "points": [{"label": f"2026-01-{i:02d}"} for i in range(1, 11)]
            }
        }
    }
    assert _crux_history_period_count({}, summary) == 10
