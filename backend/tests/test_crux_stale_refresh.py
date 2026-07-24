"""CrUX stale detection and period helpers."""

from datetime import date, datetime

from backend.main import (
    _crux_latest_period_date,
    _crux_snapshot_is_stale,
    _format_crux_series,
)


def test_crux_latest_period_date_from_points():
    snap = {
        "summary": {
            "series": {
                "largest_contentful_paint": {
                    "points": [
                        {"label": "2026-04-11", "period_last": "2026-04-11", "value": 1},
                        {"label": "2026-04-18", "period_last": "2026-04-18", "value": 2},
                    ]
                }
            }
        }
    }
    assert _crux_latest_period_date(snap) == date(2026, 4, 18)


def test_crux_snapshot_is_stale_by_period():
    snap = {
        "summary": {
            "series": {
                "lcp": {
                    "points": [{"label": "2026-04-18", "period_last": "2026-04-18", "value": 1}]
                }
            }
        },
        "collected_at": "2026-04-21T02:00:00",
    }
    assert _crux_snapshot_is_stale(snap, today=date(2026, 7, 24), max_lag_days=14) is True
    assert _crux_snapshot_is_stale(snap, today=date(2026, 4, 25), max_lag_days=14) is False


def test_crux_snapshot_fresh_when_recent_period():
    snap = {
        "summary": {
            "series": {
                "lcp": {
                    "points": [{"label": "2026-07-18", "period_last": "2026-07-18", "value": 1}]
                }
            }
        },
        "collected_at": "2026-07-20T05:00:00",
    }
    assert _crux_snapshot_is_stale(snap, today=date(2026, 7, 24), max_lag_days=14) is False


def test_format_crux_series_appends_newer_current_period():
    snap = {
        "summary": {
            "series": {
                "largest_contentful_paint": {
                    "label": "LCP",
                    "latest": 1800,
                    "points": [
                        {
                            "label": "2026-04-18",
                            "period_last": "2026-04-18",
                            "period_first": "2026-03-21",
                            "value": 1900,
                        }
                    ],
                }
            },
            "current": {"largest_contentful_paint": {"label": "LCP", "latest": 1700}},
            "current_collection_period": {
                "first_date": "2026-06-21",
                "last_date": "2026-07-18",
            },
        }
    }
    formatted = _format_crux_series(snap)
    chart = formatted["largest_contentful_paint"]["chart"]
    assert chart["x"][-1] == "2026-07-18"
    assert chart["y"][-1] == 1700
    assert formatted["largest_contentful_paint"]["latest_period_last"] == "2026-07-18"
