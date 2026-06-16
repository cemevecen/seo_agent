from datetime import date
from unittest.mock import patch

from backend.services.ga4_compare_daily import (
    merge_ga4_daily_trends,
    supplement_ga4_daily_trend,
)


def test_merge_ga4_daily_trends_overlays_dates():
    a = {"dates": ["2026-06-01"], "sessions": [10.0], "activeUsers": [8.0], "engagedSessions": [5.0], "engagementRate": [50.0]}
    b = {
        "dates": ["2025-06-01", "2026-06-01"],
        "sessions": [20.0, 99.0],
        "activeUsers": [15.0, 80.0],
        "engagedSessions": [10.0, 40.0],
        "engagementRate": [40.0, 45.0],
    }
    m = merge_ga4_daily_trends(a, b)
    assert m["dates"] == ["2025-06-01", "2026-06-01"]
    assert m["sessions"] == [20.0, 99.0]


@patch("backend.services.ga4_compare_daily.fetch_ga4_daily_kpi")
@patch("backend.services.ga4_compare_daily.get_ga4_credentials_record")
def test_supplement_fetches_yoy(mock_cred, mock_fetch):
    mock_cred.return_value = object()
    mock_fetch.return_value = {
        "dates": ["2025-06-07", "2025-06-08"],
        "sessions": [1.0, 2.0],
        "activeUsers": [1.0, 2.0],
        "engagedSessions": [1.0, 2.0],
        "engagementRate": [50.0, 50.0],
    }
    compare = {"enabled": True, "mode": "previous_year"}
    periods = {
        "7": {
            "ranges": {
                "last_start": "2026-06-07",
                "last_end": "2026-06-13",
            }
        }
    }
    daily = {"dates": ["2026-06-07"], "sessions": [100.0], "activeUsers": [80.0], "engagedSessions": [60.0], "engagementRate": [50.0]}
    out = supplement_ga4_daily_trend(None, 1, "123", daily, compare, periods)
    assert "2025-06-07" in out["dates"]
    mock_fetch.assert_called_once()
