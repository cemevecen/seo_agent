"""GA4 12 ay günlük trend — takvim doldurma ve sinyal kontrolleri."""

from datetime import date, timedelta

from backend.collectors.ga4 import _fill_daily_trend_calendar
from backend.main import _ga4_trend_has_signal


def test_12m_trend_fills_full_date_range_with_zeros():
    end = date(2026, 6, 1)
    start = end - timedelta(days=364)
    daily = {
        "dates": [(end - timedelta(days=1)).isoformat()],
        "sessions": [120.0],
        "activeUsers": [80.0],
        "engagedSessions": [60.0],
        "engagementRate": [55.0],
    }
    filled = _fill_daily_trend_calendar(daily, start=start, end=end)
    assert filled["mode"] == "last_12m"
    assert len(filled["dates"]) == 365
    assert filled["dates"][0] == start.isoformat()
    assert filled["dates"][-1] == end.isoformat()
    assert filled["sessions"][-2] == 120.0
    assert filled["sessions"][0] == 0.0


def test_ga4_trend_has_signal_requires_traffic():
    assert not _ga4_trend_has_signal(
        {"dates": ["2026-01-01"], "sessions": [0], "activeUsers": [0], "engagedSessions": [0]}
    )
    assert _ga4_trend_has_signal(
        {"dates": ["2026-01-01"], "sessions": [5], "activeUsers": [0], "engagedSessions": [0]}
    )
