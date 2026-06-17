"""Search Console 12 ay günlük trend — özet seri ve sinyal kontrolleri."""

from datetime import date, timedelta

from backend.collectors.search_console import _build_recent_trend_summary, _build_recent_trend_summary_by_device
from backend.main import _search_console_trend_has_signal, _slice_search_console_trend_last_days


def test_12m_trend_fills_full_date_range_with_zeros():
    end = date(2026, 6, 1)
    start = end - timedelta(days=364)
    rows = [
        {
            "date": (end - timedelta(days=1)).isoformat(),
            "device": "MOBILE",
            "clicks": 100.0,
            "impressions": 1000.0,
            "position": 5.0,
        }
    ]
    summary = _build_recent_trend_summary(rows, start_date=start, end_date=end)
    summary["mode"] = "last_12m"
    assert len(summary["dates"]) == 365
    assert summary["dates"][0] == start.isoformat()
    assert summary["dates"][-1] == end.isoformat()
    assert summary["clicks"][-2] == 100.0
    assert summary["clicks"][0] == 0.0


def test_12m_trend_by_device_separates_mobile_desktop():
    end = date(2026, 6, 1)
    start = end - timedelta(days=2)
    rows = [
        {"date": end.isoformat(), "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 3.0},
        {"date": end.isoformat(), "device": "DESKTOP", "clicks": 20.0, "impressions": 200.0, "position": 4.0},
    ]
    by_device = _build_recent_trend_summary_by_device(rows, start_date=start, end_date=end)
    assert by_device["MOBILE"]["clicks"][-1] == 10.0
    assert by_device["DESKTOP"]["clicks"][-1] == 20.0


def test_trend_has_signal_requires_traffic():
    assert not _search_console_trend_has_signal(
        {"dates": ["2026-01-01"], "clicks": [0], "impressions": [0]}
    )


def test_slice_trend_last_90_preserves_12m_mode():
    dates = [(date(2026, 1, 1) + timedelta(days=i)).isoformat() for i in range(120)]
    trend = {
        "mode": "last_12m",
        "dates": dates,
        "labels": dates,
        "clicks": [float(i) for i in range(120)],
        "impressions": [1.0] * 120,
        "ctr": [1.0] * 120,
        "position": [5.0] * 120,
    }
    sliced = _slice_search_console_trend_last_days(trend, 90)
    assert sliced["mode"] == "last_12m"
    assert len(sliced["dates"]) == 90
    assert sliced["dates"][0] == dates[30]
    assert sliced["dates"][-1] == dates[-1]


def test_trend_has_signal_with_clicks():
    assert _search_console_trend_has_signal(
        {"dates": ["2026-01-01"], "clicks": [5], "impressions": [0]}
    )
