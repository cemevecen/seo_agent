from backend.main import (
    _ga4_align_daily_trend,
    _ga4_daily_trends_for_ui,
    _ga4_fill_daily_trend_from_source,
    _ga4_slice_daily_trend_last_days,
)


def test_slice_daily_trend_last_14_days():
    daily = {
        "dates": [f"2026-06-{d:02d}" for d in range(1, 21)],
        "sessions": [float(i) for i in range(1, 21)],
        "activeUsers": [1.0] * 20,
        "engagedSessions": [1.0] * 20,
        "engagementRate": [50.0] * 20,
        "newUsers": [2.0] * 20,
        "screenPageViews": [3.0] * 20,
        "averageSessionDuration": [100.0] * 20,
    }
    out = _ga4_slice_daily_trend_last_days(daily, 14)
    assert len(out["dates"]) == 14
    assert out["dates"][0] == "2026-06-07"
    assert out["sessions"][-1] == 20.0


def test_fill_missing_metrics_from_long_series():
    period = {
        "dates": ["2026-06-10", "2026-06-11"],
        "sessions": [100.0, 110.0],
        "activeUsers": [80.0, 85.0],
        "engagedSessions": [50.0, 55.0],
        "engagementRate": [40.0, 42.0],
    }
    long = {
        "dates": ["2026-06-10", "2026-06-11"],
        "sessions": [100.0, 110.0],
        "activeUsers": [80.0, 85.0],
        "engagedSessions": [50.0, 55.0],
        "engagementRate": [40.0, 42.0],
        "newUsers": [30.0, 31.0],
        "screenPageViews": [200.0, 210.0],
        "averageSessionDuration": [90.0, 95.0],
    }
    merged = _ga4_fill_daily_trend_from_source(_ga4_align_daily_trend(period), long)
    assert merged["newUsers"] == [30.0, 31.0]
    assert merged["screenPageViews"] == [200.0, 210.0]


def test_daily_trends_for_ui_spark_window(monkeypatch):
    class _Snap:
        def __init__(self, payload):
            self.payload = payload

    long_dates = [f"2026-06-{d:02d}" for d in range(1, 31)]
    long_payload = {
        "daily_trend": {
            "dates": long_dates,
            "sessions": [1.0] * 30,
            "activeUsers": [1.0] * 30,
            "engagedSessions": [1.0] * 30,
            "engagementRate": [1.0] * 30,
            "newUsers": [5.0] * 30,
            "screenPageViews": [9.0] * 30,
            "averageSessionDuration": [120.0] * 30,
        }
    }

    def _fake_snap(db, *, site_id, profile, period_days):
        if period_days == 365:
            return {"payload": long_payload}
        return None

    monkeypatch.setattr("backend.main.settings.ga4_trend_12m_period_days", 365)
    monkeypatch.setattr("backend.main.get_latest_ga4_report_snapshot", _fake_snap)

    period = {
        "dates": long_dates[-7:],
        "sessions": [2.0] * 7,
        "activeUsers": [2.0] * 7,
        "engagedSessions": [2.0] * 7,
        "engagementRate": [2.0] * 7,
    }
    _daily, spark = _ga4_daily_trends_for_ui(None, site_id=1, profile="web", period_daily=period)
    assert len(spark["dates"]) == 14
    assert spark["newUsers"] == [5.0] * 14
