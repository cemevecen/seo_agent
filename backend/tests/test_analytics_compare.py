from backend.services.analytics_compare import (
    apply_ga4_period_compare,
    apply_sc_period_view_compare,
    parse_compare_options,
    resolve_sc_summary_period_range,
)


def test_parse_compare_options_requires_mode_when_enabled():
    off = parse_compare_options(enabled=False, mode="previous_year")
    assert off["enabled"] is False
    on = parse_compare_options(enabled=True, mode="previous_year")
    assert on["enabled"] is True
    assert on["mode"] == "previous_year"


def test_apply_ga4_previous_year_recomputes_prev_totals():
    period = {
        "period_days": 7,
        "ranges": {"last_start": "2026-06-02", "last_end": "2026-06-08", "prev_start": "2026-05-26", "prev_end": "2026-06-01"},
        "last_total": 100.0,
        "prev_total": 50.0,
        "users_last": 80.0,
        "users_prev": 40.0,
        "engaged_last": 60.0,
        "engaged_prev": 30.0,
        "pageviews_last": 200.0,
        "pageviews_prev": 100.0,
        "engagement_rate_last_pct": 50.0,
        "engagement_rate_prev_pct": 25.0,
    }
    daily = {
        "dates": [
            "2025-06-02",
            "2025-06-03",
            "2025-06-04",
            "2025-06-05",
            "2025-06-06",
            "2025-06-07",
            "2025-06-08",
            "2026-06-02",
            "2026-06-03",
        ],
        "sessions": [10.0, 20.0, 0.0, 0.0, 0.0, 0.0, 0.0, 40.0, 60.0],
        "activeUsers": [8.0, 16.0, 0.0, 0.0, 0.0, 0.0, 0.0, 32.0, 48.0],
        "engagedSessions": [5.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0, 24.0, 36.0],
        "screenPageViews": [20.0, 40.0, 0.0, 0.0, 0.0, 0.0, 0.0, 80.0, 120.0],
    }
    out = apply_ga4_period_compare(
        period,
        compare=parse_compare_options(enabled=True, mode="previous_year"),
        daily_long=daily,
    )
    assert out["compare_mode"] == "previous_year"
    assert out["prev_total"] == 30.0
    assert out["ranges"]["prev_start"] == "2025-06-02"
    assert out["ranges"]["prev_end"] == "2025-06-08"


def test_apply_sc_previous_year_from_daily_rows():
    view = {
        "device_code": "MOBILE",
        "summary_current": {"clicks": 100, "impressions": 1000, "ctr": 10.0, "position": 5.0},
        "summary_previous": {"clicks": 50, "impressions": 500, "ctr": 10.0, "position": 6.0},
        "table_label_previous": "Önceki 7 gün",
    }
    rows = [
        {"date": "2025-06-01", "device": "MOBILE", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-02", "device": "MOBILE", "clicks": 15.0, "impressions": 150.0, "position": 6.0},
        {"date": "2026-06-01", "device": "MOBILE", "clicks": 40.0, "impressions": 400.0, "position": 5.0},
        {"date": "2026-06-02", "device": "MOBILE", "clicks": 60.0, "impressions": 600.0, "position": 4.0},
    ]
    out = apply_sc_period_view_compare(
        view,
        period_key="7",
        primary_start="2026-06-01",
        primary_end="2026-06-02",
        compare=parse_compare_options(enabled=True, mode="previous_year"),
        daily_rows=rows,
    )
    assert out["summary_previous"]["clicks"] == 20.0
    assert out["compare_mode"] == "previous_year"
    assert out.get("compare_data_unavailable") is not True


def test_apply_sc_previous_year_missing_daily_marks_unavailable():
    view = {
        "device_code": "MOBILE",
        "summary_current": {"clicks": 100, "impressions": 1000, "ctr": 10.0, "position": 5.0},
        "summary_previous": {"clicks": 50, "impressions": 500, "ctr": 10.0, "position": 6.0},
    }
    rows = [
        {"date": "2026-06-01", "device": "MOBILE", "clicks": 40.0, "impressions": 400.0, "position": 5.0},
        {"date": "2026-06-02", "device": "MOBILE", "clicks": 60.0, "impressions": 600.0, "position": 4.0},
    ]
    out = apply_sc_period_view_compare(
        view,
        period_key="7",
        primary_start="2026-06-01",
        primary_end="2026-06-02",
        compare=parse_compare_options(enabled=True, mode="previous_year"),
        daily_rows=rows,
    )
    assert out.get("compare_data_unavailable") is True
    assert out["clicks_pct_change"] is None


def test_resolve_sc_summary_period_range_prefers_collector_summary():
    summary = {"current_7d_start": "2026-06-07", "current_7d_end": "2026-06-13"}
    assert resolve_sc_summary_period_range(summary, "7", ("2020-01-01", "2028-06-13")) == (
        "2026-06-07",
        "2026-06-13",
    )
