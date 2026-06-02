"""Ana sayfa realtime KPI — realtime.html _renderKpis ile aynı mantık."""

from backend.services.ga4_realtime import active_users_kpi_from_realtime_result


def test_kpi_uses_total_and_comparison_change_pct():
    result = {
        "total": {"activeUsers": 1200},
        "comparison": {
            "activeUsers": {"current": 500, "previous": 400, "change_pct": 25.0},
        },
        "trend": [{"active_users": 900}, {"active_users": 1200}],
    }
    val, delta_fmt, tone, delta_pct = active_users_kpi_from_realtime_result(result)
    assert val == 1200.0
    assert delta_fmt == "+25.0%"
    assert tone == "up"
    assert delta_pct == 25.0


def test_kpi_fallback_halves_when_no_comparison_block():
    result = {
        "total": {"activeUsers": 800},
        "current": {"activeUsers": 300},
        "previous": {"activeUsers": 200},
        "trend": [],
    }
    val, delta_fmt, tone, delta_pct = active_users_kpi_from_realtime_result(result)
    assert val == 800.0
    assert delta_fmt == "+50.0%"
    assert tone == "up"
    assert delta_pct == 50.0


def test_kpi_error_uses_last_trend_point():
    result = {
        "error": "quota",
        "trend": [{"active_users": 100}, {"active_users": 150}],
    }
    val, delta_fmt, tone, delta_pct = active_users_kpi_from_realtime_result(result)
    assert val == 150.0
    assert delta_fmt is None
    assert tone == "flat"
    assert delta_pct == 0.0
