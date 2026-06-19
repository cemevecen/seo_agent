"""GA4 trend grafikleri için SC position overlay yardımcıları."""

from backend.main import (
    _ga4_sc_position_trend_for_period,
    _sc_position_trend_has_values,
    _slice_search_console_trend_last_days,
)


def test_sc_position_trend_has_values():
    assert not _sc_position_trend_has_values({"position": [None, 0, None]})
    assert _sc_position_trend_has_values({"position": [None, 12.4]})


def test_ga4_sc_position_trend_for_period_slices_web_desktop():
    base = {
        "mode": "last_28d",
        "dates": [f"2026-06-{d:02d}" for d in range(1, 29)],
        "position": [float(10 + (d % 5)) for d in range(1, 29)],
        "clicks": [1.0] * 28,
    }
    sc_by_device = {"DESKTOP": {"28d": base}}
    out = _ga4_sc_position_trend_for_period(
        sc_by_device, profile="web", period_key="7", period_days=7
    )
    assert out is not None
    assert len(out["dates"]) == 7
    assert len(out["position"]) == 7
    assert out["dates"][-1] == "2026-06-28"


def test_ga4_sc_position_trend_ignores_ios():
    sc_by_device = {"DESKTOP": {"28d": {"dates": ["2026-06-01"], "position": [3.0]}}}
    assert _ga4_sc_position_trend_for_period(sc_by_device, profile="ios", period_key="7", period_days=7) is None


def test_slice_search_console_trend_last_days_keeps_position():
    trend = {
        "mode": "last_28d",
        "dates": ["2026-06-01", "2026-06-02", "2026-06-03"],
        "clicks": [1, 2, 3],
        "position": [10.0, 11.0, 12.0],
    }
    sliced = _slice_search_console_trend_last_days(trend, 2)
    assert sliced["dates"] == ["2026-06-02", "2026-06-03"]
    assert sliced["position"] == [11.0, 12.0]
