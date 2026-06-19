"""GA4 trend grafikleri için SC position overlay yardımcıları."""

from backend.main import (
    _align_search_console_trend_to_dates,
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


def test_ga4_sc_position_trend_90d_uses_12m_and_aligns_to_ga4_dates():
    dates_12m = [f"2026-03-{d:02d}" for d in range(1, 32)] + [f"2026-04-{d:02d}" for d in range(1, 31)]
    base_12m = {
        "mode": "last_12m",
        "dates": dates_12m,
        "position": [float(10 + (i % 4)) for i in range(len(dates_12m))],
        "clicks": [1.0] * len(dates_12m),
    }
    ga4_dates = [f"2026-03-{d:02d}" for d in range(22, 32)] + [f"2026-04-{d:02d}" for d in range(1, 21)]
    sc_by_device = {"DESKTOP": {"12m": base_12m, "28d": {"dates": ["2026-06-01"], "position": [9.0]}}}
    out = _ga4_sc_position_trend_for_period(
        sc_by_device,
        profile="web",
        period_key="90",
        period_days=90,
        target_dates=ga4_dates,
    )
    assert out is not None
    assert out["dates"] == ga4_dates
    assert len(out["position"]) == len(ga4_dates)
    assert out["position"][0] == 11.0


def test_align_search_console_trend_to_dates_fills_missing_with_none():
    trend = {
        "dates": ["2026-03-22", "2026-03-23"],
        "position": [11.5, 12.0],
    }
    aligned = _align_search_console_trend_to_dates(
        trend, ["2026-03-21", "2026-03-22", "2026-03-23"]
    )
    assert aligned["dates"] == ["2026-03-21", "2026-03-22", "2026-03-23"]
    assert aligned["position"] == [None, 11.5, 12.0]


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
