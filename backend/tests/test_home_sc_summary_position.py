"""Ana sayfa Search Console özeti — site-geneli pozisyon (GSC uyumlu)."""

from unittest.mock import MagicMock, patch

from backend.main import (
    _home_sc_device_aggregate,
    _home_sc_top50_device_position,
    _home_sc_trend_series,
)

_TOP50_EMPTY = {
    "top50_pos_last_fmt": "—",
    "top50_pos_prev_fmt": "—",
    "top50_pos_delta": 0.0,
    "top50_pos_tone": "flat",
    "top50_clicks_last_fmt": "—",
    "top50_clicks_prev_fmt": "—",
    "top50_has_data": False,
}


@patch("backend.main._home_sc_top_pages", return_value=[])
@patch("backend.main._home_sc_top50_device_position", return_value=_TOP50_EMPTY)
def test_home_sc_uses_sitewide_7d_summary_position(_mock_top50, _mock_pages):
    """Top-query snapshot yerine CollectorRun date×device özeti kullanılır."""
    db = MagicMock()
    summary = {
        "current_7d_summary_by_device": {
            "DESKTOP": {"clicks": 197000, "impressions": 1960000, "position": 6.9, "ctr": 10.1},
            "MOBILE": {"clicks": 1640000, "impressions": 14700000, "position": 5.3, "ctr": 11.2},
        },
        "previous_7d_summary_by_device": {
            "DESKTOP": {"clicks": 180000, "impressions": 1790000, "position": 7.1, "ctr": 10.0},
            "MOBILE": {"clicks": 1550000, "impressions": 14700000, "position": 5.3, "ctr": 10.6},
        },
    }
    desktop = _home_sc_device_aggregate(db, 1, "DESKTOP", summary_payload=summary)
    assert desktop["pos_last_fmt"] == "6.9"
    assert desktop["pos_prev_fmt"] == "7.1"
    assert round(desktop["pos_delta"], 2) == 0.2
    assert desktop["clicks_last_fmt"] == "197K"
    assert desktop["top50_has_data"] is False

    mobile = _home_sc_device_aggregate(db, 1, "MOBILE", summary_payload=summary)
    assert mobile["pos_last_fmt"] == "5.3"
    assert mobile["pos_prev_fmt"] == "5.3"
    assert mobile["pos_delta"] == 0.0


def test_home_sc_trend_series_aligns_to_current_7d_window():
    dates = [f"2026-07-{d:02d}" for d in range(1, 29)]
    clicks = [float(i) for i in range(1, 29)]
    summary = {
        "current_7d_start": "2026-07-22",
        "current_7d_end": "2026-07-28",
        "trend_28d_summary_by_device": {
            "MOBILE": {"dates": dates, "clicks": clicks, "position": [5.0] * 28},
        },
    }
    series = _home_sc_trend_series(summary, "MOBILE", "clicks", days=7)
    assert series == [22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0]


def test_home_sc_trend_series_device_key_case_insensitive():
    summary = {
        "current_7d_start": "2026-07-22",
        "current_7d_end": "2026-07-28",
        "trend_28d_summary_by_device": {
            "mobile": {
                "dates": [f"2026-07-{d:02d}" for d in range(22, 29)],
                "clicks": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0],
            },
        },
    }
    series = _home_sc_trend_series(summary, "MOBILE", "clicks", days=7)
    assert series == [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0]


@patch("backend.main._home_sc_top_pages", return_value=[])
@patch("backend.main._home_sc_top50_device_position", return_value=_TOP50_EMPTY)
def test_home_sc_aggregate_includes_spark_paths(_mock_top50, _mock_pages):
    db = MagicMock()
    dates = [f"2026-07-{d:02d}" for d in range(22, 29)]
    summary = {
        "current_7d_start": "2026-07-22",
        "current_7d_end": "2026-07-28",
        "current_7d_summary_by_device": {
            "DESKTOP": {"clicks": 100, "impressions": 1000, "position": 5.0},
        },
        "previous_7d_summary_by_device": {
            "DESKTOP": {"clicks": 80, "impressions": 900, "position": 5.5},
        },
        "trend_28d_summary_by_device": {
            "DESKTOP": {
                "dates": dates,
                "clicks": [10.0, 12.0, 11.0, 15.0, 14.0, 16.0, 18.0],
                "position": [5.6, 5.5, 5.4, 5.3, 5.2, 5.1, 5.0],
            },
        },
    }
    agg = _home_sc_device_aggregate(db, 1, "DESKTOP", summary_payload=summary)
    assert agg["clicks_spark"]["has_points"] is True
    assert agg["clicks_spark"]["path_d"]
    assert agg["pos_spark"]["has_points"] is True
    assert agg["clicks_tone"] == "up-strong"
    assert agg["pos_tone"] == "up"  # 5.5 → 5.0 = +0.5 sıra
    assert agg["top_pages"] == []
    _mock_pages.assert_called_once()


@patch("backend.services.warehouse.get_latest_search_console_rows")
def test_home_sc_top50_weighted_position(mock_rows):
    def _rows(db, *, site_id, data_scope="current_28d"):
        if data_scope == "current_7d":
            return [
                {"query": "a", "device": "MOBILE", "clicks": 100, "impressions": 1000, "position": 4.0},
                {"query": "b", "device": "MOBILE", "clicks": 50, "impressions": 500, "position": 6.0},
                {"query": "c", "device": "DESKTOP", "clicks": 999, "impressions": 999, "position": 1.0},
            ]
        return [
            {"query": "a", "device": "MOBILE", "clicks": 80, "impressions": 1000, "position": 5.0},
            {"query": "b", "device": "MOBILE", "clicks": 40, "impressions": 500, "position": 7.0},
        ]

    mock_rows.side_effect = _rows
    out = _home_sc_top50_device_position(MagicMock(), 1, "MOBILE")
    # (4*1000 + 6*500) / 1500 = 4.666… → ROUND_DOWN 4.66; prev 5.666… → 5.66
    assert out["top50_has_data"] is True
    assert out["top50_pos_last_fmt"] == "4.66"
    assert out["top50_pos_prev_fmt"] == "5.66"
    assert out["top50_pos_tone"] == "up-strong"
    assert out["top50_clicks_last_fmt"] == "150"
    assert out["top50_clicks_prev_fmt"] == "120"
