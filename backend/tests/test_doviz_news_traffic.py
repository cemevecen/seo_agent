"""Unit tests for doviz news traffic helpers."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from backend.services.doviz_news_traffic import (
    _empty_traffic,
    _gsc_scope_for_period,
    _resolve_traffic_window,
)


@patch("backend.services.doviz_news_traffic.report_calendar_yesterday", return_value=date(2026, 8, 2))
def test_resolve_traffic_window_last_7d(_mock_yest):
    start, end, meta = _resolve_traffic_window(
        {"start": "2026-07-27", "end": "2026-08-02"},
        "last_7d",
    )
    assert start == "2026-07-27"
    assert end == "2026-08-02"
    assert meta.get("note") is None


@patch("backend.services.doviz_news_traffic.report_calendar_yesterday", return_value=date(2026, 8, 2))
def test_resolve_traffic_window_all_caps_to_28d(_mock_yest):
    start, end, meta = _resolve_traffic_window(
        {"start": "2026-02-25", "end": "2026-08-02"},
        "all",
    )
    assert end == "2026-08-02"
    assert start == "2026-07-06"  # 28 days inclusive → start = end - 27
    assert meta.get("note")


def test_gsc_scope_short_vs_long():
    assert _gsc_scope_for_period("last_7d", "2026-07-27", "2026-08-02") == "current_7d_pages"
    assert _gsc_scope_for_period("this_month", "2026-07-01", "2026-08-02") == "current_30d_pages"


def test_empty_traffic_shape():
    out = _empty_traffic(error="x")
    assert out["ok"] is False
    assert out["error"] == "x"
    assert "ga4" in out and "gsc" in out
    assert "own_vs_sourced" in out
