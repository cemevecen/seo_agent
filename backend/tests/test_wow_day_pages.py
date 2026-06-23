"""Geçen hafta aynı gün sayfa listesi (GA4 tam gün)."""

from datetime import date, timedelta
from unittest.mock import patch

from backend.collectors.ga4 import same_weekday_day_meta
from backend.main import _parse_query_bool


def test_same_weekday_day_meta_labels():
    fake_yesterday = date(2026, 6, 18)  # Perşembe
    with patch("backend.collectors.ga4.report_calendar_yesterday", return_value=fake_yesterday):
        meta = same_weekday_day_meta()
    assert meta["current_day"] == "2026-06-18"
    assert meta["prev_week_day"] == (fake_yesterday - timedelta(days=7)).isoformat()
    assert "Perşembe" in meta["prev_week_day_label"]
    assert "11.06.2026" in meta["prev_week_day_label"]


def test_parse_query_bool():
    assert _parse_query_bool("0", default=True) is False
    assert _parse_query_bool("1", default=False) is True
    assert _parse_query_bool("false", default=True) is False
    assert _parse_query_bool("yes", default=False) is True
    assert _parse_query_bool("maybe", default=True) is True
    assert _parse_query_bool("maybe", default=False) is False
