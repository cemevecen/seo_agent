"""Geçen hafta aynı gün sayfa listesi (GA4 tam gün)."""

from datetime import date, timedelta
from unittest.mock import patch

from backend.collectors.ga4 import (
    _aggregate_landing_rows_by_path,
    _normalize_page_path_key,
    same_weekday_day_meta,
)
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


def test_normalize_page_path_key():
    assert _normalize_page_path_key("/") == "/"
    assert _normalize_page_path_key("/harem/") == "/harem"
    assert _normalize_page_path_key("  /gram-altin  ") == "/gram-altin"


def test_aggregate_landing_rows_by_path_merges_hosts():
    rows = [
        {
            "page": "/",
            "page_host": "www.example.com",
            "page_url": "https://www.example.com/",
            "last_total": 10.0,
            "prev_total": 100.0,
        },
        {
            "page": "/",
            "page_host": "m.example.com",
            "page_url": "https://m.example.com/",
            "last_total": 5.0,
            "prev_total": 200.0,
        },
        {
            "page": "/harem",
            "page_host": "www.example.com",
            "page_url": "https://www.example.com/harem",
            "last_total": 1.0,
            "prev_total": 50.0,
        },
    ]
    merged = _aggregate_landing_rows_by_path(rows)
    by_path = {r["page"]: r for r in merged}
    assert len(by_path) == 2
    assert by_path["/"]["prev_total"] == 300.0
    assert by_path["/"]["last_total"] == 15.0
    assert "m.example.com" in by_path["/"]["page_url"]
