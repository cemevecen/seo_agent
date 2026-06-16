from datetime import date
from unittest.mock import MagicMock, patch

from backend.services.sc_compare_daily import (
    _merge_daily_rows,
    _range_needs_daily_supplement,
    supplement_summary_for_compare,
)


def test_merge_daily_rows_dedupes_by_date_device():
    a = [{"date": "2025-06-01", "device": "MOBILE", "clicks": 1.0, "impressions": 10.0, "position": 5.0}]
    b = [{"date": "2025-06-01", "device": "MOBILE", "clicks": 9.0, "impressions": 90.0, "position": 4.0}]
    merged = _merge_daily_rows(a, b)
    assert len(merged) == 1
    assert merged[0]["clicks"] == 9.0


def test_range_needs_supplement_when_compare_year_missing():
    rows = [{"date": "2026-06-07", "device": "MOBILE", "clicks": 1.0, "impressions": 1.0, "position": 1.0}]
    start = date(2025, 6, 7)
    end = date(2025, 6, 13)
    assert _range_needs_daily_supplement(rows, start, end) is True


@patch("backend.services.sc_compare_daily.fetch_search_console_daily_rows_for_site")
def test_supplement_fetches_when_yoy_missing(mock_fetch):
    mock_fetch.return_value = [
        {"date": "2025-06-07", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-07", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-08", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-08", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-09", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-09", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-10", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-10", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-11", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-11", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-12", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-12", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
        {"date": "2025-06-13", "device": "MOBILE", "clicks": 10.0, "impressions": 100.0, "position": 6.0},
        {"date": "2025-06-13", "device": "DESKTOP", "clicks": 5.0, "impressions": 50.0, "position": 7.0},
    ]
    site = MagicMock()
    site.id = 1
    summary = {
        "current_7d_start": "2026-06-07",
        "current_7d_end": "2026-06-13",
        "trend_28d_rows": [
            {"date": "2026-06-07", "device": "MOBILE", "clicks": 100.0, "impressions": 1000.0, "position": 5.0},
        ],
        "trend_12m_rows": [],
    }
    compare = {"enabled": True, "mode": "previous_year"}
    ranges = {"7": ("2026-06-07", "2026-06-13")}
    out = supplement_summary_for_compare(MagicMock(), site, summary, compare, ranges)
    assert out.get("compare_daily_supplemented") is True
    assert len(out.get("trend_12m_rows") or []) >= 7
    mock_fetch.assert_called_once()
