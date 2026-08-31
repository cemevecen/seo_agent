"""Ana sayfa SC — snapshot satır aralığı ve tazelik meta."""

from unittest.mock import MagicMock, patch

from backend.main import (
    _home_sc_freshness_for_site,
    _home_sc_period_range_from_rows,
    _home_sc_period_range_label,
)


@patch("backend.main.get_latest_search_console_rows")
def test_home_sc_period_range_prefers_snapshot_rows(mock_rows):
    mock_rows.return_value = [
        {"start_date": "2026-08-24", "end_date": "2026-08-30"},
        {"start_date": "2026-08-24", "end_date": "2026-08-30"},
    ]
    db = MagicMock()
    start, end = _home_sc_period_range_from_rows(db, 1, 7)
    assert start == "2026-08-24"
    assert end == "2026-08-30"
    label = _home_sc_period_range_label(
        db,
        1,
        {"current_7d_start": "2026-08-22", "current_7d_end": "2026-08-28"},
        7,
    )
    assert label == "24.08–30.08.2026"


@patch("backend.main._home_sc_period_range_from_rows", return_value=("2026-08-24", "2026-08-30"))
@patch("backend.main._latest_successful_provider_summary")
@patch("backend.main._latest_provider_run")
@patch("backend.main._search_console_latest_snapshot_collected_at")
def test_home_sc_freshness_flags_newer_snapshot(mock_collected, mock_run, mock_summary, _rows):
    from datetime import datetime, timezone

    mock_collected.return_value = datetime(2026, 8, 31, 8, 0, tzinfo=timezone.utc)
    run = MagicMock()
    run.status = "success"
    run.requested_at = datetime(2026, 8, 30, 4, 0, tzinfo=timezone.utc)
    mock_run.return_value = run
    mock_summary.return_value = {"current_7d_end": "2026-08-28"}
    fresh = _home_sc_freshness_for_site(MagicMock(), 1, period_days=7)
    assert fresh["data_end"] == "2026-08-30"
    assert fresh["needs_reload"] is True
    assert fresh["needs_sync"] is False
