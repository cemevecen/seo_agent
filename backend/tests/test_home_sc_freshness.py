"""Ana sayfa SC — snapshot satır aralığı ve tazelik meta."""

from datetime import datetime, timezone
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
    # Satır end özetten yeni → satır aralığı
    assert label == "24.08–30.08.2026"


@patch("backend.main.get_latest_search_console_rows")
def test_home_sc_period_range_keeps_summary_when_newer(mock_rows):
    mock_rows.return_value = [
        {"start_date": "2026-08-20", "end_date": "2026-08-26"},
    ]
    label = _home_sc_period_range_label(
        MagicMock(),
        1,
        {"current_7d_start": "2026-08-24", "current_7d_end": "2026-08-30"},
        7,
    )
    assert label == "24.08–30.08.2026"


@patch("backend.main._latest_collector_run_recent", return_value=False)
@patch("backend.main.is_provider_daily_quota_exhausted", return_value=False)
@patch("backend.main._home_sc_period_range_from_rows", return_value=("2026-09-19", "2026-09-25"))
@patch("backend.main._latest_successful_provider_summary")
@patch("backend.main._latest_provider_run")
@patch("backend.main._search_console_latest_snapshot_collected_at")
def test_home_sc_freshness_syncs_when_rows_ahead_of_summary(
    mock_collected, mock_run, mock_summary, _rows, _quota, _cooldown
):
    """Alerts current_7d'yi ilerletti; strategy=all özet bayat → needs_sync."""
    mock_collected.return_value = datetime(2026, 9, 28, 1, 0, tzinfo=timezone.utc)
    run = MagicMock()
    run.status = "success"
    run.requested_at = datetime(2026, 9, 24, 11, 26, tzinfo=timezone.utc)
    mock_run.return_value = run
    mock_summary.return_value = {
        "current_7d_end": "2026-09-21",
        "current_7d_summary_by_device": {"DESKTOP": {"clicks": 1}},
    }
    fresh = _home_sc_freshness_for_site(MagicMock(), 1, period_days=7)
    assert fresh["data_end"] == "2026-09-25"
    assert fresh["summary_end"] == "2026-09-21"
    assert fresh["needs_sync"] is True
    assert fresh["needs_reload"] is True
    assert fresh["quota_exhausted"] is False


@patch("backend.main._latest_collector_run_recent", return_value=False)
@patch("backend.main.is_provider_daily_quota_exhausted", return_value=False)
@patch("backend.main._home_sc_period_range_from_rows", return_value=("2026-08-24", "2026-08-30"))
@patch("backend.main._latest_successful_provider_summary")
@patch("backend.main._latest_provider_run")
@patch("backend.main._search_console_latest_snapshot_collected_at")
def test_home_sc_freshness_flags_newer_snapshot(
    mock_collected, mock_run, mock_summary, _rows, _quota, _cooldown
):
    mock_collected.return_value = datetime(2026, 8, 31, 8, 0, tzinfo=timezone.utc)
    run = MagicMock()
    run.status = "success"
    run.requested_at = datetime(2026, 8, 30, 4, 0, tzinfo=timezone.utc)
    mock_run.return_value = run
    mock_summary.return_value = {
        "current_7d_end": "2026-08-28",
        "current_7d_summary_by_device": {"DESKTOP": {"clicks": 10}},
    }
    fresh = _home_sc_freshness_for_site(MagicMock(), 1, period_days=7)
    assert fresh["data_end"] == "2026-08-30"
    assert fresh["needs_reload"] is True
    assert fresh["needs_sync"] is True
    assert fresh["quota_exhausted"] is False


@patch("backend.main._latest_collector_run_recent", return_value=False)
@patch("backend.main.is_provider_daily_quota_exhausted", return_value=True)
@patch("backend.main._home_sc_period_range_from_rows", return_value=("2026-08-24", "2026-08-30"))
@patch("backend.main._latest_successful_provider_summary")
@patch("backend.main._latest_provider_run")
@patch("backend.main._search_console_latest_snapshot_collected_at")
def test_home_sc_freshness_blocks_sync_when_quota_exhausted(
    mock_collected, mock_run, mock_summary, _rows, _quota, _cooldown
):
    mock_collected.return_value = datetime(2026, 8, 20, 4, 0)
    run = MagicMock()
    run.status = "success"
    run.requested_at = datetime(2026, 8, 20, 4, 0)
    mock_run.return_value = run
    mock_summary.return_value = {
        "current_7d_end": "2026-08-30",
        "current_7d_summary_by_device": {"DESKTOP": {"clicks": 1}},
    }
    fresh = _home_sc_freshness_for_site(MagicMock(), 1, period_days=7)
    assert fresh["needs_sync"] is False
    assert fresh["quota_exhausted"] is True
