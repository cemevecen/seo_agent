"""Deferred startup DB backoff helpers."""

from unittest.mock import MagicMock, patch

from backend.database import run_with_db_backoff, wait_for_db_ready


def test_wait_for_db_ready_succeeds_on_second_attempt():
    connect = MagicMock()
    connect.side_effect = [OSError("connection refused"), connect.return_value.__enter__.return_value]
    mock_conn = MagicMock()
    connect.return_value.__enter__.return_value = mock_conn

    with patch("backend.database.engine.connect", connect), patch(
        "backend.database.time.sleep"
    ) as sleep:
        ok = wait_for_db_ready(max_attempts=3, base_delay_s=0.01, max_delay_s=0.02)

    assert ok is True
    assert connect.call_count == 2
    sleep.assert_called_once()


def test_run_with_db_backoff_returns_none_after_exhaustion():
    def boom() -> None:
        raise RuntimeError("db down")

    with patch("backend.database.time.sleep"):
        out = run_with_db_backoff(boom, label="test", max_attempts=2, base_delay_s=0.01)

    assert out is None
