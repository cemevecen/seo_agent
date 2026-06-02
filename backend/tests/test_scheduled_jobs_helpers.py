"""Zamanlanmış job yardımcıları — inbox after_unix, GA4 dönem listesi."""

from datetime import datetime, timezone

from backend.collectors.ga4 import ga4_scheduled_kpi_period_days
from backend.services.inbox_sync import scheduled_sync_after_unix


def test_scheduled_sync_after_unix_naive_utc():
    ts = datetime(2026, 6, 2, 10, 0, 0)
    assert scheduled_sync_after_unix(ts) == int(ts.replace(tzinfo=timezone.utc).timestamp())


def test_ga4_scheduled_kpi_period_days_default():
    days = ga4_scheduled_kpi_period_days()
    assert 1 in days
    assert 60 in days
    assert 90 in days
