"""Home notification week: dünden geriye 7g vs önceki 7g."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from backend.main import _home_notification_week_reference_day
from backend.services.notification_analytics_alerts import _week_windows


def test_home_notification_reference_is_yesterday(monkeypatch):
    fixed = datetime(2026, 8, 6, 11, 30, tzinfo=ZoneInfo("Europe/Istanbul"))

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed.replace(tzinfo=None)
            return fixed.astimezone(tz)

    monkeypatch.setattr("backend.main.datetime", _FixedDateTime)
    ref = _home_notification_week_reference_day()
    assert ref == date(2026, 8, 5)
    cur_start, cur_end, prev_start, prev_end = _week_windows(ref)
    assert cur_end == date(2026, 8, 5)
    assert cur_start == date(2026, 7, 30)
    assert prev_end == date(2026, 7, 29)
    assert prev_start == date(2026, 7, 23)
