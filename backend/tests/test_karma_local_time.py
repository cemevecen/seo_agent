from datetime import datetime, timezone

from backend.karma.realtime_helpers import fmt_local_time
from backend.services.timezone_utils import to_local_datetime


def test_fmt_local_time_utc_naive_to_tsi():
    # 14:41 UTC -> 17:41 TSİ (yaz saati yok, UTC+3)
    utc = datetime(2026, 6, 4, 14, 41, 0)
    assert fmt_local_time(utc) == "17:41"


def test_to_local_datetime_aware_utc():
    utc = datetime(2026, 6, 4, 14, 41, 0, tzinfo=timezone.utc)
    local = to_local_datetime(utc)
    assert local is not None
    assert local.hour == 17
    assert local.minute == 41
