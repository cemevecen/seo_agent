from datetime import date, timezone
from datetime import datetime

from backend.services.app_release_sheet import parse_release_sheet_csv, parse_tr_release_datetime


def test_parse_tr_dot_date():
    dt = parse_tr_release_datetime("07.09.2025")
    assert dt is not None
    assert dt.year == 2025 and dt.month == 9 and dt.day == 7


def test_parse_sheet_android_build_only_rows():
    csv_text = (
        "Platform,Versiyon,Build,Tarih\n"
        "Android,—,242,07.09.2025\n"
        "Android,,241,01.08.2025\n"
        "iOS,8.0.0,—,17 Eki 2025 03:33\n"
    )
    ios, android = parse_release_sheet_csv(csv_text, since=date(2025, 1, 1))
    assert len(android) == 2
    assert android[0]["version"] == "241"
    assert android[1]["version"] == "242"
    assert len(ios) == 1
    assert ios[0]["version"] == "8.0.0"
