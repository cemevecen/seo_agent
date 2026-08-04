"""Son 1 hafta: bugün veri yoksa pencere önceki aynı günden düne kayar."""

from datetime import date

from backend.services.doviz_news_sheet import resolve_period, _shift_last_7d_if_today_empty


def test_last_7d_shifts_when_today_empty():
    today = date(2026, 8, 4)  # Salı
    info = resolve_period("last_7d", today=today)
    assert info["start"] == date(2026, 7, 29)
    assert info["end"] == today

    rows = [{"date_day": "2026-08-03"}, {"date_day": "2026-07-28"}]
    shifted = _shift_last_7d_if_today_empty(info, rows, today=today)
    assert shifted["start"] == date(2026, 7, 28)  # geçen salı
    assert shifted["end"] == date(2026, 8, 3)  # dün (pazartesi)
    assert shifted.get("trimmed_empty_today") is True
    assert shifted["cmp_end"] == date(2026, 7, 27)
    assert shifted["cmp_start"] == date(2026, 7, 21)


def test_last_7d_keeps_today_when_data_exists():
    today = date(2026, 8, 4)
    info = resolve_period("last_7d", today=today)
    rows = [{"date_day": "2026-08-04"}, {"date_day": "2026-08-03"}]
    same = _shift_last_7d_if_today_empty(info, rows, today=today)
    assert same["start"] == info["start"]
    assert same["end"] == info["end"]
    assert not same.get("trimmed_empty_today")
