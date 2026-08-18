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


def test_last_2d_window_and_comparison():
    """Son 2 gün: bugün + dün; karşılaştırma önceki 2 gün."""
    today = date(2026, 8, 4)
    info = resolve_period("last_2d", today=today)
    assert info["key"] == "last_2d"
    assert info["label"] == "Last 2 days"
    assert info["start"] == date(2026, 8, 3)
    assert info["end"] == today
    assert info["cmp_start"] == date(2026, 8, 1)
    assert info["cmp_end"] == date(2026, 8, 2)


def test_last_2d_shifts_when_today_empty():
    """Aynı kayan-pencere kuralı son 2 gün için de geçerli."""
    today = date(2026, 8, 4)
    info = resolve_period("last_2d", today=today)
    rows = [{"date_day": "2026-08-03"}, {"date_day": "2026-08-02"}]
    shifted = _shift_last_7d_if_today_empty(info, rows, today=today)
    assert shifted["start"] == date(2026, 8, 2)
    assert shifted["end"] == date(2026, 8, 3)
    assert shifted["cmp_end"] == date(2026, 8, 1)
    assert shifted["cmp_start"] == date(2026, 7, 31)
    assert shifted.get("trimmed_empty_today") is True


def test_last_2d_aliases_resolve():
    today = date(2026, 8, 4)
    for alias in ("son_2_gun", "last 2 days", "2d", "LAST_2D"):
        assert resolve_period(alias, today=today)["key"] == "last_2d", alias
