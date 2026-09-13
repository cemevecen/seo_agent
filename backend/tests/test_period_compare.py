from datetime import date

from backend.services.period_compare import (
    compare_bounds,
    dates_cover_window,
    delta_pct,
    shift_years,
)


def test_previous_period_same_length():
    ps, pe = compare_bounds(date(2026, 6, 13), date(2026, 9, 12), "previous_period")
    assert (pe - ps).days == (date(2026, 9, 12) - date(2026, 6, 13)).days
    assert pe == date(2026, 6, 12)


def test_previous_year_same_dates_and_leap_day():
    ps, pe = compare_bounds(date(2026, 3, 1), date(2026, 3, 7), "previous_year")
    assert ps == date(2025, 3, 1)
    assert pe == date(2025, 3, 7)
    assert shift_years(date(2024, 2, 29), -1) == date(2023, 2, 28)


def test_window_must_exist_in_warehouse():
    stored = ["2025-08-14", "2025-09-12", "2026-09-12"]
    assert dates_cover_window(stored, date(2026, 6, 13), date(2026, 9, 12))
    assert not dates_cover_window(stored, date(2024, 6, 13), date(2024, 9, 12))
    assert not dates_cover_window(stored, date(2025, 1, 1), date(2025, 9, 12))


def test_delta_does_not_invent_from_zero():
    assert delta_pct(10, 0) is None
    assert delta_pct(80, 100) == -20.0
