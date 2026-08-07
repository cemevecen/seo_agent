"""Home Virgül gelir KPI — pencere + günlük ort. + yüzde hesabı."""

from datetime import date, timedelta

from backend.main import (
    _home_format_money_tl,
    _home_pct_delta,
    _home_virgul_stream_daily_avgs,
    _home_virgul_window_bounds,
)


def test_virgul_windows_exclude_today_and_are_7_and_90_days():
    today = date(2026, 8, 7)
    b = _home_virgul_window_bounds(today)
    assert b["yesterday"] == date(2026, 8, 6)
    assert b["week_start"] == date(2026, 7, 31)
    assert b["week_days"] == 7
    assert b["month3_start"] == date(2026, 5, 9)  # 06.08 - 89g
    assert b["month3_days"] == 90
    # Bugün pencerede yok
    assert b["week_start"] <= b["yesterday"] < today
    assert b["month3_start"] <= b["yesterday"] < today


def test_virgul_daily_avg_calendar_zeros_and_pct_match_screenshot_math():
    """Ekrandaki Döviz Web ölçeği: 166k vs 199.6k → formül -16.8% (.1f).

    Kartta görünen K ₺ yuvarlaması ile yüzde, ham ortalamadan hesaplanır;
    tam 166000/199600 için beklenen yüzde -16.833… → -16.8%.
    """
    today = date(2026, 8, 7)
    b = _home_virgul_window_bounds(today)
    week_start = b["week_start"]
    month3_start = b["month3_start"]
    week_days = int(b["week_days"])
    month3_days = int(b["month3_days"])

    series: dict[date, float] = {}
    for i in range(week_days):
        series[week_start + timedelta(days=i)] = 166_000.0

    rest_days = month3_days - week_days
    rest_per_day = 16_802_000.0 / rest_days
    for i in range(month3_days):
        d = month3_start + timedelta(days=i)
        if d not in series:
            series[d] = rest_per_day

    avgs = _home_virgul_stream_daily_avgs(
        series,
        week_start=week_start,
        week_days=week_days,
        month3_start=month3_start,
        month3_days=month3_days,
    )
    assert round(float(avgs["week_avg"]), 0) == 166_000
    assert abs(float(avgs["month3_avg"]) - 199_600) < 0.01
    delta_fmt, tone, pct = _home_pct_delta(float(avgs["week_avg"]), float(avgs["month3_avg"]))
    assert delta_fmt == "-16.8%"
    assert tone == "down-strong"
    assert pct == -16.83
    assert _home_format_money_tl(166_000) == "166,0K ₺"
    assert _home_format_money_tl(199_600) == "199,6K ₺"


def test_virgul_missing_day_counts_as_zero_in_calendar_avg():
    """Eksik gün 0 sayılır — 7 günden 1'i doluysa ort = toplam/7."""
    week_start = date(2026, 7, 31)
    month3_start = date(2026, 5, 9)
    series = {week_start: 700_000.0}  # sadece 1 gün
    avgs = _home_virgul_stream_daily_avgs(
        series,
        week_start=week_start,
        week_days=7,
        month3_start=month3_start,
        month3_days=90,
    )
    assert avgs["week_avg"] == 100_000.0
    assert avgs["week_days_present"] == 1
    assert avgs["month3_avg"] == 700_000.0 / 90.0


def test_virgul_android_positive_pct_from_screenshot():
    """Android ölçeği: 9.34k vs 8.50k → formül +9.9% (.1f)."""
    week_avg = 9_340.0
    month3_avg = 8_500.0
    delta_fmt, tone, pct = _home_pct_delta(week_avg, month3_avg)
    assert delta_fmt == "+9.9%"
    assert pct == 9.88
    assert tone.startswith("up")
    assert _home_format_money_tl(9_340) == "9,34K ₺"
    assert _home_format_money_tl(8_500) == "8,50K ₺"


def test_virgul_all_six_rounded_display_pcts():
    """Yuvarlanmış K değerlerinden formülün .1f çıktısı (ekran ≈ ham farkı normal)."""
    cases = [
        (166_000, 199_600, "-16.8%"),
        (38_100, 70_100, "-45.6%"),
        (8_150, 8_500, "-4.1%"),
        (9_340, 8_500, "+9.9%"),
        (34_200, 74_800, "-54.3%"),
        (24_300, 62_600, "-61.2%"),
    ]
    for week_avg, month3_avg, expected in cases:
        delta_fmt, _tone, _pct = _home_pct_delta(week_avg, month3_avg)
        assert delta_fmt == expected, (week_avg, month3_avg, delta_fmt, expected)


def test_virgul_pct_uses_full_precision_not_display_rounding():
    """Yüzde ham ortalamadan; K gösterimi ayrı yuvarlanır."""
    week_avg = 165_867.0
    month3_avg = 199_640.0
    delta_fmt, _tone, pct = _home_pct_delta(week_avg, month3_avg)
    assert _home_format_money_tl(week_avg) == "165,9K ₺"
    assert _home_format_money_tl(month3_avg) == "199,6K ₺"
    assert delta_fmt.startswith("-")
    expected = (week_avg - month3_avg) / month3_avg * 100.0
    assert abs(pct - round(expected, 2)) < 0.001
    assert delta_fmt == f"{expected:.1f}%"
