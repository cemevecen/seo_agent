"""GSC CWV grafik serisi — tooltip asıl kaynak, SVG Y karışmasın."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "gsc_cwv_scrape", ROOT / "scripts" / "gsc_cwv_scrape.py"
)
mod = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(mod)


def test_parse_gsc_tooltip_july_23_tr():
    text = (
        "23 Tem Perşembe\n"
        "Yetersiz URL'ler 0\n"
        "URL'ler iyileştirme gerektiriyor 12.359\n"
        "İyi URL'ler 4.941"
    )
    parsed = mod._parse_gsc_chart_tooltip(text)
    assert parsed is not None
    assert parsed["date"] == "2026-07-23"
    assert parsed["poor"] == 0
    assert parsed["needs_improvement"] == 12359
    assert parsed["good"] == 4941


def test_tooltip_daily_series_keeps_exact_day():
    samples = {
        "2026-07-22": {"poor": 0, "needs_improvement": 15000, "good": 5000},
        "2026-07-23": {"poor": 0, "needs_improvement": 12359, "good": 4941},
        "2026-07-24": {"poor": 0, "needs_improvement": 14000, "good": 5200},
        "2026-07-25": {"poor": 0, "needs_improvement": 14100, "good": 5300},
        "2026-07-26": {"poor": 0, "needs_improvement": 14200, "good": 5400},
    }
    ser = mod._series_from_tooltip_samples(
        samples, start_iso="2026-07-22", end_iso="2026-07-26"
    )
    assert ser is not None
    idx = ser["dates"].index("2026-07-23")
    assert ser["needs_improvement"][idx] == 12359
    assert ser["good"][idx] == 4941
    assert ser["poor"][idx] == 0


def test_tooltip_gap_is_linear_not_zero():
    samples = {
        "2026-07-01": {"poor": 0, "needs_improvement": 10000, "good": 4000},
        "2026-07-02": {"poor": 0, "needs_improvement": 10000, "good": 4000},
        "2026-07-03": {"poor": 0, "needs_improvement": 10000, "good": 4000},
        "2026-07-05": {"poor": 0, "needs_improvement": 12000, "good": 6000},
        "2026-07-06": {"poor": 0, "needs_improvement": 12000, "good": 6000},
    }
    ser = mod._series_from_tooltip_samples(
        samples, start_iso="2026-07-01", end_iso="2026-07-06"
    )
    assert ser is not None
    idx = ser["dates"].index("2026-07-04")
    assert ser["needs_improvement"][idx] == 11000
    assert ser["good"][idx] == 5000
    assert 0 not in ser["needs_improvement"]


def test_reject_implausible_two_digit_year():
    assert mod._gsc_label_to_iso("8.08.07") == ""
    assert mod._gsc_label_to_iso("8/8/07") == ""
    assert mod._gsc_label_to_iso("8.08.26") == "2026-08-08"
    assert mod._gsc_label_to_iso("8.08.25") == "2025-08-08"


def test_daily_range_caps_from_the_end_not_2007():
    from datetime import datetime

    dates = mod._daily_iso_range(datetime(2007, 8, 8), datetime(2026, 8, 11))
    assert dates[-1] == "2026-08-11"
    assert dates[0].startswith("2025-")
    assert all(not d.startswith("2007") for d in dates)
    assert len(dates) == 401


def test_tooltip_ignores_svg_start_in_2007():
    samples = {
        f"2026-07-{d:02d}": {"poor": 0, "needs_improvement": 6592, "good": 6592}
        for d in range(1, 12)
    }
    ser = mod._series_from_tooltip_samples(
        samples, start_iso="2007-08-08", end_iso="2026-08-11"
    )
    assert ser is not None
    assert ser["dates"][0].startswith("2026-07-")
    assert all(not d.startswith("2007") for d in ser["dates"])
    assert ser["poor"][0] == 0
    assert ser["poor"][-1] == 0
    assert ser["needs_improvement"][0] == 6592


def test_snap_always_locks_last_to_card_kpi():
    chart = {
        "mobile": {
            "dates": [f"2026-08-{d:02d}" for d in range(1, 11)],
            "poor": [8000] * 10,
            "needs_improvement": [18216] * 10,
            "good": [3657] * 10,
        }
    }
    overview = {"mobile": {"poor": 0, "needs_improvement": 15557, "good": 3123}}
    mod._snap_series_to_kpis(chart, overview)
    assert chart["mobile"]["poor"][-1] == 0
    assert chart["mobile"]["needs_improvement"][-1] == 15557
    assert chart["mobile"]["good"][-1] == 3123
    assert chart["mobile"]["needs_improvement"][0] == 18216


def test_snap_zeros_cloned_poor_series():
    chart = {
        "mobile": {
            "dates": [f"2026-07-{d:02d}" for d in range(1, 12)],
            "poor": [6592] * 11,
            "needs_improvement": [6592] * 11,
            "good": [6592] * 11,
        }
    }
    overview = {"mobile": {"poor": 0, "needs_improvement": 6592, "good": 6592}}
    mod._snap_series_to_kpis(chart, overview)
    assert chart["mobile"]["poor"] == [0] * 11
    assert chart["mobile"]["needs_improvement"][-1] == 6592


def test_sanitize_rebinds_2007_axis_to_sibling_dates():
    from backend.services.gsc_cwv_scrape_store import sanitize_chart_series

    desk_dates = [f"2026-05-{d:02d}" for d in range(13, 32)] + [
        f"2026-06-{d:02d}" for d in range(1, 16)
    ]
    n = len(desk_dates)
    chart = {
        "mobile": {
            "dates": [f"2007-08-{d:02d}" for d in range(1, n + 1)],
            "poor": [0] * n,
            "needs_improvement": list(range(n)),
            "good": [1000 + i for i in range(n)],
        },
        "desktop": {
            "dates": desk_dates,
            "poor": [0] * n,
            "needs_improvement": [100] * n,
            "good": [50] * n,
        },
    }
    out = sanitize_chart_series(chart, year_now=2026)
    assert out["desktop"]["dates"][0] == "2026-05-13"
    assert out["mobile"]["dates"][0] == "2026-05-13"
    assert out["mobile"]["dates"][-1] == desk_dates[-1]
    assert out["mobile"]["needs_improvement"] == list(range(n))
    assert out["mobile"]["poor"][-1] == 0


def test_sanitize_keeps_mixed_axis_2026_points():
    from backend.services.gsc_cwv_scrape_store import sanitize_chart_series

    chart = {
        "mobile": {
            "dates": ["2007-08-08", "2026-05-13", "2026-05-14", "2026-08-11"],
            "poor": [9, 0, 0, 0],
            "needs_improvement": [1, 10, 11, 12],
            "good": [1, 20, 21, 22],
        },
        "desktop": {
            "dates": ["2026-05-13", "2026-05-14", "2026-08-11"],
            "poor": [0, 0, 0],
            "needs_improvement": [100, 110, 120],
            "good": [50, 50, 50],
        },
    }
    out = sanitize_chart_series(chart, year_now=2026)
    assert out["mobile"]["dates"][0] == "2026-05-13"
    assert "2007" not in "".join(out["mobile"]["dates"])
    assert out["mobile"]["needs_improvement"] == [10, 11, 12]


def test_sanitize_does_not_bind_401_onto_89():
    from backend.services.gsc_cwv_scrape_store import sanitize_chart_series

    desk_dates = [f"2026-05-{d:02d}" for d in range(13, 32)]  # 19 pts
    chart = {
        "mobile": {
            "dates": [f"2007-01-{(i % 28) + 1:02d}" for i in range(401)],
            "poor": [6592] * 401,
            "needs_improvement": [6592] * 401,
            "good": [6592] * 401,
        },
        "desktop": {
            "dates": desk_dates,
            "poor": [0] * 19,
            "needs_improvement": [100] * 19,
            "good": [50] * 19,
        },
    }
    out = sanitize_chart_series(chart, year_now=2026)
    assert out["mobile"] is None
    assert out["desktop"]["dates"][0] == "2026-05-13"


def test_recover_short_plausible_does_not_beat_rebindable_mobile():
    from backend.services.gsc_cwv_scrape_store import recover_chart_series

    desk_dates = [f"2026-05-{d:02d}" for d in range(13, 32)]
    n = len(desk_dates)
    current = {
        "mobile": {
            "dates": ["2026-08-09", "2026-08-10", "2026-08-11"],
            "poor": [0, 0, 0],
            "needs_improvement": [1, 2, 3],
            "good": [4, 5, 6],
        },
        "desktop": {
            "dates": desk_dates,
            "poor": [0] * n,
            "needs_improvement": [100] * n,
            "good": [50] * n,
        },
    }
    older = {
        "mobile": {
            "dates": [f"2007-08-{d:02d}" for d in range(1, n + 1)],
            "poor": [0] * n,
            "needs_improvement": [40] * n,
            "good": [80] * n,
        }
    }
    out = recover_chart_series(current, [older], year_now=2026)
    assert out["mobile"]["dates"][0] == "2026-05-13"
    assert len(out["mobile"]["dates"]) == n
    assert out["mobile"]["needs_improvement"][0] == 40


def test_sanitize_fills_sawtooth_zeros_and_snaps_kpis():
    from backend.services.gsc_cwv_scrape_store import sanitize_chart_series

    dates = [f"2026-05-{d:02d}" for d in range(13, 32)]
    n = len(dates)
    ni = []
    good = []
    poor = []
    for i in range(n):
        ni.append(0 if i % 2 else 12000)
        good.append(0 if i % 2 else 8000)
        poor.append(4000 if i % 2 else 0)
    chart = {
        "mobile": {
            "dates": dates,
            "poor": poor,
            "needs_improvement": ni,
            "good": good,
        },
        "desktop": {
            "dates": dates,
            "poor": [0] * n,
            "needs_improvement": [100] * n,
            "good": [50] * n,
        },
    }
    out = sanitize_chart_series(
        chart,
        year_now=2026,
        kpis_by_device={"mobile": {"poor": 0, "needs_improvement": 12000, "good": 8000}},
    )
    assert out["mobile"]["dates"][0] == "2026-05-13"
    assert 0 not in out["mobile"]["needs_improvement"][1:-1]
    assert out["mobile"]["poor"] == [0] * n
    assert out["mobile"]["needs_improvement"][-1] == 12000
    assert out["mobile"]["good"][-1] == 8000


def test_sanitize_flattens_nonzero_bar_sawtooth():
    from backend.services.gsc_cwv_scrape_store import sanitize_chart_series, _is_sawtooth

    dates = [f"2026-05-{d:02d}" for d in range(13, 32)]
    n = len(dates)
    ni = [5000 if i % 2 else 25000 for i in range(n)]
    good = [12000 if i % 2 else 2000 for i in range(n)]
    chart = {
        "mobile": {
            "dates": dates,
            "poor": [0] * n,
            "needs_improvement": ni,
            "good": good,
        },
        "desktop": {
            "dates": dates,
            "poor": [0] * n,
            "needs_improvement": [100] * n,
            "good": [50] * n,
        },
    }
    out = sanitize_chart_series(
        chart,
        year_now=2026,
        kpis_by_device={"mobile": {"poor": 0, "needs_improvement": 15557, "good": 3123}},
    )
    ni_out = out["mobile"]["needs_improvement"]
    good_out = out["mobile"]["good"]
    assert not _is_sawtooth(ni_out)
    assert not _is_sawtooth(good_out)
    assert max(ni_out) - min(ni_out) < 12000
    assert out["mobile"]["needs_improvement"][-1] == 15557
    assert out["mobile"]["poor"][-1] == 0


def test_apply_kpis_does_not_scale_whole_series():
    from backend.services.gsc_cwv_scrape_store import _apply_kpis_to_series

    ser = {
        "dates": [f"2026-07-{d:02d}" for d in range(1, 12)],
        "poor": [0] * 11,
        "needs_improvement": [10000] * 11,
        "good": [3000] * 11,
    }
    out = _apply_kpis_to_series(
        ser, {"poor": 0, "needs_improvement": 24584, "good": 10913}
    )
    assert out["needs_improvement"] == [10000] * 11
    assert out["good"] == [3000] * 11


def test_parse_gsc_cards_tr_bin_suffix():
    text = (
        "Önemli Web Verileri > Mobil\n"
        "Yetersiz\n0\nSorun yok\n"
        "İyileştirme gerekiyor...\n15,6 B\n2 sorun\n"
        "İyi\n3,12 B"
    )
    parsed = mod._parse_gsc_kpi_triplet(text)
    assert parsed is not None
    assert parsed["poor"] == 0
    assert parsed["needs_improvement"] == 15600
    assert parsed["good"] == 3120


def test_parse_gsc_tooltip_aug_10_matches_cards():
    text = (
        "10 Ağu Pazartesi\n"
        "Yetersiz 0\n"
        "İyileştirme gerektiriyor 15.557\n"
        "İyi 3.123"
    )
    parsed = mod._parse_gsc_chart_tooltip(text)
    assert parsed is not None
    assert parsed["date"] == "2026-08-10"
    assert parsed["poor"] == 0
    assert parsed["needs_improvement"] == 15557
    assert parsed["good"] == 3123


def test_svg_bar_path_bins_height_not_sawtooth():
    dates = [f"2026-08-{d:02d}" for d in range(1, 9)]
    pts = []
    for i in range(8):
        x0 = i * 10
        pts.extend([[x0, 20], [x0 + 8, 20], [x0 + 8, 90], [x0, 90], [x0, 20]])
    y_bottom, y_top, y_max = 90.0, 20.0, 18000.0

    def y_to_val(y: float) -> float:
        return (y_bottom - y) / (y_bottom - y_top) * y_max

    vals = mod._svg_pts_to_daily(pts, dates, 0.0, 80.0, y_to_val)
    assert len(vals) == 8
    assert min(vals) >= 15000
    assert max(vals) <= 20000
    assert max(vals) - min(vals) < 3000


def test_recover_prefers_longer_previous_mobile():
    from backend.services.gsc_cwv_scrape_store import recover_chart_series

    desk_dates = [f"2026-05-{d:02d}" for d in range(13, 32)]
    n = len(desk_dates)
    current = {
        "mobile": None,
        "desktop": {
            "dates": desk_dates,
            "poor": [0] * n,
            "needs_improvement": [100] * n,
            "good": [50] * n,
        },
    }
    older = {
        "mobile": {
            "dates": [f"2007-08-{d:02d}" for d in range(1, n + 1)],
            "poor": [0] * n,
            "needs_improvement": [200 + i for i in range(n)],
            "good": [300] * n,
        },
        "desktop": None,
    }
    out = recover_chart_series(current, [older], year_now=2026)
    assert out["mobile"]["dates"][0] == "2026-05-13"
    assert out["mobile"]["needs_improvement"][0] == 200
    assert out["desktop"]["dates"][0] == "2026-05-13"
