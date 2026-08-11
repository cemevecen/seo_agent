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
