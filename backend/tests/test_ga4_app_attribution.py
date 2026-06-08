"""GA4 app banner attribution — parse / top-N."""

from types import SimpleNamespace

from backend.services.ga4_app_attribution import (
    _aggregate_rows,
    _ga4_date_to_iso,
    _series_from_buckets,
    slice_asc_downloads_daily,
)
from datetime import date


def _row(d: str, campaign: str, value: float):
    return SimpleNamespace(
        dimension_values=[
            SimpleNamespace(value=d),
            SimpleNamespace(value=campaign),
        ],
        metric_values=[SimpleNamespace(value=str(int(value)))],
    )


def test_ga4_date_to_iso():
    assert _ga4_date_to_iso("20260511") == "2026-05-11"


def test_aggregate_top_campaigns():
    start = date(2026, 5, 11)
    end = date(2026, 5, 12)
    rows = [
        _row("20260511", "banner_a", 10),
        _row("20260511", "banner_b", 5),
        _row("20260512", "banner_a", 3),
        _row("20260512", "rare", 100),
    ]
    total, by_camp = _aggregate_rows(rows, start=start, end=end, top_n=2)
    assert total["2026-05-11"] == 15
    assert total["2026-05-12"] == 103
    assert set(by_camp.keys()) == {"rare", "banner_a"}
    assert "banner_b" not in by_camp


def test_slice_asc_downloads():
    asc = {
        "ok": True,
        "dates": ["2026-06-05", "2026-06-06"],
        "total_downloads_series": [32, 27],
        "first_downloads_series": [30, 25],
        "redownloads_series": [2, 2],
    }
    out = slice_asc_downloads_daily(asc, start=date(2026, 6, 5), end=date(2026, 6, 6))
    assert out["ok"] is True
    assert out["daily"]["total_downloads"] == [32.0, 27.0]


def test_series_fill_zeros():
    s = _series_from_buckets({"2026-05-11": 4.0}, start=date(2026, 5, 10), end=date(2026, 5, 11))
    assert s["dates"] == ["2026-05-10", "2026-05-11"]
    assert s["values"] == [0.0, 4.0]
