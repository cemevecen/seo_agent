"""GA4 app banner attribution — parse / top-N."""

from types import SimpleNamespace

from backend.services.ga4_app_attribution import (
    _aggregate_rows,
    _ga4_date_to_iso,
    _series_from_buckets,
    is_hidden_banner_first_user_campaign,
    slice_asc_downloads_daily,
    trim_banner_payload_to_observed_start,
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


def test_hidden_banner_campaigns_excluded_from_breakdown():
    assert is_hidden_banner_first_user_campaign("mdoviz_app_download_banner_currency_detail")
    assert is_hidden_banner_first_user_campaign("mdoviz app download banner")
    assert is_hidden_banner_first_user_campaign("app_banner_in_web")
    assert not is_hidden_banner_first_user_campaign("(direct)")

    start = date(2026, 5, 11)
    end = date(2026, 5, 11)
    rows = [
        _row("20260511", "mdoviz app download banner", 50),
        _row("20260511", "(direct)", 10),
    ]
    total, by_camp = _aggregate_rows(rows, start=start, end=end, top_n=5)
    assert total["2026-05-11"] == 60
    assert set(by_camp.keys()) == {"(direct)"}


def test_trim_strips_hidden_campaigns_from_payload():
    payload = {
        "start": "2026-05-01",
        "end": "2026-05-02",
        "total_daily": {
            "dates": ["2026-05-01"],
            "values": [5.0],
        },
        "campaigns": [
            {
                "campaign": "mdoviz_app_download_banner",
                "total": 3,
                "daily": {"dates": ["2026-05-01"], "values": [3.0]},
            },
            {
                "campaign": "(direct)",
                "total": 2,
                "daily": {"dates": ["2026-05-01"], "values": [2.0]},
            },
        ],
    }
    trim_banner_payload_to_observed_start(payload)
    names = [c["campaign"] for c in payload["campaigns"]]
    assert names == ["(direct)"]


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


def test_trim_leading_zeros():
    payload = {
        "start": "2025-11-01",
        "end": "2026-06-07",
        "total_daily": {
            "dates": ["2025-11-01", "2026-04-08", "2026-04-09"],
            "values": [0.0, 10.0, 12.0],
        },
        "campaigns": [],
    }
    trim_banner_payload_to_observed_start(payload)
    assert payload["chart_start"] == "2026-04-08"
    assert payload["total_daily"]["dates"] == ["2026-04-08", "2026-04-09"]


def test_trim_drops_all_zero_campaigns_in_chart_range():
    payload = {
        "start": "2025-11-01",
        "end": "2026-06-07",
        "total_daily": {
            "dates": ["2025-11-01", "2026-04-08"],
            "values": [0.0, 5.0],
        },
        "campaigns": [
            {
                "campaign": "active",
                "total": 5,
                "daily": {"dates": ["2025-11-01", "2026-04-08"], "values": [0.0, 5.0]},
            },
            {
                "campaign": "dead",
                "total": 99,
                "daily": {"dates": ["2025-11-01", "2026-04-08"], "values": [0.0, 0.0]},
            },
        ],
    }
    trim_banner_payload_to_observed_start(payload)
    names = [c["campaign"] for c in payload["campaigns"]]
    assert names == ["active"]


def test_series_fill_zeros():
    s = _series_from_buckets({"2026-05-11": 4.0}, start=date(2026, 5, 10), end=date(2026, 5, 11))
    assert s["dates"] == ["2026-05-10", "2026-05-11"]
    assert s["values"] == [0.0, 4.0]
