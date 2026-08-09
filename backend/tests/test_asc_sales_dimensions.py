"""ASC Sales boyut kırılımı (ülke / cihaz / sürüm)."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from backend.services import asc_client
from backend.services.asc_metrics_warehouse import query_asc_metric


def _row(
    *,
    units: int,
    product: str,
    country: str = "TR",
    device: str = "iPhone",
    version: str = "9.0.1",
    proceeds: float = 0.0,
    currency: str = "USD",
) -> dict:
    return {
        "Units": str(units),
        "Product Type Identifier": product,
        "Country Code": country,
        "Device": device,
        "Version": version,
        "Developer Proceeds": str(proceeds),
        "Currency of Proceeds": currency,
    }


def test_sales_dimension_supported():
    assert asc_client.sales_dimension_supported("units")
    assert asc_client.sales_dimension_supported("proceeds")
    assert not asc_client.sales_dimension_supported("sessions")
    assert not asc_client.sales_dimension_supported("crashes")


@patch.object(asc_client, "_env", return_value="123456")
@patch.object(asc_client, "_fetch_sales_report")
def test_fetch_sales_dimension_by_device(mock_fetch, _env):
    mock_fetch.return_value = [
        _row(units=10, product="1", device="iPhone"),
        _row(units=3, product="1", device="iPad"),
        _row(units=2, product="3", device="iPhone"),  # update — ignore for units
    ]
    out = asc_client.fetch_sales_dimension_series(
        start=date(2026, 8, 1),
        end=date(2026, 8, 1),
        metric="units",
        dim="device",
        segment="all",
        breakdown="segment",
    )
    assert out and out["ok"]
    by_key = {r["key"]: r["value"] for r in out["series"]}
    assert by_key["iPhone"] == 10
    assert by_key["iPad"] == 3
    assert "iPhone" in [s["key"] for s in out["segments"]]


@patch.object(asc_client, "_env", return_value="123456")
@patch.object(asc_client, "_fetch_sales_report")
def test_fetch_sales_dimension_segment_timeseries(mock_fetch, _env):
    def _side(report_type=None, report_sub_type=None, frequency=None, report_date=None, vendor_number=None):
        if report_date == "2026-08-01":
            return [_row(units=5, product="1", country="TR")]
        if report_date == "2026-08-02":
            return [_row(units=7, product="1", country="TR"), _row(units=2, product="1", country="DE")]
        return []

    mock_fetch.side_effect = _side
    out = asc_client.fetch_sales_dimension_series(
        start=date(2026, 8, 1),
        end=date(2026, 8, 2),
        metric="units",
        dim="country",
        segment="TR",
        breakdown="date",
    )
    assert out and out["ok"]
    by_key = {r["key"]: r["value"] for r in out["series"]}
    assert by_key["2026-08-01"] == 5
    assert by_key["2026-08-02"] == 7


@patch.object(asc_client, "is_configured", return_value=True)
@patch.object(asc_client, "fetch_sales_dimension_series")
@patch("backend.services.asc_metrics_warehouse._cached_scrape_facts", return_value=([], {}))
def test_query_asc_metric_dim_path(mock_scrape, mock_dim, _cfg):
    mock_dim.return_value = {
        "ok": True,
        "series": [{"key": "iPhone", "value": 42}],
        "segments": [{"key": "iPhone", "label": "iPhone", "total": 42}],
        "total": 42.0,
        "dim": "device",
        "segment": "all",
        "breakdown": "segment",
    }
    out = query_asc_metric(
        start="2026-08-01",
        end="2026-08-07",
        metric="units",
        dim="device",
        breakdown="segment",
        segment="all",
    )
    assert out["ok"] is True
    assert out["source"] == "asc_sales_dim"
    assert out["series"][0]["key"] == "iPhone"
    assert "device" in out["facets"]["dims"]
    assert "iPhone" in out["facets"]["segments"]
