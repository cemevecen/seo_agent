"""GA4 mobil event parametre kırılımı — birim testleri."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.collectors.ga4 import (
    _build_dimension_candidate_list,
    _event_dimension_candidates,
    _is_junk_event_param_key,
    _run_event_param_compare_report,
    _split_compare_row_dimensions,
    fetch_ga4_event_param_breakdown,
)
from backend.services.ga4_app_event_config import app_event_detail_config


def test_app_event_detail_config_ios_screen_view():
    cfg = app_event_detail_config("ios")
    assert cfg is not None
    assert cfg["event_name"] == "screen_view"
    labels = [s["label"] for s in cfg["sections"]]
    assert "Haberler" in labels
    assert "Ekranlar" in labels
    assert "firebase_screen" not in str(cfg["sections"])


def test_app_event_detail_config_android_news():
    cfg = app_event_detail_config("android")
    assert cfg is not None
    assert cfg["event_name"] == "news_detail_opened"
    labels = [s.get("label") for s in cfg["sections"]]
    assert "Haberler" in labels
    assert "from (kaynak)" in labels
    assert len(cfg["sections"]) == 2


def test_build_dimension_candidate_list_firebase_screen_fallback():
    dims = _build_dimension_candidate_list("firebase_screen")
    assert "unifiedScreenName" in dims
    assert "customEvent:firebase_screen" in dims


def test_build_dimension_candidate_list_news_variants():
    dims = _build_dimension_candidate_list("news_title", ["newsTitle"])
    assert "customEvent:news_title" in dims
    assert "customEvent:newsTitle" in dims


@patch("backend.collectors.ga4._ga4_valid_dimensions")
def test_event_dimension_candidates_filters_invalid(mock_valid):
    mock_valid.return_value = {"customEvent:news_title", "unifiedScreenName"}
    dims = _event_dimension_candidates("firebase_screen", property_id="123")
    assert "unifiedScreenName" in dims
    assert "customEvent:firebase_screen" not in dims
    assert "customEvent:news_title" not in dims


def _mock_row(value: str, count: float, count_prev: float | None = None):
    row = MagicMock()
    row.dimension_values = [MagicMock(value=value)]
    if count_prev is None:
        row.metric_values = [MagicMock(value=str(count))]
    else:
        row.metric_values = [MagicMock(value=str(count)), MagicMock(value=str(count_prev))]
    return row


@patch("backend.collectors.ga4._client")
@patch("backend.collectors.ga4._calendar_windows")
@patch("backend.collectors.ga4._event_dimension_candidates")
def test_fetch_ga4_event_param_breakdown_merges_periods(mock_dims, mock_windows, mock_client):
    mock_dims.return_value = ["customEvent:news_title"]
    mock_windows.return_value = (("2026-06-01", "2026-06-07"), ("2026-05-25", "2026-05-31"))
    client = MagicMock()
    mock_client.return_value = client

    def _side_effect(req):
        resp = MagicMock()
        resp.rows = [
            _mock_row("home", 100, 80),
            _mock_row("(not set)", 50, 0),
        ]
        return resp

    client.run_report.side_effect = _side_effect

    rows = fetch_ga4_event_param_breakdown(
        property_id="163175967",
        event_name="screen_view",
        param_key="news_title",
        days=7,
        limit=50,
    )

    assert len(rows) >= 2
    home = next(r for r in rows if r["value"] == "home")
    assert home["count"] == 100
    assert home["count_prev"] == 80
    assert home["change_pct"] == 25.0
    assert client.run_report.call_count == 1
    mock_dims.assert_called()
    first_req = client.run_report.call_args_list[0][0][0]
    assert first_req.dimension_filter.filter.field_name == "eventName"
    assert len(first_req.date_ranges) == 2


def test_split_compare_row_dimensions_strips_date_range():
    class _DV:
        def __init__(self, v):
            self.value = v

    key, tag = _split_compare_row_dimensions(
        [_DV("(not set)"), _DV("date_range_1")],
        requested_dim_count=1,
    )
    assert key == "(not set)"
    assert tag == "date_range_1"
    assert _is_junk_event_param_key("(not set) · date_range_1") is True


@patch("backend.collectors.ga4._client")
def test_compare_report_aggregates_date_range_rows(mock_client):
    client = MagicMock()
    mock_client.return_value = client

    class _DV:
        def __init__(self, v):
            self.value = v

    class _MV:
        def __init__(self, v):
            self.value = str(v)

    row0 = MagicMock()
    row0.dimension_values = [_DV("(not set)"), _DV("date_range_0")]
    row0.metric_values = [_MV(100)]

    row1 = MagicMock()
    row1.dimension_values = [_DV("(not set)"), _DV("date_range_1")]
    row1.metric_values = [_MV(80)]

    resp = MagicMock()
    resp.rows = [row0, row1]
    client.run_report.return_value = resp

    last_map, prev_map = _run_event_param_compare_report(
        client,
        property_id="1",
        event_name="screen_view",
        dimension_names=["customEvent:news_id"],
        last_start="2026-06-01",
        last_end="2026-06-07",
        prev_start="2026-05-25",
        prev_end="2026-05-31",
        limit=100,
    )
    assert last_map == {"(not set)": 100.0}
    assert prev_map == {"(not set)": 80.0}
