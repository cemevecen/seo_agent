"""GA4 mobil event parametre kırılımı — birim testleri."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.collectors.ga4 import fetch_ga4_event_param_breakdown
from backend.services.ga4_app_event_config import app_event_detail_config


def test_app_event_detail_config_ios_screen_view():
    cfg = app_event_detail_config("ios")
    assert cfg is not None
    assert cfg["event_name"] == "screen_view"
    labels = [s["label"] for s in cfg["sections"]]
    assert "firebase_screen (ekran adı)" in labels
    assert "news_title (haber başlığı)" in labels


def test_app_event_detail_config_android_news():
    cfg = app_event_detail_config("android")
    assert cfg is not None
    assert cfg["event_name"] == "news_detail_opened"
    params = [s.get("param") for s in cfg["sections"]]
    assert "news_id" in params
    assert "news_title" in params


def _mock_row(value: str, count: float):
    row = MagicMock()
    row.dimension_values = [MagicMock(value=value)]
    row.metric_values = [MagicMock(value=str(count))]
    return row


@patch("backend.collectors.ga4._client")
@patch("backend.collectors.ga4._calendar_windows")
def test_fetch_ga4_event_param_breakdown_merges_periods(mock_windows, mock_client):
    mock_windows.return_value = (("2026-06-01", "2026-06-07"), ("2026-05-25", "2026-05-31"))
    client = MagicMock()
    mock_client.return_value = client

    def _side_effect(req):
        resp = MagicMock()
        end = req.date_ranges[0].end_date
        if end == "2026-06-07":
            resp.rows = [_mock_row("home", 100), _mock_row("(not set)", 50)]
        else:
            resp.rows = [_mock_row("home", 80)]
        return resp

    client.run_report.side_effect = _side_effect

    rows = fetch_ga4_event_param_breakdown(
        property_id="163175967",
        event_name="screen_view",
        param_key="firebase_screen",
        days=7,
        limit=50,
    )

    assert len(rows) >= 2
    home = next(r for r in rows if r["value"] == "home")
    assert home["count"] == 100
    assert home["count_prev"] == 80
    assert home["change_pct"] == 25.0
    assert client.run_report.call_count == 2
    first_req = client.run_report.call_args_list[0][0][0]
    assert first_req.dimension_filter.filter.field_name == "eventName"
    assert first_req.dimensions[0].name == "customEvent:firebase_screen"
