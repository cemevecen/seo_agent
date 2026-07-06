"""Realtime app event detail — birim testleri."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from backend.services import ga4_realtime_app_event as rt_ev


def test_filter_unified_screen_news_only():
    rows = [
        {"value": "Dolar ne kadar oldu?", "count": 10},
        {"value": "Anasayfa", "count": 5},
    ]
    with patch.object(rt_ev, "_screen_unified_news_article", side_effect=lambda v, **_: "Dolar" in v):
        out = rt_ev._filter_unified_screen_rows(rows, news_only=True, site_domain="doviz.com")
    assert len(out) == 1
    assert out[0]["value"] == "Dolar ne kadar oldu?"


def test_filter_unified_screen_non_news():
    rows = [
        {"value": "Dolar ne kadar oldu?", "count": 10},
        {"value": "Anasayfa", "count": 5},
    ]
    with patch.object(rt_ev, "_screen_unified_news_article", side_effect=lambda v, **_: "Dolar" in v):
        out = rt_ev._filter_unified_screen_rows(rows, news_only=False, site_domain="doviz.com")
    assert len(out) == 1
    assert out[0]["value"] == "Anasayfa"


def test_from_param_returns_note_when_custom_fails():
    with patch.object(rt_ev, "_realtime_dimension_candidates", return_value=["customEvent:from"]):
        with patch.object(rt_ev, "_run_realtime_dim_report", side_effect=Exception("invalid dim")):
            rows, note = rt_ev._fetch_realtime_section_rows(
                "123",
                event_name="news_detail_opened",
                param="from",
                param2=None,
                alt_params=None,
                alt_params_2=None,
                site_domain="doviz.com",
                window_minutes=15,
                limit=10,
                client=MagicMock(),
            )
    assert rows == []
    assert note and "from" in note


def test_news_section_falls_back_to_unified_screen():
    mock_counts = {"Altın fiyatları yükseldi": 12.0, "Anasayfa": 3.0}

    def fake_report(*_a, dimension_names=None, **_kw):
        if dimension_names == ["customEvent:news_id"]:
            raise Exception("not supported")
        if dimension_names == ["unifiedScreenName"]:
            return mock_counts
        return {}

    with patch.object(rt_ev, "_realtime_dimension_candidates", return_value=["customEvent:news_id"]):
        with patch.object(rt_ev, "_run_realtime_dim_report", side_effect=fake_report):
            with patch.object(rt_ev, "_screen_unified_news_article", side_effect=lambda v, **_: v != "Anasayfa"):
                rows, note = rt_ev._fetch_realtime_section_rows(
                    "123",
                    event_name="screen_view",
                    param="news_id",
                    param2="news_title",
                    alt_params=["newsId"],
                    alt_params_2=["newsTitle"],
                    site_domain="doviz.com",
                    window_minutes=15,
                    limit=10,
                    client=MagicMock(),
                )
    assert len(rows) == 1
    assert rows[0]["value"] == "Altın fiyatları yükseldi"
    assert note and "unifiedScreenName" in note


def test_fetch_realtime_app_event_detail_calls_enrich():
    cfg = {
        "event_name": "screen_view",
        "title": "test",
        "sections": [{"param": "from", "label": "from (kaynak)"}],
    }
    rt_ev._REALTIME_DETAIL_CACHE.clear()
    with patch.object(rt_ev, "app_event_detail_config", return_value=cfg):
        with patch.object(rt_ev, "_fetch_realtime_section_rows", return_value=([], "note")):
            with patch.object(rt_ev, "enrich_app_event_detail_sections", return_value=[]) as mock_enrich:
                out = rt_ev.fetch_realtime_app_event_detail(
                    "163175967",
                    "ios",
                    site_domain="doviz.com",
                    lookup_property_ids=[],
                    window_minutes=15,
                    limit=10,
                    client=MagicMock(),
                )
    assert out["event_name"] == "screen_view"
    mock_enrich.assert_called_once()
