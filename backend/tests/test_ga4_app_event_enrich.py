"""GA4 app event satır zenginleştirme — birim testleri."""

from __future__ import annotations

from unittest.mock import patch

from backend.services.ga4_app_event_enrich import (
    enrich_app_event_detail_sections,
    enrich_event_param_row,
    section_enriches_news,
)


def test_section_enriches_news_detects_params():
    assert section_enriches_news("news_id")
    assert section_enriches_news("news_id", "news_title")
    assert not section_enriches_news("firebase_screen")
    assert not section_enriches_news("from")


def test_enrich_event_param_row_maps_news_id_to_url():
    lookup = {
        "by_id": {
            "894744": {
                "page": "/ekonomi/haziran-enflasyonu/894744",
                "page_host": "www.doviz.com",
                "page_url": "https://www.doviz.com/ekonomi/haziran-enflasyonu/894744",
                "page_title": "Haziran ayı enflasyonu açıklandı",
                "views": 100.0,
            }
        },
        "by_title": {},
    }
    row = enrich_event_param_row(
        {"value": "894744", "count": 10, "count_prev": 0},
        param="news_id",
        param2=None,
        lookup=lookup,
        site_domain="doviz.com",
    )
    assert row["page_url"] == "https://www.doviz.com/ekonomi/haziran-enflasyonu/894744"
    assert row["display_text"] == "haziran-enflasyonu"
    assert "Haziran" in row["display_sub"]


def test_enrich_event_param_row_combined_value():
    lookup = {
        "by_id": {
            "895029": {
                "page": "/gundem/memur-zam/895029",
                "page_host": "www.doviz.com",
                "page_url": "https://www.doviz.com/gundem/memur-zam/895029",
                "page_title": "Memur ve emeklinin zam oranı",
                "views": 50.0,
            }
        },
        "by_title": {},
    }
    row = enrich_event_param_row(
        {"value": "895029 · Memur ve emeklinin zam", "count": 5, "count_prev": 1},
        param="news_id",
        param2="news_title",
        lookup=lookup,
        site_domain="doviz.com",
    )
    assert row["page_url"].endswith("/895029")
    assert row["display_text"] == "memur-zam"


@patch("backend.services.ga4_app_event_enrich.build_news_article_lookup")
def test_enrich_sections_only_for_news_params(mock_lookup):
    mock_lookup.return_value = {
        "by_id": {
            "1": {
                "page": "/haber/test/1",
                "page_host": "www.example.com",
                "page_url": "https://www.example.com/haber/test/1",
                "page_title": "Test",
                "views": 1.0,
            }
        },
        "by_title": {},
    }
    sections = [
        {
            "label": "News ID",
            "param": "news_id",
            "param2": None,
            "rows": [{"value": "1", "count": 2, "count_prev": 0}],
        },
        {
            "label": "from",
            "param": "from",
            "param2": None,
            "rows": [{"value": "home", "count": 9, "count_prev": 0}],
        },
    ]
    out = enrich_app_event_detail_sections(
        sections,
        property_ids=["123"],
        days=7,
        site_domain="example.com",
    )
    assert out[0]["rows"][0]["page_url"].endswith("/1")
    assert "page_url" not in out[1]["rows"][0]
    mock_lookup.assert_called_once()
