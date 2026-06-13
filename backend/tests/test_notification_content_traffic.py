"""Notification içerik ID → URL eşleme testleri."""

from unittest.mock import MagicMock, patch

from backend.services.notification_content_traffic import (
    _aggregate_source_breakdown,
    _classify_traffic_bucket,
    _fetch_ga4_live,
    _filter_urls_for_article,
    _headline_match_score,
    normalize_article_id,
    page_url_matches_article_id,
    resolve_traffic_date_range,
)


def test_normalize_article_id_digits():
    assert normalize_article_id("705471") == "705471"
    assert normalize_article_id(" 705.471 ") == "705471"


def test_page_url_matches_article_id():
    assert page_url_matches_article_id(
        "https://www.doviz.com/ekonomi-haberleri/fed-faiz-karari/705471",
        "705471",
    )
    assert page_url_matches_article_id(
        "https://haber.doviz.com/fed-faiz-karari/705471/amp",
        "705471",
    )
    assert not page_url_matches_article_id(
        "https://www.doviz.com/gram-altin",
        "705471",
    )


def test_headline_match_score():
    assert _headline_match_score(
        "Eski kiracı olmak cazibesini yitirdi",
        "Eski kiracı olmak cazibesini yitirdi - Doviz.com",
    ) >= 0.9
    assert _headline_match_score("abc def ghi", "xyz") == 0.0


def test_resolve_traffic_date_range_from_send_date():
    start, end, meta = resolve_traffic_date_range(send_date="2026-05-20", days=14)
    assert start == "2026-05-20"
    assert meta["mode"] == "send_date"
    assert meta["send_date"] == "2026-05-20"


@patch("backend.services.ga4_page_urls.enrich_ga4_page_rows")
@patch("backend.collectors.ga4.fetch_ga4_article_traffic_sources")
@patch("backend.collectors.ga4.fetch_ga4_news_detail_pages_metrics")
@patch("backend.collectors.ga4.fetch_ga4_article_paths_metrics")
@patch("backend.services.notification_content_traffic.get_ga4_connection_status")
def test_ga4_web_mweb_use_separate_headline_pools(
    mock_status,
    mock_paths,
    mock_pool,
    mock_sources,
    mock_enrich,
):
    mock_status.return_value = {
        "connected": True,
        "properties": {"web": "111", "mweb": "222"},
    }
    mock_paths.return_value = []
    mock_sources.return_value = {"channels": [], "source_medium": []}
    mock_pool.side_effect = [
        [
            {
                "page": "/haber/borsada-devre-kesici/873945",
                "page_title": "Borsada devre kesici çalıştı",
                "views": 2705.0,
                "sessions": 2100.0,
            }
        ],
        [
            {
                "page": "/haber/borsada-devre-kesici/873945",
                "page_title": "Borsada devre kesici çalıştı",
                "views": 890.0,
                "sessions": 720.0,
            }
        ],
    ]
    mock_enrich.side_effect = lambda rows, **kwargs: list(rows or [])

    out = _fetch_ga4_live(
        MagicMock(),
        1,
        "3453741",
        "Borsada devre kesici çalıştı",
        "2026-05-21",
        "2026-06-03",
        14,
    )

    assert mock_pool.call_count == 2
    assert mock_pool.call_args_list[0].kwargs["property_id"] == "111"
    assert mock_pool.call_args_list[1].kwargs["property_id"] == "222"
    assert out["profile_totals"]["web"]["views"] == 2705.0
    assert out["profile_totals"]["mweb"]["views"] == 890.0
    assert out["totals"]["views"] == 3595.0
    assert out["match_method"] == "headline"
    assert out["resolved_article_id"] == "873945"


def test_filter_urls_for_article_excludes_wrong_ids():
    urls = [
        "https://haber.doviz.com/merkez-bankasi/882951",
        "https://haber.doviz.com/baska-haber/882249",
        "https://m.doviz.com/haber/x/882951/amp",
    ]
    out = _filter_urls_for_article(urls, "882951")
    assert len(out) == 2
    assert all("882951" in u for u in out)
    assert not any("882249" in u for u in out)


def test_classify_traffic_bucket():
    assert _classify_traffic_bucket(channel="Organic Search") == "organic"
    assert _classify_traffic_bucket(channel="Direct") == "direct"
    assert _classify_traffic_bucket(channel="Referral") == "referral"
    assert _classify_traffic_bucket(source_medium="firebase / push") == "notification"
    assert _classify_traffic_bucket(source_medium="google / cpc") == "paid"


def test_aggregate_source_breakdown_merges_source_medium():
    out = _aggregate_source_breakdown(
        [{"channel": "Organic Search", "sessions": 10, "views": 12}],
        [
            {"source_medium": "google / organic", "sessions": 80, "views": 95},
            {"source_medium": "(direct) / (none)", "sessions": 40, "views": 42},
            {"source_medium": "firebase / push", "sessions": 25, "views": 30},
            {"source_medium": "twitter.com / referral", "sessions": 5, "views": 6},
        ],
    )
    by_key = {b["key"]: b for b in out["buckets"]}
    assert by_key["organic"]["sessions"] == 80
    assert by_key["direct"]["sessions"] == 40
    assert by_key["notification"]["sessions"] == 25
    assert by_key["referral"]["sessions"] == 5
    assert out["source_medium"][0]["source_medium"] == "google / organic"
