"""Notification içerik ID → URL eşleme testleri."""

from backend.services.notification_content_traffic import (
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
