"""Notification içerik ID → URL eşleme testleri."""

from backend.services.notification_content_traffic import (
    normalize_article_id,
    page_url_matches_article_id,
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
