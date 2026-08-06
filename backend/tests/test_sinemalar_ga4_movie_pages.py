from backend.services.ga4_page_urls import (
    enrich_ga4_page_rows,
    ga4_pages_news_for_ui,
    ga4_pages_no_news_for_ui,
)
from backend.services.realtime_news_paths import is_news_detail_path, is_sinemalar_content_id_path


def test_sinemalar_movie_paths_are_content_not_news():
    assert is_sinemalar_content_id_path("/movieInfo/264007")
    assert is_sinemalar_content_id_path("/mobileweb/movieCast/291982")
    assert is_sinemalar_content_id_path("/film/264007/dune")
    assert is_sinemalar_content_id_path("https://www.sinemalar.com/movieInfo/1")
    assert not is_sinemalar_content_id_path("/gundem-haberleri/baslik/837872")
    assert not is_sinemalar_content_id_path("/filmler/en-iyi-filmler")


def test_news_detail_keeps_sinemalar_movies():
    assert not is_news_detail_path("/movieInfo/264007")
    assert not is_news_detail_path("/movieCast/291982")
    assert not is_news_detail_path("/mobileweb/movieInfo/99")
    assert is_news_detail_path("/gundem-haberleri/baslik/837872")
    assert is_news_detail_path("/haber/slug/123456")


def test_enrich_keeps_movie_pages_in_no_news():
    rows = [
        {"page": "/movieInfo/264007", "page_host": "m.sinemalar.com", "last_total": 100},
        {"page": "/gundem/foo/999", "page_host": "www.doviz.com", "last_total": 50},
        {"page": "/filmler/en-iyi-filmler", "page_host": "m.sinemalar.com", "last_total": 80},
    ]
    out = enrich_ga4_page_rows(rows, keep_news_articles=False)
    pages = {r["page"] for r in out}
    assert "/movieInfo/264007" in pages
    assert "/filmler/en-iyi-filmler" in pages
    assert "/gundem/foo/999" not in pages


def test_ui_merge_pulls_movies_from_pages_news():
    payload = {
        "pages_no_news": [
            {"page": "/filmler/en-iyi-filmler", "page_host": "m.sinemalar.com", "last_total": 10},
        ],
        "pages_news": [
            {"page": "/movieCast/291982", "page_host": "m.sinemalar.com", "last_total": 99},
            {"page": "/haber/x/555", "page_host": "www.doviz.com", "last_total": 40},
        ],
    }
    no_news = ga4_pages_no_news_for_ui(payload)
    pages = {r["page"] for r in no_news}
    assert "/movieCast/291982" in pages
    assert "/filmler/en-iyi-filmler" in pages
    assert "/haber/x/555" not in pages

    news = ga4_pages_news_for_ui(payload)
    news_pages = {r["page"] for r in news}
    assert "/movieCast/291982" not in news_pages
    assert "/haber/x/555" in news_pages
