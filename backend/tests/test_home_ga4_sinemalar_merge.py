"""Home GA4 Top 25: Sayfalar + Haberler birleşimi (doviz + sinemalar)."""

from backend.main import _home_ga4_merge_page_rows, _home_is_sinemalar_site


def test_home_is_sinemalar_site():
    assert _home_is_sinemalar_site(2, None)
    assert _home_is_sinemalar_site(1, "www.sinemalar.com")
    assert not _home_is_sinemalar_site(1, "www.doviz.com")


def test_merge_pages_and_news_by_traffic_sinemalar():
    pages = [
        {
            "page": "/filmler/en-iyi-animasyon-filmleri",
            "page_host": "m.sinemalar.com",
            "last_total": 21244,
            "prev_total": 24640,
        },
        {
            "page": "/filmler/en-iyi-filmler",
            "page_host": "m.sinemalar.com",
            "last_total": 18972,
            "prev_total": 17410,
        },
    ]
    news = [
        {
            "page": "/orumcek-adam-yepyeni-bir-gun/264007",
            "page_host": "m.sinemalar.com",
            "views": 144577,
        },
        {
            "page": "/movieSeances/1",
            "page_host": "m.sinemalar.com",
            "views": 42794,
        },
        {
            "page": "/filmler/en-iyi-animasyon-filmleri",
            "page_host": "m.sinemalar.com",
            "views": 100,
        },
    ]
    merged = _home_ga4_merge_page_rows(pages, news)
    by_page = {r["page"]: r for r in merged}
    assert len(merged) == 4
    assert float(by_page["/orumcek-adam-yepyeni-bir-gun/264007"]["last_total"]) == 144577
    assert float(by_page["/movieSeances/1"]["last_total"]) == 42794
    assert float(by_page["/filmler/en-iyi-animasyon-filmleri"]["last_total"]) == 21244

    ranked = sorted(merged, key=lambda r: float(r.get("last_total") or 0), reverse=True)
    assert ranked[0]["page"] == "/orumcek-adam-yepyeni-bir-gun/264007"
    assert ranked[1]["page"] == "/movieSeances/1"


def test_merge_pages_and_news_by_traffic_doviz():
    pages = [
        {
            "page": "/altin",
            "page_host": "kur.doviz.com",
            "last_total": 50000,
            "prev_total": 48000,
        },
        {
            "page": "/",
            "page_host": "www.doviz.com",
            "last_total": 40000,
            "prev_total": 39000,
        },
    ]
    news = [
        {
            "page": "/gundem-haberleri/baslik/837872",
            "page_host": "www.doviz.com",
            "views": 120000,
        },
        {
            "page": "/ekonomi-haberleri/foo/111",
            "page_host": "www.doviz.com",
            "views": 35000,
        },
        {
            "page": "/altin",
            "page_host": "kur.doviz.com",
            "views": 10,
        },
    ]
    merged = _home_ga4_merge_page_rows(pages, news)
    by_page = {r["page"]: float(r.get("last_total") or 0) for r in merged}
    assert by_page["/gundem-haberleri/baslik/837872"] == 120000
    assert by_page["/ekonomi-haberleri/foo/111"] == 35000
    assert by_page["/altin"] == 50000

    ranked = sorted(merged, key=lambda r: float(r.get("last_total") or 0), reverse=True)
    assert ranked[0]["page"] == "/gundem-haberleri/baslik/837872"
    assert ranked[1]["page"] == "/altin"
