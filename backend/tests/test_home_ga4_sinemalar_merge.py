"""Sinemalar home Top 25: Sayfalar + Haberler birleşimi."""

from backend.main import _home_ga4_merge_page_rows, _home_is_sinemalar_site


def test_home_is_sinemalar_site():
    assert _home_is_sinemalar_site(2, None)
    assert _home_is_sinemalar_site(1, "www.sinemalar.com")
    assert not _home_is_sinemalar_site(1, "www.doviz.com")


def test_merge_pages_and_news_by_traffic():
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
            # Aynı kategori — daha düşük; pages kazanmalı
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
