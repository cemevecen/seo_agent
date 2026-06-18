from backend.services.ga4_realtime import (
    REALTIME_DETAIL_TOP_N,
    _domain_shows_web_mweb_top_detail,
    _html_web_mweb_top_content_block,
)


def test_domain_shows_detail_top():
    assert _domain_shows_web_mweb_top_detail("doviz.com")
    assert _domain_shows_web_mweb_top_detail("www.sinemalar.com")
    assert not _domain_shows_web_mweb_top_detail("example.com")


def test_html_web_mweb_top_block():
    html = _html_web_mweb_top_content_block(
        {
            "web": [{"title": "Altın", "active_users": 100, "pageviews": 200}],
            "mweb": [{"title": "Dolar", "active_users": 80, "pageviews": 90}],
        }
    )
    assert "TOP İÇERİK" in html
    assert "Altın" in html
    assert "Mweb" in html
    assert REALTIME_DETAIL_TOP_N == 6
