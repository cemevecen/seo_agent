"""4 saatlik SEO Realtime özet maili — 6 alan + pencere agregasyonu."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from backend.services.ga4_realtime import (
    REALTIME_DIGEST_AREAS,
    _digest_profile_block,
    _site_for_digest_brand,
    build_realtime_periodic_digest_html,
)


def test_realtime_digest_areas_are_six_streams():
    assert len(REALTIME_DIGEST_AREAS) == 6
    brands = {brand for brand, _ in REALTIME_DIGEST_AREAS}
    assert brands == {"doviz", "sinemalar"}


def test_site_for_digest_brand():
    sites = [
        SimpleNamespace(domain="www.doviz.com"),
        SimpleNamespace(domain="www.sinemalar.com"),
    ]
    assert _site_for_digest_brand(sites, "doviz").domain == "www.doviz.com"
    assert _site_for_digest_brand(sites, "sinemalar").domain == "www.sinemalar.com"


def test_digest_profile_block_uses_window_aggregation(monkeypatch):
    site = SimpleNamespace(id=1, domain="www.doviz.com")
    db = MagicMock()
    db.query.return_value.filter.return_value.order_by.return_value.first.return_value = SimpleNamespace(
        active_users_current=100,
        active_users_previous=90,
        pageviews_current=200,
        pageviews_previous=180,
    )

    monkeypatch.setattr(
        "backend.services.ga4_realtime.aggregate_page_snapshots_over_window",
        lambda *_a, **_k: {
            "pages": [
                {"page": "/altin-fiyatlari", "activeUsers": 42, "screenPageViews": 88},
            ]
        },
    )
    monkeypatch.setattr(
        "backend.services.ga4_realtime.aggregate_news_snapshots_over_window",
        lambda *_a, **_k: {"pages": []},
    )

    html = _digest_profile_block(db, site, "web", top_n=10, window_minutes=240)
    assert "Top sayfalar · son 4s zirve" in html
    assert "altin-fiyatlari" in html
    assert "42 kul" in html


def test_build_periodic_digest_html_lists_six_areas(monkeypatch):
    doviz = SimpleNamespace(id=1, domain="www.doviz.com", is_active=True)
    sinemalar = SimpleNamespace(id=2, domain="www.sinemalar.com", is_active=True)

    class _FakeQuery:
        def filter(self, *_a, **_k):
            return self

        def all(self):
            return [doviz, sinemalar]

    db = MagicMock()
    db.query.return_value = _FakeQuery()

    blocks_seen: list[tuple[str, str]] = []

    def _fake_block(_db, site, profile, *, top_n, window_minutes):
        blocks_seen.append((site.domain, profile))
        return f'<div data-area="{site.domain}:{profile}"></div>'

    monkeypatch.setattr("backend.services.ga4_realtime._digest_profile_block", _fake_block)

    html = build_realtime_periodic_digest_html(db)
    assert len(blocks_seen) == 6
    assert "6 alanda" in html
    assert 'data-area="www.doviz.com:web"' in html
    assert 'data-area="www.sinemalar.com:mweb"' in html
