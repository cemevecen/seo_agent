"""Periyodik SEO Realtime özet maili — 6 alan + pencere agregasyonu."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from backend.services.ga4_realtime import (
    REALTIME_DIGEST_AREAS,
    _digest_interval_short_label,
    _digest_profile_block,
    _site_for_digest_brand,
    build_realtime_periodic_digest_html,
    realtime_periodic_digest_skip_no_site_match,
    realtime_periodic_digest_subject,
)


def test_digest_interval_short_label_90_minutes():
    assert _digest_interval_short_label(90) == "1,5s"


def test_realtime_periodic_digest_subject_format(monkeypatch):
    monkeypatch.setattr(
        "backend.services.ga4_realtime._digest_window_minutes",
        lambda: 90,
    )
    subj = realtime_periodic_digest_subject()
    assert subj.startswith("SEO 90 - ")
    assert len(subj.split(" - ", 1)[1]) == 5  # HH:MM


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


def test_site_for_digest_brand_normalizes_url():
    sites = [SimpleNamespace(domain="https://www.doviz.com/path")]
    assert _site_for_digest_brand(sites, "doviz").domain.startswith("https://")


def test_periodic_digest_skip_when_no_site_match():
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = []
    assert realtime_periodic_digest_skip_no_site_match(db) is True

    db.query.return_value.filter.return_value.all.return_value = [
        SimpleNamespace(domain="example.com", is_active=True),
    ]
    assert realtime_periodic_digest_skip_no_site_match(db) is True


def test_periodic_digest_send_when_brands_match():
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [
        SimpleNamespace(domain="www.doviz.com", is_active=True),
    ]
    assert realtime_periodic_digest_skip_no_site_match(db) is False


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

    html = _digest_profile_block(db, site, "web", top_n=10, window_minutes=90)
    assert "Top sayfalar · son 1,5s zirve" in html
    assert "altin-fiyatlari" in html
    assert "42 kul" in html


def test_build_periodic_digest_empty_shows_diagnostics(monkeypatch):
    doviz = SimpleNamespace(id=1, domain="www.doviz.com", is_active=True)
    sinemalar = SimpleNamespace(id=2, domain="www.sinemalar.com", is_active=True)

    class _FakeQuery:
        def filter(self, *_a, **_k):
            return self

        def all(self):
            return [doviz, sinemalar]

    db = MagicMock()
    db.query.return_value = _FakeQuery()

    monkeypatch.setattr("backend.services.ga4_realtime._digest_profile_block", lambda *_a, **_k: "")
    monkeypatch.setattr(
        "backend.services.ga4_realtime._latest_collected_at",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_credentials_record",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "backend.services.ga4_auth.load_ga4_properties",
        lambda *_a, **_k: {"web": "properties/1"},
    )
    monkeypatch.setattr(
        "backend.services.ga4_realtime_quota.paused_property_resume_times",
        lambda: {},
    )
    monkeypatch.setattr(
        "backend.services.ga4_realtime_quota.is_property_paused",
        lambda *_a, **_k: False,
    )

    html = build_realtime_periodic_digest_html(db)
    assert "Hiç KPI snapshot yok" in html
    assert "saniye değil" in html
    assert "run-realtime-job-now" in html


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
