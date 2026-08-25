from datetime import datetime, timedelta

import pytest

from backend.database import Base, SessionLocal, engine
from backend.models import RealtimeNewsArticleBucket, Site
from backend.services import realtime_news_buckets as rnb


def setup_module():
    Base.metadata.create_all(bind=engine)


def teardown_module():
    Base.metadata.drop_all(bind=engine)


NEWS_ROWS = [
    {"id": "913794", "title": "Bitcoin 63 bin doların altına geriledi"},
    {"id": "913800", "title": "Altın fiyatları yükselişe geçti"},
    {"id": "913801", "title": "Aynı Başlık"},
    {"id": "913802", "title": "Aynı Başlık"},
]


def _fake_fetch(pages):
    def _fetch(property_id, **kwargs):
        return {
            "property_id": property_id,
            "window_minutes": 29,
            "pages": list(pages),
        }

    return _fetch


def _patch_ga4(monkeypatch, *, properties, profiles=("web", "mweb")):
    import backend.services.ga4_auth as ga4_auth
    import backend.services.ga4_realtime_quota as quota

    monkeypatch.setattr(ga4_auth, "get_ga4_credentials_record", lambda db, site_id: None)
    monkeypatch.setattr(ga4_auth, "load_ga4_properties", lambda record: dict(properties))
    monkeypatch.setattr(
        quota, "scheduler_profiles_for_site", lambda domain, props: tuple(profiles)
    )


@pytest.fixture()
def db_site():
    db = SessionLocal()
    db.query(RealtimeNewsArticleBucket).delete()
    site = Site(domain="doviz.com", display_name="doviz")
    db.add(site)
    db.commit()
    try:
        yield db, site
    finally:
        db.query(RealtimeNewsArticleBucket).delete()
        db.query(Site).delete()
        db.commit()
        db.close()


# ── Kova hizalaması ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "now,expected_start",
    [
        (datetime(2026, 8, 15, 12, 30, 4), datetime(2026, 8, 15, 12, 0)),
        (datetime(2026, 8, 15, 12, 0, 1), datetime(2026, 8, 15, 11, 30)),
        (datetime(2026, 8, 15, 13, 0, 0), datetime(2026, 8, 15, 12, 30)),
        # Cron birkaç saniye erken uyanırsa kova kaymamalı.
        (datetime(2026, 8, 15, 12, 29, 58), datetime(2026, 8, 15, 12, 0)),
    ],
)
def test_resolve_bucket_aligns_to_grid(now, expected_start):
    start, end = rnb.resolve_bucket(now)
    assert start == expected_start
    assert end - start == timedelta(minutes=30)


# ── ID eşleştirme ────────────────────────────────────────────────────────────


def test_build_article_index_drops_ambiguous_titles():
    ids, by_title = rnb.build_article_index(NEWS_ROWS)
    assert "913794" in ids
    assert by_title["bitcoin 63 bin doların altına geriledi"] == "913794"
    # Aynı başlıklı iki haber varsa tahmin yapılmaz.
    assert "aynı başlık" not in by_title


def test_resolve_row_article_id_prefers_path():
    ids, by_title = rnb.build_article_index(NEWS_ROWS)
    row = {
        "page": "Bitcoin 63 bin doların altına geriledi - Doviz.com",
        "page_path": "/kripto-para-haberleri/bitcoin-63-bin-dolarin-altina-geriledi/913794",
        "link_url": "",
    }
    assert rnb.resolve_row_article_id(row, known_ids=ids, title_index=by_title) == (
        "913794",
        "path",
    )


def test_resolve_row_article_id_falls_back_to_title():
    ids, by_title = rnb.build_article_index(NEWS_ROWS)
    row = {"page": "Altın fiyatları yükselişe geçti - Doviz.com", "page_path": ""}
    assert rnb.resolve_row_article_id(row, known_ids=ids, title_index=by_title) == (
        "913800",
        "title",
    )


def test_resolve_row_article_id_returns_empty_when_unknown():
    ids, by_title = rnb.build_article_index(NEWS_ROWS)
    row = {"page": "Bilinmeyen bir sayfa", "page_path": "/canli-doviz"}
    assert rnb.resolve_row_article_id(row, known_ids=ids, title_index=by_title) == ("", "")


# ── Toplama: mükerrer kayıt yok ──────────────────────────────────────────────


def test_collect_is_idempotent_within_same_bucket(db_site, monkeypatch):
    db, site = db_site
    _patch_ga4(monkeypatch, properties={"web": "111"}, profiles=("web",))
    pages = [
        {
            "page": "Bitcoin 63 bin doların altına geriledi",
            "page_path": "/kripto-para-haberleri/bitcoin/913794",
            "activeUsers": 12,
            "screenPageViews": 40,
        }
    ]
    now = datetime(2026, 8, 15, 12, 30, 5)

    first = rnb.collect_news_article_buckets(
        db, site_id=site.id, now=now, fetch=_fake_fetch(pages), news_rows=NEWS_ROWS
    )
    second = rnb.collect_news_article_buckets(
        db, site_id=site.id, now=now, fetch=_fake_fetch(pages), news_rows=NEWS_ROWS
    )

    assert first["created"] == 1
    assert second["created"] == 0 and second["updated"] == 1

    rows = db.query(RealtimeNewsArticleBucket).all()
    assert len(rows) == 1
    assert rows[0].window_minutes == 29

    totals = rnb.get_article_realtime_totals(db, ["913794"], site_id=site.id)
    assert totals["913794"]["rt_views"] == 40
    assert totals["913794"]["rt_buckets"] == 1


def test_disjoint_buckets_are_summed(db_site, monkeypatch):
    db, site = db_site
    _patch_ga4(monkeypatch, properties={"web": "111"}, profiles=("web",))
    pages = [
        {
            "page": "Bitcoin 63 bin doların altına geriledi",
            "page_path": "/kripto-para-haberleri/bitcoin/913794",
            "activeUsers": 10,
            "screenPageViews": 40,
        }
    ]

    rnb.collect_news_article_buckets(
        db,
        site_id=site.id,
        now=datetime(2026, 8, 15, 12, 30, 5),
        fetch=_fake_fetch(pages),
        news_rows=NEWS_ROWS,
    )
    rnb.collect_news_article_buckets(
        db,
        site_id=site.id,
        now=datetime(2026, 8, 15, 13, 0, 5),
        fetch=_fake_fetch(pages),
        news_rows=NEWS_ROWS,
    )

    totals = rnb.get_article_realtime_totals(db, ["913794"], site_id=site.id)
    assert totals["913794"]["rt_views"] == 80
    assert totals["913794"]["rt_buckets"] == 2
    # Aktif kullanıcı eşzamanlı metrik — toplanmaz, zirve alınır.
    assert totals["913794"]["rt_peak_users"] == 10


def test_shared_property_is_collected_once(db_site, monkeypatch):
    """mweb, web property'sine düşüyorsa aynı trafik iki kez sayılmamalı."""
    db, site = db_site
    _patch_ga4(monkeypatch, properties={"web": "111", "mweb": ""}, profiles=("web", "mweb"))
    pages = [
        {
            "page": "Bitcoin 63 bin doların altına geriledi",
            "page_path": "/kripto-para-haberleri/bitcoin/913794",
            "activeUsers": 10,
            "screenPageViews": 40,
        }
    ]

    summary = rnb.collect_news_article_buckets(
        db,
        site_id=site.id,
        now=datetime(2026, 8, 15, 12, 30, 5),
        fetch=_fake_fetch(pages),
        news_rows=NEWS_ROWS,
    )

    assert [p["profile"] for p in summary["profiles"]] == ["web"]
    totals = rnb.get_article_realtime_totals(db, ["913794"], site_id=site.id)
    assert totals["913794"]["rt_views"] == 40


def test_separate_properties_are_both_collected(db_site, monkeypatch):
    db, site = db_site
    _patch_ga4(monkeypatch, properties={"web": "111", "mweb": "222"}, profiles=("web", "mweb"))
    pages = [
        {
            "page": "Bitcoin 63 bin doların altına geriledi",
            "page_path": "/kripto-para-haberleri/bitcoin/913794",
            "activeUsers": 10,
            "screenPageViews": 40,
        }
    ]

    rnb.collect_news_article_buckets(
        db,
        site_id=site.id,
        now=datetime(2026, 8, 15, 12, 30, 5),
        fetch=_fake_fetch(pages),
        news_rows=NEWS_ROWS,
    )

    rows = db.query(RealtimeNewsArticleBucket).all()
    assert sorted(r.profile for r in rows) == ["mweb", "web"]
    totals = rnb.get_article_realtime_totals(db, ["913794"], site_id=site.id)
    assert totals["913794"]["rt_views"] == 80
    assert totals["913794"]["rt_buckets"] == 1


def test_payload_items_expose_realtime_totals_only_with_traffic(db_site, monkeypatch):
    """rt_views yalnız include_traffic=True iken gelir (Show traffic)."""
    from backend.services import doviz_news_sheet

    db, site = db_site
    _patch_ga4(monkeypatch, properties={"web": "111"}, profiles=("web",))
    pages = [
        {
            "page": "Bitcoin 63 bin doların altına geriledi",
            "page_path": "/kripto-para-haberleri/bitcoin/913794",
            "activeUsers": 7,
            "screenPageViews": 40,
        }
    ]
    rnb.collect_news_article_buckets(
        db,
        site_id=site.id,
        now=datetime(2026, 8, 15, 12, 30, 5),
        fetch=_fake_fetch(pages),
        news_rows=NEWS_ROWS,
    )

    monkeypatch.setattr(
        doviz_news_sheet,
        "fetch_doviz_news_rows",
        lambda **kwargs: [
            {
                "id": "913794",
                "title": "Bitcoin 63 bin doların altına geriledi",
                "category": "Kripto Para",
                "date": "2026-08-15 12:05:00",
                "date_day": "2026-08-15",
                "active": True,
                "is_own": True,
            }
        ],
    )

    bare = doviz_news_sheet.doviz_news_payload(
        db=db, include_traffic=False, site_id=site.id, period="last_7d"
    )
    bare_item = next(i for i in bare["items"] if str(i["id"]) == "913794")
    assert bare_item["rt_views"] is None
    assert bare_item["views"] is None
    assert bare_item["platforms"] is None
    assert bare.get("platform_traffic") is None
    assert bare.get("traffic") is None

    monkeypatch.setattr(
        "backend.services.doviz_news_traffic.enrich_doviz_news_traffic",
        lambda *a, **k: {"ok": True, "by_article": {}},
    )
    monkeypatch.setattr(
        "backend.services.doviz_news_traffic.fetch_news_platform_breakdown",
        lambda *a, **k: {"ok": True, "by_article": {}, "urls": {}},
    )
    monkeypatch.setattr(
        "backend.services.doviz_news_traffic.clear_doviz_news_traffic_caches",
        lambda: None,
    )

    payload = doviz_news_sheet.doviz_news_payload(
        db=db, include_traffic=True, site_id=site.id, period="last_7d"
    )
    item = next(i for i in payload["items"] if str(i["id"]) == "913794")
    assert item["rt_views"] == 40
    assert item["rt_peak_users"] == 7
    assert item["rt_buckets"] == 1


def test_unmatched_rows_are_counted_not_written(db_site, monkeypatch):
    db, site = db_site
    _patch_ga4(monkeypatch, properties={"web": "111"}, profiles=("web",))
    pages = [
        {"page": "Canlı Döviz Kurları", "page_path": "/canli-doviz", "screenPageViews": 900},
        {
            "page": "Bitcoin 63 bin doların altına geriledi",
            "page_path": "/kripto-para-haberleri/bitcoin/913794",
            "screenPageViews": 40,
        },
    ]

    summary = rnb.collect_news_article_buckets(
        db,
        site_id=site.id,
        now=datetime(2026, 8, 15, 12, 30, 5),
        fetch=_fake_fetch(pages),
        news_rows=NEWS_ROWS,
    )

    assert summary["matched_rows"] == 1
    assert summary["unmatched_rows"] == 1
    assert db.query(RealtimeNewsArticleBucket).count() == 1
