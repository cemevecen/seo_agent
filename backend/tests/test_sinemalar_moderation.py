"""Sinemalar moderasyon parse + ingest."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.database import Base
from backend.models import SinemalarModerationDailyRow, SinemalarModerationDetailItem, SinemalarModerationMeta
from backend.services import sinemalar_moderation as mod


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_resolve_user_id_fallback():
    assert mod.resolve_user_id("Gözde.", None) == 935786
    assert mod.resolve_user_id("berend", "883754") == 883754


def test_parse_summary_rows_tracks_only_target_users():
    raw = [
        {
            "moderator": "gezginozlem",
            "metrics": {
                "Film": {"type": "movie", "userId": "873391", "count": 3, "href": "/x?type=movie&userId=873391"},
                "Sanatçı": {"type": "person", "userId": "873391", "count": 1},
            },
        },
        {
            "moderator": "other_user",
            "metrics": {"Film": {"type": "movie", "userId": "1", "count": 99}},
        },
        {
            "moderator": "Gözde.",
            "metrics": {"Haber": {"type": "news", "userId": "935786", "count": 2}},
        },
    ]
    parsed = mod.parse_summary_rows(raw)
    assert len(parsed) == 3
    assert all(mod.is_tracked_username(p["username"]) for p in parsed)
    assert parsed[0]["metric_type"] == "movie"
    assert parsed[0]["count"] == 3


def test_parse_without_href_user_id():
    raw = [
        {
            "moderator": "berend",
            "metrics": {
                "Film": {"type": "movie", "count": 2},
            },
        }
    ]
    parsed = mod.parse_summary_rows(raw)
    assert len(parsed) == 1
    assert parsed[0]["user_id"] == 883754


def test_panel_backfill_shows_from_january():
    db = _session()
    mod.ingest_daily_batch(
        db,
        report_date="2026-01-03",
        rows=[
            {
                "moderator": "gezginozlem",
                "metrics": {"Film": {"type": "movie", "userId": "873391", "count": 5}},
            }
        ],
        mode="backfill",
    )
    meta = db.query(SinemalarModerationMeta).filter(SinemalarModerationMeta.id == 1).one()
    meta.backfill_complete = False
    meta.backfill_cursor = "2026-01-04"
    db.commit()
    panel = mod.get_panel_payload(db)
    assert panel["start"] == "2026-01-01"
    assert panel["row_count"] >= 1


def test_ingest_daily_batch_upserts():
    db = _session()
    rows = [
        {
            "moderator": "berend",
            "metrics": {
                "Liste": {"type": "list", "userId": "883754", "count": 4},
            },
        }
    ]
    res = mod.ingest_daily_batch(db, report_date="2026-03-01", rows=rows, scraped_at=datetime(2026, 3, 2, 12, 0))
    assert res["ok"] is True
    assert res["upserted"] == 1

    res2 = mod.ingest_daily_batch(
        db,
        report_date="2026-03-01",
        rows=[{"moderator": "berend", "metrics": {"Liste": {"type": "list", "userId": "883754", "count": 7}}}],
        scraped_at=datetime(2026, 3, 2, 13, 0),
    )
    assert res2["upserted"] == 1
    row = (
        db.query(SinemalarModerationDailyRow)
        .filter(
            SinemalarModerationDailyRow.report_date == date(2026, 3, 1),
            SinemalarModerationDailyRow.user_id == 883754,
            SinemalarModerationDailyRow.metric_type == "list",
        )
        .one()
    )
    assert row.count == 7


def test_summary_url_for_day_uses_next_day_end():
    from scripts.sinemalar_moderation_scrape import exclusive_detail_end, summary_url_for_day

    u = summary_url_for_day(date(2026, 3, 1))
    assert "startDate=2026-03-01" in u
    assert "endDate=2026-03-02" in u
    assert "endDate=2026-03-01" not in u
    assert exclusive_detail_end(date(2026, 8, 13)) == date(2026, 8, 14)
    detail = mod.detail_url(53, start=date(2026, 8, 13), end=date(2026, 8, 14), metric_type="movie")
    assert "startDate=2026-08-13" in detail
    assert "endDate=2026-08-14" in detail


def test_ingest_incremental_heartbeat_updates_last_sync():
    db = _session()
    res = mod.ingest_backfill_payload(
        db,
        {
            "mode": "detail_incremental",
            "scraped_at": "2026-08-14T10:00:00",
            "detail_batches": [],
            "backfill_complete": True,
            "message": "detail_incremental 2026-08-13 · yeni kayıt yok",
        },
    )
    assert res.get("ok") is True
    assert res.get("heartbeat") is True
    meta = mod.get_meta_summary(db)
    assert meta["last_mode"] == "detail_incremental"
    assert meta["last_scraped_at"]
    assert "2026-08-13" in (meta.get("message") or "")


def test_parse_detail_rows_movie():
    raw = [
        {
            "cells": [
                {"text": "299129", "href": "https://www.sinemalar.com/management/movie/299129"},
                {"text": "Amerika Deneyi", "href": "https://www.sinemalar.com/management/movie/299129"},
                {"text": "2026-06-03 18:45:20", "href": None},
            ]
        }
    ]
    parsed = mod.parse_detail_rows(raw, user_id=873391, username="gezginozlem", metric_type="movie")
    assert len(parsed) == 1
    assert parsed[0]["item_id"] == "299129"
    assert parsed[0]["title"] == "Amerika Deneyi"
    assert parsed[0]["metric_type"] == "movie"


def test_ingest_detail_batch_rebuilds_daily():
    db = _session()
    items = mod.parse_detail_rows(
        [
            {
                "cells": [
                    {"text": "1", "href": "/m/1"},
                    {"text": "Film A", "href": "/m/1"},
                    {"text": "2026-03-01 10:00:00", "href": None},
                ]
            },
            {
                "cells": [
                    {"text": "2", "href": "/m/2"},
                    {"text": "Film B", "href": "/m/2"},
                    {"text": "2026-03-01 11:00:00", "href": None},
                ]
            },
        ],
        user_id=883754,
        username="berend",
        metric_type="movie",
    )
    res = mod.ingest_detail_batch(
        db,
        user_id=883754,
        username="berend",
        metric_type="movie",
        items=items,
        range_start=date(2026, 3, 1),
        range_end=date(2026, 3, 31),
        recompute_daily=True,
    )
    assert res["ok"] is True
    assert res["items_inserted"] == 2
    assert res["items_upserted"] == 2
    row = (
        db.query(SinemalarModerationDailyRow)
        .filter(
            SinemalarModerationDailyRow.report_date == date(2026, 3, 1),
            SinemalarModerationDailyRow.user_id == 883754,
            SinemalarModerationDailyRow.metric_type == "movie",
        )
        .one()
    )
    assert row.count == 2


def test_detail_ingest_skips_duplicates_append_only():
    db = _session()
    items = mod.parse_detail_rows(
        [
            {
                "cells": [
                    {"text": "99", "href": "/m/99"},
                    {"text": "Tek", "href": "/m/99"},
                    {"text": "2026-04-01 09:00:00", "href": None},
                ]
            }
        ],
        user_id=873391,
        username="gezginozlem",
        metric_type="news",
    )
    first = mod.ingest_detail_batch(
        db,
        user_id=873391,
        username="gezginozlem",
        metric_type="news",
        items=items,
        range_start=date(2026, 4, 1),
        range_end=date(2026, 4, 1),
        sync_daily_date=date(2026, 4, 1),
    )
    assert first["items_inserted"] == 1
    second = mod.ingest_detail_batch(
        db,
        user_id=873391,
        username="gezginozlem",
        metric_type="news",
        items=items,
        range_start=date(2026, 4, 1),
        range_end=date(2026, 4, 1),
        sync_daily_date=date(2026, 4, 1),
    )
    assert second["items_inserted"] == 0
    assert second["items_skipped"] == 1
    assert db.query(SinemalarModerationDetailItem).count() == 1
    row = (
        db.query(SinemalarModerationDetailItem)
        .filter(SinemalarModerationDetailItem.item_id == "99")
        .one()
    )
    assert row.username == "gezginozlem"
    assert row.title == "Tek"


def test_purge_all_data():
    db = _session()
    mod.ingest_detail_batch(
        db,
        user_id=883754,
        username="berend",
        metric_type="movie",
        items=mod.parse_detail_rows(
            [{
                "cells": [
                    {"text": "1", "href": "/m/1"},
                    {"text": "X", "href": "/m/1"},
                    {"text": "2026-03-01 10:00:00", "href": None},
                ]
            }],
            user_id=883754,
            username="berend",
            metric_type="movie",
        ),
        range_start=date(2026, 3, 1),
        range_end=date(2026, 3, 31),
        recompute_daily=True,
    )
    out = mod.purge_all_data(db)
    assert out["ok"] is True
    assert out["deleted_details"] >= 1
    assert db.query(SinemalarModerationDetailItem).count() == 0


def test_backfill_payload_updates_meta():
    db = _session()
    payload = {
        "mode": "backfill",
        "scraped_at": "2026-03-10T10:00:00+00:00",
        "days": [
            {
                "date": "2026-03-01",
                "rows": [
                    {
                        "moderator": "gezginozlem",
                        "metrics": {
                            "Film": {"type": "movie", "userId": "873391", "count": 1},
                        },
                    }
                ],
            }
        ],
        "backfill_cursor": "2026-03-02",
        "backfill_complete": False,
    }
    out = mod.ingest_backfill_payload(db, payload)
    assert out["ok"] is True
    meta = db.query(SinemalarModerationMeta).filter(SinemalarModerationMeta.id == 1).one()
    assert meta.backfill_cursor == "2026-03-02"
    assert meta.backfill_complete is False

    panel = mod.get_panel_payload(db, start="2026-03-01", end="2026-03-01")
    assert panel["ok"] is True
    assert panel["row_count"] == 1
    assert len(panel["users"]) == 6
    assert panel["users"][2]["username"] == "gezginozlem"
    assert panel["users"][2]["total_all"] == 1


def test_aquuamarine_alias():
    assert mod.resolve_user_id("Aquuamarine", None) == 245939
    assert mod.is_tracked_username("Aquuamarine")
    raw = [
        {
            "moderator": "Aquuamarine",
            "metrics": {"Film": {"type": "movie", "userId": "245939", "count": 9}},
        }
    ]
    parsed = mod.parse_summary_rows(raw)
    assert len(parsed) == 1
    assert parsed[0]["user_id"] == 245939
    db = _session()
    mod.ingest_detail_batch(
        db,
        user_id=245939,
        username="Aquamarine",
        metric_type="movie",
        items=[
            {
                "user_id": 245939,
                "username": "Aquamarine",
                "metric_type": "movie",
                "item_id": "1",
                "title": "A",
                "subtitle": "",
                "event_at": "2026-02-01 10:00:00",
            }
        ],
        range_start=date(2026, 1, 1),
        range_end=date(2026, 8, 13),
        recompute_daily=True,
    )
    cov = mod.get_detail_coverage(db, start="2026-01-01", end="2026-08-13")
    assert cov["counts"]["245939|movie"] == 1
    gaps = mod.compute_gaps(
        {"245939|movie": 5, "245939|person": 3},
        cov["counts"],
        user_ids=[245939],
    )
    assert len(gaps) == 2
    assert gaps[0]["missing"] == 4


def test_build_panel_analytics_from_details():
    db = _session()
    mod.ingest_detail_batch(
        db,
        user_id=873391,
        username="gezginozlem",
        metric_type="movie",
        items=[
            {
                "user_id": 873391,
                "username": "gezginozlem",
                "metric_type": "movie",
                "item_id": "10",
                "title": "Film",
                "subtitle": "",
                "event_at": "2026-03-01 12:00:00",
            },
            {
                "user_id": 873391,
                "username": "gezginozlem",
                "metric_type": "movie",
                "item_id": "11",
                "title": "Film2",
                "subtitle": "",
                "event_at": "2026-03-02 12:00:00",
            },
        ],
        range_start=date(2026, 3, 1),
        range_end=date(2026, 3, 3),
        recompute_daily=True,
    )
    panel = mod.get_panel_payload(db, start="2026-03-01", end="2026-03-03")
    analytics = panel.get("analytics") or {}
    assert analytics.get("calendar_days")
    assert analytics["calendars"]["873391"]["active_days"] == 2
    assert analytics["overall_rank"][0]["username"] == "gezginozlem"


def test_gozde_join_date_excludes_pre_join_inactive():
    db = _session()
    mod.ingest_detail_batch(
        db,
        user_id=935786,
        username="Gözde.",
        metric_type="movie",
        items=[
            {
                "user_id": 935786,
                "username": "Gözde.",
                "metric_type": "movie",
                "item_id": "1",
                "title": "A",
                "subtitle": "",
                "event_at": "2026-05-05 10:00:00",
            },
            {
                "user_id": 935786,
                "username": "Gözde.",
                "metric_type": "movie",
                "item_id": "2",
                "title": "B",
                "subtitle": "",
                "event_at": "2026-05-06 10:00:00",
            },
        ],
        range_start=date(2026, 5, 1),
        range_end=date(2026, 5, 10),
        recompute_daily=False,
    )
    panel = mod.get_panel_payload(db, start="2026-05-01", end="2026-05-10")
    cal = (panel.get("analytics") or {}).get("calendars", {}).get("935786") or {}
    assert cal.get("joined_at") == "2026-05-04"
    assert cal.get("pre_join_days") == 3
    assert cal.get("eligible_days") == 7
    assert cal.get("active_days") == 2
    assert cal.get("inactive_days") == 5


def test_panel_metric_labels_are_english():
    assert mod.metric_display_label("movie") == "Movie"
    assert mod.metric_display_label("summary") == "Movie summary"
    db = _session()
    panel = mod.get_panel_payload(db, start="2026-01-01", end="2026-01-02")
    labels = {mt["key"]: mt["label"] for mt in panel["metric_types"]}
    assert labels["movie"] == "Movie"
    assert labels["person"] == "Artist"
    assert labels["movie_cast_add"] == "Cast add"
    assert panel["range_min"] == "2026-01-01"
