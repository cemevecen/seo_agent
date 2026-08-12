"""Sinemalar moderasyon parse + ingest."""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.database import Base
from backend.models import SinemalarModerationDailyRow, SinemalarModerationMeta
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
    assert len(panel["users"]) == 1
    assert panel["users"][0]["username"] == "gezginozlem"
