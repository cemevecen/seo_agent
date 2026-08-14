"""Notification ingest: mühürlü merge, wipe yok."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.database import Base
from backend.models import NotificationAnalyticsWorkspace
from backend.services.notification_analytics_store import (
    WORKSPACE_ID,
    ingest_notification_rows,
    replace_workspace_from_rows,
)


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def test_notification_ingest_merge_keeps_sealed_rows(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    replace_workspace_from_rows(
        db,
        [
            {"id": 1, "text": "old", "date": "2026-08-10"},
            {"id": 2, "text": "mid", "date": "2026-08-13"},
        ],
        source="seed",
    )

    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=date(2026, 8, 15),
    ):
        ingest_notification_rows(
            db,
            [
                {"id": 2, "text": "mid", "date": "2026-08-13"},
                {"id": 3, "text": "new", "date": "2026-08-14"},
                {"id": 9, "text": "today", "date": "2026-08-15"},  # drop when allow_today=False
            ],
            source="bridge",
            replace=False,
            allow_today=False,
        )

    import json

    rows = json.loads(db.get(NotificationAnalyticsWorkspace, WORKSPACE_ID).rows_json)
    by_id = {int(r["id"]): r for r in rows}
    assert 1 in by_id  # sealed kept
    assert by_id[3]["date"] == "2026-08-14"
    assert 9 not in by_id  # today not stored


def test_notification_live_allow_today_keeps_today(monkeypatch):
    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    replace_workspace_from_rows(
        db,
        [{"id": 1, "text": "old", "date": "2026-08-10"}],
        source="seed",
    )
    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=date(2026, 8, 15),
    ):
        ingest_notification_rows(
            db,
            [
                {"id": 1, "text": "old", "date": "2026-08-10"},
                {"id": 9, "text": "today", "date": "2026-08-15"},
            ],
            source="bridge",
            replace=False,
            allow_today=True,
        )
    import json

    rows = json.loads(db.get(NotificationAnalyticsWorkspace, WORKSPACE_ID).rows_json)
    by_id = {int(r["id"]): r for r in rows}
    assert 1 in by_id
    assert by_id[9]["date"] == "2026-08-15"
