"""ASC ingest: mühürlü tarihler wipe edilmez."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.database import Base
from backend.models import AscConsoleWorkspace
from backend.services.asc_console_store import _pack_metrics_blob, ingest_asc_console_payload


def _session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine)()


def test_asc_ingest_upserts_without_wiping_history(monkeypatch):
    from unittest.mock import patch

    monkeypatch.setenv("HISTORY_SEALED", "1")
    db = _session()
    row = AscConsoleWorkspace(id=1)
    existing = {
        "version": 1,
        "explorer_facts": [
            {"metric": "units", "date": "2026-08-10", "dim": "overview", "value": 10},
            {"metric": "units", "date": "2026-08-13", "dim": "overview", "value": 13},
        ],
    }
    row.metrics_json = _pack_metrics_blob([], existing)
    db.add(row)
    db.commit()

    with patch(
        "backend.services.history_seal.calendar_today",
        return_value=__import__("datetime").date(2026, 8, 15),
    ):
        ingest_asc_console_payload(
            db,
            metrics=[],
            panels={
                "explorer_facts": [
                    {"metric": "units", "date": "2026-08-14", "dim": "overview", "value": 14},
                    {"metric": "units", "date": "2026-08-13", "dim": "overview", "value": 130},
                ]
            },
            sync_ok=True,
            sync_mode="analytics_scrape",
        )

    from backend.services.asc_console_store import _unpack_metrics_blob

    _, panels = _unpack_metrics_blob(db.get(AscConsoleWorkspace, 1).metrics_json)
    by_date = {
        f["date"]: f["value"]
        for f in panels["explorer_facts"]
        if f["metric"] == "units"
    }
    assert by_date["2026-08-10"] == 10  # sealed history kept
    assert by_date["2026-08-13"] == 130  # upsert
    assert by_date["2026-08-14"] == 14  # new day
