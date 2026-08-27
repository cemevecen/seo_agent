"""İndirilen rapor arşivi — 7 gün retention."""

from datetime import datetime, timedelta

from backend.database import SessionLocal, init_db
from backend.models import ReportExportArchive
from backend.services import report_export_archive as rea


def _wipe() -> None:
    init_db()
    with SessionLocal() as db:
        db.query(ReportExportArchive).delete()
        db.commit()


def test_save_and_list_export():
    _wipe()
    with SessionLocal() as db:
        export_id = rea.save_export(
            db,
            report_kind="sheet_ayilma",
            export_format="xlsx",
            filename="ayilma_cizelge_2026-09.xlsx",
            content=b"fake-xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            actor_email="evecensema@gmail.com",
            actor_display_name="Sema Evecen",
            client_ip="10.0.0.1",
            meta={"year": 2026, "month": 9},
        )
        assert export_id > 0

    rows = rea.list_recent()
    assert len(rows) == 1
    row = rows[0]
    assert row["report_kind"] == "sheet_ayilma"
    assert row["report_label"] == "Ayılma çizelgesi"
    assert row["actor_email"] == "evecensema@gmail.com"
    assert row["who"] == "Sema Evecen"
    assert row["meta"]["month"] == 9

    got = rea.get_export_bytes(row["id"])
    assert got is not None
    assert got[0] == b"fake-xlsx"
    assert got[1] == "ayilma_cizelge_2026-09.xlsx"


def test_purge_expired_exports():
    _wipe()
    with SessionLocal() as db:
        row = ReportExportArchive(
            report_kind="sheet_ayilma",
            export_format="csv",
            filename="old.csv",
            media_type="text/csv",
            content=b"old",
            actor_email="a@b.com",
            actor_display_name="A",
            client_ip="1.1.1.1",
            meta_json="{}",
            downloaded_at=datetime.utcnow() - timedelta(days=8),
            expires_at=datetime.utcnow() - timedelta(days=1),
        )
        db.add(row)
        db.commit()

    assert rea.list_recent() == []
    assert rea.get_export_bytes(1) is None
