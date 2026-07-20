"""Data Explorer otomatik yenileme kaydı ve trigger_source testleri."""

import json
from datetime import datetime

from backend.database import Base, SessionLocal, engine
from backend.main import (
    _build_data_explorer_auto_refresh_log,
    _collector_run_trigger_source,
    _data_explorer_last_auto_refresh_label,
)
from backend.models import CollectorRun, Site
from backend.services.warehouse import finish_collector_run, start_collector_run


def _seed_site(db) -> Site:
    site = Site(domain="doviz.com", display_name="Döviz", is_active=True)
    db.add(site)
    db.commit()
    db.refresh(site)
    return site


def test_trigger_source_preserved_on_finish():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = _seed_site(db)
        run = start_collector_run(
            db,
            site_id=site.id,
            provider="pagespeed",
            strategy="mobile",
            trigger_source="system",
        )
        finish_collector_run(db, run, status="success", summary={"source": "live"})
        db.commit()
        db.refresh(run)
        data = json.loads(run.summary_json or "{}")
        assert data.get("trigger_source") == "system"
        assert data.get("source") == "live"
        assert _collector_run_trigger_source(run) == "system"
    finally:
        db.query(CollectorRun).delete()
        db.query(Site).delete()
        db.commit()
        db.close()


def test_auto_refresh_log_excludes_manual_runs():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = _seed_site(db)
        finished = datetime(2026, 6, 14, 4, 7, 0)
        for provider, strategy, ts, status in (
            ("pagespeed", "mobile", "system", "success"),
            ("pagespeed", "mobile", "manual", "success"),
            ("crux_history", "mobile", "system", "failed"),
        ):
            run = start_collector_run(
                db,
                site_id=site.id,
                provider=provider,
                strategy=strategy,
                trigger_source=ts,
            )
            finish_collector_run(db, run, status=status, finished_at=finished)
        db.commit()

        rows = _build_data_explorer_auto_refresh_log(db, site.id)
        assert len(rows) == 2
        assert all("Manuel" not in (r.get("trigger_label") or "") for r in rows)
        assert any(r["label"] == "PSI · Mobil" and r["status_ok"] for r in rows)
        assert any(r["label"] == "CrUX · Mobil" and r["status"] == "failed" for r in rows)
        assert "14.06.2026" in _data_explorer_last_auto_refresh_label(db, site.id)
    finally:
        db.query(CollectorRun).delete()
        db.query(Site).delete()
        db.commit()
        db.close()


def test_last_auto_refresh_finds_success_beyond_recent_manual_flood():
    """Son 80+ manuel run olsa bile eski otomatik başarıyı bulmalı."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = _seed_site(db)
        auto_finished = datetime(2026, 4, 21, 2, 0, 0)
        run = start_collector_run(
            db,
            site_id=site.id,
            provider="pagespeed",
            strategy="mobile",
            trigger_source="system",
        )
        finish_collector_run(db, run, status="success", finished_at=auto_finished)
        for i in range(120):
            m = start_collector_run(
                db,
                site_id=site.id,
                provider="pagespeed",
                strategy="mobile",
                trigger_source="manual",
            )
            finish_collector_run(
                db,
                m,
                status="success",
                finished_at=datetime(2026, 6, 1, 10, 0, 0),
            )
        db.commit()
        label = _data_explorer_last_auto_refresh_label(db, site.id)
        assert "21.04.2026" in label
        log = _build_data_explorer_auto_refresh_log(db, site.id)
        assert any(r["label"] == "PSI · Mobil" and r["status_ok"] for r in log)
    finally:
        db.query(CollectorRun).delete()
        db.query(Site).delete()
        db.commit()
        db.close()
