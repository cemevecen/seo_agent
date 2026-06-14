"""SEO Audit otomatik yenileme kaydı ve scheduler health testleri."""

from datetime import datetime

from backend.database import Base, SessionLocal, engine
from backend.main import _seo_audit_last_auto_run_label, _seo_audit_scheduler_health
from backend.models import CollectorRun, Site
from backend.services.warehouse import finish_collector_run, start_collector_run


def _seed_site(db) -> Site:
    site = Site(domain="doviz.com", display_name="Döviz", is_active=True)
    db.add(site)
    db.commit()
    db.refresh(site)
    return site


def test_seo_audit_last_auto_run_excludes_manual():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = _seed_site(db)
        finished = datetime(2026, 6, 4, 3, 15, 0)
        for strategy, status in (
            ("manual", "success"),
            ("scheduled", "success"),
            ("scheduled", "failed"),
        ):
            run = start_collector_run(
                db,
                site_id=site.id,
                provider="seo_audit",
                strategy=strategy,
                trigger_source=strategy,
            )
            finish_collector_run(db, run, status=status, finished_at=finished)
        db.commit()

        label = _seo_audit_last_auto_run_label(db, site.id)
        assert "Henüz" not in label
        assert "04.06.2026" in label or "4.06.2026" in label
    finally:
        db.query(CollectorRun).delete()
        db.query(Site).delete()
        db.commit()
        db.close()


def test_seo_audit_scheduler_health_shape():
    health = _seo_audit_scheduler_health()
    assert "enabled" in health
    assert "schedule" in health
    assert "scheduler_running" in health
    assert "job_registered" in health
    assert "next_run" in health
