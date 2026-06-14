"""Data Explorer domain alias ve context testleri."""

from datetime import datetime

from backend.database import Base, SessionLocal, engine
from backend.main import (
    _data_explorer_context,
    _resolve_site_by_domain,
    _site_domain_candidates,
)
from backend.models import CruxHistorySnapshot, Metric, Site


def _seed_site(db, *, domain: str, display_name: str) -> Site:
    site = Site(domain=domain, display_name=display_name, is_active=True)
    db.add(site)
    db.commit()
    db.refresh(site)
    return site


def test_site_domain_candidates_aliases():
    assert _site_domain_candidates("www.doviz.com") == ["www.doviz.com", "doviz.com"]
    assert _site_domain_candidates("doviz.com") == ["doviz.com", "www.doviz.com"]
    assert _site_domain_candidates("example.org") == ["example.org"]


def test_resolve_site_by_domain_www_alias():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = _seed_site(db, domain="doviz.com", display_name="Döviz")
        assert _resolve_site_by_domain(db, "www.doviz.com") is not None
        assert _resolve_site_by_domain(db, "www.doviz.com").id == site.id
        assert _resolve_site_by_domain(db, "unknown.example") is None
    finally:
        db.query(CruxHistorySnapshot).delete()
        db.query(Metric).delete()
        db.query(Site).delete()
        db.commit()
        db.close()


def test_data_explorer_context_resolves_www_and_sets_section_dates():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = _seed_site(db, domain="www.sinemalar.com", display_name="Sinemalar")
        collected = datetime(2026, 4, 21, 2, 4, 0)
        db.add(
            CruxHistorySnapshot(
                site_id=site.id,
                form_factor="mobile",
                target_url="https://www.sinemalar.com/",
                summary_json='{"series": {}}',
                payload_json='{"history": {}}',
                collected_at=collected,
            )
        )
        db.add(
            CruxHistorySnapshot(
                site_id=site.id,
                form_factor="desktop",
                target_url="https://www.sinemalar.com/",
                summary_json='{"series": {}}',
                payload_json='{"history": {}}',
                collected_at=collected,
            )
        )
        db.commit()

        ctx = _data_explorer_context("sinemalar.com")
        assert ctx["domain"] == "www.sinemalar.com"
        assert "21.04.2026" in ctx["crux_history_last_updated"]
        assert "21.04.2026" in ctx["crux_mobile_last_updated"]
        assert "21.04.2026" in ctx["crux_desktop_last_updated"]
        assert ctx["data_explorer_schedule"] == "07:00"
    finally:
        db.query(CruxHistorySnapshot).delete()
        db.query(Metric).delete()
        db.query(Site).delete()
        db.commit()
        db.close()
