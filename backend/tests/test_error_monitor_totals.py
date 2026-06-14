"""GA4 hata özeti — 200 limit / meta row testleri."""

import json
from datetime import datetime

from backend.database import Base, SessionLocal, engine
from backend.models import Site, SiteErrorLog
from backend.services.error_monitor import (
    _FETCH_META_URL,
    _ga4_source_key,
    get_error_summary,
    save_error_logs,
)


def test_get_error_summary_uses_ga4_unique_count_from_meta():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = Site(domain="doviz.com", display_name="Döviz", is_active=True)
        db.add(site)
        db.commit()
        db.refresh(site)

        source = _ga4_source_key(7)
        errors = [
            {"url": f"/broken-{i}", "status_code": 404, "users": 1, "source": "ga4"}
            for i in range(5)
        ]
        save_error_logs(
            db,
            site.id,
            errors,
            source=source,
            fetch_meta={
                "ga4_unique_url_count": 847,
                "fetched_url_count": 5,
                "fetch_limit": 10000,
                "truncated": True,
            },
        )

        summary = get_error_summary(db, site.id, days=7)
        assert summary["total_404"] == 847
        assert summary["truncated"] is True
        assert summary["fetched_url_count"] == 5
        assert len(summary["errors"]) == 5
        assert all(e["url"] != _FETCH_META_URL for e in summary["errors"])
    finally:
        db.query(SiteErrorLog).delete()
        db.query(Site).delete()
        db.commit()
        db.close()


def test_get_error_summary_without_meta_counts_rows():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = Site(domain="x.com", display_name="X", is_active=True)
        db.add(site)
        db.commit()
        db.refresh(site)

        source = _ga4_source_key(7)
        now = datetime.utcnow()
        db.add(
            SiteErrorLog(
                site_id=site.id,
                url="/a",
                status_code=404,
                source=source,
                error_type="not_found",
                hit_count=3,
                first_seen=now,
                last_seen=now,
            )
        )
        db.add(
            SiteErrorLog(
                site_id=site.id,
                url="/b",
                status_code=404,
                source=source,
                error_type="not_found",
                hit_count=1,
                first_seen=now,
                last_seen=now,
            )
        )
        db.commit()

        summary = get_error_summary(db, site.id, days=7)
        assert summary["total_404"] == 2
        assert summary["truncated"] is False
    finally:
        db.query(SiteErrorLog).delete()
        db.query(Site).delete()
        db.commit()
        db.close()
