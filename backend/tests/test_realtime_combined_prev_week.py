"""Site geneli combined trend — geçen hafta aynı saat hizalaması."""

from datetime import datetime, timedelta
from unittest.mock import patch

from backend.database import Base, SessionLocal, engine
from backend.models import RealtimeSnapshot, Site
from backend.services.ga4_realtime import get_combined_site_snapshots


def setup_module():
    Base.metadata.create_all(bind=engine)


def teardown_module():
    Base.metadata.drop_all(bind=engine)


def test_combined_site_snapshots_aligns_prev_week_same_clock():
    fixed_now = datetime(2026, 6, 22, 11, 0, 0)
    bucket_ms = 15 * 60 * 1000
    now_ms = int(fixed_now.timestamp() * 1000)
    key_now = (now_ms // bucket_ms) * bucket_ms
    ts_now = datetime.utcfromtimestamp(key_now / 1000.0)
    ts_prev = ts_now - timedelta(days=7)

    db = SessionLocal()
    try:
        site = Site(domain="doviz.com", display_name="doviz")
        db.add(site)
        db.flush()

        db.add_all(
            [
                RealtimeSnapshot(
                    site_id=site.id,
                    profile="web",
                    active_users_current=40000,
                    pageviews_current=120000,
                    collected_at=ts_now,
                ),
                RealtimeSnapshot(
                    site_id=site.id,
                    profile="mweb",
                    active_users_current=5000,
                    pageviews_current=8000,
                    collected_at=ts_now,
                ),
                RealtimeSnapshot(
                    site_id=site.id,
                    profile="web",
                    active_users_current=38000,
                    pageviews_current=110000,
                    collected_at=ts_prev,
                ),
            ]
        )
        db.commit()

        with patch("backend.services.ga4_realtime.datetime") as mock_dt:
            mock_dt.utcnow.return_value = fixed_now
            mock_dt.utcfromtimestamp = datetime.utcfromtimestamp
            trend = get_combined_site_snapshots(db, site.id, hours=24, include_prev_week=True)

        match = [r for r in trend if r["active_users"] == 45000]
        assert len(match) == 1
        row = match[0]
        assert row["active_users_prev_week"] == 38000
        assert row["pageviews_prev_week"] == 110000
    finally:
        db.close()
