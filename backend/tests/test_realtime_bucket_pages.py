from datetime import datetime, timedelta, timezone

from backend.database import Base, SessionLocal, engine
from backend.models import RealtimePageSnapshot, Site
from backend.services.ga4_realtime import get_combined_bucket_top_pages


def setup_module():
    Base.metadata.create_all(bind=engine)


def teardown_module():
    Base.metadata.drop_all(bind=engine)


def test_combined_bucket_top_pages_splits_web_and_mweb():
    db = SessionLocal()
    try:
        site = Site(domain="doviz.com", display_name="doviz")
        db.add(site)
        db.flush()

        # Sorgu «son 1 saat»e bakıyor; sabit tarih takvim ilerleyince pencerenin
        # dışına düşüp testi kendiliğinden kırıyordu. Kova şimdiye göre seçilir.
        bucket_ms = 15 * 60 * 1000
        base = datetime.now(timezone.utc)
        key = int(base.timestamp() * 1000)
        key = (key // bucket_ms) * bucket_ms
        ts = datetime.fromtimestamp(key / 1000.0, tz=timezone.utc).replace(tzinfo=None)

        db.add_all(
            [
                RealtimePageSnapshot(
                    site_id=site.id,
                    profile="web",
                    page_path="Web A",
                    active_users=100,
                    collected_at=ts,
                    rank=1,
                ),
                RealtimePageSnapshot(
                    site_id=site.id,
                    profile="mweb",
                    page_path="Mweb A",
                    active_users=80,
                    collected_at=ts,
                    rank=1,
                ),
                RealtimePageSnapshot(
                    site_id=site.id,
                    profile="ios",
                    page_path="App screen",
                    active_users=500,
                    collected_at=ts,
                    rank=1,
                ),
            ]
        )
        db.commit()

        pages = get_combined_bucket_top_pages(db, site.id, hours=1, top_n=3)
        bucket = pages[str(key)]
        assert bucket["web"][0]["page_path"] == "Web A"
        assert bucket["mweb"][0]["page_path"] == "Mweb A"
        assert "ios" not in bucket
    finally:
        db.close()
