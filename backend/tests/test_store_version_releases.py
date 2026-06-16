from datetime import datetime, timezone

from backend.services.store_version_releases import _releases_from_reviews


def test_releases_from_reviews_first_seen():
    since = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows = [
        {"version": "9.5.0", "at": datetime(2025, 6, 1, tzinfo=timezone.utc)},
        {"version": "9.5.0", "at": datetime(2025, 5, 1, tzinfo=timezone.utc)},
        {"version": "9.4.0", "at": datetime(2024, 12, 1, tzinfo=timezone.utc)},
    ]
    out = _releases_from_reviews(rows, since=since)
    assert len(out) == 1
    assert out[0]["version"] == "9.5.0"
    assert out[0]["released_at"].startswith("2025-05-01")
