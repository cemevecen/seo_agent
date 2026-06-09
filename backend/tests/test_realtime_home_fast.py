"""Ana sayfa realtime hızlı yolu — snapshot + cache peek."""

from backend.services.ga4_realtime import bundle_from_snapshot_trend
from backend.services.realtime_cache import get_cached_only, get_or_call


def test_bundle_from_snapshot_trend_builds_comparison():
    trend = [
        {"active_users": 100, "active_users_prev": 80},
        {"active_users": 120, "active_users_prev": 100},
    ]
    b = bundle_from_snapshot_trend(trend)
    assert b is not None
    assert b["total"]["activeUsers"] == 120.0
    assert b["comparison"]["activeUsers"]["change_pct"] == 20.0
    assert b.get("from_snapshot") is True


def test_get_cached_only_without_producer():
    calls = {"n": 0}

    def producer():
        calls["n"] += 1
        return {"ok": True}

    key = "test:peek:1"
    get_or_call(key, 60.0, producer, is_error=lambda r: False)
    assert calls["n"] == 1
    peek = get_cached_only(key, 60.0)
    assert peek is not None
    assert peek.get("ok") is True
    assert calls["n"] == 1
