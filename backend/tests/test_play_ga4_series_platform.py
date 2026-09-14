"""GA4 overlay series must stay on the requested app profile."""

from types import SimpleNamespace

from backend.api.play_analytics import _virgul_overlay_branch
from backend.api import play_analytics as pa


def test_merge_keeps_newer_overlap_and_prepends_older_days():
    older = {
        "dates": ["2025-01-01", "2025-09-14"],
        "sessions": [100.0, 1.0],
        "activeUsers": [80.0, 1.0],
    }
    newer = {
        "dates": ["2025-09-14", "2025-09-15"],
        "sessions": [200.0, 210.0],
        "activeUsers": [150.0, 160.0],
    }
    merged = pa.merge_ga4_daily_trends(older, newer)
    assert merged["dates"] == ["2025-01-01", "2025-09-14", "2025-09-15"]
    assert merged["sessions"] == [100.0, 200.0, 210.0]
    assert merged["activeUsers"][0] == 80.0


def test_ga4_series_android_does_not_fall_back_to_ios(monkeypatch):
    calls = []

    def fake_payload(db, *, site_id, profile):
        calls.append(profile)
        return None

    monkeypatch.setattr(pa, "_ga4_daily_trend_payload", fake_payload)
    monkeypatch.setattr(pa, "_resolve_doviz_site", lambda db, project: SimpleNamespace(id=1))

    out = pa.get_play_ga4_overlay_series(
        db=None,  # type: ignore[arg-type]
        profile="android",
        metric="ga4:sessions",
    )
    assert calls == ["android"]
    assert out["ok"] is False
    assert out["profile"] == "android"


def test_ga4_series_ios_stays_on_ios(monkeypatch):
    calls = []

    def fake_payload(db, *, site_id, profile):
        calls.append(profile)
        return None

    monkeypatch.setattr(pa, "_ga4_daily_trend_payload", fake_payload)
    monkeypatch.setattr(pa, "_resolve_doviz_site", lambda db, project: SimpleNamespace(id=1))

    out = pa.get_play_ga4_overlay_series(
        db=None,  # type: ignore[arg-type]
        profile="ios",
        metric="ga4:users",
    )
    assert calls == ["ios"]
    assert out["ok"] is False
    assert out["profile"] == "ios"


def test_ga4_series_web_and_mweb_stay_on_profile(monkeypatch):
    calls = []

    def fake_payload(db, *, site_id, profile):
        calls.append(profile)
        return None

    monkeypatch.setattr(pa, "_ga4_daily_trend_payload", fake_payload)
    monkeypatch.setattr(pa, "_resolve_doviz_site", lambda db, project: SimpleNamespace(id=1))

    out = pa.get_play_ga4_overlay_series(
        db=None,  # type: ignore[arg-type]
        profile="web",
        metric="ga4:sessions",
    )
    assert calls == ["web"]
    assert out["ok"] is False
    assert out["profile"] == "web"

    calls.clear()
    out = pa.get_play_ga4_overlay_series(
        db=None,  # type: ignore[arg-type]
        profile="mweb",
        metric="ga4:users",
    )
    assert calls == ["mweb"]
    assert out["profile"] == "mweb"


def test_virgul_overlay_branch_web_maps_to_desktop():
    assert _virgul_overlay_branch("web") == "desktop"
    assert _virgul_overlay_branch("desktop") == "desktop"
    assert _virgul_overlay_branch("mweb") == "mweb"
    assert _virgul_overlay_branch("ios") == "ios"
    assert _virgul_overlay_branch("android") == "android"
