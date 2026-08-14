"""GA4 overlay series must stay on the requested app profile."""

from types import SimpleNamespace

from backend.api import play_analytics as pa


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
