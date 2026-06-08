from backend.api.ad_analytics import _mz_ga4_overlay_profiles


def test_ga4_overlay_profiles_web_branches():
    kind, profiles = _mz_ga4_overlay_profiles("desktop")
    assert kind == "web"
    assert profiles == ["web"]
    kind, profiles = _mz_ga4_overlay_profiles("mweb")
    assert kind == "web"
    assert profiles == ["mweb"]


def test_ga4_overlay_profiles_app_branches():
    kind, profiles = _mz_ga4_overlay_profiles("android")
    assert kind == "app"
    assert profiles == ["android", "ios"]
