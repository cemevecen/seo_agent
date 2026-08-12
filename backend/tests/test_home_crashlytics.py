"""Ana sayfa Crashlytics semver helpers."""

from backend.main import _home_cf_fmt, _home_crash_latest_version


def test_home_cf_fmt():
    assert _home_cf_fmt(99.9123) == "99.91%"
    assert _home_cf_fmt(99.9951) == "99.9951%"
    assert _home_cf_fmt(None) == "—"


def test_home_crash_latest_prefers_store():
    payload = {
        "filter_versions_by_platform": {"ios": ["9.4.1", "9.4.0"]},
    }
    assert _home_crash_latest_version(payload, "ios", store_version="9.5.0") == "9.5.0"
    assert _home_crash_latest_version(payload, "ios", store_version="9.0.0") == "9.4.1"
    assert _home_crash_latest_version(payload, "ios", store_version=None) == "9.4.1"
