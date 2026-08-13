"""Ana sayfa Crashlytics semver helpers."""

from unittest.mock import patch

from backend.main import (
    _home_cf_fmt,
    _home_coerce_star_hist,
    _home_crash_latest_version,
    _home_ensure_android_star_hist,
)


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


def test_home_ensure_android_star_hist_hydrates_missing():
    raw = {
        "android": {
            "meta": {
                "category_rank": {"rank": 146, "total": 200},
            }
        }
    }
    patch_play = {
        "score": 4.75,
        "ratings": 2051,
        "histogram": {"1": 40, "2": 9, "3": 53, "4": 149, "5": 1800},
    }
    with patch(
        "backend.services.app_intel._fetch_android_play_store_meta",
        return_value=patch_play,
    ), patch(
        "backend.services.app_intel._android_stars_from_play_console",
        return_value={},
    ), patch("backend.services.app_intel._write_disk_raw"):
        out = _home_ensure_android_star_hist(raw, "doviz")
    meta = out["android"]["meta"]
    assert meta["score"] == 4.75
    assert meta["ratings"] == 2051
    assert _home_coerce_star_hist(meta, key="android")["5"] == 1800


def test_home_ensure_android_star_hist_skips_when_complete():
    raw = {
        "android": {
            "meta": {
                "score": 4.8,
                "ratings": 2000,
                "histogram": {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5},
            }
        }
    }
    with patch("backend.services.app_intel._fetch_android_play_store_meta") as fetch:
        out = _home_ensure_android_star_hist(raw, "doviz")
    fetch.assert_not_called()
    assert out is raw
