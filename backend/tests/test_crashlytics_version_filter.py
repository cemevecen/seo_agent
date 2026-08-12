"""Crashlytics sürüm filtresi — scrape payload helpers."""

from backend.main import (
    _crash_fetch_filter_cache_key,
    _version_list_from_params,
)
from backend.services.crashlytics_payload import pick_higher_version, semver_sort_versions
from backend.services.firebase_from_store_tabs import _filter_payload


def test_version_list_from_params():
    p = {"versions": ["9.5.5"], "version": "9.5.4"}
    got = _version_list_from_params(p)
    assert "9.5.5" in got
    assert "9.5.4" in got


def test_pick_higher_version_semver():
    assert pick_higher_version("9.5.4", "9.5.5") == "9.5.5"
    assert pick_higher_version("9.5.10", "9.5.5") == "9.5.10"


def test_semver_sort_versions():
    assert semver_sort_versions(["9.5.4", "9.5.10", "9.5.5"])[0] == "9.5.10"


def test_filter_payload_by_version():
    base = {
        "ok": True,
        "issues": [
            {"issue_id": "a", "event_count": 10, "error_type": "FATAL", "latest_version": "9.5.5"},
            {"issue_id": "b", "event_count": 3, "error_type": "FATAL", "latest_version": "9.5.4"},
        ],
        "anr": [],
        "issues_by_platform": {},
        "anr_by_platform": {},
        "versions": [],
        "versions_by_platform": {},
    }
    out = _filter_payload(base, versions=["9.5.5"], error_type=None)
    assert len(out["issues"]) == 1
    assert out["issues"][0]["issue_id"] == "a"


def test_crash_fetch_filter_cache_key_includes_versions_and_type():
    key = _crash_fetch_filter_cache_key(
        {
            "product": "doviz",
            "days": 7,
            "platform": "all",
            "versions": ["8.12.4"],
            "error_type": "FATAL",
        }
    )
    assert key == "doviz:7:all:8.12.4:FATAL"
    assert _crash_fetch_filter_cache_key({"product": "doviz", "platform": "all", "days": 7}) is None
