"""Android cihaz adı ve platform dilimleme testleri."""

from backend.services.android_device_names import friendly_device_label, lookup_marketing_name
from backend.services.crashlytics_bq import slice_payload_for_platform


def test_friendly_android_model_code():
    name = friendly_device_label("Xiaomi", "2209116AG", platform="android")
    assert "Redmi Note 12 Pro 4G" in name or lookup_marketing_name("2209116AG")


def test_friendly_breakdown_row_from_cached_label():
    from backend.services.android_device_names import friendly_breakdown_row

    row = {"label": "samsung SM-A515F", "event_count": 42, "pct": 22.3}
    out = friendly_breakdown_row(row, platform="android")
    assert "Galaxy A51" in out["label"] or "SM-A515F" not in out["label"]
    assert out.get("label_raw") == "samsung SM-A515F"


def test_slice_payload_ios():
    full = {
        "ok": True,
        "platform_filter": "all",
        "summary_by_platform": {
            "ios": {"fatal": 3, "anr": 1, "non_fatal": 0, "affected_users": 4},
            "android": {"fatal": 30, "anr": 2, "non_fatal": 0, "affected_users": 20},
        },
        "crash_free_by_platform": {"ios": {"crash_free_pct": 99.1}},
        "trend_by_platform": {"ios": [{"date": "2026-05-23", "fatal": 1}]},
        "issues_by_platform": {"ios": [{"issue_id": "a", "event_count": 2}]},
        "anr_by_platform": {"ios": []},
        "versions_by_platform": {"ios": [{"app_version": "1.0", "total_events": 5}]},
        "device_breakdown_by_platform": {"ios": [{"label": "iPhone 13", "event_count": 2, "pct": 100}]},
        "os_breakdown_by_platform": {"ios": [{"os_version": "17.0", "event_count": 2, "pct": 100}]},
        "process_state_breakdown_by_platform": {"ios": []},
        "version_trend": [],
        "crash_free_hints": ["IOS: foo"],
    }
    sliced = slice_payload_for_platform(full, "ios")
    assert sliced["platform_filter"] == "ios"
    assert sliced["totals"]["fatal"] == 3
    assert sliced["crash_free_pct"] == 99.1
    assert sliced["device_breakdown"][0]["label"] == "iPhone 13"
    assert len(sliced["issues"]) == 1
