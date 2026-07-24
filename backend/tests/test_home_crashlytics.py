"""Ana sayfa Crashlytics kartı."""

from backend.main import _home_cf_fmt, _home_crashlytics_card


def test_home_cf_fmt():
    assert _home_cf_fmt(99.9123) == "99.91%"
    assert _home_cf_fmt(99.9951) == "99.9951%"
    assert _home_cf_fmt(None) == "—"


def test_home_crashlytics_card_from_cache(monkeypatch):
    sample = {
        "ok": True,
        "product": "doviz",
        "days": 7,
        "totals": {"fatal": 12, "anr": 3, "non_fatal": 1},
        "crash_free_sessions_pct": 99.91,
        "summary_by_platform": {
            "ios": {"fatal": 7, "anr": 1},
            "android": {"fatal": 5, "anr": 2},
        },
        "crash_free_by_platform": {
            "ios": {"crash_free_sessions_pct": 99.95},
            "android": {"crash_free_sessions_pct": 99.88},
        },
        "issues_by_platform": {
            "ios": [{"title": "SIGABRT in main", "event_count": 7}],
            "android": [{"title": "NullPointer in Feed", "event_count": 5}],
        },
        "issues": [{"title": "SIGABRT in main", "event_count": 7}],
        "filter_versions_by_platform": {
            "ios": ["9.4.1", "9.4.0"],
            "android": ["9.5.7", "9.5.6"],
        },
        "versions_by_platform": {
            "ios": [{"app_version": "9.4.1", "fatal_count": 4, "anr_count": 0}],
            "android": [{"app_version": "9.5.7", "fatal_count": 2, "anr_count": 8}],
        },
        "device_breakdown_by_platform": {
            "ios": [
                {"label": "iPhone 15 Pro", "event_count": 48},
                {"label": "iPhone 13", "event_count": 31},
                {"label": "iPhone 14", "event_count": 19},
            ],
            "android": [
                {"label": "Samsung Galaxy A54", "event_count": 40},
                {"label": "Xiaomi Redmi Note", "event_count": 22},
                {"label": "Pixel 7", "event_count": 11},
            ],
        },
        "os_breakdown_by_platform": {
            "ios": [
                {"os_version": "18.5", "event_count": 55},
                {"os_version": "17.6", "event_count": 20},
            ],
        },
    }

    monkeypatch.setattr(
        "backend.services.crashlytics_bq.peek_cached_payload",
        lambda *args, **kwargs: sample,
    )

    card = _home_crashlytics_card("doviz")
    assert card["ok"] is True
    assert card["fatal_fmt"] == "12"
    assert card["anr_fmt"] == "3"
    assert card["crash_free_fmt"] == "99.91%"
    assert len(card["platforms"]) == 2
    assert card["ios"]["latest_version"] == "9.4.1"
    assert card["ios"]["fatal_fmt"] == "4"
    assert len(card["ios"]["top_devices"]) == 3
    assert card["ios"]["top_devices"][0]["label"] == "iPhone 15 Pro"
    assert card["ios"]["top_issues"][0]["label"] == "SIGABRT in main"
    assert card["ios"]["top_os"][0]["label"] == "iOS 18.5"
    assert card["android"]["latest_version"] == "9.5.7"
    assert card["android"]["anr_fmt"] == "8"
    assert len(card["android"]["top_devices"]) == 3
    assert card["android"]["top_devices"][0]["label"] == "Samsung Galaxy A54"
    assert card["platforms"][0]["top_issue_title"] == "SIGABRT in main"


def test_home_crashlytics_card_warming(monkeypatch):
    monkeypatch.setattr("backend.services.crashlytics_bq.peek_cached_payload", lambda *a, **k: None)
    monkeypatch.setattr("backend.services.crashlytics_bq.is_cache_warm", lambda *a, **k: False)
    called = {"prewarm": False}

    def _prewarm(pid):
        called["prewarm"] = True

    monkeypatch.setattr("backend.services.crashlytics_bq.prewarm_cache", _prewarm)

    card = _home_crashlytics_card("doviz")
    assert card["warming"] is True
    assert called["prewarm"] is True
