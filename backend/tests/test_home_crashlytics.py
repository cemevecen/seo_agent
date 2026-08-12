"""Ana sayfa Crashlytics kartı."""

from backend.main import _home_cf_fmt, _home_crashlytics_card, _home_crash_latest_version


def test_home_cf_fmt():
    assert _home_cf_fmt(99.9123) == "99.91%"
    assert _home_cf_fmt(99.9951) == "99.9951%"
    assert _home_cf_fmt(None) == "—"


def test_home_crash_latest_prefers_store():
    payload = {
        "filter_versions_by_platform": {"ios": ["9.4.1", "9.4.0"]},
    }
    # Mağaza daha yeni
    assert _home_crash_latest_version(payload, "ios", store_version="9.5.0") == "9.5.0"
    # Crashlytics daha yeni (stale mağaza)
    assert _home_crash_latest_version(payload, "ios", store_version="9.0.0") == "9.4.1"
    assert _home_crash_latest_version(payload, "ios", store_version=None) == "9.4.1"


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
        "versions_7d_by_platform": {
            "ios": [{"app_version": "9.4.1", "fatal_count": 4, "anr_count": 0}],
            "android": [{"app_version": "9.5.7", "fatal_count": 2, "anr_count": 8}],
        },
        "versions_by_platform": {
            "ios": [{"app_version": "9.4.1", "fatal_count": 40, "anr_count": 0}],
            "android": [{"app_version": "9.5.7", "fatal_count": 20, "anr_count": 80}],
        },
        "latest_version_stats_by_platform": {
            "ios": {
                "version": "9.4.1",
                "fatal": 4,
                "anr": 0,
                "devices": [
                    {"label": "iPhone 15 Pro", "event_count": 48},
                    {"label": "iPhone 13", "event_count": 31},
                    {"label": "iPhone 14", "event_count": 19},
                ],
                "issues": [{"title": "SIGABRT in main", "event_count": 7}],
            },
            "android": {
                "version": "9.5.7",
                "fatal": 2,
                "anr": 8,
                "devices": [
                    {"label": "Samsung Galaxy A54", "event_count": 40},
                    {"label": "Xiaomi Redmi Note", "event_count": 22},
                    {"label": "Pixel 7", "event_count": 11},
                ],
                "issues": [{"title": "NullPointer in Feed", "event_count": 5}],
            },
        },
        "device_breakdown_by_platform": {
            "ios": [
                {"label": "OLD DEVICE", "event_count": 999},
            ],
            "android": [
                {"label": "OLD ANDROID", "event_count": 999},
            ],
        },
        "os_breakdown_by_platform": {
            "ios": [
                {"os_version": "18.5", "event_count": 55},
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
    assert card["ios"]["fatal_fmt"] == "4"  # 7g, not 30d chip 40
    assert len(card["ios"]["top_devices"]) == 3
    assert card["ios"]["top_devices"][0]["label"] == "iPhone 15 Pro"
    assert card["ios"]["top_issues"][0]["label"] == "SIGABRT in main"
    assert card["ios"]["top_os"] == []  # version-scoped: OS list hidden
    assert card["android"]["latest_version"] == "9.5.7"
    assert card["android"]["anr_fmt"] == "8"
    assert len(card["android"]["top_devices"]) == 3
    assert card["android"]["top_devices"][0]["label"] == "Samsung Galaxy A54"
    assert card["platforms"][0]["top_issue_title"] == "SIGABRT in main"


def test_home_crashlytics_card_uses_store_version(monkeypatch):
    sample = {
        "ok": True,
        "product": "doviz",
        "days": 7,
        "totals": {"fatal": 12, "anr": 3, "non_fatal": 1},
        "summary_by_platform": {"ios": {"fatal": 7}, "android": {"fatal": 5, "anr": 2}},
        "crash_free_by_platform": {
            "ios": {"crash_free_sessions_pct": 99.95},
            "android": {"crash_free_sessions_pct": 99.88},
        },
        "filter_versions_by_platform": {
            "ios": ["9.4.1"],
            "android": ["9.5.7"],
        },
        "versions_7d_by_platform": {
            "ios": [{"app_version": "9.0.0", "fatal_count": 2, "anr_count": 0}],
            "android": [{"app_version": "9.5.8", "fatal_count": 1, "anr_count": 1}],
        },
        "versions_by_platform": {},
        "latest_version_stats_by_platform": {
            "ios": {"version": "9.4.1", "fatal": 4, "anr": 0, "devices": [], "issues": []},
            "android": {"version": "9.5.7", "fatal": 2, "anr": 8, "devices": [], "issues": []},
        },
        "device_breakdown_by_platform": {},
        "issues_by_platform": {},
        "os_breakdown_by_platform": {},
    }
    monkeypatch.setattr(
        "backend.services.crashlytics_bq.peek_cached_payload",
        lambda *a, **k: sample,
    )
    card = _home_crashlytics_card(
        "doviz",
        store_by_key={
            "ios": {"version": "9.0.0"},
            "android": {"version": "9.5.8"},
        },
    )
    # Crashlytics filter max (9.4.1) > store 9.0.0 → 9.4.1; Android store 9.5.8 > 9.5.7
    assert card["ios"]["latest_version"] == "9.4.1"
    assert card["ios"]["fatal_fmt"] == "4"
    assert card["android"]["latest_version"] == "9.5.8"
    assert card["android"]["anr_fmt"] == "1"
    # Android store sürümü scoped cache (9.5.7) ile farklı → cihaz listesi boş
    assert card["android"]["top_devices"] == []
    assert card["ios"]["top_devices"] == []  # scoped issues/devices empty lists in sample



def test_home_crashlytics_card_warming_when_cold_bq_disabled(monkeypatch):
    """Legacy BQ kart — soğuk cache'te warming; BigQuery kapalıyken prewarm no-op."""
    prewarm_calls = {"n": 0}

    monkeypatch.setattr("backend.services.crashlytics_bq.peek_cached_payload", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.services.crashlytics_bq.prewarm_cache",
        lambda *a, **k: prewarm_calls.__setitem__("n", prewarm_calls["n"] + 1),
    )

    card = _home_crashlytics_card("doviz")
    assert card.get("warming") is True
    assert prewarm_calls["n"] == 1
    assert "hazırlanıyor" in str(card.get("message") or "").lower()
