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
