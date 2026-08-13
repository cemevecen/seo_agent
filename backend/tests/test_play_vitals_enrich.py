"""Play Vitals Firebase yedeklemesi."""

from backend.services.play_vitals_enrich import (
    count_vitals_issues,
    enrich_vitals_with_firebase,
)


def _empty_vitals() -> dict:
    return {
        "version": 3,
        "metrics_overview": {
            "rows": [{"key": "crash", "metric": "User-perceived crash rate", "value_28d": "%0,03"}]
        },
        "crashes": {
            "ANR": {
                "categories": [
                    {
                        "id": "general",
                        "label": "Genel",
                        "issue_count": "0",
                        "issues": [],
                        "issue_row_count": 0,
                    }
                ]
            }
        },
        "versions": [{"code": "290", "name": "9.5.10"}],
        "version_name_map": {"290": "9.5.10"},
        "by_version": {
            "290": {
                "crashes": {
                    "ANR": {
                        "categories": [
                            {
                                "id": "general",
                                "issue_count": "0",
                                "issues": [],
                                "issue_row_count": 0,
                            }
                        ]
                    }
                }
            }
        },
    }


def _play_shell_with_fake_count() -> dict:
    vitals = _empty_vitals()
    vitals["crashes"]["ANR"]["categories"][0]["issue_count"] = "12"
    vitals["by_version"]["290"]["crashes"]["ANR"]["categories"][0]["issue_count"] = "12"
    return vitals


def _firebase_android() -> dict:
    return {
        "latest_version": "9.5.10 (290)",
        "by_version": [{"version": "9.5.10", "build": "290", "label": "9.5.10 (290)"}],
        "pages": {
            "crashlytics": "https://console.firebase.google.com/project/doviz-android/crashlytics/app/android:com.Doviz/issues"
        },
        "issues": [
            {
                "id": "crash-1",
                "title": "NullPointerException in MainActivity",
                "event_count": 42,
                "affected_users": 12,
                "version": "9.5.10",
            }
        ],
        "anr_issues": [
            {
                "id": "anr-1",
                "title": "ANR in com.doviz.service",
                "event_count": 96,
                "affected_users": 28,
                "version": "9.5.10",
            }
        ],
    }


def test_enrich_fills_empty_vitals_from_firebase():
    out = enrich_vitals_with_firebase(_empty_vitals(), _firebase_android())
    assert out["issues_fallback"] == "firebase_console_scrape"
    assert count_vitals_issues(out) >= 2
    anr = out["crashes"]["ANR"]["categories"][0]
    assert anr["issue_row_count"] == 1
    assert anr["issues"][0]["issue_id"] == "anr-1"
    assert anr["issues"][0]["source"] == "firebase_console_scrape"
    assert anr["issues"][0]["error_type"] == "ANR"
    assert anr["issues"][0]["affected_versions"] == "9.5.10 (290)"
    byv = out["by_version"]["290"]["crashes"]["ANR"]["categories"][0]
    assert byv["issue_row_count"] == 1


def test_enrich_ignores_play_issue_count_without_rows():
    out = enrich_vitals_with_firebase(_play_shell_with_fake_count(), _firebase_android())
    assert out["issues_fallback"] == "firebase_console_scrape"
    assert count_vitals_issues(out) >= 2
    assert out["crashes"]["ANR"]["categories"][0]["issue_count"] == "1"


def test_enrich_skips_when_play_has_issues():
    vitals = _empty_vitals()
    vitals["crashes"]["ANR"]["categories"][0]["issues"] = [
        {"issue_id": "play-1", "title": "Play issue", "events": "1", "users": "1"}
    ]
    vitals["crashes"]["ANR"]["categories"][0]["issue_row_count"] = 1
    vitals["crashes"]["ANR"]["categories"][0]["issue_count"] = "1"
    out = enrich_vitals_with_firebase(vitals, _firebase_android())
    assert "issues_fallback" not in out
    assert out["crashes"]["ANR"]["categories"][0]["issues"][0]["issue_id"] == "play-1"
