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


def _firebase_android() -> dict:
    return {
        "issues": [
            {
                "id": "crash-1",
                "title": "NullPointerException in MainActivity",
                "event_count": 42,
                "affected_users": 12,
                "version": "9.5.10",
                "url": "https://console.firebase.google.com/issue/crash-1",
            }
        ],
        "anr_issues": [
            {
                "id": "anr-1",
                "title": "ANR in com.doviz.service",
                "event_count": 96,
                "affected_users": 28,
                "version": "9.5.10",
                "url": "https://console.firebase.google.com/issue/anr-1",
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
    byv = out["by_version"]["290"]["crashes"]["ANR"]["categories"][0]
    assert byv["issue_row_count"] == 1


def test_enrich_skips_when_play_has_issues():
    vitals = _empty_vitals()
    vitals["crashes"]["ANR"]["categories"][0]["issues"] = [
        {"issue_id": "play-1", "title": "Play issue"}
    ]
    out = enrich_vitals_with_firebase(vitals, _firebase_android())
    assert "issues_fallback" not in out
    assert out["crashes"]["ANR"]["categories"][0]["issues"][0]["issue_id"] == "play-1"
