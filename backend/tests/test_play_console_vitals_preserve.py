"""Boş vitals full/merge ingest mevcut Android Vitals'i ezmesin."""

from backend.services.play_console_store import (
    _preserve_existing_vitals_if_incoming_empty,
    _vitals_has_usable_data,
)


def _good_vitals() -> dict:
    return {
        "version": 3,
        "metrics_overview": {
            "rows": [
                {
                    "key": "crash",
                    "metric": "User-perceived crash rate",
                    "value_28d": "%0,03",
                }
            ]
        },
        "crashes": {
            "ANR": {
                "categories": [
                    {
                        "id": "general",
                        "issue_count": "2",
                        "issues": [{"issue_id": "abc", "title": "ANR in foo"}],
                    }
                ]
            }
        },
    }


def test_vitals_has_usable_data_detects_overview_and_issues():
    assert _vitals_has_usable_data(_good_vitals()) is True
    assert _vitals_has_usable_data({"error": "timeout"}) is False
    assert _vitals_has_usable_data({"metrics_overview": {"rows": []}, "crashes": {}}) is False


def test_preserve_existing_vitals_on_empty_incoming():
    existing = {"vitals": _good_vitals(), "vitals_overview_row_count": 3}
    incoming = {
        "vitals": {"version": 1, "error": "scrape failed", "metrics_overview": {"rows": []}},
        "explorer_facts": [{"metric": "x"}],
    }
    out = _preserve_existing_vitals_if_incoming_empty(incoming, existing)
    assert out is not None
    assert out["vitals"] == existing["vitals"]
    assert out["vitals_overview_row_count"] == 3
    assert out["explorer_facts"] == incoming["explorer_facts"]


def test_preserve_keeps_good_incoming():
    existing = {"vitals": _good_vitals()}
    incoming = {"vitals": _good_vitals()}
    out = _preserve_existing_vitals_if_incoming_empty(incoming, existing)
    assert out is incoming
