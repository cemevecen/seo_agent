"""Google Search Status — core/spam update normalize."""

from backend.services.google_search_status import _classify, _normalize_incident, updates_in_range


def test_classify_core_and_spam():
    assert _classify("May 2026 core update") == "core"
    assert _classify("September 2026 spam update") == "spam"
    assert _classify("Serving outage") is None


def test_normalize_ranking_only():
    row = _normalize_incident(
        {
            "id": "abc",
            "service_name": "Ranking",
            "external_desc": "May 2026 core update",
            "begin": "2026-05-21T10:00:00+00:00",
            "end": "2026-06-02T10:00:00+00:00",
            "uri": "incidents/abc",
        }
    )
    assert row is not None
    assert row["kind"] == "core"
    assert row["start"] == "2026-05-21"
    assert row["end"] == "2026-06-02"
    assert "status.search.google.com" in row["uri"]


def test_normalize_skips_non_ranking():
    assert (
        _normalize_incident(
            {
                "service_name": "Serving",
                "external_desc": "Serving issue",
                "begin": "2026-01-01T00:00:00+00:00",
            }
        )
        is None
    )


def test_updates_in_range_filters(monkeypatch):
    monkeypatch.setattr(
        "backend.services.google_search_status.fetch_ranking_updates",
        lambda force=False: [
            {
                "id": "1",
                "kind": "spam",
                "label": "September 2026 spam update",
                "start": "2026-09-24",
                "end": "2026-10-08",
                "uri": "https://status.search.google.com/incidents/1",
            },
            {
                "id": "2",
                "kind": "core",
                "label": "May 2026 core update",
                "start": "2026-05-21",
                "end": "2026-06-02",
                "uri": "https://status.search.google.com/incidents/2",
            },
        ],
    )
    out = updates_in_range(start="2026-09-01", end="2026-10-11", kinds=("core", "spam"))
    assert out["ok"] is True
    assert out["count"] == 1
    assert out["updates"][0]["kind"] == "spam"
