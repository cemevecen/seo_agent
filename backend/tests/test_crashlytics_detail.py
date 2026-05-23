"""Crashlytics detail yardımcı birim testleri."""

from backend.services.crashlytics_detail import enrich_issue_row, merge_breakdown_rows


def test_enrich_issue_row_repetitive_badge():
    row = enrich_issue_row({
        "event_count": 30,
        "affected_users": 5,
        "first_seen": "2026-05-20T10:00:00+00:00",
        "last_seen": "2026-05-23T10:00:00+00:00",
    }, days=7)
    assert row["events_per_user"] == 6.0
    assert "repetitive" in row["badges"]


def test_merge_breakdown_rows_pct():
    merged = merge_breakdown_rows([
        [{"label": "Samsung A", "event_count": 80}, {"label": "Xiaomi", "event_count": 20}],
    ], "label")
    assert merged[0]["pct"] == 80.0
    assert merged[1]["pct"] == 20.0
