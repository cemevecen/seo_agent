"""Notification analytics paylaşımlı store."""

from backend.services.notification_analytics_store import _merge_rows, parse_csv_text


def test_parse_csv_minimal():
    csv = (
        "id,text,date,android app click,android app impression\n"
        "1,Hello world,01.03.2026,10,100\n"
    )
    rows = parse_csv_text(csv)
    assert len(rows) == 1
    assert rows[0]["text"] == "Hello world"
    assert rows[0]["platforms"]["android"]["click"] == 10.0


def test_merge_rows_dedupes():
    a = [{"id": "1", "text": "A", "date": "2026-01-01T00:00:00"}]
    b = [{"id": "1", "text": "A", "date": "2026-01-01T00:00:00"}]
    merged = _merge_rows(a, b)
    assert len(merged) == 1


def test_merge_rows_same_id_different_date():
    """Aynı bildirim id, farklı tarih → iki ayrı kayıt (upload’ta ikisi de kalmalı)."""
    a = [{"id": "99", "text": "Headline", "date": "2026-01-01T00:00:00"}]
    b = [{"id": "99", "text": "Headline", "date": "2026-02-01T00:00:00"}]
    merged = _merge_rows(a, b)
    assert len(merged) == 2
