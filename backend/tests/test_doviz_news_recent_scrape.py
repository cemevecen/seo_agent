"""Son 7 gün scrape (min_day) + merge ingest."""

from __future__ import annotations

from backend.services.doviz_news_admin import _row_day, parse_news_admin_html


SAMPLE_HTML = """
<table>
<tr><td>910600</td><td>✅</td><td>Yeni haber</td><td>-</td><td>07.08.2026 12:00</td><td>Ekonomi</td></tr>
<tr><td>910500</td><td>✅</td><td>Eski haber</td><td>-</td><td>01.07.2026 10:00</td><td>Ekonomi</td></tr>
</table>
"""


def test_parse_and_row_day():
    rows = parse_news_admin_html(SAMPLE_HTML)
    assert len(rows) == 2
    assert _row_day(rows[0]) == "2026-08-07"
    assert _row_day(rows[1]) == "2026-07-01"


def test_ingest_merge_keeps_old_ids(monkeypatch):
    from backend.services import doviz_news_sheet as sheet

    existing = [
        {
            "id": "800001",
            "title": "Eski",
            "active": True,
            "source": "",
            "source_key": "",
            "is_own": True,
            "category": "Ekonomi",
            "date": "2026-01-01 10:00:00",
            "date_day": "2026-01-01",
            "hour": 10,
            "weekday": 3,
            "iso_week": "2026-W01",
        }
    ]
    monkeypatch.setattr(sheet, "_load_doviz_news_rows_from_db", lambda: list(existing))
    saved: dict = {}

    def _fake_cache(rows, **kwargs):
        saved["rows"] = list(rows)
        saved["kwargs"] = kwargs
        sheet._CACHE = {
            "fetched_at": "2026-08-07T12:00:00Z",
            "background_synced_at": "2026-08-07T12:00:00Z",
            "rows": list(rows),
            "source": kwargs.get("source"),
            "source_url": kwargs.get("source_url"),
        }

    monkeypatch.setattr(sheet, "set_doviz_news_rows_cache", _fake_cache)
    out = sheet.ingest_doviz_news_rows(
        [
            {
                "id": "910600",
                "title": "Yeni",
                "active": True,
                "source": "",
                "category": "Ekonomi",
                "date": "2026-08-07 12:00:00",
                "date_day": "2026-08-07",
            }
        ],
        merge=True,
        sync_mode="recent_7d",
    )
    assert out["ok"] is True
    ids = {str(r["id"]) for r in saved["rows"]}
    assert ids == {"800001", "910600"}
    assert saved["kwargs"]["sync_mode"] == "recent_7d"
    assert saved["kwargs"]["sync_ok"] is True
