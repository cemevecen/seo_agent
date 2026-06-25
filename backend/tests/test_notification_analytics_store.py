"""Notification analytics paylaşımlı store."""

import io

from openpyxl import Workbook

from backend.services.notification_analytics_store import (
    _iso_utc_z,
    _merge_rows,
    filter_rows_by_date,
    parse_csv_text,
    parse_xlsx_bytes,
)
from datetime import datetime


def test_iso_utc_z_suffix():
    assert _iso_utc_z(datetime(2026, 6, 25, 6, 55, 36)) == "2026-06-25T06:55:36Z"


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


def test_filter_rows_by_date():
    rows = [
        {"id": "1", "text": "A", "date": "2026-01-15T00:00:00"},
        {"id": "2", "text": "B", "date": "2026-03-01T00:00:00"},
    ]
    assert len(filter_rows_by_date(rows, start="2026-02-01", end="2026-12-31")) == 1


def test_merge_rows_same_id_different_date():
    """Aynı bildirim id, farklı tarih → iki ayrı kayıt (upload’ta ikisi de kalmalı)."""
    a = [{"id": "99", "text": "Headline", "date": "2026-01-01T00:00:00"}]
    b = [{"id": "99", "text": "Headline", "date": "2026-02-01T00:00:00"}]
    merged = _merge_rows(a, b)
    assert len(merged) == 2


def test_parse_csv_turkish_thousands_click():
    csv = (
        "id,text,date,android app click,android app ctr\n"
        "1,Hello,09.06.2026,84.295,3.877\n"
    )
    rows = parse_csv_text(csv)
    assert len(rows) == 1
    assert rows[0]["platforms"]["android"]["click"] == 84295.0
    assert rows[0]["platforms"]["android"]["ctr"] == 3.877


def test_parse_csv_reference_row_format():
    csv = (
        "id,text,date,android app impression,android app ctr,ios app impression,"
        "ios app ctr,android app click,ios app click\n"
        "9133656,Akaryakıt fiyatlarına çifte indirim geliyor,09.04.2026 09:38,"
        "48.521,12.434,8.633,14.822,423,336\n"
    )
    rows = parse_csv_text(csv)
    assert len(rows) == 1
    p = rows[0]["platforms"]
    assert p["android"]["impression"] == 48521.0
    assert p["android"]["click"] == 423.0
    assert p["ios"]["click"] == 336.0
    assert "impression" not in p["ios"]


def test_ios_impression_column_ignored():
    csv = (
        "id,text,date,ios app impression,ios app click\n"
        "1,Test,01.03.2026,99.999,42\n"
    )
    rows = parse_csv_text(csv)
    assert rows[0]["platforms"]["ios"]["click"] == 42.0
    assert "impression" not in rows[0]["platforms"]["ios"]


def test_parse_xlsx_same_as_csv_reference_row():
    wb = Workbook()
    ws = wb.active
    ws.append(
        [
            "id",
            "text",
            "date",
            "android app impression",
            "android app ctr",
            "ios app impression",
            "ios app ctr",
            "android app click",
            "ios app click",
        ]
    )
    ws.append(
        [
            9133656,
            "Akaryakıt fiyatlarına çifte indirim geliyor",
            "09.04.2026 09:38",
            48521,
            12.434,
            8633,
            14.822,
            423,
            336,
        ]
    )
    buf = io.BytesIO()
    wb.save(buf)
    rows = parse_xlsx_bytes(buf.getvalue())
    assert len(rows) == 1
    p = rows[0]["platforms"]
    assert p["android"]["impression"] == 48521.0
    assert p["android"]["click"] == 423.0
    assert p["ios"]["click"] == 336.0
    assert "impression" not in p["ios"]


def test_rows_date_bounds():
    from backend.services.notification_analytics_store import _rows_date_bounds

    rows = [
        {"date": "2026-06-05T10:00:00"},
        {"date": "2026-06-09T12:24:00"},
    ]
    assert _rows_date_bounds(rows) == ("2026-06-05", "2026-06-09")
