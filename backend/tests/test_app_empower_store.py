"""Empower xlsx import ve dosya adı kuralları."""

from __future__ import annotations

import io

import pytest
from openpyxl import Workbook

from backend.services.app_empower_store import (
    is_empower_filename,
    parse_filename_meta,
    parse_empower_xlsx,
    partition_mz_upload_files,
)


def test_parse_filename_meta():
    assert parse_filename_meta("dovizandroidempower1.xlsx") == ("android", 1)
    assert parse_filename_meta("doviziosempower3.xlsx") == ("ios", 3)
    assert parse_filename_meta("doviz Android Empower 1.xlsx") == ("android", 1)
    assert parse_filename_meta("doviz.android.empower.2.xlsx") == ("android", 2)
    assert parse_filename_meta("dovizandroidempove1.xlsx") == ("android", 1)


def test_parse_empower_xlsx_excel_serial_date():
    wb = Workbook()
    ws = wb.active
    ws.append(["Date", "Sessions"])
    ws.append([45323, 42])
    buf = io.BytesIO()
    wb.save(buf)
    rows = parse_empower_xlsx(buf.getvalue())
    assert len(rows) == 1
    assert rows[0]["sessions"] == 42
    assert rows[0]["report_date"].year >= 2020


def test_parse_empower_xlsx_minimal():
    wb = Workbook()
    ws = wb.active
    ws.append(
        [
            "Date",
            "Sessions",
            "DAU (7 Days)",
            "Crash Affected Users",
            "Average Session Duration",
            "Engagement Rate",
            "ARPDAU ($)",
            "App Version",
        ]
    )
    ws.append(["2026-06-01", 100, 5000, 2, 400.5, 0.55, 1.2, "9.0"])
    buf = io.BytesIO()
    wb.save(buf)
    rows = parse_empower_xlsx(buf.getvalue())
    assert len(rows) == 1
    assert rows[0]["sessions"] == 100
    assert rows[0]["dau_7d"] == 5000
    assert rows[0]["engagement_rate"] == pytest.approx(0.55)


def test_parse_filename_invalid():
    with pytest.raises(ValueError):
        parse_filename_meta("dovizweb1.xlsx")


def test_is_empower_filename():
    assert is_empower_filename("dovizandroidempower1.xlsx")
    assert not is_empower_filename("dovizweb1.xlsx")


def test_partition_mz_upload_files():
    ad, emp = partition_mz_upload_files(
        [
            (b"a", "dovizweb1.xlsx"),
            (b"b", "doviziosempower2.xlsx"),
        ]
    )
    assert len(ad) == 1 and ad[0][1] == "dovizweb1.xlsx"
    assert len(emp) == 1 and emp[0][1] == "doviziosempower2.xlsx"
