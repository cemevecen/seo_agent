"""Empower xlsx import ve dosya adı kuralları."""

from __future__ import annotations

import io

import pytest
from openpyxl import Workbook

from backend.services.app_empower_store import parse_filename_meta, parse_empower_xlsx


def test_parse_filename_meta():
    assert parse_filename_meta("dovizandroidempower1.xlsx") == ("android", 1)
    assert parse_filename_meta("doviziosempower3.xlsx") == ("ios", 3)


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
