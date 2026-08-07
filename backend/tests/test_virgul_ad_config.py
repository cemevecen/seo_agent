"""Virgül ad warehouse isolation."""

from __future__ import annotations

from datetime import date

from backend.services.ad_analytics_store import _row_fingerprint
from backend.services.virgul_ad_config import (
    VIRGUL_AD_SOURCES,
    is_virgul_source_file,
    source_by_sid,
)


def test_six_virgul_sids_configured():
    assert len(VIRGUL_AD_SOURCES) == 6
    assert source_by_sid("5062c6cc87354585c0e19ac2").stream_key == "sinemalar:mweb"


def test_virgul_source_file_prefix():
    assert is_virgul_source_file("virgul_5062c6cc.xlsx")
    assert not is_virgul_source_file("doviz_mweb_google_sheet.csv")


def test_virgul_fingerprint_isolated_from_sheets():
    kwargs = dict(
        report_date=date(2026, 8, 1),
        ad_unit="m_sinemalar_x",
        income_type="Open Auction",
        project="sinemalar",
        branch="mweb",
    )
    a = _row_fingerprint(**kwargs)
    b = _row_fingerprint(**kwargs, namespace="virgul")
    assert a != b
