"""Vitals issue tablo satırı / URL yardımcıları."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "play_console_scrape", ROOT / "scripts" / "play_console_scrape.py"
)
_pcs = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_pcs)


def test_vitals_issue_id_from_url_issues_path():
    url = (
        "https://play.google.com/console/u/0/developers/x/app/y/"
        "vitals/crashes/issues/abc123def4567890/details?days=28"
    )
    assert _pcs._vitals_issue_id_from_url(url) == "abc123def4567890"


def test_parse_vitals_row_text_extracts_columns():
    text = (
        "SourceFile - com.google.ads.interactivemedia\n"
        "java.lang.IllegalStateException\n"
        "SDK ile ilgili olabilir\n"
        "Kilitlenme\n"
        "288 (9.5.8)\n"
        "30\n"
        "197\n"
        "%66,3\n"
        "6 gün önce"
    )
    row = _pcs._parse_vitals_row_text(
        text,
        issue_id="abc123def4567890",
        detail_url="https://example/details",
    )
    assert row["issue_id"] == "abc123def4567890"
    assert row["issue_type"] == "Kilitlenme"
    assert row["affected_versions"] == "288 (9.5.8)"
    assert row["users"] == "30"
    assert row["events"] == "197"
    assert "IllegalStateException" in row["subtitle"] or "IllegalStateException" in row["title"]
