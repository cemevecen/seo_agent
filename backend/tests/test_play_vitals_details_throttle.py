"""Vitals issue-detail throttle defaults."""

from __future__ import annotations

import importlib.util
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "play_console_scrape", ROOT / "scripts" / "play_console_scrape.py"
)
assert _SPEC and _SPEC.loader
mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mod)


def test_vitals_defaults_sparse_drilldown(monkeypatch, tmp_path):
    monkeypatch.delenv("PLAY_CONSOLE_VITALS_ROW_NAV", raising=False)
    monkeypatch.delenv("PLAY_CONSOLE_VITALS_DETAIL_LIMIT", raising=False)
    monkeypatch.delenv("PLAY_CONSOLE_VITALS_DETAILS", raising=False)
    assert mod._vitals_row_nav_enabled() is False
    assert mod._vitals_detail_limit() == 8

    marker = tmp_path / "play-vitals-details-last.json"
    monkeypatch.setattr(mod, "_vitals_details_marker_path", lambda: marker)
    assert mod._vitals_issue_details_due() is True
    marker.write_text('{"ts": %s}' % time.time(), encoding="utf-8")
    assert mod._vitals_issue_details_due() is False
    monkeypatch.setenv("PLAY_CONSOLE_VITALS_DETAILS", "1")
    assert mod._vitals_issue_details_due() is True
    monkeypatch.setenv("PLAY_CONSOLE_VITALS_DETAILS", "0")
    assert mod._vitals_issue_details_due() is False
