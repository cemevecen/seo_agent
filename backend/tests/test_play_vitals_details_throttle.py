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
    # Mühürlü lean modda fonksiyon zaten erken çıkıyor; 20 saatlik throttle
    # mantığını sınamak için mühür durumu açıkça sabitlenmeli — aksi halde test
    # makinenin mühür durumuna göre geçip kalıyordu.
    monkeypatch.setattr(mod, "_play_sealed_lean", lambda: False)

    assert mod._vitals_issue_details_due() is True          # işaret yok → sırası
    marker.write_text('{"ts": %s}' % time.time(), encoding="utf-8")
    assert mod._vitals_issue_details_due() is False         # az önce koştu
    monkeypatch.setenv("PLAY_CONSOLE_VITALS_DETAILS", "1")
    assert mod._vitals_issue_details_due() is True          # zorla
    monkeypatch.setenv("PLAY_CONSOLE_VITALS_DETAILS", "0")
    assert mod._vitals_issue_details_due() is False         # hiç


def test_vitals_details_skipped_in_sealed_lean(monkeypatch, tmp_path):
    """Mühürlü lean'de detay taraması atlanır (Statistics ANR/Crash yeter)."""
    monkeypatch.delenv("PLAY_CONSOLE_VITALS_DETAILS", raising=False)
    marker = tmp_path / "play-vitals-details-last.json"
    monkeypatch.setattr(mod, "_vitals_details_marker_path", lambda: marker)
    monkeypatch.setattr(mod, "_play_sealed_lean", lambda: True)
    assert mod._vitals_issue_details_due() is False
    # Açık bayrak lean'i de ezebilmeli
    monkeypatch.setenv("PLAY_CONSOLE_VITALS_DETAILS", "1")
    assert mod._vitals_issue_details_due() is True


def test_vitals_details_due_again_after_the_throttle_window(monkeypatch, tmp_path):
    marker = tmp_path / "play-vitals-details-last.json"
    monkeypatch.delenv("PLAY_CONSOLE_VITALS_DETAILS", raising=False)
    monkeypatch.setattr(mod, "_vitals_details_marker_path", lambda: marker)
    monkeypatch.setattr(mod, "_play_sealed_lean", lambda: False)
    marker.write_text('{"ts": %s}' % (time.time() - 21 * 3600), encoding="utf-8")
    assert mod._vitals_issue_details_due() is True
