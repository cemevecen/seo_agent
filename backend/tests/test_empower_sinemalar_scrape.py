"""Sinemalar Empower scrape / bridge wiring (no live browser)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_scrape():
    path = ROOT / "scripts" / "empower_intelligence_scrape.py"
    spec = importlib.util.spec_from_file_location("empower_intelligence_scrape", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_normalize_project_and_report_base():
    mod = _load_scrape()
    assert mod._normalize_project("sinemalar") == "sinemalar"
    assert mod._normalize_project("DOVIZ") == "doviz"
    assert mod._normalize_project("other") == "doviz"
    assert "sinemalar-report" in mod._report_base("sinemalar")
    assert "doviz-report" in mod._report_base("doviz")


def test_report_url_uses_project():
    mod = _load_scrape()
    from datetime import date

    url = mod._report_url(
        platform="web",
        start=date(2025, 1, 1),
        end=date(2025, 1, 2),
        project="sinemalar",
    )
    assert "sinemalar-report" in url
    assert "platform=web" in url


def test_bridge_sinemalar_slots_are_five_minutes_after_doviz():
    path = ROOT / "scripts" / "doviz_admin_notification_bridge.py"
    text = path.read_text(encoding="utf-8")
    assert "EMPOWER_INTEL_SINEMALAR_SLOTS" in text
    assert "run_empower_intel_sinemalar_bridge_once" in text
    assert "--project" in text and "sinemalar" in text
    assert "/sync-empower-intel-sinemalar" in text


def test_datas_tab_files_exist():
    assert (ROOT / "templates/partials/sinemalar_datas_content.html").is_file()
    assert (ROOT / "static/js/sinemalar_datas.js").is_file()
    js = (ROOT / "static/js/sinemalar_datas.js").read_text(encoding="utf-8")
    assert 'project=sinemalar' in js or 'PROJECT = "sinemalar"' in js
    assert "/api/empower-intel/series" in js
