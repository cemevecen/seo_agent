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


def test_read_property_ids_filters_by_project_report_path():
    """doviz prefs, sinemalar scrape'e sızmamalı; sinemalar defaults ayrı."""
    mod = _load_scrape()

    class FakeDriver:
        def execute_script(self, script, *args):
            needle = args[0] if args else ""
            all_prefs = {
                "web": "376928120",  # doviz — yalnızca doviz needle ile
                "mweb": "329808608",
            }
            if "/sinemalar-report/" in str(needle):
                return {}  # browser LS'de yok → defaults kullanılır
            if "/doviz-report/" in str(needle):
                return all_prefs
            return all_prefs

    # Sinemalar: doviz ID sızmaz; built-in defaults gelir
    got = mod._read_property_ids(FakeDriver(), "sinemalar")
    assert got.get("web") == "375681147", got
    assert got.get("mweb") == "375681811", got
    assert got.get("web") != "376928120"
    assert got.get("mweb") != "329808608"

    # Doviz: doviz prefs gelir
    got_d = mod._read_property_ids(FakeDriver(), "doviz")
    assert got_d.get("web") == "376928120"
    assert got_d.get("mweb") == "329808608"


def test_virgul_id_mapped_per_project_platform():
    """Reklam metrikleri için virgul sid = VIRGUL_AD_SOURCES (web→desktop)."""
    mod = _load_scrape()
    from backend.services.virgul_ad_config import VIRGUL_AD_SOURCES

    by = {s.stream_key: s.sid for s in VIRGUL_AD_SOURCES}
    assert mod._virgul_id_for_platform("doviz", "web") == by["doviz:desktop"]
    assert mod._virgul_id_for_platform("doviz", "mweb") == by["doviz:mweb"]
    assert mod._virgul_id_for_platform("doviz", "ios") == by["doviz:ios"]
    assert mod._virgul_id_for_platform("doviz", "android") == by["doviz:android"]
    assert mod._virgul_id_for_platform("sinemalar", "web") == by["sinemalar:desktop"]
    assert mod._virgul_id_for_platform("sinemalar", "mweb") == by["sinemalar:mweb"]
    # Eski tek-id (android) Sinemalar web'e sızmamalı
    assert mod._virgul_id_for_platform("sinemalar", "web") != by["doviz:android"]
    assert mod._virgul_id_for_platform("doviz", "web") != by["doviz:android"]