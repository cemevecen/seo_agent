"""Play / ASC scrape skip — Metrik (Empower) örtüşmesi."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from backend.services.empower_intel_config import (
    PLAY_CONSOLE_SKIP_METRIC_KEYS,
    STORE_EMPOWER_OVERLAP,
    play_console_skip_metric_keys,
)

ROOT = Path(__file__).resolve().parents[2]


def test_overlap_rows_cover_skip_keys():
    play_keys = {row["play_key"] for row in STORE_EMPOWER_OVERLAP if row.get("play_key")}
    assert play_keys == set(PLAY_CONSOLE_SKIP_METRIC_KEYS)
    assert play_console_skip_metric_keys() == PLAY_CONSOLE_SKIP_METRIC_KEYS


def test_play_statistics_views_skip_dau_family():
    spec = importlib.util.spec_from_file_location(
        "play_console_scrape", ROOT / "scripts" / "play_console_scrape.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    skip = play_console_skip_metric_keys()
    ids = {
        str(v.get("id"))
        for v in mod.STATISTICS_VIEWS
        if str(v.get("metric_key") or "") in skip
    }
    assert ids == {"dau", "dau_mau", "active_users"}
    kept = {str(v.get("metric_key")) for v in mod.STATISTICS_VIEWS if str(v.get("metric_key") or "") not in skip}
    assert "revenue" in kept
    assert "crashes" in kept
    assert "dau" not in kept


def test_play_known_drops_dau_titles():
    spec = importlib.util.spec_from_file_location(
        "play_console_scrape_known", ROOT / "scripts" / "play_console_scrape.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    known = mod._known(mod._KNOWN_DASHBOARD, mod._KNOWN_GROW, mod._KNOWN_STATISTICS)
    folded = {t.casefold() for t in known}
    assert "günlük etkin kullanıcı sayısı" not in folded
    assert "dau/mau" not in folded
    assert "etkin cihazlar" in folded


def test_asc_batches_drop_sessions():
    spec = importlib.util.spec_from_file_location(
        "asc_console_scrape", ROOT / "scripts" / "asc_console_scrape.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mmap = mod._asc_measure_map()
    assert "sessions" not in mmap
    assert "activeDevices" in mmap
    assert "crashes" in mmap
    batches = mod._asc_measure_batches()
    assert all("sessions" not in b for b in batches)
    assert "sessions" not in mod._asc_required_metrics()
    dropped = mod._asc_drop_overlap_facts(
        [
            {"metric": "sessions", "view_id": "sessions", "date": "2026-08-01"},
            {"metric": "installs", "view_id": "installs", "date": "2026-08-01"},
        ]
    )
    assert [f["metric"] for f in dropped] == ["installs"]
