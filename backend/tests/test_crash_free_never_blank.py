"""Crash-free hücreleri boş kalmasın — ana sayfa, /ios ve /android aynı kaynağı okur.

İki ayrı kusur test ediliyor:
1. Kısmi tarama, önceden gelmiş crash-free bloklarını boşla ezmemeli (ingest).
2. Pencere hiç yoksa okuma katmanı yedeğe düşmeli ve değerin hangi dönemden
   taşındığını belirtmeli (okuma).
"""

from __future__ import annotations

from backend.services.firebase_console_store import (
    _block_has_crash_free,
    _merge_platform_block,
)
from backend.services.stability_free import _fb_kpi_with_fallback

GOOD_24H = {"crash_free_pct": 99.81, "crash_free_fmt": "99,81%", "series": []}
GOOD_7D = {"crash_free_pct": 99.62, "crash_free_fmt": "99,62%", "series": []}


def test_partial_scrape_does_not_wipe_crash_free_windows():
    old = {"latest_version": "9.5.10", "latest_24h": GOOD_24H, "latest_7d": GOOD_7D}
    # Yeni tarama 24s penceresini getiremedi (RPC boş döndü)
    new = {"latest_version": "9.5.10", "latest_24h": {}, "latest_7d": GOOD_7D}
    merged = _merge_platform_block(old, new)
    assert merged["latest_24h"] == GOOD_24H
    assert merged["latest_7d"] == GOOD_7D


def test_fresh_window_still_overwrites_the_old_one():
    old = {"latest_24h": GOOD_24H}
    fresh = {"crash_free_pct": 99.95, "crash_free_fmt": "99,95%"}
    merged = _merge_platform_block(old, {"latest_24h": fresh})
    assert merged["latest_24h"] == fresh


def test_top_level_crash_free_is_not_cleared_by_a_partial_scrape():
    old = {"crash_free_pct": 99.4, "crash_free_fmt": "99,40%"}
    merged = _merge_platform_block(old, {"crash_free_pct": None, "crash_free_fmt": None})
    assert merged["crash_free_pct"] == 99.4
    assert merged["crash_free_fmt"] == "99,40%"


def test_windows_dict_keeps_populated_periods():
    old = {"windows": {"24h": GOOD_24H, "7d": GOOD_7D}}
    merged = _merge_platform_block(old, {"windows": {"24h": {}, "30d": {"crash_free_pct": 99.1}}})
    assert merged["windows"]["24h"] == GOOD_24H
    assert merged["windows"]["7d"] == GOOD_7D
    assert merged["windows"]["30d"]["crash_free_pct"] == 99.1


def test_block_has_crash_free_detects_empty_payloads():
    assert _block_has_crash_free(GOOD_24H) is True
    assert _block_has_crash_free({}) is False
    assert _block_has_crash_free({"version": "9.5.10"}) is False
    assert _block_has_crash_free(None) is False


def test_missing_24h_window_falls_back_to_7d_and_says_so():
    block = {"latest_version": "9.0.2", "latest_7d": GOOD_7D}
    kpi = _fb_kpi_with_fallback(block, {}, "24h", "9.0.2")
    assert kpi is not None
    assert kpi["crash_free_fmt"] == "99,62%"
    assert kpi["fallback_from"] == "7d"
    assert "7 gün verisi" in (kpi["extra"] or "")


def test_falls_back_to_block_level_value():
    block = {"latest_version": "9.0.2", "crash_free_pct": 99.5, "crash_free_fmt": "99,50%"}
    kpi = _fb_kpi_with_fallback(block, {}, "24h", "9.0.2")
    assert kpi is not None
    assert kpi["crash_free_fmt"] == "99,50%"
    assert kpi["fallback_from"] == "latest"
    assert "son bilinen" in (kpi["extra"] or "")


def test_last_resort_carries_the_last_day_from_the_series():
    block = {
        "latest_version": "9.0.2",
        "series": [
            {"date": "2026-08-14", "crash_free_pct": 99.2},
            {"date": "2026-08-15", "crash_free_pct": 99.44},
            {"date": "2026-08-16"},  # boş gün — atlanmalı
        ],
    }
    kpi = _fb_kpi_with_fallback(block, {}, "24h", "9.0.2")
    assert kpi is not None
    assert kpi["crash_free_fmt"]
    assert kpi["fallback_from"] == "series"
    assert kpi["carried_from"] == "2026-08-15"
    assert "2026-08-15 verisi" in (kpi["extra"] or "")


def test_real_window_wins_over_fallback():
    block = {"latest_version": "9.0.2", "latest_24h": GOOD_24H, "latest_7d": GOOD_7D}
    kpi = _fb_kpi_with_fallback(block, {}, "24h", "9.0.2")
    assert kpi["crash_free_fmt"] == "99,81%"
    assert "fallback_from" not in kpi


def test_no_data_at_all_still_returns_none():
    """Hiç veri yoksa uydurma yok — panel 'hazırlanıyor' mesajını göstersin."""
    assert _fb_kpi_with_fallback({"latest_version": "9.0.2"}, {}, "24h", "9.0.2") is None
