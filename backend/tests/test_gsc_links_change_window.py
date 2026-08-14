"""GSC Links change-window diff helpers."""

from __future__ import annotations

from backend.services.gsc_links_scrape_store import (
    _attach_rank_index,
    _diff_key_maps,
    _fingerprint_key_map,
)


def test_diff_detects_count_and_rank_changes():
    latest = {
        "https://www.doviz.com/": {"label": "https://www.doviz.com/", "count": 100, "sites": 10},
        "https://www.doviz.com/altin": {"label": "https://www.doviz.com/altin", "count": 80, "sites": 8},
        "https://www.doviz.com/new": {"label": "https://www.doviz.com/new", "count": 5, "sites": 1},
    }
    base = {
        "https://www.doviz.com/": {"label": "https://www.doviz.com/", "count": 90, "sites": 10},
        "https://www.doviz.com/altin": {"label": "https://www.doviz.com/altin", "count": 80, "sites": 9},
        "https://www.doviz.com/old": {"label": "https://www.doviz.com/old", "count": 7, "sites": 2},
    }
    _attach_rank_index(latest, rt="external")
    _attach_rank_index(base, rt="external")
    new_keys, lost_keys, changed = _diff_key_maps(latest, base, rt="external")
    assert "https://www.doviz.com/new" in new_keys
    assert "https://www.doviz.com/old" in lost_keys
    deltas = {c["key"]: c for c in changed}
    assert deltas["https://www.doviz.com/"]["delta"] == 10
    assert deltas["https://www.doviz.com/altin"]["delta_sites"] == -1


def test_fingerprint_equal_when_same_payload():
    a = {"x": {"count": 1, "sites": 2, "rank": 0}}
    b = {"x": {"count": 1, "sites": 2, "rank": 0}}
    assert _fingerprint_key_map(a) == _fingerprint_key_map(b)
    b["x"]["count"] = 3
    assert _fingerprint_key_map(a) != _fingerprint_key_map(b)
