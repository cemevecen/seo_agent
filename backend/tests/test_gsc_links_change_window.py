"""GSC Links change-window diff helpers."""

from __future__ import annotations

from backend.services.gsc_links_scrape_store import (
    _attach_rank_index,
    _cancel_false_churn,
    _change_keys_similar,
    _diff_key_maps,
    _fingerprint_key_map,
    _fold_change_key,
    _phonetic_change_key,
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


def test_fold_turkish_diacritics():
    assert _fold_change_key("mutfakcılar.com.tr") == _fold_change_key("mutfakcilar.com.tr")
    assert _fold_change_key("yıldırımalkan.info") == _fold_change_key("yildirimalkan.info")
    assert _fold_change_key("www.Example.COM") == "example.com"


def test_phonetic_fold_tr_en_variants():
    assert _phonetic_change_key("technopat.net") == _phonetic_change_key("teknopat.net")
    assert _phonetic_change_key("popneoism.com") == _phonetic_change_key("popneoizm.com")
    assert _change_keys_similar("liveinorthodoxy.com", "liveinortodoxy.com")


def test_cancel_encoding_mirrors_like_screenshot():
    latest = {
        _fold_change_key("mutfakcilar.com.tr"): {
            "label": "mutfakcilar.com.tr",
            "count": 413,
            "sites": 1,
        },
        _fold_change_key("popneoism.com"): {"label": "popneoism.com", "count": 336, "sites": 1},
        _fold_change_key("liveinorthodoxy.com"): {
            "label": "liveinorthodoxy.com",
            "count": 336,
            "sites": 1,
        },
        _fold_change_key("unbearablecampaign.com"): {
            "label": "unbearablecampaign.com",
            "count": 336,
            "sites": 1,
        },
        _fold_change_key("technopat.net"): {"label": "technopat.net", "count": 273, "sites": 1},
        _fold_change_key("yildirimalkan.info"): {
            "label": "yildirimalkan.info",
            "count": 267,
            "sites": 1,
        },
        _fold_change_key("real-new.example"): {
            "label": "real-new.example",
            "count": 12,
            "sites": 1,
        },
    }
    base = {
        _fold_change_key("mutfakcılar.com.tr"): {
            "label": "mutfakcılar.com.tr",
            "count": 413,
            "sites": 1,
        },
        _fold_change_key("popneoizm.com"): {"label": "popneoizm.com", "count": 336, "sites": 1},
        _fold_change_key("liveinortodoxy.com"): {
            "label": "liveinortodoxy.com",
            "count": 336,
            "sites": 1,
        },
        _fold_change_key("dayanılmazkampanya.com"): {
            "label": "dayanılmazkampanya.com",
            "count": 336,
            "sites": 1,
        },
        _fold_change_key("teknopat.net"): {"label": "teknopat.net", "count": 273, "sites": 1},
        _fold_change_key("yıldırımalkan.info"): {
            "label": "yıldırımalkan.info",
            "count": 267,
            "sites": 1,
        },
        _fold_change_key("real-lost.example"): {
            "label": "real-lost.example",
            "count": 9,
            "sites": 1,
        },
    }
    # Fold already collapses diacritic twins into same key → they won't be in new∪lost.
    # Phonetic / typo / translation twins remain as separate keys until cancel.
    new_keys = sorted(set(latest) - set(base))
    lost_keys = sorted(set(base) - set(latest))
    new_keys, lost_keys = _cancel_false_churn(new_keys, lost_keys, latest, base, rt="domain")
    assert _fold_change_key("real-new.example") in new_keys
    assert _fold_change_key("real-lost.example") in lost_keys
    assert not any("technopat" in k or "teknopat" in k for k in new_keys + lost_keys)
    assert not any("popneo" in k for k in new_keys + lost_keys)
    assert not any("orthodox" in k or "ortodox" in k for k in new_keys + lost_keys)
    assert not any("campaign" in k or "kampanya" in k for k in new_keys + lost_keys)


def test_diff_key_maps_cancels_mirrors():
    latest = {
        "technopat.net": {"label": "technopat.net", "count": 273, "sites": 1},
        "brand-new.com": {"label": "brand-new.com", "count": 40, "sites": 2},
    }
    base = {
        "teknopat.net": {"label": "teknopat.net", "count": 273, "sites": 1},
        "gone-old.com": {"label": "gone-old.com", "count": 11, "sites": 1},
    }
    new_keys, lost_keys, _changed = _diff_key_maps(latest, base, rt="domain")
    assert new_keys == ["brand-new.com"]
    assert lost_keys == ["gone-old.com"]
