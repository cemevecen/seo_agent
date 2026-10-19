"""Empower Creative Review — keyword / OCR eşleşme birimleri."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "empower_creative_review.py"


def _load():
    spec = importlib.util.spec_from_file_location("empower_creative_review_test", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_match_terms_vw_aliases():
    mod = _load()
    terms = mod.match_terms("vw")
    assert "vw" in terms
    assert "volkswagen" in terms


def test_card_matches_prefers_ocr_banner_text():
    mod = _load()
    terms = mod.match_terms("volkswagen")
    card = {
        "id": "6abd885912f8b233402f655a",
        "text": "BANNER 123 impressions OPEN",
        "alt": "",
        "img_src": "https://static.virgul.com/banner.png",
        "ocr_text": "Volkswagen Golf kampanya YUKLEYIN",
    }
    assert mod.card_matches(card, terms) is True


def test_card_matches_ignores_id_hex_noise():
    mod = _load()
    # "aa" id içinde geçebilir ama keyword "aa" değil — vw ararken id false-positive olmasın
    terms = mod.match_terms("vw")
    card = {
        "id": "aaaaaaaaaaaaaaaaaaaaaaaa",
        "text": "BANNER",
        "alt": "",
        "img_src": "",
        "ocr_text": "Midas yatırım",
    }
    assert mod.card_matches(card, terms) is False


def test_page_tarama_creative_review_job():
    from backend.services import page_tarama as store

    jobs = store.jobs_for("creative-review")
    assert [j["id"] for j in jobs] == ["empower_creative"]
    assert "mode=charts" not in (jobs[0].get("path") or "")
    assert "/sync-empower-creative" in (jobs[0].get("path") or "")
