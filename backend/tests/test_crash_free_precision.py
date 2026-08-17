"""Crash-free / ANR-free yüzdeleri üç haneli gösterilir (99,994%).

Değerler 99,9x aralığında sıkıştığı için iki hane gerçek farkları gizliyordu:
99,991 ile 99,994 ekranda aynı görünüyordu.
"""

from __future__ import annotations

from backend.services.stability_free import _fb_window_kpi, _fmt_free


def test_three_decimals_by_default():
    assert _fmt_free(99.9938) == "99,994%"
    assert _fmt_free(99.9912) == "99,991%"
    assert _fmt_free(99.5) == "99,500%"
    assert _fmt_free(97.0) == "97,000%"


def test_turkish_decimal_separator():
    assert "," in (_fmt_free(99.9938) or "")
    assert "." not in (_fmt_free(99.9938) or "")


def test_does_not_round_up_to_hundred():
    """99,9998 → 100,000% yazmak yanıltıcı olur; dört haneye çıkar."""
    assert _fmt_free(99.9998) == "99,9998%"
    assert _fmt_free(99.99951) == "99,9995%"
    # Eşiğin altı üç hanede kalır
    assert _fmt_free(99.9994) == "99,999%"


def test_none_and_garbage_stay_none():
    assert _fmt_free(None) is None
    assert _fmt_free("abc") is None  # type: ignore[arg-type]


def test_window_kpi_reformats_stored_two_digit_text():
    """Eski kayıtlardaki iki haneli metin yeniden tarama beklemeden düzelsin."""
    win = {"crash_free_pct": 99.9938, "crash_free_fmt": "99,99%", "series": []}
    kpi = _fb_window_kpi(win, period="24h", version="9.6.0")
    assert kpi is not None
    assert kpi["crash_free_fmt"] == "99,994%"


def test_window_kpi_falls_back_to_stored_text_without_a_number():
    win = {"crash_free_fmt": "99,99%", "series": []}
    kpi = _fb_window_kpi(win, period="24h", version="9.6.0")
    assert kpi is not None
    assert kpi["crash_free_fmt"] == "99,99%"


def test_store_tabs_block_uses_the_same_precision():
    from backend.services.firebase_from_store_tabs import _cf_from_sf_block

    out = _cf_from_sf_block(
        {"crash_free_pct": 99.9938, "crash_free_fmt": "99,99%", "anr_free_pct": 99.9321},
        method="test",
    )
    assert out is not None
    assert out["crash_free_fmt"] == "99,994%"
    # ANR tarafı iki hanede kalır (mevcut sözleşme; test_stability_free_anr.py)
    assert out["anr_free_fmt"] == "99,93%"


def test_anr_keeps_two_decimals_and_does_not_round_to_hundred():
    assert _fmt_free(99.9321, digits=2) == "99,93%"
    assert _fmt_free(99.996, digits=2) == "99,996%"
