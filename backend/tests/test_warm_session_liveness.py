"""Sıcak Firefox penceresi: ölü context yeniden kullanılmamalı.

Tarayıcı dışarıdan öldürülebiliyor (Selenium yolu Playwright süreçlerini
kapatıyordu). Slot ölü context tutunca sonraki her tarama
«Target page, context or browser has been closed» ile düşüyordu.
"""

from __future__ import annotations

from backend.services import scrape_browser as sb


class _DeadCtx:
    """Kapanmış context: pages boş, sürücü çağrısı patlar."""

    pages: list = []

    def cookies(self):
        raise RuntimeError("Target page, context or browser has been closed")


class _LiveCtxNoPages:
    pages: list = []

    def cookies(self):
        return []


class _LivePage:
    url = "https://appstoreconnect.apple.com/apps"


class _LiveCtx:
    pages = [_LivePage()]


def test_dead_context_is_detected():
    assert sb._warm_alive(_DeadCtx()) is False


def test_live_context_without_pages_is_kept():
    assert sb._warm_alive(_LiveCtxNoPages()) is True


def test_live_context_with_page_is_kept():
    assert sb._warm_alive(_LiveCtx()) is True


def test_none_is_not_alive():
    assert sb._warm_alive(None) is False


def test_empty_pages_alone_is_not_proof_of_life():
    """Eski davranış: pages boşsa True dönüyordu — ceset yeniden kullanılıyordu."""
    src = (sb.__file__).replace(".pyc", ".py")
    with open(src, encoding="utf-8") as fh:
        body = fh.read().split("def _warm_alive", 1)[1].split("\ndef ", 1)[0]
    assert "probe()" in body                     # sürücüye gerçek çağrı
    assert "if not pages:\n            return True" not in body


# ── Yan etkisiz sorgu: koruma kontrolü kaydı silmemeli ─────────────────────

def test_profile_query_has_no_side_effects():
    """`warm_session_get_for_profile` yanlış thread'den çağrılınca kaydı siliyor.

    Warm-up'ın «başka pencere var mı» kontrolü bunu kullanınca, korumaya
    çalıştığı sıcak oturumu uçuruyordu: ASC penceresi yetim ilan edilip
    öldürülüyordu. Sorgu yalnızca okumalı.
    """
    import threading
    from pathlib import Path

    prof = Path("/tmp/fx-test-profile")
    key = "testkey"
    sb._WARM_SESSIONS[key] = {
        "pw": object(), "ctx": _LiveCtx(), "label": "t",
        "thread": threading.get_ident() + 1,   # BAŞKA thread
        "profile": prof,
    }
    sb._WARM_BY_PROFILE[sb._profile_key(prof)] = key
    try:
        assert sb.warm_session_registered_for_profile(prof) is True
        # sorgudan sonra kayıt hâlâ durmalı
        assert key in sb._WARM_SESSIONS
        assert sb.warm_session_registered_for_profile(prof) is True
    finally:
        sb._WARM_SESSIONS.pop(key, None)
        sb._WARM_BY_PROFILE.pop(sb._profile_key(prof), None)


def test_unknown_profile_reports_false():
    from pathlib import Path

    assert sb.warm_session_registered_for_profile(Path("/tmp/yok-boyle-profil")) is False


def test_warmup_uses_the_side_effect_free_query():
    from pathlib import Path as _P

    src = (_P(sb.__file__).parents[2] / "scripts/scrape_login_warmup.py").read_text(encoding="utf-8")
    assert "warm_session_registered_for_profile" in src
    assert "warm_session_get_for_profile" not in src
