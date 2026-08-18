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
