"""Mobilde parmakla çekme (pull-to-refresh) sayfayı yeniler, tarama başlatmaz.

Eskiden page_tarama.js «pc:page-refresh» olayını dinleyip tam taramayı
başlatıyordu: mobilde her çekişte Mac taraması tetikleniyor, saatlik kota
harcanıyor ve sayfa dakikalarca beklemede kalıyordu.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PTR_JS = ROOT / "static/js/app-refresh.js"
QUEUE_JS = ROOT / "static/js/page_tarama.js"


def test_pull_to_refresh_only_reloads_the_page():
    js = PTR_JS.read_text(encoding="utf-8")
    assert "window.location.reload()" in js
    # Kuyruk/tarama uçlarına dokunmamalı
    assert "page-tarama" not in js
    assert "18765" not in js


def test_queue_script_does_not_hijack_the_refresh_event():
    js = QUEUE_JS.read_text(encoding="utf-8")
    assert 'addEventListener("pc:page-refresh"' not in js
    assert "pc:page-refresh" in js, "davranışı açıklayan not kalsın"


def test_scan_still_starts_from_the_update_page_button():
    js = QUEUE_JS.read_text(encoding="utf-8")
    assert "js-page-tarama" in js
    assert "function start(" in js


def test_refresh_event_stays_cancelable_for_future_listeners():
    js = PTR_JS.read_text(encoding="utf-8")
    assert "cancelable: true" in js
    assert "ev.defaultPrevented" in js
