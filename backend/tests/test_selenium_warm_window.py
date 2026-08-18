"""Selenium penceresi de tur bitince kapanmamalı.

İlke: açılan pencere/sekme bir daha kapatılmaz; sonraki tarama aynı pencereden
devam eder. Playwright tarafında bu zaten vardı (warm session); Selenium yolu
(Play / Firebase) her turda `driver.quit()` ile kapatıyordu — kullanıcı iki
adımlı doğrulamayı tamamlayamadan ekran kayboluyordu.
"""

from __future__ import annotations

from pathlib import Path

from backend.services import selenium_playwright_shim as shim


class _Driver:
    def __init__(self, alive=True):
        self._alive = alive
        self.quit_called = False

    @property
    def current_url(self):
        if not self._alive:
            raise RuntimeError("pencere kapalı")
        return "https://play.google.com/console/u/0/"

    def quit(self):
        self.quit_called = True
        self._alive = False


class _Ctx:
    def __init__(self, driver, *, download_dir=None):
        self._driver = driver
        self.download_dir = download_dir
        self.closed = False

    def close(self):
        self.closed = True
        self._driver.quit()


def _clear():
    shim._SELENIUM_WARM.clear()


def test_release_keeps_the_window_open(monkeypatch):
    _clear()
    monkeypatch.setattr(shim, "selenium_keep_window_open", lambda: True)
    ctx = _Ctx(_Driver())
    shim.release_selenium_context(None, ctx)
    assert ctx.closed is False, "pencere kapatılmamalı"


def test_release_closes_when_keep_open_disabled(monkeypatch):
    _clear()
    monkeypatch.setattr(shim, "selenium_keep_window_open", lambda: False)
    ctx = _Ctx(_Driver())
    shim.release_selenium_context(None, ctx)
    assert ctx.closed is True


def test_next_launch_reuses_the_same_window(monkeypatch):
    _clear()
    monkeypatch.setattr(shim, "selenium_keep_window_open", lambda: True)
    prof = Path("/tmp/fx-google-test")
    launched = {"n": 0}

    def _fake_launch(profile, *, headed, download_dir, prefs=None):
        launched["n"] += 1
        return _Driver()

    monkeypatch.setattr(shim, "launch_system_firefox_driver", _fake_launch)
    monkeypatch.setattr(shim, "SeleniumContext", _Ctx)

    _, first, reused1 = shim.launch_selenium_context(prof, headed=True)
    shim.release_selenium_context(None, first)
    _, second, reused2 = shim.launch_selenium_context(prof, headed=True)

    assert reused1 is False and reused2 is True
    assert second is first, "aynı pencere dönmeliydi"
    assert launched["n"] == 1, "ikinci turda yeni tarayıcı açılmamalı"


def test_dead_window_is_replaced_not_reused(monkeypatch):
    _clear()
    monkeypatch.setattr(shim, "selenium_keep_window_open", lambda: True)
    prof = Path("/tmp/fx-google-test2")
    monkeypatch.setattr(shim, "SeleniumContext", _Ctx)
    monkeypatch.setattr(shim, "launch_system_firefox_driver",
                        lambda profile, *, headed, download_dir, prefs=None: _Driver())

    _, first, _ = shim.launch_selenium_context(prof, headed=True)
    first._driver._alive = False           # pencere dışarıdan öldü
    _, second, reused = shim.launch_selenium_context(prof, headed=True)
    assert reused is False and second is not first


def test_liveness_probe_detects_closed_window():
    assert shim._selenium_alive(None) is False
    assert shim._selenium_alive(_Ctx(_Driver(alive=False))) is False
    assert shim._selenium_alive(_Ctx(_Driver(alive=True))) is True


def test_profiles_are_tracked_separately(monkeypatch):
    _clear()
    monkeypatch.setattr(shim, "selenium_keep_window_open", lambda: True)
    monkeypatch.setattr(shim, "SeleniumContext", _Ctx)
    monkeypatch.setattr(shim, "launch_system_firefox_driver",
                        lambda profile, *, headed, download_dir, prefs=None: _Driver())
    _, a, _ = shim.launch_selenium_context(Path("/tmp/p-a"), headed=True)
    _, b, _ = shim.launch_selenium_context(Path("/tmp/p-b"), headed=True)
    assert a is not b
    assert len(shim._SELENIUM_WARM) == 2
