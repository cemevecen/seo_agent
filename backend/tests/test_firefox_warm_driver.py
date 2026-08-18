"""Alt katman sıcak pencere: launch/quit çifti oturumu öldürmemeli.

Empower gibi shim'den geçmeyen işler doğrudan `launch_system_firefox_driver`
çağırıyor. Sıcak pencere davranışı orada da geçerli olmalı, yoksa her tur
yeniden giriş isteniyor.
"""

from __future__ import annotations

from pathlib import Path

import backend.services.system_firefox_driver as sfd


class _FakeDriver:
    def __init__(self) -> None:
        self.quit_calls = 0
        self.timeouts: list[int] = []
        self.alive = True

    @property
    def current_url(self) -> str:
        if not self.alive:
            raise RuntimeError("pencere kapalı")
        return "https://example.test/"

    def set_page_load_timeout(self, value: int) -> None:
        self.timeouts.append(value)

    def quit(self) -> None:
        self.quit_calls += 1
        self.alive = False


def _install(monkeypatch, profile: Path, *, keep: bool, headed: bool = True) -> list[_FakeDriver]:
    """launch'ı gerçek Firefox'a gitmeden sınanabilir hale getir."""
    made: list[_FakeDriver] = []
    monkeypatch.setattr(sfd, "_WARM_DRIVERS", {})
    monkeypatch.setattr(sfd, "firefox_keep_window_open", lambda: keep)
    monkeypatch.setattr(sfd, "resolve_system_firefox_executable", lambda: "/Applications/Firefox.app/x")
    monkeypatch.setattr(sfd, "profile_login_lock_active", lambda p: False)
    monkeypatch.setattr(sfd, "ban_playwright_nightly_processes", lambda p=None: 0)
    monkeypatch.setattr(sfd, "ensure_profile_free_for_launch", lambda p, **kw: None)
    monkeypatch.setattr(sfd, "align_firefox_profile_compatibility", lambda p: None)

    def _fake_webdriver_firefox(options=None):  # noqa: ANN001
        d = _FakeDriver()
        made.append(d)
        return d

    class _Opts:
        def add_argument(self, *_a):
            pass

        def set_preference(self, *_a):
            pass

        binary_location = ""

    import sys
    import types

    fake_sel = types.ModuleType("selenium")
    fake_sel.webdriver = types.SimpleNamespace(Firefox=_fake_webdriver_firefox)
    fake_opts_mod = types.ModuleType("selenium.webdriver.firefox.options")
    fake_opts_mod.Options = _Opts
    monkeypatch.setitem(sys.modules, "selenium", fake_sel)
    monkeypatch.setitem(sys.modules, "selenium.webdriver.firefox.options", fake_opts_mod)
    _ = profile, headed
    return made


def test_second_launch_reuses_the_same_window(monkeypatch, tmp_path):
    prof = tmp_path / "fx-empower"
    made = _install(monkeypatch, prof, keep=True)

    first = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    sfd.quit_system_firefox_driver(first)          # tur bitti → kapanmamalı
    assert first.quit_calls == 0

    second = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    assert second is first, "ikinci tur yeni pencere açtı — oturum ölürdü"
    assert len(made) == 1


def test_dead_window_is_replaced_not_reused(monkeypatch, tmp_path):
    prof = tmp_path / "fx-empower"
    made = _install(monkeypatch, prof, keep=True)

    first = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    sfd.quit_system_firefox_driver(first)
    first.alive = False                            # pencere dışarıdan öldü

    second = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    assert second is not first
    assert len(made) == 2


def test_keep_open_disabled_still_quits(monkeypatch, tmp_path):
    prof = tmp_path / "fx-empower"
    _install(monkeypatch, prof, keep=False)

    d = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    sfd.quit_system_firefox_driver(d)
    assert d.quit_calls == 1

    again = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    assert again is not d


def test_different_download_dir_does_not_reuse(monkeypatch, tmp_path):
    """İndirme dizini açılışta sabitlenir; yeniden kullanım CSV'yi yanlış yere indirirdi."""
    prof = tmp_path / "fx-empower"
    _install(monkeypatch, prof, keep=True)

    first = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "a")
    sfd.quit_system_firefox_driver(first)
    second = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "b")
    assert second is not first


def test_headless_does_not_reuse_a_headed_window(monkeypatch, tmp_path):
    prof = tmp_path / "fx-empower"
    _install(monkeypatch, prof, keep=True)

    first = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    sfd.quit_system_firefox_driver(first)
    second = sfd.launch_system_firefox_driver(prof, headed=False, download_dir=tmp_path / "dl")
    assert second is not first


def test_reuse_does_not_reset_the_profile(monkeypatch, tmp_path):
    """Profili boşaltan adımlar korunan pencereyi kapatmamalı."""
    prof = tmp_path / "fx-empower"
    _install(monkeypatch, prof, keep=True)
    calls: list[str] = []
    monkeypatch.setattr(
        sfd, "ensure_profile_free_for_launch", lambda p, **kw: calls.append("free")
    )
    monkeypatch.setattr(
        sfd, "ban_playwright_nightly_processes", lambda p=None: calls.append("ban")
    )

    first = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    sfd.quit_system_firefox_driver(first)
    calls.clear()
    sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    assert calls == [], f"yeniden kullanımda profil sıfırlandı: {calls}"


def test_separate_profiles_keep_separate_windows(monkeypatch, tmp_path):
    a = tmp_path / "fx-empower"
    b = tmp_path / "fx-google"
    _install(monkeypatch, a, keep=True)

    da = sfd.launch_system_firefox_driver(a, headed=True, download_dir=tmp_path / "dl")
    db = sfd.launch_system_firefox_driver(b, headed=True, download_dir=tmp_path / "dl")
    assert da is not db
    sfd.quit_system_firefox_driver(da)
    sfd.quit_system_firefox_driver(db)
    assert sfd.launch_system_firefox_driver(a, headed=True, download_dir=tmp_path / "dl") is da
    assert sfd.launch_system_firefox_driver(b, headed=True, download_dir=tmp_path / "dl") is db
