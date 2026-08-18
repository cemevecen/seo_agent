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

        def set_capability(self, *_a):
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


# ── Köprü yeniden başlasa da yaşayan pencere ────────────────────────────────

def test_attach_is_skipped_when_the_recorded_port_is_dead(monkeypatch, tmp_path):
    """Eski kayıt duruyor ama geckodriver ölmüş → yeni pencere açılmalı."""
    prof = tmp_path / "fx-asc"
    monkeypatch.setattr(sfd, "firefox_detached_enabled", lambda: True)
    monkeypatch.setattr(sfd, "_detach_state_path", lambda p: tmp_path / "state.json")
    monkeypatch.setattr(sfd, "_port_open", lambda port, timeout=0.35: False)
    sfd._write_detach_state(prof, {
        "url": "http://127.0.0.1:9", "port": 9, "session": "abc",
        "gecko_pid": 1, "headed": True, "download_dir": str(tmp_path / "dl"),
    })
    got = sfd._attach_detached_driver(
        prof, headed=True, download_dir=tmp_path / "dl", page_load_timeout=30
    )
    assert got is None
    assert not (tmp_path / "state.json").exists(), "ölü kayıt silinmeliydi"


def test_attach_refuses_a_mismatched_download_dir(monkeypatch, tmp_path):
    """İndirme dizini oturum açılışında sabitlenir; bağlanmak CSV'yi kaybettirir."""
    prof = tmp_path / "fx-asc"
    monkeypatch.setattr(sfd, "firefox_detached_enabled", lambda: True)
    monkeypatch.setattr(sfd, "_detach_state_path", lambda p: tmp_path / "state.json")
    monkeypatch.setattr(sfd, "_port_open", lambda port, timeout=0.35: True)
    sfd._write_detach_state(prof, {
        "url": "http://127.0.0.1:1234", "port": 1234, "session": "abc",
        "gecko_pid": 1, "headed": True, "download_dir": str(tmp_path / "a"),
    })
    assert sfd._attach_detached_driver(
        prof, headed=True, download_dir=tmp_path / "b", page_load_timeout=30
    ) is None
    # Uyuşmazlık kaydı silmez — başka bir çağıran doğru dizinle bağlanabilir
    assert (tmp_path / "state.json").exists()


def test_launch_falls_back_to_normal_when_detached_spawn_fails(monkeypatch, tmp_path):
    """Ayrık açılış tutmazsa bugünkü davranışa dönülmeli, hata verilmemeli."""
    prof = tmp_path / "fx-asc"
    made = _install(monkeypatch, prof, keep=True)
    monkeypatch.setattr(sfd, "firefox_detached_enabled", lambda: True)
    monkeypatch.setattr(sfd, "_attach_detached_driver", lambda *a, **kw: None)
    monkeypatch.setattr(sfd, "_spawn_detached_driver", lambda *a, **kw: None)
    monkeypatch.setattr(sfd, "_clear_detach_state", lambda p: None)

    d = sfd.launch_system_firefox_driver(prof, headed=True, download_dir=tmp_path / "dl")
    assert d is made[0], "ayrık açılış tutmayınca normal açılış kullanılmadı"


def test_detached_disabled_when_keep_open_is_off(monkeypatch):
    monkeypatch.delenv("SELENIUM_DETACHED", raising=False)
    monkeypatch.setattr(sfd, "firefox_keep_window_open", lambda: False)
    assert sfd.firefox_detached_enabled() is False
    monkeypatch.setattr(sfd, "firefox_keep_window_open", lambda: True)
    assert sfd.firefox_detached_enabled() is True
    monkeypatch.setenv("SELENIUM_DETACHED", "0")
    assert sfd.firefox_detached_enabled() is False


def test_geckodriver_is_spawned_in_its_own_session_group(monkeypatch):
    """start_new_session olmadan köprü ölünce geckodriver da ölür — asıl mesele bu."""
    src = (
        __import__("pathlib").Path(sfd.__file__).read_text(encoding="utf-8")
    )
    spawn = src.split("def _spawn_detached_driver(", 1)[1].split("\ndef ", 1)[0]
    assert "start_new_session=True" in spawn


def test_cdp_port_is_disabled_so_two_windows_can_coexist():
    """geckodriver sabit 9222 veriyor; ikinci pencere çakışıp kapanıyordu.

    Canlı belirti: NS_ERROR_SOCKET_ADDRESS_IN_USE → RemoteAgent başlayamıyor →
    Firefox kapanıyor → «Tried to run command without establishing a connection».
    CDP'yi kullanmadığımız için yetenek kapatılır.
    """
    import pathlib as _pl

    src = _pl.Path(sfd.__file__).read_text(encoding="utf-8")
    body = src.split("def _firefox_options(", 1)[1].split("\ndef ", 1)[0]
    assert 'set_capability("moz:debuggerAddress", False)' in body


def test_boot_cleanup_spares_a_live_detached_window(monkeypatch, tmp_path):
    """Köprü açılışındaki kalıntı temizliği korunan pencereyi öldürmemeli."""
    import backend.services.scrape_browser as sb

    killed: list[str] = []
    monkeypatch.setattr(sb, "STATE_DIR", tmp_path)
    monkeypatch.setattr(sb, "kill_profile_browsers", lambda p, **kw: killed.append(p.name) or 1)
    monkeypatch.setattr(sb, "_detached_window_is_live", lambda p: p.name == "fx-google")
    monkeypatch.setattr(sb.subprocess, "check_output", lambda *a, **kw: "")

    sb.kill_legacy_chrome_scrapers()
    assert "fx-google" not in killed, "canlı ayrık pencere öldürüldü"
    assert "fx-asc" in killed, "kalıntı profiller yine temizlenmeli"


def test_detached_window_is_live_needs_an_open_port(monkeypatch, tmp_path):
    prof = tmp_path / "fx-asc"
    monkeypatch.setattr(sfd, "_detach_state_path", lambda p: tmp_path / "s.json")
    # Oturum canlılığı ayrıca sınanıyor; burada port koşulu izole edilir
    monkeypatch.setattr(sfd, "_session_alive", lambda url, sid, timeout=2.5: True)
    assert sfd.detached_window_is_live(prof) is False       # kayıt yok
    sfd._write_detach_state(prof, {
        "url": "http://127.0.0.1:1234", "port": 1234, "session": "abc",
    })
    monkeypatch.setattr(sfd, "_port_open", lambda port, timeout=0.35: False)
    assert sfd.detached_window_is_live(prof) is False       # kayıt var, süreç ölü
    monkeypatch.setattr(sfd, "_port_open", lambda port, timeout=0.35: True)
    assert sfd.detached_window_is_live(prof) is True


def test_live_check_rejects_a_dead_session_behind_a_live_port(monkeypatch, tmp_path):
    """Firefox ölüp geckodriver kalınca port hâlâ cevap veriyor.

    Yalnızca porta bakmak «canlı» diyordu; köprü açılış temizliği de bu yalana
    bakıp gerçekten kalıntı olan profili atlıyordu.
    """
    prof = tmp_path / "fx-asc"
    monkeypatch.setattr(sfd, "_detach_state_path", lambda p: tmp_path / "s.json")
    sfd._write_detach_state(prof, {
        "url": "http://127.0.0.1:1234", "port": 1234, "session": "abc",
    })
    monkeypatch.setattr(sfd, "_port_open", lambda port, timeout=0.35: True)

    monkeypatch.setattr(sfd, "_session_alive", lambda url, sid, timeout=2.5: False)
    assert sfd.detached_window_is_live(prof) is False, "ölü oturum canlı sayıldı"

    monkeypatch.setattr(sfd, "_session_alive", lambda url, sid, timeout=2.5: True)
    assert sfd.detached_window_is_live(prof) is True


def test_failed_attach_reaps_the_orphan_geckodriver(monkeypatch, tmp_path):
    """Ayrık başlattığımız için öksüz geckodriver'ı kimse toplamıyor."""
    prof = tmp_path / "fx-asc"
    monkeypatch.setattr(sfd, "_detach_state_path", lambda p: tmp_path / "s.json")
    monkeypatch.setattr(sfd, "_port_open", lambda port, timeout=0.35: True)
    monkeypatch.setattr(sfd, "resolve_system_firefox_executable", lambda: "/x/firefox")
    monkeypatch.setattr(sfd, "_firefox_options", lambda *a, **kw: object())

    def _boom(*_a, **_kw):
        raise RuntimeError("oturum yok")

    monkeypatch.setattr(sfd, "_attached_driver", _boom)
    reaped: list[int] = []
    monkeypatch.setattr(sfd.os, "kill", lambda pid, sig: reaped.append(pid))
    sfd._write_detach_state(prof, {
        "url": "http://127.0.0.1:1234", "port": 1234, "session": "abc",
        "gecko_pid": 4242, "headed": True, "download_dir": str(tmp_path / "dl"),
    })

    got = sfd._attach_detached_driver(
        prof, headed=True, download_dir=tmp_path / "dl", page_load_timeout=30
    )
    assert got is None
    assert reaped == [4242], "öksüz geckodriver toplanmadı"
    assert not (tmp_path / "s.json").exists()
