"""Sistem Firefox.app (Selenium) — Playwright Nightly yok.

Google login / Sheets scrape için gerçek /Applications/Firefox.app.
Playwright'ın ms-playwright Nightly build'i Google'da 'insecure' engeline takılır
ve juggler ile sistem Firefox uyumsuzdur.
"""

from __future__ import annotations

import os
import re
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from backend.services.scrape_browser import (
    STATE_DIR,
    align_firefox_profile_compatibility,
    assert_browser_scrape_allowed,
    ensure_profile_free_for_launch,
    profile_login_lock_active,
    resolve_system_firefox_executable,
)

_NIGHTLY_MARKERS = (
    "ms-playwright/firefox",
    "Nightly.app",
    "firefox-1471",
    "firefox-1509",
)


def ban_playwright_nightly_processes(profile: Any = None) -> int:
    """Playwright Nightly scrape süreçlerini kapat.

    `profile` verilirse YALNIZCA o profili kullanan süreçler kapatılır.

    Neden önemli: Google, Playwright Nightly'yi engellediği için Selenium yolu
    açılmadan önce Nightly süreçleri kapatılıyor. Ama bu tarama profil ayrımı
    yapmadığında, Firebase taraması ASC'nin açık Firefox penceresini de
    öldürüyordu — ASC oturumu yalnızca tarayıcı süreci yaşadığı sürece geçerli
    olduğu için kullanıcı her Firebase turundan sonra ASC'ye yeniden giriş
    yapmak zorunda kalıyordu.
    """
    killed = 0
    marker = None
    if profile is not None:
        try:
            marker = str(Path(str(profile)).expanduser().resolve())
        except Exception:
            marker = str(profile)
    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        return 0
    for line in out.splitlines():
        if not any(m in line for m in _NIGHTLY_MARKERS):
            continue
        if marker is not None and marker not in line:
            continue  # başka profilin penceresi — dokunma
        try:
            pid = int(line.split(None, 1)[0])
        except Exception:
            continue
        if pid <= 1 or pid == os.getpid():
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            killed += 1
        except (ProcessLookupError, PermissionError, OSError):
            pass
    if killed:
        time.sleep(0.5)
    return killed


def default_firefox_profile_dir() -> Path | None:
    """En iyi Google oturumlu sistem Firefox profili (Nightly/ms-playwright değil)."""
    root = Path.home() / "Library/Application Support/Firefox/Profiles"
    if not root.is_dir():
        return None
    session_names = {
        "SID",
        "HSID",
        "SSID",
        "APISID",
        "SAPISID",
        "__Secure-1PSID",
        "__Secure-3PSID",
        "LSID",
    }
    best: Path | None = None
    best_score = -1
    for p in root.iterdir():
        if not p.is_dir():
            continue
        cookies = _read_google_cookies(p)
        names = {c["name"] for c in cookies}
        score = len(names & session_names) * 100 + len(cookies)
        if score > best_score:
            best_score = score
            best = p
    if best is not None and best_score >= 100:
        return best

    # profiles.ini Default=1 yedek
    ini = Path.home() / "Library/Application Support/Firefox/profiles.ini"
    if not ini.is_file():
        return best
    try:
        text = ini.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return best
    ff_root = Path.home() / "Library/Application Support/Firefox"
    for block in re.split(r"\n\s*\[", text):
        low = block.lower()
        path_m = re.search(r"(?im)^path=(.+)$", block)
        if not path_m:
            continue
        rel = path_m.group(1).strip()
        is_rel = "isrelative=1" in low
        p = (ff_root / rel) if is_rel else Path(rel)
        if p.is_dir() and ("default=1" in low or "default-release" in p.name.lower()):
            return p
    return best


def _read_google_cookies(profile: Path) -> list[dict[str, Any]]:
    db = profile / "cookies.sqlite"
    if not db.is_file():
        return []
    tmp = Path(tempfile.mkdtemp(prefix="fxcook-"))
    try:
        for name in ("cookies.sqlite", "cookies.sqlite-wal", "cookies.sqlite-shm"):
            src = profile / name
            if src.is_file():
                shutil.copy2(src, tmp / name)
        con = sqlite3.connect(f"file:{tmp / 'cookies.sqlite'}?mode=ro", uri=True)
        rows = con.execute(
            """
            SELECT host, name, value, path, expiry, isSecure, isHttpOnly
            FROM moz_cookies
            WHERE host LIKE '%google%' OR host LIKE '%.google.%'
               OR host LIKE '%googleusercontent%'
            """
        ).fetchall()
        con.close()
    except Exception:
        return []
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    now = int(time.time())
    out: list[dict[str, Any]] = []
    for host, name, value, path, expiry, is_secure, is_http_only in rows:
        if expiry and int(expiry) < now:
            continue
        # Skip empty / session-less noise
        if not name or value is None:
            continue
        out.append(
            {
                "domain": host,
                "name": name,
                "value": value,
                "path": path or "/",
                "expiry": int(expiry) if expiry else None,
                "secure": bool(is_secure),
                "httpOnly": bool(is_http_only),
            }
        )
    return out


_GOOGLE_SESSION_COOKIE_NAMES = frozenset(
    {
        "SID",
        "HSID",
        "SSID",
        "APISID",
        "SAPISID",
        "__Secure-1PSID",
        "__Secure-3PSID",
        "LSID",
    }
)


def google_profile_has_session(profile: Path) -> bool:
    """Profilde geçerli Google oturum çerezi var mı?"""
    cookies = _read_google_cookies(profile.expanduser())
    names = {str(c.get("name") or "") for c in cookies}
    return bool(names & _GOOGLE_SESSION_COOKIE_NAMES)


def profile_has_session_cookie(
    profile: Path,
    host_like: str,
    cookie_names: Iterable[str],
) -> bool | None:
    """Profilde ilgili site için süresi geçmemiş oturum çerezi var mı?

    True/False kesin cevap; profil veya çerez veritabanı okunamıyorsa None döner
    (bilinmiyor → çağıran taraf işi engellememeli).
    """
    profile = profile.expanduser()
    db = profile / "cookies.sqlite"
    if not db.is_file():
        return False if profile.is_dir() else None
    wanted = {str(n) for n in cookie_names}
    tmp = Path(tempfile.mkdtemp(prefix="fxsess-"))
    try:
        for name in ("cookies.sqlite", "cookies.sqlite-wal", "cookies.sqlite-shm"):
            src = profile / name
            if src.is_file():
                shutil.copy2(src, tmp / name)
        con = sqlite3.connect(f"file:{tmp / 'cookies.sqlite'}?mode=ro", uri=True)
        rows = con.execute(
            "SELECT name, expiry FROM moz_cookies WHERE host LIKE ?",
            (f"%{host_like}%",),
        ).fetchall()
        con.close()
    except Exception:  # noqa: BLE001
        return None
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    now = int(time.time())
    for name, expiry in rows:
        if str(name or "") not in wanted:
            continue
        if expiry and int(expiry) < now:
            continue
        return True
    return False


def bootstrap_google_cookies_into_profile(target_profile: Path) -> int:
    """Eski sqlite kopya yolu — profil şemasını bozmamak için no-op.

    Oturum aktarımı `inject_google_cookies_from_default` ile Selenium üzerinde yapılır.
    """
    _ = target_profile
    return 0


# ── Sıcak pencere ────────────────────────────────────────────────────────────
# Her tarama turu kendi penceresini açıp kapatınca oturum da ölüyordu (ASC/
# empower gibi yerlerde tekrar tekrar giriş isteniyordu). Kayıt burada, en alt
# katmanda tutulur; hem shim üzerinden geçen işler (play, firebase) hem de
# doğrudan çağıran işler (empower) aynı davranışı alsın.
_WARM_DRIVERS: dict[str, Any] = {}


def _driver_profile_key(profile: Path) -> str:
    try:
        return str(Path(str(profile)).expanduser().resolve())
    except Exception:  # noqa: BLE001
        return str(profile)


def _driver_alive(driver: Any) -> bool:
    """Sürücüye gerçek bir çağrı yap — kapanmış pencere canlı sayılmasın."""
    if driver is None:
        return False
    try:
        _ = driver.current_url
        return True
    except Exception:  # noqa: BLE001
        return False


def firefox_keep_window_open() -> bool:
    from backend.services.scrape_browser import scrape_keep_window_open

    return scrape_keep_window_open(env_key="SELENIUM_KEEP_OPEN")


# ── Köprü yeniden başlasa da yaşayan pencere ────────────────────────────────
# Sıcak pencere kaydı yalnızca bellekte olduğu için köprü her yeniden
# başladığında pencere de ölüyordu — ASC'de oturum sürecin içinde yaşadığından
# bu «yine şifre iste» demekti. Çözüm: geckodriver'ı ayrık bir oturum grubunda
# başlat (köprüyle birlikte ölmesin), oturum kimliğini diske yaz, sonraki
# süreçte yeni oturum açmak yerine mevcuduna bağlan.
#
# Her adım güvenli düşer: bağlanma başarısız olursa normal açılışa dönülür,
# yani en kötü ihtimalle bugünkü davranış.

def firefox_detached_enabled() -> bool:
    raw = (os.environ.get("SELENIUM_DETACHED") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return firefox_keep_window_open()


def _detach_state_path(profile: Path) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", Path(str(profile)).name or "profile")
    return STATE_DIR / "state" / f"warm-driver-{safe}.json"


def _read_detach_state(profile: Path) -> dict[str, Any] | None:
    import json

    path = _detach_state_path(profile)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    return data if isinstance(data, dict) else None


def _write_detach_state(profile: Path, data: dict[str, Any]) -> None:
    import json

    path = _detach_state_path(profile)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


def _clear_detach_state(profile: Path) -> None:
    try:
        _detach_state_path(profile).unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass


def _resolve_geckodriver() -> str | None:
    """geckodriver yolu — PATH, sonra Selenium Manager önbelleği."""
    found = shutil.which("geckodriver")
    if found:
        return found
    try:
        from selenium.webdriver.common.selenium_manager import SeleniumManager

        out = SeleniumManager().binary_paths(["--browser", "firefox"])
        path = str((out or {}).get("driver_path") or "").strip()
        return path or None
    except Exception:  # noqa: BLE001
        return None


def _port_open(port: int, timeout: float = 0.35) -> bool:
    import socket

    try:
        socket.create_connection(("127.0.0.1", int(port)), timeout).close()
        return True
    except OSError:
        return False


def _free_port() -> int:
    import socket

    s = socket.socket()
    try:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


def _firefox_options(
    profile: Path,
    *,
    exe: str,
    headed: bool,
    download_dir: Path,
    prefs: dict[str, Any] | None = None,
) -> Any:
    from selenium.webdriver.firefox.options import Options

    opts = Options()
    opts.binary_location = exe
    # geckodriver, moz:debuggerAddress istendiğinde Firefox'a SABİT
    # `--remote-debugging-port 9222` verir. Tek pencereyle sorun değildi; artık
    # birden çok profil aynı anda açık kaldığı için ikinci pencere
    # NS_ERROR_SOCKET_ADDRESS_IN_USE alıp kapanıyordu. CDP'yi hiç kullanmıyoruz.
    opts.set_capability("moz:debuggerAddress", False)
    opts.add_argument("-profile")
    opts.add_argument(str(profile))
    if not headed:
        opts.add_argument("-headless")
    # CSV export indirilsin
    opts.set_preference("browser.download.folderList", 2)
    opts.set_preference("browser.download.dir", str(download_dir))
    opts.set_preference("browser.download.useDownloadDir", True)
    opts.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        "text/csv,application/csv,application/vnd.ms-excel,text/plain",
    )
    opts.set_preference("pdfjs.disabled", True)
    opts.set_preference("browser.download.manager.showWhenStarting", False)
    opts.set_preference("browser.helperApps.alwaysAsk.force", False)
    # Çağırana özel tercihler (ör. ASC'de service worker kapatma — Playwright
    # bağlamı `service_workers: "block"` ile aynı işi yapıyordu; Selenium'a
    # geçince bu güvence kaybolmuş ve ASC giriş bileşeni bayat bir worker
    # yüzünden sonsuz spinner'da kalmıştı).
    for key, value in (prefs or {}).items():
        opts.set_preference(str(key), value)
    return opts


def _attached_driver(url: str, session_id: str, options: Any) -> Any:
    """Yeni oturum açmadan, diskte kayıtlı oturuma bağlan."""
    from selenium import webdriver

    class _Attached(webdriver.Remote):
        def start_session(self, capabilities):  # noqa: ANN001, ANN202
            self.session_id = session_id
            self.caps = {}

    return _Attached(command_executor=url, options=options)


def _attach_detached_driver(
    profile: Path, *, headed: bool, download_dir: Path, page_load_timeout: int
) -> Any | None:
    """Önceki süreçten kalan pencereye bağlan; olmazsa None."""
    state = _read_detach_state(profile)
    if not state:
        return None
    url = str(state.get("url") or "")
    session_id = str(state.get("session") or "")
    port = int(state.get("port") or 0)
    if not url or not session_id or not port or not _port_open(port):
        _clear_detach_state(profile)
        return None
    # Açılışta sabitlenen ayarlar tutmuyorsa bağlanmak yanlış olur
    if bool(state.get("headed", True)) != headed:
        return None
    if Path(str(state.get("download_dir") or "")) != download_dir:
        return None

    exe = resolve_system_firefox_executable() or ""
    try:
        driver = _attached_driver(
            url, session_id,
            _firefox_options(profile, exe=exe, headed=headed, download_dir=download_dir),
        )
        _ = driver.current_url  # oturum gerçekten canlı mı
    except Exception:  # noqa: BLE001
        # Oturum ölmüş ama geckodriver ayakta kalmış olabilir; ayrık
        # başlattığımız için kimse toplamıyor, biriktikçe port sızdırıyor.
        shutdown_detached_firefox(profile)
        return None
    try:
        driver.set_page_load_timeout(page_load_timeout)
    except Exception:  # noqa: BLE001
        pass
    driver._seo_profile = profile  # type: ignore[attr-defined]
    driver._seo_download_dir = download_dir  # type: ignore[attr-defined]
    driver._seo_headed = headed  # type: ignore[attr-defined]
    driver._seo_detached = True  # type: ignore[attr-defined]
    return driver


def _spawn_detached_driver(
    profile: Path, *, exe: str, headed: bool, download_dir: Path, page_load_timeout: int
) -> Any | None:
    """geckodriver'ı ayrık başlat + yeni oturum aç; olmazsa None (normal yola düş)."""
    gecko = _resolve_geckodriver()
    if not gecko:
        return None
    port = _free_port()
    log_dir = STATE_DIR / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_fh = open(log_dir / f"geckodriver-{Path(str(profile)).name}.log", "ab")  # noqa: SIM115
    except Exception:  # noqa: BLE001
        log_fh = subprocess.DEVNULL  # type: ignore[assignment]
    try:
        proc = subprocess.Popen(  # noqa: S603
            [gecko, "--port", str(port)],
            stdout=log_fh, stderr=log_fh, stdin=subprocess.DEVNULL,
            start_new_session=True,  # köprü ölünce birlikte ölmesin
        )
    except Exception:  # noqa: BLE001
        return None

    deadline = time.time() + 12
    while time.time() < deadline and not _port_open(port):
        if proc.poll() is not None:
            return None
        time.sleep(0.2)
    if not _port_open(port):
        return None

    url = f"http://127.0.0.1:{port}"
    try:
        from selenium import webdriver

        driver = webdriver.Remote(
            command_executor=url,
            options=_firefox_options(profile, exe=exe, headed=headed, download_dir=download_dir),
        )
    except Exception:  # noqa: BLE001
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        return None

    try:
        driver.set_page_load_timeout(page_load_timeout)
    except Exception:  # noqa: BLE001
        pass
    driver._seo_profile = profile  # type: ignore[attr-defined]
    driver._seo_download_dir = download_dir  # type: ignore[attr-defined]
    driver._seo_headed = headed  # type: ignore[attr-defined]
    driver._seo_detached = True  # type: ignore[attr-defined]
    _write_detach_state(profile, {
        "url": url,
        "port": port,
        "session": driver.session_id,
        "gecko_pid": proc.pid,
        "headed": headed,
        "download_dir": str(download_dir),
    })
    return driver


def _session_alive(url: str, session_id: str, timeout: float = 2.5) -> bool:
    """geckodriver'daki oturum gerçekten kullanılabilir mi?

    Port açık olması yetmiyor: Firefox öldüğünde geckodriver ayakta kalıyor
    (ayrık başlattığımız için kimse toplamıyor) ve port hâlâ cevap veriyor.
    Bu yüzden oturuma gerçek bir WebDriver çağrısı yapılır.
    """
    if not url or not session_id:
        return False
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(  # noqa: S310
            f"{url}/session/{session_id}/url", timeout=timeout
        ) as resp:
            return 200 <= int(resp.status) < 300
    except urllib.error.HTTPError:
        return False  # 404/500 → oturum yok
    except Exception:  # noqa: BLE001
        return False


def detached_window_is_live(profile: Path) -> bool:
    """Bu profil için bilerek açık bırakılmış, ÇALIŞAN ayrık pencere var mı?

    Köprü açılışındaki kalıntı temizliği bunu sormadan öldürüyordu; korumaya
    çalıştığımız oturum her yeniden başlatmada gidiyordu. Ama yalnızca porta
    bakmak da yanlış: ölü Firefox + yaşayan geckodriver "canlı" görünüyor ve
    temizlik gerçekten kalıntı olan profili atlıyordu.
    """
    state = _read_detach_state(Path(str(profile)).expanduser())
    if not state:
        return False
    port = int(state.get("port") or 0)
    if not port or not _port_open(port):
        return False
    return _session_alive(str(state.get("url") or ""), str(state.get("session") or ""))


def shutdown_detached_firefox(profile: Path) -> bool:
    """Ayrık pencereyi bilerek kapat (teşhis / temizlik)."""
    state = _read_detach_state(profile)
    _clear_detach_state(profile)
    if not state:
        return False
    pid = int(state.get("gecko_pid") or 0)
    if pid > 0:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:  # noqa: BLE001
            return False
    _WARM_DRIVERS.pop(_driver_profile_key(profile), None)
    return True


def _reusable_warm_driver(
    profile: Path,
    *,
    headed: bool,
    download_dir: Path,
    page_load_timeout: int,
) -> Any | None:
    """Aynı profil/başlık/indirme dizini için canlı pencere varsa onu döndür."""
    if not firefox_keep_window_open():
        return None
    key = _driver_profile_key(profile)
    warm = _WARM_DRIVERS.get(key)
    if warm is None:
        return None
    if not _driver_alive(warm):
        _WARM_DRIVERS.pop(key, None)  # ölü kayıt — yenisi açılsın
        return None
    # İndirme dizini ve headed durumu açılışta sabitlenir; farklıysa yeniden
    # kullanmak sessizce yanlış yere indirme yapar.
    if getattr(warm, "_seo_headed", True) != headed:
        return None
    if Path(str(getattr(warm, "_seo_download_dir", ""))) != download_dir:
        return None
    try:
        warm.set_page_load_timeout(page_load_timeout)
    except Exception:  # noqa: BLE001
        pass
    return warm


def launch_system_firefox_driver(
    profile: Path,
    *,
    headed: bool = True,
    download_dir: Path | None = None,
    page_load_timeout: int = 120,
    prefs: dict[str, Any] | None = None,
) -> Any:
    """Selenium WebDriver → yalnızca sistem Firefox.app + verilen profil."""
    assert_browser_scrape_allowed(context="launch_system_firefox_driver")
    from selenium import webdriver

    exe = resolve_system_firefox_executable()
    if not exe:
        raise RuntimeError(
            "Sistem Firefox.app yok (/Applications/Firefox.app). "
            "Playwright Nightly kullanılmaz."
        )
    if "ms-playwright" in exe or "Nightly" in exe:
        raise RuntimeError(f"Nightly yasak: {exe}")

    profile = profile.expanduser()
    profile.mkdir(parents=True, exist_ok=True)

    if profile_login_lock_active(profile):
        raise RuntimeError(f"Login kilidi aktif: {profile}")

    dl = download_dir or (STATE_DIR / "cache" / "downloads")
    dl.mkdir(parents=True, exist_ok=True)

    # Sıcak pencere kontrolü, profili boşaltan adımlardan ÖNCE olmalı; aksi
    # halde korumaya çalıştığımız pencereyi kendimiz kapatırız.
    warm = _reusable_warm_driver(
        profile,
        headed=headed,
        download_dir=dl,
        page_load_timeout=page_load_timeout,
    )
    if warm is not None:
        print(
            f"Firefox: mevcut pencere yeniden kullanılıyor ({profile.name}) — oturum korunuyor",
            flush=True,
        )
        return warm

    # Önceki süreçten (köprü yeniden başlamış olabilir) kalan pencere
    if firefox_detached_enabled():
        attached = _attach_detached_driver(
            profile, headed=headed, download_dir=dl, page_load_timeout=page_load_timeout
        )
        if attached is not None:
            print(
                f"Firefox: önceki süreçten kalan pencereye bağlanıldı ({profile.name}) — "
                "oturum korunuyor",
                flush=True,
            )
            _WARM_DRIVERS[_driver_profile_key(profile)] = attached
            return attached

    ban_playwright_nightly_processes(profile)

    ensure_profile_free_for_launch(
        profile,
        takeover=True,
        reason="system_firefox_driver",
    )

    # Ayrık açılış: pencere köprüden bağımsız yaşasın. Başarısız olursa
    # aşağıdaki normal açılışa düşer — en kötü ihtimalle bugünkü davranış.
    if firefox_detached_enabled():
        _clear_detach_state(profile)
        spawned = _spawn_detached_driver(
            profile, exe=exe, headed=headed, download_dir=dl,
            page_load_timeout=page_load_timeout,
        )
        if spawned is not None:
            print(
                f"Firefox: ayrık pencere açıldı ({profile.name}) — köprü yeniden "
                "başlasa da oturum yaşar",
                flush=True,
            )
            _WARM_DRIVERS[_driver_profile_key(profile)] = spawned
            return spawned
        print(
            f"Firefox: ayrık açılış olmadı ({profile.name}), normal açılışa dönülüyor",
            flush=True,
        )

    opts = _firefox_options(profile, exe=exe, headed=headed, download_dir=dl, prefs=prefs)
    driver = webdriver.Firefox(options=opts)
    driver.set_page_load_timeout(page_load_timeout)
    driver._seo_profile = profile  # type: ignore[attr-defined]
    driver._seo_download_dir = dl  # type: ignore[attr-defined]
    driver._seo_headed = headed  # type: ignore[attr-defined]
    if firefox_keep_window_open():
        _WARM_DRIVERS[_driver_profile_key(profile)] = driver
    return driver


def quit_system_firefox_driver(driver: Any) -> None:
    """Pencereyi kapat — sıcak pencere açıkken kapatma, oturum yaşasın."""
    profile = getattr(driver, "_seo_profile", None)
    if firefox_keep_window_open() and _driver_alive(driver):
        name = Path(str(profile)).name if profile is not None else "?"
        print(
            f"Firefox: pencere açık bırakıldı ({name}) — sonraki tarama buradan "
            "devam eder; kapatmak için SELENIUM_KEEP_OPEN=0",
            flush=True,
        )
        return
    for key, val in list(_WARM_DRIVERS.items()):
        if val is driver:
            _WARM_DRIVERS.pop(key, None)
    try:
        driver.quit()
    except Exception:
        pass
    if profile is not None:
        # Ayrık geckodriver quit() ile ölmez; kaydı da bırakmayalım
        if getattr(driver, "_seo_detached", False):
            shutdown_detached_firefox(Path(profile))
        align_firefox_profile_compatibility(Path(profile))


def inject_google_cookies_from_default(driver: Any) -> int:
    """Açık driver'a günlük Firefox Google çerezlerini ekle."""
    src = default_firefox_profile_dir()
    if not src:
        return 0
    cookies = _read_google_cookies(src)
    session_names = {c["name"] for c in cookies}
    if not session_names.intersection(
        {"SID", "HSID", "SSID", "APISID", "SAPISID", "__Secure-1PSID", "__Secure-3PSID", "LSID"}
    ):
        return 0
    try:
        driver.get("https://www.google.com/")
        time.sleep(1.0)
    except Exception:
        return 0
    n = 0
    for c in cookies:
        payload = {
            "name": c["name"],
            "value": c["value"],
            "path": c["path"],
            "secure": c["secure"],
        }
        dom = c["domain"]
        if dom.startswith("."):
            payload["domain"] = dom
        else:
            payload["domain"] = dom
        try:
            if c.get("expiry"):
                payload["expiry"] = c["expiry"]
            driver.add_cookie(payload)
            n += 1
        except Exception:
            continue
    return n
