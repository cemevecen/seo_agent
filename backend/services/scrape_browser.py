"""Tarama tarayıcısı — yalnızca Firefox.

Chrome / Chromium / Chrome for Testing açılmaz. Google oturumu
~/.seo-agent/fx-google altında tutulur (Play, GSC, Firebase, Policy).
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

STATE_DIR = Path.home() / ".seo-agent"

# Elle giriş bekleyen tüm taramalar (ASC / Play / Firebase / GSC / Policy…)
# Update page & headed sync: 15 dakika; giriş sonrası aynı pencerede kazıma devam eder.
LOGIN_WAIT_SEC = 900

_NAV_URL_BAD_PREFIX = re.compile(r"^[a-z0-9._+-]+(?=https?://)", re.I)


def login_wait_sec(*, env_key: str | None = None, default: int = LOGIN_WAIT_SEC) -> int:
    """Giriş bekleme süresi (sn). Env varsa onu kullanır; taban en az 900 (15 dk)."""
    raw = ""
    if env_key:
        raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        raw = (os.environ.get("SCRAPE_LOGIN_WAIT_SEC") or "").strip()
    try:
        val = int(raw) if raw else int(default)
    except ValueError:
        val = int(default)
    return max(LOGIN_WAIT_SEC, val)


def normalize_nav_url(raw: str, *, fallback: str = "") -> str:
    """Geçersiz önekleri temizle (örn. cemhttps:// → https://)."""
    u = (raw or "").strip()
    if not u:
        fb = (fallback or "").strip()
        if not fb:
            raise ValueError("nav url empty")
        return normalize_nav_url(fb)
    u = _NAV_URL_BAD_PREFIX.sub("", u)
    if u.startswith("//"):
        return "https:" + u
    if not re.match(r"https?://", u, re.I):
        u = "https://" + u.lstrip("/")
    return u


def google_blocks_automation_text(body: str) -> bool:
    low = (body or "")[:4000].lower()
    needles = (
        "may not be secure",
        "couldn't sign you in",
        "couldn’t sign you in",
        "güvenli olmayabilir",
        "güvenli değil",
        "oturum açmanız mümkün değil",
        "this browser or app",
    )
    return any(n in low for n in needles)

_BROWSER_MARKERS = (
    "chrome",
    "chromium",
    "firefox",
    "gecko",
    "headless_shell",
    "chrome for testing",
)


def _fx_dir(name: str) -> Path:
    return (STATE_DIR / name).expanduser()


def _from_env_or_fx(env_keys: tuple[str, ...], fx_name: str) -> Path:
    for key in env_keys:
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        path = Path(raw).expanduser()
        if "fx-" in path.name:
            return path
        return path.parent / fx_name
    return _fx_dir(fx_name)


def google_profile_dir() -> Path:
    return _from_env_or_fx(
        ("PLAY_CONSOLE_PROFILE_DIR", "GSC_CWV_PROFILE_DIR", "GSC_LINKS_PROFILE_DIR"),
        "fx-google",
    )


def asc_profile_dir() -> Path:
    return _from_env_or_fx(("ASC_CONSOLE_PROFILE_DIR",), "fx-asc")


def firebase_profile_dir() -> Path:
    return _from_env_or_fx(("FIREBASE_CONSOLE_PROFILE_DIR",), "fx-google")


def sinemalar_profile_dir() -> Path:
    return _from_env_or_fx(("SINEMALAR_NOADS_PROFILE_DIR",), "fx-sinemalar")


def empower_profile_dir() -> Path:
    """Empower Intelligence Cognito oturumu — Google fx-google'dan ayrı."""
    return _from_env_or_fx(("EMPOWER_INTEL_PROFILE_DIR",), "fx-empower")


def profile_login_lock_path(profile: Path) -> Path:
    return profile.expanduser().resolve().parent / f"{profile.expanduser().resolve().name}.login-lock"


def acquire_profile_login_lock(profile: Path, *, reason: str = "login") -> Path:
    """Manuel --login sırasında diğer scrape'lerin SIGTERM atmasını engelle."""
    lock = profile_login_lock_path(profile)
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(f"{reason}\npid={os.getpid()}\nts={time.time():.0f}\n", encoding="utf-8")
    return lock


def release_profile_login_lock(profile: Path) -> None:
    try:
        profile_login_lock_path(profile).unlink(missing_ok=True)
    except Exception:
        pass


def profile_login_lock_active(profile: Path) -> bool:
    lock = profile_login_lock_path(profile)
    if not lock.is_file():
        return False
    try:
        age = time.time() - lock.stat().st_mtime
    except Exception:
        return True
    # 20 dk'dan eski kilitleri yok say (çökmüş login)
    if age > 20 * 60:
        try:
            lock.unlink(missing_ok=True)
        except Exception:
            pass
        return False
    return True


def kill_profile_browsers(profile: Path) -> int:
    if profile_login_lock_active(profile):
        return 0
    marker = str(profile.resolve())
    killed = 0
    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        return 0
    for line in out.splitlines():
        if marker not in line:
            continue
        low = line.lower()
        if not any(m in low for m in _BROWSER_MARKERS):
            continue
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


def kill_legacy_chrome_scrapers() -> int:
    """Eski Chrome / Chromium / Chrome for Testing tarama süreçlerini kapat.

    Kişisel Google Chrome.app ve Cursor Browser DevTools profiline dokunmaz.
    """
    n = 0
    for name in (
        "play-console-profile",
        "asc-console-profile",
        "firebase-console-profile",
        "sinemalar-admin-profile",
        "fx-google",
        "fx-asc",
        "fx-sinemalar",
    ):
        n += kill_profile_browsers(STATE_DIR / name)
    # STATE_DIR'in tamamını tarama — ~/.seo-agent yolu canlı Firefox'u da SIGTERM eder.

    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        return n
    for line in out.splitlines():
        low = line.lower()
        is_pw_chrome = (
            "chrome for testing" in low
            or "ms-playwright/chromium" in low
            or "chromium_headless_shell" in low
        )
        if not is_pw_chrome:
            continue
        # Cursor MCP: Google Chrome.app + playwright_chromiumdev_profile — dokunma
        if "playwright_chromiumdev_profile" in low:
            continue
        if "/applications/google chrome.app" in low and "chrome for testing" not in low:
            continue
        try:
            pid = int(line.split(None, 1)[0])
        except Exception:
            continue
        if pid <= 1 or pid == os.getpid():
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            n += 1
        except (ProcessLookupError, PermissionError, OSError):
            pass
    if n:
        time.sleep(0.6)
    return n


def assert_firefox_only(pw: Any) -> None:
    """Yanlışlıkla chromium API'sine düşülmesin diye koruma."""
    if not hasattr(pw, "firefox"):
        raise RuntimeError("Playwright Firefox yok — `playwright install firefox`")


def resolve_firefox_executable() -> str | None:
    """Opsiyonel Firefox binary (Playwright).

    PLAYWRIGHT_FIREFOX_EXECUTABLE verilirse onu kullan — ms-playwright/Nightly
    path'leri reddedilir. Google Sheets scrape için `system_firefox_driver`
    (Selenium + Firefox.app) kullanın; Playwright Nightly Google'da engellenir.
    """
    env = (os.environ.get("PLAYWRIGHT_FIREFOX_EXECUTABLE") or "").strip()
    if env:
        p = Path(env).expanduser()
        if p.is_file() and "ms-playwright" not in str(p) and "Nightly" not in str(p):
            return str(p)
    return None


def resolve_system_firefox_executable() -> str | None:
    """Gerçek Firefox.app — Google login için (Playwright Nightly'yi 'insecure' sayıyor)."""
    env = (os.environ.get("SYSTEM_FIREFOX_EXECUTABLE") or "").strip()
    if env:
        p = Path(env).expanduser()
        if p.is_file():
            return str(p)
    mac = Path("/Applications/Firefox.app/Contents/MacOS/firefox")
    if mac.is_file():
        return str(mac)
    return None


def launch_system_firefox_login(
    profile: Path,
    url: str,
    *,
    timeout_sec: int = LOGIN_WAIT_SEC,
    success_hint: str = "",
    verify_session: bool = True,
) -> dict[str, Any]:
    """Google hesabı için gerçek Firefox.app + aynı fx-* profil.

    Playwright Nightly / juggler Google'da 'This browser may not be secure' alır.
    Firefox kapanınca (verify_session) Google oturum çerezi kontrol edilir.
    """
    exe = resolve_system_firefox_executable()
    if not exe:
        raise RuntimeError(
            "Gerçek Firefox.app yok — /Applications/Firefox.app kurulu olmalı "
            "(Playwright Nightly ile Google giriş engelleniyor)"
        )
    profile = profile.expanduser()
    profile.mkdir(parents=True, exist_ok=True)
    # Önce Playwright Nightly'yi kapat (kilit henüz yok)
    kill_profile_browsers(profile)
    time.sleep(0.4)
    for name in (".parentlock", "lock", "SingletonLock"):
        try:
            (profile / name).unlink(missing_ok=True)
        except Exception:
            pass

    # Login sırasında scrapeler bu profili öldürmesin / çakışmasın
    acquire_profile_login_lock(profile, reason="system_firefox_login")
    try:
        cmd = [exe, "-no-remote", "-profile", str(profile), url]
        print(f"Sistem Firefox · {exe}\nprofil={profile}\nurl={url}", flush=True)
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        timeout_sec = max(LOGIN_WAIT_SEC, int(timeout_sec or LOGIN_WAIT_SEC))
        deadline = time.time() + timeout_sec
        hint = (success_hint or "").strip() or (
            f"cemevecen@nokta.com ile giriş yap → hedef sayfa açılsın → Firefox penceresini KAPAT "
            f"(en fazla {timeout_sec // 60} dk)."
        )
        print(f"{hint}\n(en fazla {timeout_sec // 60} dk)", flush=True)
        while time.time() < deadline:
            rc = proc.poll()
            if rc is not None:
                align_firefox_profile_compatibility(profile)
                if verify_session:
                    from backend.services.system_firefox_driver import google_profile_has_session

                    if not google_profile_has_session(profile):
                        return {
                            "ok": False,
                            "exit_code": rc,
                            "profile": str(profile),
                            "mode": "system_firefox",
                            "message": (
                                "Firefox kapandı ama Google oturumu kaydedilmedi. "
                                "Giriş formunda cemevecen@nokta.com + 2FA tamamla; "
                                "hedef sayfa (Play Console dashboard) görünmeden kapatma."
                            ),
                        }
                return {"ok": True, "exit_code": rc, "profile": str(profile), "mode": "system_firefox"}
            time.sleep(1.5)
        try:
            proc.terminate()
        except Exception:
            pass
        align_firefox_profile_compatibility(profile)
        return {"ok": False, "message": "login timeout — Firefox kapatılmadı", "profile": str(profile)}
    finally:
        release_profile_login_lock(profile)


def align_firefox_profile_compatibility(profile: Path) -> None:
    """Profil daha yeni Firefox ile açıldıysa, mevcut Playwright binary'sine izin ver."""
    ini = profile / "compatibility.ini"
    if not ini.is_file():
        return
    try:
        text = ini.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return
    # Downgrade engelini kaldır — bir sonraki açılış LastVersion'ı yeniden yazar
    if "LastVersion=" not in text:
        return
    try:
        ini.write_text(
            "[Compatibility]\n"
            "LastVersion=0\n"
            "LastOSABI=Darwin_aarch64-gcc3\n",
            encoding="utf-8",
        )
    except Exception:
        pass


def _persistent_kwargs(
    profile: Path,
    *,
    headed: bool,
    viewport: dict[str, int] | None = None,
    locale: str = "tr-TR",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile.mkdir(parents=True, exist_ok=True)
    align_firefox_profile_compatibility(profile)
    kwargs: dict[str, Any] = {
        "user_data_dir": str(profile),
        "headless": not headed,
        "viewport": viewport or {"width": 1440, "height": 1100},
        "locale": locale,
        "accept_downloads": True,
    }
    exe = resolve_firefox_executable()
    if exe:
        kwargs["executable_path"] = exe
    if extra:
        extra = dict(extra)
        extra.pop("channel", None)
        extra.pop("args", None)
        extra.pop("ignore_default_args", None)
        kwargs.update(extra)
    return kwargs


def launch_persistent(
    pw: Any,
    profile: Path,
    *,
    headed: bool = True,
    viewport: dict[str, int] | None = None,
    locale: str = "tr-TR",
    extra: dict[str, Any] | None = None,
    kill_existing: bool = True,
) -> Any:
    assert_firefox_only(pw)
    # Manuel --login kilidi varken scrape yeni pencere açmasın (profil çakışması).
    if kill_existing and profile_login_lock_active(profile):
        raise RuntimeError(
            f"Login kilidi aktif: {profile_login_lock_path(profile)} — "
            "manuel giriş bitene kadar scrapeyi ertele"
        )
    # Login sırasında False: başka scrape SIGTERM ile pencereyi 2–3 sn'de kapatmasın.
    if kill_existing:
        kill_profile_browsers(profile)
    else:
        for name in (".parentlock", "lock", "SingletonLock"):
            try:
                (profile / name).unlink(missing_ok=True)
            except Exception:
                pass
    kwargs = _persistent_kwargs(
        profile, headed=headed, viewport=viewport, locale=locale, extra=extra
    )
    return pw.firefox.launch_persistent_context(**kwargs)


def launch_ephemeral(
    pw: Any,
    *,
    headed: bool = False,
    **context_kwargs: Any,
) -> tuple[Any, Any]:
    assert_firefox_only(pw)
    launch_kwargs: dict[str, Any] = {"headless": not headed}
    exe = resolve_firefox_executable()
    if exe:
        launch_kwargs["executable_path"] = exe
    browser = pw.firefox.launch(**launch_kwargs)
    context_kwargs.pop("channel", None)
    context_kwargs.pop("args", None)
    ctx = browser.new_context(**context_kwargs)
    return browser, ctx
