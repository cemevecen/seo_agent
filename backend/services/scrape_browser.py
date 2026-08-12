"""Tarama tarayıcısı — yalnızca Firefox.

Chrome / Chromium / Chrome for Testing açılmaz. Google oturumu
~/.seo-agent/fx-google altında tutulur (Play, GSC, Firebase, Policy).
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

STATE_DIR = Path.home() / ".seo-agent"

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


def kill_profile_browsers(profile: Path) -> int:
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
    """Opsiyonel Firefox binary.

    PLAYWRIGHT_FIREFOX_EXECUTABLE verilirse onu kullan.
    Aksi halde Playwright'ın kendi sürümünü bırak (juggler uyumu için);
    farklı revision zorlamak TargetClosed / NS_ERROR_FAILURE üretebiliyor.
    """
    env = (os.environ.get("PLAYWRIGHT_FIREFOX_EXECUTABLE") or "").strip()
    if env:
        p = Path(env).expanduser()
        if p.is_file():
            return str(p)
    return None


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
) -> Any:
    assert_firefox_only(pw)
    kill_profile_browsers(profile)
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
