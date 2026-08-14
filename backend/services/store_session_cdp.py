"""Play Console / App Store Connect — kalıcı Firefox oturumu.

Tarama pencereleri Firefox’tur (Chrome / Chromium / Chrome for Testing yok).
Çerezler ~/.seo-agent/fx-google ve fx-asc altında kalır.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

STATE_DIR = Path.home() / ".seo-agent"

KIND_DEFAULTS: dict[str, dict[str, Any]] = {
    "play": {
        "port": 9222,
        "port_env": "PLAY_CONSOLE_CDP_PORT",
        "profile_env": "PLAY_CONSOLE_PROFILE_DIR",
        "profile_name": "fx-google",
        "channel_env": "PLAY_CONSOLE_BROWSER_CHANNEL",
        "ping_env": "PLAY_CONSOLE_DASHBOARD_URL",
        "ping_fallback": "https://play.google.com/console",
        "login_hints": ("accounts.google.com", "signin"),
    },
    "asc": {
        "port": 9223,
        "port_env": "ASC_CONSOLE_CDP_PORT",
        "profile_env": "ASC_CONSOLE_PROFILE_DIR",
        "profile_name": "fx-asc",
        "channel_env": "ASC_CONSOLE_BROWSER_CHANNEL",
        "ping_env": "ASC_CONSOLE_PING_URL",
        "ping_fallback": "https://appstoreconnect.apple.com/apps/465599322/analytics/metrics",
        "login_hints": ("idmsa.apple.com", "appleid.apple.com", "sign-in", "signin"),
    },
}


def _kind_cfg(kind: str) -> dict[str, Any]:
    if kind not in KIND_DEFAULTS:
        raise ValueError(f"Bilinmeyen oturum türü: {kind}")
    return KIND_DEFAULTS[kind]


def cdp_port(kind: str) -> int:
    cfg = _kind_cfg(kind)
    raw = (os.environ.get(cfg["port_env"]) or "").strip()
    if raw.isdigit():
        return int(raw)
    return int(cfg["port"])


def cdp_url(kind: str) -> str:
    return f"http://127.0.0.1:{cdp_port(kind)}"


def endpoint_path(kind: str) -> Path:
    return STATE_DIR / f"{kind}-cdp.json"


def profile_dir(kind: str) -> Path:
    from backend.services.scrape_browser import asc_profile_dir, google_profile_dir

    if kind == "asc":
        return asc_profile_dir()
    return google_profile_dir()


def ping_url(kind: str) -> str:
    cfg = _kind_cfg(kind)
    return (os.environ.get(cfg["ping_env"]) or cfg["ping_fallback"]).strip()


def cdp_alive(kind: str, *, timeout: float = 1.5) -> bool:
    url = cdp_url(kind) + "/json/version"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return int(getattr(resp, "status", 200) or 200) < 400
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def write_endpoint(kind: str, *, pid: int) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "kind": kind,
        "url": cdp_url(kind),
        "port": cdp_port(kind),
        "pid": pid,
        "ts": int(time.time()),
    }
    endpoint_path(kind).write_text(json.dumps(payload), encoding="utf-8")


def chrome_bin() -> str | None:
    env = (os.environ.get("STORE_SESSION_CHROME_BIN") or "").strip()
    if env and Path(env).is_file():
        return env
    mac = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    if mac.is_file():
        return str(mac)
    return None


def _clear_profile_locks(prof: Path) -> None:
    from backend.services.scrape_browser import clear_stale_profile_locks

    clear_stale_profile_locks(prof)


def _kill_profile_browsers(prof: Path) -> int:
    """Yalnızca eski Chrome süreçleri — Firefox soft-release scrape_browser'da."""
    marker = str(prof.resolve())
    killed = 0
    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        return 0
    for line in out.splitlines():
        if marker not in line:
            continue
        low = line.lower()
        if "chromium" not in low and "google chrome" not in low and "chrome" not in low:
            continue
        if "firefox" in low:
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
        time.sleep(0.6)
        _clear_profile_locks(prof)
    return killed


def ensure_chrome(kind: str) -> bool:
    """Eski Chrome CDP yolu kapalı — tarama Firefox."""
    return False


def attach_or_launch(
    kind: str,
    *,
    headed: bool,
    extra_kwargs: dict[str, Any] | None = None,
) -> tuple[Any, Any, bool]:
    """Firefox persistent profil. attached her zaman False (CDP yok)."""
    from playwright.sync_api import sync_playwright

    from backend.services.scrape_browser import launch_persistent

    pw = sync_playwright().start()
    prof = profile_dir(kind)
    extra = dict(extra_kwargs or {})
    extra.pop("args", None)
    extra.pop("channel", None)
    locale = "tr-TR" if kind == "asc" else "en-US"
    context = launch_persistent(pw, prof, headed=headed, locale=locale, extra=extra or None)
    try:
        write_endpoint(kind, pid=os.getpid())
    except Exception:
        pass
    return pw, context, False


def release_browser(pw: Any, context: Any, *, attached: bool) -> None:
    """Firefox penceresini kapat (profil diskte kalır)."""
    if attached:
        try:
            pw.stop()
        except Exception:
            pass
        return
    try:
        context.close()
    except Exception:
        pass
    try:
        pw.stop()
    except Exception:
        pass


def _url_looks_logged_out(kind: str, url: str) -> bool:
    low = (url or "").lower()
    return any(h in low for h in _kind_cfg(kind)["login_hints"])


def run_keeper(kind: str, *, ping_sec: int | None = None, stop_event: Any = None) -> None:
    """Eski Chrome bekçi kapalı — Firefox profili diskte kalır, pencere tarama anında açılır."""
    return None


def start_keeper_threads(*, ping_sec: int | None = None) -> list[Any]:
    from backend.services.scrape_browser import kill_legacy_chrome_scrapers

    n = kill_legacy_chrome_scrapers()
    print(
        f"Tarama tarayıcısı Firefox · eski Chrome tarama pencereleri kapatıldı ({n})",
        flush=True,
    )
    return []
