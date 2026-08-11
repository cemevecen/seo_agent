"""Play Console / App Store Connect — kalıcı Chrome (CDP).

Mac köprüsü Chrome’u kapatmadan açık tutar; tarama aynı sürece bağlanır.
Google/Apple her seferinde yeni giriş saymasın, oturum çerezi yaşasın.
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
        "profile_name": "play-console-profile",
        "channel_env": "PLAY_CONSOLE_BROWSER_CHANNEL",
        "ping_env": "PLAY_CONSOLE_DASHBOARD_URL",
        "ping_fallback": "https://play.google.com/console",
        "login_hints": ("accounts.google.com", "signin"),
    },
    "asc": {
        "port": 9223,
        "port_env": "ASC_CONSOLE_CDP_PORT",
        "profile_env": "ASC_CONSOLE_PROFILE_DIR",
        "profile_name": "asc-console-profile",
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
    cfg = _kind_cfg(kind)
    raw = (os.environ.get(cfg["profile_env"]) or "").strip()
    if raw:
        return Path(raw).expanduser()
    return (STATE_DIR / str(cfg["profile_name"])).expanduser()


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
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (prof / name).unlink(missing_ok=True)
        except Exception:
            pass


def _kill_profile_browsers(prof: Path) -> int:
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
    """Sistem Chrome’unu CDP portunda ayakta tut (kapatma)."""
    if cdp_alive(kind):
        return True
    prof = profile_dir(kind)
    prof.mkdir(parents=True, exist_ok=True)
    _kill_profile_browsers(prof)
    _clear_profile_locks(prof)
    binary = chrome_bin()
    if not binary:
        return False
    log_path = STATE_DIR / f"{kind}-chrome.log"
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    log_f = open(log_path, "ab")  # noqa: SIM115
    cmd = [
        binary,
        f"--user-data-dir={prof}",
        f"--remote-debugging-port={cdp_port(kind)}",
        "--disable-blink-features=AutomationControlled",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-session-crashed-bubble",
        ping_url(kind),
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=log_f,
        start_new_session=True,
    )
    write_endpoint(kind, pid=int(proc.pid or 0))
    for _ in range(50):
        if cdp_alive(kind):
            return True
        time.sleep(0.2)
    return cdp_alive(kind)


def attach_or_launch(
    kind: str,
    *,
    headed: bool,
    extra_kwargs: dict[str, Any] | None = None,
) -> tuple[Any, Any, bool]:
    """CDP canlıysa bağlan (Chrome açık kalır); değilse persistent launch.

    Dönüş: (playwright, context, attached)
    """
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    if ensure_chrome(kind) and cdp_alive(kind):
        browser = pw.chromium.connect_over_cdp(cdp_url(kind))
        if not browser.contexts:
            return pw, browser.new_context(), True
        return pw, browser.contexts[0], True

    cfg = _kind_cfg(kind)
    prof = profile_dir(kind)
    prof.mkdir(parents=True, exist_ok=True)
    _kill_profile_browsers(prof)
    _clear_profile_locks(prof)
    channel = (os.environ.get(cfg["channel_env"]) or "chrome").strip()
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(prof),
        "headless": not headed,
        "viewport": {"width": 1440, "height": 1100},
        "locale": "tr-TR",
        "accept_downloads": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            f"--remote-debugging-port={cdp_port(kind)}",
        ],
    }
    if extra_kwargs:
        extra = dict(extra_kwargs)
        extra_args = list(extra.pop("args", []) or [])
        launch_kwargs["args"] = list(dict.fromkeys([*launch_kwargs["args"], *extra_args]))
        launch_kwargs.update(extra)
    if channel and channel.lower() not in ("0", "none", "chromium"):
        launch_kwargs["channel"] = channel
    try:
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    except Exception:
        launch_kwargs.pop("channel", None)
        try:
            context = pw.chromium.launch_persistent_context(**launch_kwargs)
        except TypeError:
            launch_kwargs.pop("service_workers", None)
            context = pw.chromium.launch_persistent_context(**launch_kwargs)
    try:
        write_endpoint(kind, pid=os.getpid())
    except Exception:
        pass
    return pw, context, False


def release_browser(pw: Any, context: Any, *, attached: bool) -> None:
    """CDP bağlıysa yalnızca istemciyi bırak — Chrome kapanmaz."""
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
    """Chrome’u açık tut, arada paneli ziyaret et (çerez tazele)."""
    interval = ping_sec if ping_sec is not None else int(os.environ.get("STORE_SESSION_PING_SEC") or "5400")
    interval = max(300, interval)
    extra: dict[str, Any] = {}
    if kind == "asc":
        extra["service_workers"] = "block"
    print(f"Oturum bekçi · {kind} CDP {cdp_url(kind)} · ping {interval}s", flush=True)
    while stop_event is None or not stop_event.is_set():
        pw = context = None
        attached = False
        try:
            if not ensure_chrome(kind):
                print(f"Oturum bekçi · {kind} Chrome başlatılamadı", flush=True)
            else:
                pw, context, attached = attach_or_launch(kind, headed=True, extra_kwargs=extra or None)
                page = context.pages[0] if context.pages else context.new_page()
                target = ping_url(kind)
                page.goto(target, wait_until="domcontentloaded", timeout=120_000)
                time.sleep(4)
                url = page.url or ""
                if _url_looks_logged_out(kind, url):
                    print(
                        f"Oturum bekçi · {kind} giriş gerekli — açık pencerede bir kez giriş yapın "
                        f"(uyarı maili yok). url={url[:120]}",
                        flush=True,
                    )
                    deadline = time.time() + 15 * 60
                    while time.time() < deadline:
                        if stop_event is not None and stop_event.is_set():
                            break
                        time.sleep(5)
                        try:
                            url = page.url or ""
                        except Exception:
                            break
                        if not _url_looks_logged_out(kind, url):
                            print(f"Oturum bekçi · {kind} giriş tamam", flush=True)
                            break
                else:
                    print(f"Oturum bekçi · {kind} oturum OK", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"Oturum bekçi · {kind} hata: {exc}", flush=True)
        finally:
            if pw is not None:
                release_browser(pw, context, attached=attached)
        slept = 0
        while slept < interval:
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(min(15, interval - slept))
            slept += 15


def start_keeper_threads(*, ping_sec: int | None = None) -> list[Any]:
    import threading

    threads = []
    for kind in ("play", "asc"):
        t = threading.Thread(
            target=run_keeper,
            kwargs={"kind": kind, "ping_sec": ping_sec},
            name=f"store-session-{kind}",
            daemon=True,
        )
        t.start()
        threads.append(t)
    return threads
