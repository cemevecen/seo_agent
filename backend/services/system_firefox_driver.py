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


def ban_playwright_nightly_processes() -> int:
    """Çalışan Playwright Nightly scrape süreçlerini kapat."""
    killed = 0
    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        return 0
    for line in out.splitlines():
        if not any(m in line for m in _NIGHTLY_MARKERS):
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


def launch_system_firefox_driver(
    profile: Path,
    *,
    headed: bool = True,
    download_dir: Path | None = None,
    page_load_timeout: int = 120,
) -> Any:
    """Selenium WebDriver → yalnızca sistem Firefox.app + verilen profil."""
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options

    exe = resolve_system_firefox_executable()
    if not exe:
        raise RuntimeError(
            "Sistem Firefox.app yok (/Applications/Firefox.app). "
            "Playwright Nightly kullanılmaz."
        )
    if "ms-playwright" in exe or "Nightly" in exe:
        raise RuntimeError(f"Nightly yasak: {exe}")

    ban_playwright_nightly_processes()
    profile = profile.expanduser()
    profile.mkdir(parents=True, exist_ok=True)

    if profile_login_lock_active(profile):
        raise RuntimeError(f"Login kilidi aktif: {profile}")

    ensure_profile_free_for_launch(
        profile,
        takeover=True,
        reason="system_firefox_driver",
    )

    dl = download_dir or (STATE_DIR / "cache" / "downloads")
    dl.mkdir(parents=True, exist_ok=True)

    opts = Options()
    opts.binary_location = exe
    opts.add_argument("-profile")
    opts.add_argument(str(profile))
    if not headed:
        opts.add_argument("-headless")
    # CSV export indirilsin
    opts.set_preference("browser.download.folderList", 2)
    opts.set_preference("browser.download.dir", str(dl))
    opts.set_preference("browser.download.useDownloadDir", True)
    opts.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        "text/csv,application/csv,application/vnd.ms-excel,text/plain",
    )
    opts.set_preference("pdfjs.disabled", True)
    opts.set_preference("browser.download.manager.showWhenStarting", False)
    opts.set_preference("browser.helperApps.alwaysAsk.force", False)

    driver = webdriver.Firefox(options=opts)
    driver.set_page_load_timeout(page_load_timeout)
    driver._seo_profile = profile  # type: ignore[attr-defined]
    driver._seo_download_dir = dl  # type: ignore[attr-defined]
    return driver


def quit_system_firefox_driver(driver: Any) -> None:
    profile = getattr(driver, "_seo_profile", None)
    try:
        driver.quit()
    except Exception:
        pass
    if profile is not None:
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
