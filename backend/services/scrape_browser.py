"""Tarama tarayıcısı — yalnızca Firefox.

Chrome / Chromium / Chrome for Testing açılmaz. Google oturumu
~/.seo-agent/fx-google altında tutulur (Play, GSC, Firebase, Policy).

Profil kuralları:
- Auth state (cookies.sqlite, storage, sessionstore…) silinmez / recreate edilmez.
- Stale SingletonLock / .parentlock temizlenebilir.
- launch öncesi: bekle → SIGTERM → SIGKILL yalnızca son çare.

Pencere (üyelik koruma):
- Varsayılan SCRAPE_KEEP_OPEN=1 — headed scrape bitince Firefox kapanmaz.
- Per-scrape: PLAY_CONSOLE_KEEP_OPEN, ASC_CONSOLE_KEEP_OPEN, GSC_LINKS_KEEP_OPEN,
  GSC_CWV_KEEP_OPEN, FIREBASE_CONSOLE_KEEP_OPEN, ADMANAGER_POLICY_KEEP_OPEN,
  SINEMALAR_KEEP_OPEN (=0 ile kapat).
- acquire_persistent_context / release_persistent_context kullan.
- Aynı profil (fx-google) paylaşan scrapeler bridge sürecinde aynı warm
  pencereyi kullanır. Playwright Firefox CDP attach etmez; süreç-dışı yetim
  pencere varsa hızlı takeover (çerezler diskte kalır).
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

STATE_DIR = Path.home() / ".seo-agent"

# Elle giriş bekleyen tüm taramalar (ASC / Play / Firebase / GSC / Policy…)
# Update page & headed sync: 15 dakika; giriş sonrası aynı pencerede kazıma devam eder.
LOGIN_WAIT_SEC = 900

_NAV_URL_BAD_PREFIX = re.compile(r"^[a-z0-9._+-]+(?=https?://)", re.I)


LOGIN_WAIT_MIN_SEC = 30


def login_wait_sec(*, env_key: str | None = None, default: int = LOGIN_WAIT_SEC) -> int:
    """Giriş bekleme süresi (sn).

    Env verilmişse onu kullanır (taban 30 sn). Gözetimsiz koşan bridge daemon'ı
    kısa bir değer verir: oturum ölüyse 15 dk beklemek yerine hızlı düşüp
    kuyruğu ve global tarayıcı slotunu serbest bırakmak gerekir.
    """
    raw = ""
    if env_key:
        raw = (os.environ.get(env_key) or "").strip()
    if not raw:
        raw = (os.environ.get("SCRAPE_LOGIN_WAIT_SEC") or "").strip()
    try:
        val = int(raw) if raw else int(default)
    except ValueError:
        val = int(default)
    return max(LOGIN_WAIT_MIN_SEC, val)


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


# Çalışma kilidi dosyaları — auth state değil; stale ise silinebilir.
_PROFILE_LOCK_NAMES = (".parentlock", "lock", "SingletonLock", "SingletonCookie", "SingletonSocket")

# Authentication state — asla silinmez / recreate edilmez.
_PROFILE_AUTH_NAMES = frozenset(
    {
        "cookies.sqlite",
        "cookies.sqlite-wal",
        "cookies.sqlite-shm",
        "webappsstore.sqlite",
        "webappsstore.sqlite-wal",
        "webappsstore.sqlite-shm",
        "storage",
        "sessionstore.jsonlz4",
        "sessionstore-backups",
        "logins.json",
        "key4.db",
        "cert9.db",
    }
)

# Graceful SIGTERM sonrası bekleme; sonra isteğe bağlı SIGKILL.
_PROFILE_RELEASE_WAIT_SEC = float(os.environ.get("SCRAPE_PROFILE_RELEASE_WAIT_SEC") or "12")
_PROFILE_BUSY_WAIT_SEC = float(os.environ.get("SCRAPE_PROFILE_BUSY_WAIT_SEC") or "8")


class ProfileBusyError(RuntimeError):
    """Profil sağlıklı bir tarayıcı tarafından tutuluyor; force takeover yok."""


def list_profile_browser_pids(profile: Path) -> list[int]:
    """Bu profile path'ini tutan Firefox/Chrome süreç PID'leri."""
    try:
        marker = str(profile.expanduser().resolve())
    except Exception:
        marker = str(profile.expanduser())
    pids: list[int] = []
    try:
        out = subprocess.check_output(["ps", "ax", "-o", "pid=,command="], text=True)
    except Exception:
        return pids
    seen: set[int] = set()
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
        if pid <= 1 or pid == os.getpid() or pid in seen:
            continue
        seen.add(pid)
        pids.append(pid)
    return pids


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def clear_stale_profile_locks(profile: Path) -> list[str]:
    """Yalnızca kilit dosyalarını sil. cookies/storage/session'a dokunmaz.

    Canlı browser PID'i varken kilit silinmez (yanlışlıkla ikinci instance açılmasın).
    """
    profile = profile.expanduser()
    if list_profile_browser_pids(profile):
        return []
    removed: list[str] = []
    for name in _PROFILE_LOCK_NAMES:
        path = profile / name
        try:
            if path.exists() or path.is_symlink():
                path.unlink(missing_ok=True)
                removed.append(name)
        except Exception:
            pass
    return removed


def assert_profile_auth_untouched(path: Path) -> None:
    """Auth dosyası / storage silme girişimlerini reddet (güvenlik kemeri)."""
    name = path.expanduser().name
    if name in _PROFILE_AUTH_NAMES or name.startswith("cookies.sqlite"):
        raise RuntimeError(
            f"Profil auth state silinemez: {path} — "
            "yalnızca stale lock / orphan process temizliği serbest"
        )


def release_profile_browsers(
    profile: Path,
    *,
    force: bool = False,
    wait_sec: float | None = None,
    reason: str = "",
) -> dict[str, Any]:
    """Profili tutan tarayıcıları yumuşak bırak.

    Akış: SIGTERM → bekle → hâlâ yaşıyorsa ve force=True ise SIGKILL.
    Login kilidi varken (manuel --login) hiçbir şey öldürmez.
    """
    if profile_login_lock_active(profile):
        return {"term": 0, "kill": 0, "skipped": "login_lock", "reason": reason}
    wait = _PROFILE_RELEASE_WAIT_SEC if wait_sec is None else max(0.0, float(wait_sec))
    pids = list_profile_browser_pids(profile)
    if not pids:
        cleared = clear_stale_profile_locks(profile)
        return {"term": 0, "kill": 0, "cleared_locks": cleared, "reason": reason}

    term = 0
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            term += 1
        except (ProcessLookupError, PermissionError, OSError):
            pass
    if term:
        label = f" ({reason})" if reason else ""
        print(
            f"Profil browser SIGTERM ×{term}{label} · {wait:.0f}s bekleniyor · force={force}",
            flush=True,
        )
        deadline = time.time() + wait
        while time.time() < deadline:
            if not any(_pid_alive(p) for p in pids):
                break
            time.sleep(0.4)

    kill_n = 0
    still = [p for p in pids if _pid_alive(p)]
    if still and force:
        for pid in still:
            try:
                os.kill(pid, signal.SIGKILL)
                kill_n += 1
            except (ProcessLookupError, PermissionError, OSError):
                pass
        if kill_n:
            print(f"Profil browser SIGKILL ×{kill_n} (son çare) · {reason or 'takeover'}", flush=True)
            time.sleep(0.5)

    cleared = clear_stale_profile_locks(profile)
    return {
        "term": term,
        "kill": kill_n,
        "remaining": [p for p in pids if _pid_alive(p)],
        "cleared_locks": cleared,
        "reason": reason,
    }


def ensure_profile_free_for_launch(
    profile: Path,
    *,
    takeover: bool = True,
    busy_wait_sec: float | None = None,
    release_wait_sec: float | None = None,
    reason: str = "launch",
) -> dict[str, Any]:
    """launch_persistent öncesi: stale lock temizle; canlı browser varsa önce bekle.

    Playwright Firefox'a CDP attach yok — 'attach' = mevcut job bitsin / kilit düşsün.
    Takeover gerekirse: graceful SIGTERM → bekle → SIGKILL son çare.
    """
    profile = profile.expanduser()
    busy_wait = _PROFILE_BUSY_WAIT_SEC if busy_wait_sec is None else max(0.0, float(busy_wait_sec))
    info: dict[str, Any] = {"profile": str(profile), "reason": reason}

    if takeover and profile_login_lock_active(profile):
        raise RuntimeError(
            f"Login kilidi aktif: {profile_login_lock_path(profile)} — "
            "manuel giriş bitene kadar scrapeyi ertele"
        )

    pids = list_profile_browser_pids(profile)
    if not pids:
        info["cleared_locks"] = clear_stale_profile_locks(profile)
        info["action"] = "stale_locks_only"
        return info

    # Başka scrape kapanıyorsa kısa bekle — hemen öldürme
    if busy_wait > 0:
        deadline = time.time() + busy_wait
        while time.time() < deadline:
            pids = list_profile_browser_pids(profile)
            if not pids:
                info["cleared_locks"] = clear_stale_profile_locks(profile)
                info["action"] = "waited_for_release"
                return info
            time.sleep(0.4)

    pids = list_profile_browser_pids(profile)
    if not pids:
        info["cleared_locks"] = clear_stale_profile_locks(profile)
        info["action"] = "released_during_wait"
        return info

    if not takeover:
        raise ProfileBusyError(
            f"Profil meşgul ({len(pids)} browser): {profile} — takeover=False"
        )

    # Graceful, sonra force
    soft = release_profile_browsers(
        profile, force=False, wait_sec=release_wait_sec, reason=f"{reason}:graceful"
    )
    info["soft"] = soft
    pids = list_profile_browser_pids(profile)
    if pids:
        hard = release_profile_browsers(
            profile, force=True, wait_sec=2.0, reason=f"{reason}:force"
        )
        info["hard"] = hard
        pids = list_profile_browser_pids(profile)
    info["cleared_locks"] = clear_stale_profile_locks(profile)
    info["action"] = "takeover"
    info["remaining"] = pids
    return info


def kill_profile_browsers(profile: Path, *, force: bool = True) -> int:
    """Geriye uyumlu: profil browser bırakma.

    Varsayılan force=True (eski çağrılar yolu açabilsin) ama önce SIGTERM + bekleme.
    Login kilidinde 0 döner.
    """
    result = release_profile_browsers(
        profile, force=force, reason="kill_profile_browsers"
    )
    return int(result.get("term") or 0) + int(result.get("kill") or 0)


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
    # Önce soft release (graceful → bekle → force); cookie/storage silinmez
    ensure_profile_free_for_launch(
        profile,
        takeover=True,
        reason="system_firefox_login",
    )

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
    """Firefox persistent context.

    kill_existing=True: soft takeover (bekle → SIGTERM → SIGKILL son çare).
    kill_existing=False: yalnızca stale lock temizliği; canlı browser varsa ProfileBusyError.
    Auth state (cookies/storage) asla silinmez.
    """
    assert_firefox_only(pw)
    if kill_existing:
        ensure_profile_free_for_launch(
            profile,
            takeover=True,
            reason="launch_persistent",
        )
    else:
        if profile_login_lock_active(profile):
            raise RuntimeError(
                f"Login kilidi aktif: {profile_login_lock_path(profile)} — "
                "manuel giriş bitene kadar scrapeyi ertele"
            )
        if list_profile_browser_pids(profile):
            raise ProfileBusyError(
                f"Profil meşgul (kill_existing=False): {profile}"
            )
        clear_stale_profile_locks(profile)
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


# ── Keep window open (üyelik / Google oturumu koruma) ─────────────────────────
# Play/ASC zaten KEEP_OPEN kullanıyordu; varsayılan artık TÜM headed scrapeler için açık.
# Kapatmak: SCRAPE_KEEP_OPEN=0  veya  <SCRAPE>_KEEP_OPEN=0 (örn. GSC_LINKS_KEEP_OPEN=0)
#
# Playwright Firefox'a CDP attach YOK. Açık pencereyi yeniden kullanmak yalnız
# aynı Python sürecinde (bridge daemon warm session) mümkün.
# Aynı profili (fx-google) paylaşan scrapeler profil anahtarıyla aynı pencerede
# devam eder (Play → Firebase → GSC). Subprocess yetim Firefox: hızlı takeover.

_WARM_SESSIONS: dict[str, dict[str, Any]] = {}
_WARM_BY_PROFILE: dict[str, str] = {}  # resolved profile path → warm key


def scrape_keep_window_open(*, env_key: str | None = None) -> bool:
    """Varsayılan True. Per-scrape veya global env ile kapatılabilir."""
    if env_key:
        raw = (os.environ.get(env_key) or "").strip().lower()
        if raw in ("0", "false", "no", "off"):
            return False
        if raw in ("1", "true", "yes", "on"):
            return True
    raw = (os.environ.get("SCRAPE_KEEP_OPEN") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _profile_key(profile: Path) -> str:
    return str(profile.expanduser().resolve())


def _warm_alive(ctx: Any) -> bool:
    if ctx is None:
        return False
    try:
        pages = ctx.pages
    except Exception:
        return False
    # Playwright: pages list dolu olsa bile sekmeler ölü olabilir
    try:
        if not pages:
            return True
        page0 = pages[0]
        _ = page0.url
        return True
    except Exception:
        return False


def warm_session_forget_profile(profile: Path) -> None:
    """Profildeki tüm warm slot'ları düşür (Playwright↔Selenium geçişi)."""
    pk = _profile_key(profile)
    owner = _WARM_BY_PROFILE.get(pk)
    if owner:
        warm_session_forget(owner)
    # Alias temizliği
    for key, slot in list(_WARM_SESSIONS.items()):
        if str(slot.get("profile") or "") == pk:
            warm_session_forget(key)


def warm_session_get(key: str) -> tuple[Any, Any] | None:
    """(pw, context) veya None — süreç içi sıcak pencere."""
    slot = _WARM_SESSIONS.get(key) or {}
    owner = slot.get("thread")
    if owner is not None and owner != threading.get_ident():
        # Sync Playwright nesneleri thread'e bağlı; başka thread'den dokunmak
        # "cannot switch to a different thread" hatası verir. Slot'u düşür —
        # pencere süreç-dışı yetim gibi devralınır (çerezler diskte kalır).
        warm_session_forget(key)
        return None
    pw, ctx = slot.get("pw"), slot.get("ctx")
    if _warm_alive(ctx):
        return pw, ctx
    if key in _WARM_SESSIONS:
        warm_session_forget(key)
    return None


def warm_session_get_for_profile(profile: Path) -> tuple[Any, Any] | None:
    """Aynı profili kullanan başka scrape'in sıcak penceresi."""
    owner = _WARM_BY_PROFILE.get(_profile_key(profile))
    if not owner:
        return None
    return warm_session_get(owner)


def warm_session_remember(
    key: str,
    pw: Any,
    ctx: Any,
    *,
    label: str = "",
    profile: Path | None = None,
) -> None:
    prev = _WARM_SESSIONS.get(key) or {}
    prev_prof = prev.get("profile")
    if prev_prof and _WARM_BY_PROFILE.get(str(prev_prof)) == key:
        _WARM_BY_PROFILE.pop(str(prev_prof), None)
    slot: dict[str, Any] = {
        "pw": pw,
        "ctx": ctx,
        "label": label or key,
        "thread": threading.get_ident(),
    }
    if profile is not None:
        pk = _profile_key(profile)
        slot["profile"] = pk
        # Profil başına tek sahip — eski alias'ı düşür
        old_owner = _WARM_BY_PROFILE.get(pk)
        if old_owner and old_owner != key:
            old_slot = _WARM_SESSIONS.get(old_owner) or {}
            if old_slot.get("ctx") is ctx:
                _WARM_SESSIONS.pop(old_owner, None)
        _WARM_BY_PROFILE[pk] = key
    _WARM_SESSIONS[key] = slot


def warm_session_forget(key: str) -> None:
    slot = _WARM_SESSIONS.pop(key, None) or {}
    prof = slot.get("profile")
    if prof and _WARM_BY_PROFILE.get(str(prof)) == key:
        _WARM_BY_PROFILE.pop(str(prof), None)


# Sync Playwright'ın event loop'u thread'e bağlıdır: ilk oturum yaşarken aynı
# thread'de ikinci sync_playwright().start() "Sync API inside the asyncio loop"
# hatası verir. Bridge daemon tüm scrapeleri tek scheduler thread'inde koşturduğu
# ve KEEP_OPEN ile pencereleri açık bıraktığı için farklı profiller (fx-google +
# fx-sinemalar) tek sürücüyü paylaşmak zorunda.
_PW_BY_THREAD: dict[int, dict[str, Any]] = {}


def _prune_dead_thread_playwrights() -> None:
    """Biten thread'in sürücüsü yeniden kullanılamaz (ident geri dönüşebilir)."""
    alive = {t.ident for t in threading.enumerate()}
    for tid in [k for k in _PW_BY_THREAD if k not in alive]:
        _PW_BY_THREAD.pop(tid, None)


def _thread_playwright() -> Any:
    from playwright.sync_api import sync_playwright

    _prune_dead_thread_playwrights()
    tid = threading.get_ident()
    slot = _PW_BY_THREAD.get(tid)
    if slot is not None and slot.get("pw") is not None:
        slot["refs"] = int(slot.get("refs") or 0) + 1
        return slot["pw"]
    pw = sync_playwright().start()
    _PW_BY_THREAD[tid] = {"pw": pw, "refs": 1}
    return pw


def _thread_playwright_release(pw: Any) -> None:
    """Bu thread'deki son context kapanınca sürücüyü durdur."""
    if pw is None:
        return
    tid = threading.get_ident()
    slot = _PW_BY_THREAD.get(tid)
    if slot is None or slot.get("pw") is not pw:
        try:
            pw.stop()
        except Exception:
            pass
        return
    slot["refs"] = int(slot.get("refs") or 0) - 1
    if slot["refs"] > 0:
        return
    _PW_BY_THREAD.pop(tid, None)
    try:
        pw.stop()
    except Exception:
        pass


def acquire_persistent_context(
    key: str,
    *,
    profile: Path,
    headed: bool = True,
    env_key: str | None = None,
    label: str = "",
    viewport: dict[str, int] | None = None,
    locale: str = "tr-TR",
    extra: dict[str, Any] | None = None,
    kill_existing: bool = True,
) -> tuple[Any, Any, bool]:
    """(pw, context, reused). Headed + KEEP_OPEN ise önceki pencereyi yeniden kullanır.

    Aynı profil (örn. fx-google) için Play/Firebase/GSC aynı warm pencereyi paylaşır.
    Süreç dışı yetim Firefox varsa (subprocess KEEP_OPEN kalıntısı) hızlı takeover —
    Playwright Firefox attach etmez; çerezler diskte kalır, pencere yeniden açılır.
    """
    tag = label or key
    profile = profile.expanduser()
    keep = bool(headed and scrape_keep_window_open(env_key=env_key))

    if keep:
        warm = warm_session_get(key) or warm_session_get_for_profile(profile)
        if warm is not None:
            # Bu scrape anahtarına da bağla (release doğru bulsun)
            warm_session_remember(key, warm[0], warm[1], label=tag, profile=profile)
            print(
                f"{tag}: mevcut Firefox penceresi yeniden kullanılıyor "
                f"(profil {profile.name}, kapatılmadı)",
                flush=True,
            )
            return warm[0], warm[1], True

        # Warm yok ama profilde canlı browser → başka süreçten yetim
        orphan_pids = list_profile_browser_pids(profile)
        if orphan_pids and kill_existing:
            print(
                f"{tag}: profilde süreç-dışı Firefox açık (pid={orphan_pids[:4]}) — "
                "Playwright attach yok; pencereyi devralıp yeniden açıyoruz "
                "(Google oturumu disk profilinde kalır)",
                flush=True,
            )
            ensure_profile_free_for_launch(
                profile,
                takeover=True,
                busy_wait_sec=1.5,
                release_wait_sec=3.0,
                reason=f"{key}:orphan_takeover",
            )

    pw = _thread_playwright()
    try:
        ctx = launch_persistent(
            pw,
            profile,
            headed=headed,
            viewport=viewport,
            locale=locale,
            extra=extra,
            kill_existing=kill_existing,
        )
    except Exception:
        _thread_playwright_release(pw)
        raise

    if keep:
        warm_session_remember(key, pw, ctx, label=tag, profile=profile)
    return pw, ctx, False


def release_persistent_context(
    key: str,
    pw: Any,
    ctx: Any,
    *,
    headed: bool = True,
    env_key: str | None = None,
    label: str = "",
    profile: Path | None = None,
) -> None:
    """Headed + KEEP_OPEN: pencereyi kapatma (oturum açık kalsın). Aksi halde close+stop."""
    tag = label or key
    keep = bool(headed and scrape_keep_window_open(env_key=env_key) and ctx is not None)
    if keep:
        # Profil bilgisini slot'tan veya argümandan koru
        prev = _WARM_SESSIONS.get(key) or {}
        prof = profile
        if prof is None and prev.get("profile"):
            try:
                prof = Path(str(prev["profile"]))
            except Exception:
                prof = None
        warm_session_remember(key, pw, ctx, label=tag, profile=prof)
        print(
            f"{tag}: Firefox penceresi açık bırakıldı (scrape bitse de kapanmaz; "
            f"aynı bridge sürecinde sonraki tarama bu pencereden devam eder; "
            f"kapatmak için {(env_key or 'SCRAPE_KEEP_OPEN')}=0)",
            flush=True,
        )
        return

    warm = _WARM_SESSIONS.get(key) or {}
    if warm.get("ctx") is ctx:
        warm_session_forget(key)
    # Profil index'te bu ctx varsa temizle
    for pk, owner in list(_WARM_BY_PROFILE.items()):
        slot = _WARM_SESSIONS.get(owner) or {}
        if slot.get("ctx") is ctx:
            warm_session_forget(owner)
            break
        if owner == key:
            _WARM_BY_PROFILE.pop(pk, None)
    try:
        if ctx is not None:
            ctx.close()
    except Exception:
        pass
    _thread_playwright_release(pw)


def close_context_maybe_keep(
    context: Any,
    *,
    key: str,
    headed: bool,
    env_key: str | None = None,
    label: str = "",
    pw: Any = None,
    profile: Path | None = None,
) -> None:
    """`with sync_playwright()` kullanan eski kod yolları için: keep ise close etme.

    Not: `with` bloğu çıkınca Playwright yine stop edebilir — o yüzden mümkünse
    acquire_persistent_context / release_persistent_context tercih edin.
    """
    if headed and scrape_keep_window_open(env_key=env_key) and context is not None:
        if pw is not None:
            warm_session_remember(
                key, pw, context, label=label or key, profile=profile
            )
        print(
            f"{label or key}: context.close atlandı (KEEP_OPEN) — oturum korunur",
            flush=True,
        )
        return
    try:
        context.close()
    except Exception:
        pass
