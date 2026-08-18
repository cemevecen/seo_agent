#!/usr/bin/env python3
"""Login warm-up — ana scrape'lerden önce oturumları doğrular.

Neden gerekli: kalıcı Firefox profilleri (fx-asc / fx-google) oturumu günlerce
taşıyor, ama Apple ve Google oturumu periyodik olarak düşürüyor. Bugünkü akışta
bu ancak asıl scrape saatinde fark ediliyor — tur boşa gidiyor ve kimse haberdar
olmuyor. Warm-up bunu scrape'ten önce yakalar.

Akış (hedef başına):

    oturum geçerli mi?
        evet → bitti
        hayır → Keychain'de kimlik var mı?
            evet → e-posta + parola doldur, gönder, tekrar kontrol et
            hayır / 2FA / CAPTCHA → pencereyi açık bırak, «müdahale gerekiyor»
                                     olarak Project Control'a bildir

2FA veya CAPTCHA **kırılmaya çalışılmaz**. Bunlar bilinçli güvenlik katmanı;
otomatik aşma denemesi hem hesabı kilitletir hem de yanlış olur. Warm-up
yalnızca durumu bildirir, doğrulamayı sen yaparsın ve yeni oturum profile
kaydolduğu için genelde günlerce tekrar sorulmaz.

Kullanım:
    python3 scripts/scrape_login_warmup.py            # asc + firebase
    python3 scripts/scrape_login_warmup.py --only asc
    python3 scripts/scrape_login_warmup.py --check    # yalnızca durum, login yok
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bridge_interpreter() -> Path | None:
    """Köprü LaunchAgent'ının kullandığı python — çalıştığı bilinen tek yorumlayıcı."""
    plist = Path.home() / "Library/LaunchAgents/com.cemevecen.doviz-admin-notification-bridge.plist"
    try:
        import plistlib

        data = plistlib.loads(plist.read_bytes())
        args = data.get("ProgramArguments") or []
        if args:
            candidate = Path(str(args[0]))
            return candidate if candidate.exists() else None
    except Exception:  # noqa: BLE001
        return None
    return None


def interpreter_candidates() -> list[Path]:
    """playwright aranacak yorumlayıcılar — ilk uyan kullanılır.

    Tek bir `.venv` yoluna güvenmek yetmiyor: makineler farklı kurulmuş
    olabiliyor (ofis Mac'inde sistem python3 seçiliyordu ve orada playwright
    yoktu). Köprünün kendi yorumlayıcısı en güvenilir aday, çünkü zamanlanmış
    scrape'ler onunla koşuyor.
    """
    out: list[Path] = []
    seen: set[str] = set()

    def _add(path: Path | None) -> None:
        if path is None:
            return
        try:
            key = str(path.resolve())
        except Exception:  # noqa: BLE001
            key = str(path)
        if key in seen or not path.exists():
            return
        try:
            if path.resolve() == Path(sys.executable).resolve():
                return  # zaten buradayız
        except Exception:  # noqa: BLE001
            pass
        seen.add(key)
        out.append(path)

    _add(_bridge_interpreter())
    for name in (".venv", "venv", "env"):
        _add(ROOT / name / "bin" / "python")
    return out


def _interpreter_has_playwright(python_path: Path) -> bool:
    import subprocess

    try:
        proc = subprocess.run(
            [str(python_path), "-c", "import playwright"],
            capture_output=True,
            timeout=25,
            check=False,
        )
        return proc.returncode == 0
    except Exception:  # noqa: BLE001
        return False


def _has_playwright() -> bool:
    try:
        import playwright  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


def _ensure_playwright_interpreter() -> None:
    """playwright bulunmuyorsa proje venv'ine geç.

    Köprü zaten `.venv/bin/python` ile koşuyor, ama elle çalıştırırken sistem
    `python3`'ü seçilebiliyor ve orada playwright kurulu olmayabiliyor (ofis
    Mac'inde tam olarak bu oldu). Kullanıcıyı doğru komutu hatırlamaya
    zorlamak yerine betik kendini doğru yorumlayıcıya taşır.
    """
    if _has_playwright():
        return
    if os.environ.get("_WARMUP_REEXEC") == "1":
        return  # bir kez denendi; sonsuz döngü olmasın
    for candidate in interpreter_candidates():
        if _interpreter_has_playwright(candidate):
            os.environ["_WARMUP_REEXEC"] = "1"
            os.execv(str(candidate), [str(candidate), *sys.argv])
    # Hiçbiri yoksa akış devam eder; teşhis/çalışma net hata verir.


_ensure_playwright_interpreter()

from backend.services.scrape_browser import (  # noqa: E402
    acquire_persistent_context,
    list_profile_browser_pids,
    asc_profile_dir,
    firebase_profile_dir,
    google_profile_dir,
    release_persistent_context,
    warm_session_registered_for_profile,
)
from backend.services.scrape_credentials import load_credentials  # noqa: E402

LOGGER = logging.getLogger("scrape-login-warmup")

# Oturum doğrulaması için açılacak sayfa ve «giriş gerekiyor» işaretleri
# `key` ve `env_key` asıl scrape'lerle AYNI olmalı: warm pencere paylaşılsın,
# warm-up'tan hemen sonra çalışan scrape aynı oturumu devralsın.
TARGETS: dict[str, dict[str, Any]] = {
    "asc": {
        "label": "App Store Connect",
        "key": "asc",
        "env_key": "ASC_CONSOLE_KEEP_OPEN",
        "profile": asc_profile_dir,
        "url": "https://appstoreconnect.apple.com/apps",
        "credential_key": "asc",
        "login_host_hints": ("idmsa.apple.com", "appleid.apple.com", "signin"),
        "logged_out_markers": ("idmsa.apple.com", "appleid.apple.com", "/login"),
        # authResult=FAILED kalıntısında form render olmuyor; temiz adrese gidilir
        "login_url": "https://appstoreconnect.apple.com/login",
        "stale_markers": ("authresult=failed",),
        # Apple giriş bileşeni KAPALI shadow root içinde: sayfada görünüyor ama
        # DOM'dan (locator/evaluate) erişilemiyor. Bu yüzden alan konumuna
        # tıklanıp klavyeyle yazılır. Oranlar viewport'a göre; pencere boyutu
        # değişse de alan yatayda ortada, dikeyde ~%27'de kalıyor.
        "blind_login": {"field_xy": (0.5, 0.273), "settle_sec": 9.0},
        "email_selectors": ("#account_name_text_field", 'input[name="accountName"]',
                            'input[type="email"]'),
        "password_selectors": ("#password_text_field", 'input[name="password"]',
                               'input[type="password"]'),
    },
    "firebase": {
        "label": "Firebase Console",
        "key": "firebase",
        "env_key": "FIREBASE_CONSOLE_KEEP_OPEN",
        "profile": firebase_profile_dir,
        "url": "https://console.firebase.google.com/",
        "credential_key": "google",
        "login_host_hints": ("accounts.google.com", "signin/v2", "ServiceLogin"),
        "logged_out_markers": ("accounts.google.com", "servicelogin", "/signin/"),
        "login_url": "https://accounts.google.com/ServiceLogin?continue=https://console.firebase.google.com/",
        "email_selectors": ('input[type="email"]', "#identifierId"),
        "password_selectors": ('input[type="password"]', 'input[name="Passwd"]'),
    },
    # Play, Firebase ile aynı Google profilini (fx-google) paylaşır: biri giriş
    # yapınca diğeri de açılır. Yine de ayrı kontrol edilir, çünkü Play konsolu
    # bazen kendi ek onayını isteyebiliyor.
    "play": {
        "label": "Play Console",
        "key": "play",
        "env_key": "PLAY_CONSOLE_KEEP_OPEN",
        "profile": google_profile_dir,
        # Çıplak /console adresi «Geliştirici hesabı seçin» ekranını açıyor;
        # asıl scraper zaten geliştirici kimliğiyle derin bağlantıya gidip bu
        # adımı atlıyor. Warm-up da aynı adrese gitmeli ki hem seçim ekranı
        # çıkmasın hem de oturum kontrolü gerçek hedefi yansıtsın.
        "url": (
            "https://play.google.com/console/u/0/developers/"
            + (os.environ.get("PLAY_CONSOLE_DEVELOPER_ID") or "7587799419591090593").strip()
        ),
        "credential_key": "google",
        "login_host_hints": ("accounts.google.com", "signin/v2", "ServiceLogin"),
        # Oturum yokken Play giriş formu göstermiyor, /console/about/ pazarlama
        # sayfasına yönlendiriyor. Yalnızca "form var mı" bakmak bunu "oturum
        # geçerli" sanıyordu — pozitif işaret şart.
        "logged_out_markers": ("accounts.google.com", "servicelogin", "/console/about"),
        "login_url": "https://accounts.google.com/ServiceLogin?continue=https://play.google.com/console/u/0/",
        "email_selectors": ('input[type="email"]', "#identifierId"),
        "password_selectors": ('input[type="password"]', 'input[name="Passwd"]'),
    },
}

# Elle tetiklenen «yenile / güncelle» işlerinde önce bu hedefin oturumu doğrulanır
JOB_WARMUP_TARGETS: dict[str, tuple[str, ...]] = {
    "asc": ("asc",),
    "firebase": ("firebase",),
    "play": ("play",),
}

# Bu metinler görünürse ikinci faktör / bot kontrolü var demektir — durulur
INTERVENTION_MARKERS = (
    "two-factor", "iki faktörlü", "doğrulama kodu", "verification code",
    "trust this browser", "bu tarayıcıya güven", "recaptcha", "captcha",
    "2-step verification", "2 adımlı doğrulama", "try another way",
    "couldn't verify", "unusual activity",
)

NAV_TIMEOUT_MS = 90_000
SETTLE_SEC = 3.0
LOGIN_SETTLE_SEC = 6.0


@dataclass
class TargetResult:
    target: str
    label: str
    status: str = "unknown"          # ok | logged_in | intervention | error | skipped
    message: str = ""
    needs_action: bool = False
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "label": self.label,
            "status": self.status,
            "message": self.message,
            "needs_action": self.needs_action,
            "detail": self.detail,
        }


def _page_text(page: Any, limit: int = 6000) -> str:
    try:
        return (page.inner_text("body") or "")[:limit].lower()
    except Exception:  # noqa: BLE001
        try:
            return (page.content() or "")[:limit].lower()
        except Exception:  # noqa: BLE001
            return ""


def _looks_like_login(page: Any, hints: tuple[str, ...], extra: tuple[str, ...] = ()) -> bool:
    """Oturum düşmüş mü?

    Yalnızca "giriş formu görünüyor mu" yetmiyor: Play, oturum yokken form
    göstermeden pazarlama sayfasına yönlendiriyor ve bu "oturum geçerli" gibi
    okunuyordu. Bu yüzden hedefe özel «çıkış yapılmış» URL işaretleri de var.
    """
    url = (getattr(page, "url", "") or "").lower()
    for marker in tuple(hints) + tuple(extra):
        if marker and marker.lower() in url:
            return True
    for sel in ('input[type="password"]', "#account_name_text_field", "#identifierId"):
        try:
            if page.locator(sel).first.is_visible(timeout=1200):
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _needs_human(page: Any) -> str:
    """2FA / CAPTCHA işareti varsa hangisi olduğunu döndür, yoksa boş."""
    text = _page_text(page)
    for marker in INTERVENTION_MARKERS:
        if marker in text:
            return marker
    return ""


def _search_roots(page: Any) -> list[Any]:
    """Ana sayfa + tüm frame'ler. Apple giriş formu iframe içinde gelebiliyor."""
    roots = [page]
    try:
        roots.extend(list(page.frames or [])[:12])
    except Exception:  # noqa: BLE001
        pass
    return roots


def _any_visible(page: Any, selectors: tuple[str, ...]) -> bool:
    for root in _search_roots(page):
        for sel in selectors:
            try:
                if root.locator(sel).first.is_visible(timeout=800):
                    return True
            except Exception:  # noqa: BLE001
                continue
    return False


def _fill_first(page: Any, selectors: tuple[str, ...], value: str) -> bool:
    for root in _search_roots(page):
        for sel in selectors:
            try:
                loc = root.locator(sel).first
                if loc.is_visible(timeout=1200):
                    loc.click()
                    loc.fill(value)
                    return True
            except Exception:  # noqa: BLE001
                continue
    return False


def _submit(page: Any) -> None:
    try:
        page.keyboard.press("Enter")
    except Exception:  # noqa: BLE001
        pass



def _blind_login(page: Any, spec: dict[str, Any], creds: Any, result: TargetResult) -> bool:
    """DOM'a erişilemeyen giriş formunu koordinat + klavye ile doldur.

    Yalnızca normal yol (DOM alanları) başarısız olunca çağrılır. Apple, giriş
    bileşenini kapalı shadow root içinde render ettiği için locator hiçbir input
    görmüyor; ekran görüntüsüyle doğrulandı ki alan görünür ve yazılabilir.
    """
    cfg = spec.get("blind_login") or {}
    if not cfg:
        return False
    # Varsayılan KAPALI. Koordinat+klavye girişi tek başına çalıştığı ekran
    # görüntüsüyle doğrulandı, ama otomatik akışta güvenilir biçimde
    # tamamlanmıyor. Açık bırakılırsa sistem günde 4 kez Apple'a başarısız
    # giriş denemesi yapar ve bu hesap kilitlenmesine yol açabilir — arıza
    # bildirip beklemek, körlemesine denemekten iyidir.
    if (os.environ.get("WARMUP_BLIND_LOGIN") or "").strip().lower() not in ("1", "true", "yes", "on"):
        result.detail["blind_login"] = "kapalı (WARMUP_BLIND_LOGIN=1 ile açılır)"
        return False
    ratio_x, ratio_y = cfg.get("field_xy", (0.5, 0.27))
    settle = float(cfg.get("settle_sec", SETTLE_SEC))
    try:
        if spec.get("login_url"):
            page.goto(spec["login_url"], wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        try:
            page.bring_to_front()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(settle)
        vp = page.viewport_size or {"width": 1440, "height": 1100}
        x, y = vp["width"] * ratio_x, vp["height"] * ratio_y

        page.mouse.click(x, y)
        time.sleep(0.4)
        page.keyboard.type(creds.email, delay=40)
        time.sleep(0.3)
        page.keyboard.press("Enter")
        time.sleep(settle)

        blocker = _needs_human(page)
        if blocker:
            result.detail["blocker"] = blocker
            return False

        # Parola alanı e-postanın ALTINDA beliriyor ve zaten odaklı geliyor.
        # Aynı koordinata tıklamak parolayı e-posta alanına yazdırıyordu —
        # bu yüzden burada tıklanmaz, doğrudan yazılır.
        page.keyboard.type(creds.password, delay=40)
        time.sleep(0.3)
        page.keyboard.press("Enter")
        time.sleep(settle + 4)
        result.detail["blind_login"] = True
        return True
    except Exception as exc:  # noqa: BLE001
        result.detail["blind_login_error"] = str(exc)[:140]
        return False


def _attempt_login(page: Any, spec: dict[str, Any], result: TargetResult) -> None:
    """Kimlik bilgisi varsa doldur. 2FA çıkarsa dur ve bildir."""
    creds = load_credentials(spec["credential_key"])
    if not creds.complete:
        result.status = "intervention"
        result.needs_action = True
        result.message = (
            f"{spec['label']}: oturum düşmüş ve Keychain'de kimlik yok. "
            "Pencere açık bırakıldı; elle giriş yapın."
        )
        result.detail["reason"] = "no_credentials"
        return

    # İki durumda temiz giriş adresine gidilir:
    #  · Play gibi konsollar oturum yokken form göstermiyor (pazarlama sayfası)
    #  · ASC'de authResult=FAILED kalıntısıyla form hiç render olmuyor
    cur_url = (getattr(page, "url", "") or "").lower()
    stale = any(mark in cur_url for mark in spec.get("stale_markers", ()))
    if spec.get("login_url") and (stale or not _any_visible(page, spec["email_selectors"])):
        try:
            page.goto(spec["login_url"], wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            time.sleep(SETTLE_SEC)
        except Exception as exc:  # noqa: BLE001
            result.detail["login_url_error"] = str(exc)[:120]

    filled_email = _fill_first(page, spec["email_selectors"], creds.email)
    if filled_email:
        _submit(page)
        time.sleep(LOGIN_SETTLE_SEC)

    blocker = _needs_human(page)
    if blocker:
        result.status = "intervention"
        result.needs_action = True
        result.message = f"{spec['label']}: ek doğrulama istendi ({blocker}). Pencere açık."
        result.detail["blocker"] = blocker
        return

    filled_password = _fill_first(page, spec["password_selectors"], creds.password)
    if filled_password:
        _submit(page)
        time.sleep(LOGIN_SETTLE_SEC)

    result.detail["filled"] = {"email": filled_email, "password": filled_password}

    # DOM'dan hiçbir alan doldurulamadıysa koordinat yedeği (kapalı shadow DOM)
    if not filled_email and not filled_password:
        _blind_login(page, spec, creds, result)
        # Başarıyı iddia etmiyoruz: ölçüt, hedef sayfada oturumun geçerli olması.
        # Giriş hemen sonrasında URL bir süre /login'de kalabiliyor ve çerez
        # yerleşmemiş olabiliyor; tek atışta karar vermek başarılı girişi
        # "başarısız" göstermişti. Kısa aralıklarla birkaç kez doğrulanır.
        for attempt in range(3):
            try:
                page.goto(spec["url"], wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            except Exception as exc:  # noqa: BLE001
                result.detail["verify_error"] = str(exc)[:120]
            time.sleep(SETTLE_SEC + 2 * attempt)
            if not _looks_like_login(page, spec["login_host_hints"],
                                     spec.get("logged_out_markers", ())):
                result.detail["verified_after"] = attempt + 1
                break

    blocker = _needs_human(page)
    if blocker:
        result.status = "intervention"
        result.needs_action = True
        result.message = f"{spec['label']}: ikinci faktör gerekiyor ({blocker}). Pencere açık."
        result.detail["blocker"] = blocker
        return

    if _looks_like_login(page, spec["login_host_hints"], spec.get("logged_out_markers", ())):
        result.status = "intervention"
        result.needs_action = True
        result.message = (
            f"{spec['label']}: otomatik giriş tamamlanamadı, giriş ekranı duruyor. Pencere açık."
        )
        result.detail["reason"] = "still_on_login"
        return

    result.status = "logged_in"
    result.message = f"{spec['label']}: oturum yenilendi."


def warm_target(
    name: str,
    *,
    check_only: bool = False,
    headed: bool = True,
    takeover: bool = False,
) -> TargetResult:
    spec = TARGETS[name]
    result = TargetResult(target=name, label=spec["label"])
    profile = spec["profile"]()

    # ASC oturumu tarayıcı süreci yaşadığı sürece geçerli: 30 günlük Apple güven
    # çerezi diskte dursa bile yeniden başlatmada sessiz doğrulama FAILED
    # dönüyor (ölçüldü). Playwright var olan pencereye attach edemediği için
    # başka bir süreçten profile dokunmak = pencereyi öldürmek = oturumu
    # kaybetmek. Bu yüzden köprünün penceresi açıkken buradan el sürülmez.
    if not takeover and not warm_session_registered_for_profile(profile):
        others = list_profile_browser_pids(profile)
        if others:
            result.status = "ok"
            result.message = (
                f"{spec['label']}: başka süreçte açık pencere var (pid={others[:2]}) — "
                "oturumu bozmamak için dokunulmadı."
            )
            result.detail["skipped_reason"] = "foreign_window"
            result.detail["pids"] = others[:4]
            return result

    pw = ctx = None
    try:
        # kill_existing=False kritik: Apple/Google oturum çerezleri tarayıcı
        # yeniden başlatılınca kayboluyor. Bu kod tabanı oturumu pencereyi açık
        # tutarak koruyor (KEEP_OPEN); warm-up her koşuda Firefox'u öldürseydi
        # kendi kurduğu oturumu da düşürürdü — nitekim düşürüyordu.
        pw, ctx, reused = acquire_persistent_context(
            spec["key"],
            profile=profile,
            headed=headed,
            env_key=spec["env_key"],
            label=f"warmup:{spec['label']}",
            locale="tr-TR",
            kill_existing=False,
        )
        result.detail["profile"] = str(profile)
        result.detail["reused_warm_session"] = bool(reused)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(spec["url"], wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        time.sleep(SETTLE_SEC)

        if not _looks_like_login(page, spec["login_host_hints"], spec.get("logged_out_markers", ())):
            result.status = "ok"
            result.message = f"{spec['label']}: oturum geçerli."
            return result

        if check_only:
            result.status = "intervention"
            result.needs_action = True
            result.message = f"{spec['label']}: oturum düşmüş (yalnızca kontrol modu)."
            return result

        _attempt_login(page, spec, result)
        return result
    except Exception as exc:  # noqa: BLE001
        result.status = "error"
        result.needs_action = True
        result.message = f"{spec['label']}: warm-up hatası — {str(exc)[:180]}"
        return result
    finally:
        # Pencerenin açık kalması KEEP_OPEN ayarına bağlı (asıl scrape'lerle aynı
        # davranış). Müdahale gerekiyorsa kapatmamak kritik — kullanıcı devralacak.
        if pw is not None and ctx is not None:
            try:
                release_persistent_context(
                    spec["key"],
                    pw,
                    ctx,
                    headed=headed,
                    env_key=spec["env_key"],
                    label=f"warmup:{spec['label']}",
                    profile=profile,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("warm-up context bırakılamadı (%s): %s", name, exc)



# ── E-posta uyarısı (iki Mac için de) ───────────────────────────────────────

# Aynı arıza her turda mail atmasın; ama sessizce de kaybolmasın.
ALERT_REPEAT_HOURS = 6.0


def _alert_state_path() -> Path:
    return Path.home() / ".seo-agent" / "cache" / "login-warmup-alerts.json"


def _load_alert_state() -> dict[str, Any]:
    try:
        return json.loads(_alert_state_path().read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _save_alert_state(state: dict[str, Any]) -> None:
    try:
        path = _alert_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Uyarı durumu yazılamadı: %s", exc)


def _machine_name() -> str:
    import platform

    return platform.node() or "bilinmeyen-mac"


def _alert_html(machine: str, rows: list[TargetResult], *, recovered: bool) -> str:
    if recovered:
        items = "".join(
            f"<li><b>{r.label}</b> — oturum yeniden geçerli</li>" for r in rows
        )
        return (
            f"<p><b>{machine}</b> üzerinde konsol oturumları düzeldi.</p>"
            f"<ul>{items}</ul>"
        )
    items = "".join(
        f"<li><b>{r.label}</b> — {r.message}</li>" for r in rows
    )
    return (
        f"<p><b>{machine}</b> üzerinde konsol girişi müdahale bekliyor.</p>"
        f"<ul>{items}</ul>"
        "<p>Otomatik giriş Keychain'deki kimlikle denendi. İkinci faktör (2FA) "
        "veya bot kontrolü çıktıysa bilerek durulur — otomatik aşılmaya "
        "çalışılmaz. Bu Mac'te açık bırakılan Firefox penceresinden doğrulamayı "
        "tamamlayın; oturum profile kaydolur ve genelde günlerce tekrar sorulmaz.</p>"
        "<p>Durumu görmek için: "
        "<code>python3 scripts/scrape_login_warmup.py --doctor</code></p>"
    )


def send_alert_emails(results: list[TargetResult]) -> dict[str, Any]:
    """Arıza başlayınca uyar, düzelince haber ver. Tekrarları saatle sınırla."""
    machine = _machine_name()
    state = _load_alert_state()
    now = time.time()

    failing = [r for r in results if r.needs_action]
    healthy = [r for r in results if not r.needs_action]

    to_alert: list[TargetResult] = []
    for r in failing:
        key = f"{machine}|{r.target}"
        last = float((state.get(key) or {}).get("last_sent") or 0)
        if (now - last) >= ALERT_REPEAT_HOURS * 3600:
            to_alert.append(r)

    recovered = [r for r in healthy if state.get(f"{machine}|{r.target}")]

    sent = {"alert": False, "recovery": False}
    try:
        from backend.services.mailer import send_email
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Mailer yüklenemedi: %s", exc)
        return sent

    def _try_send(subject: str, html: str) -> bool:
        """SMTP arızası warm-up'ı düşürmemeli — uyarı yan iş, asıl iş oturum."""
        try:
            return bool(send_email(subject, html))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Uyarı e-postası gönderilemedi: %s", exc)
            return False

    if to_alert:
        subject = f"[{machine}] Konsol girişi müdahale bekliyor — " + ", ".join(
            r.target for r in to_alert
        )
        sent["alert"] = _try_send(subject, _alert_html(machine, to_alert, recovered=False))
        # Gönderilemese bile işaretle: her turda tekrar denemek posta kuyruğunu
        # döver; bir sonraki pencerede yeniden denenir.
        for r in to_alert:
            state[f"{machine}|{r.target}"] = {"last_sent": now, "message": r.message[:200]}

    if recovered:
        subject = f"[{machine}] Konsol oturumu düzeldi — " + ", ".join(r.target for r in recovered)
        sent["recovery"] = _try_send(subject, _alert_html(machine, recovered, recovered=True))
        for r in recovered:
            state.pop(f"{machine}|{r.target}", None)

    if to_alert or recovered:
        _save_alert_state(state)
    return sent


def report_to_panel(results: list[TargetResult]) -> bool:
    """Project Control'a bildir — panelde «login müdahalesi gerekiyor» görünsün."""
    base = (os.environ.get("PROJECT_CONTROL_BASE_URL")
            or os.environ.get("NOTIFICATION_INGEST_BASE_URL") or "").strip().rstrip("/")
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not base or not token:
        LOGGER.info("Panel bildirimi atlandı (base url veya token yok).")
        return False

    import urllib.error
    import urllib.request

    ok = True
    for r in results:
        payload = json.dumps({
            "source": "login_warmup",
            "target": r.target,
            "status": "error" if r.needs_action else "ok",
            "row_count": 0,
            "message": r.message[:500],
            "detail": r.detail,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{base}/api/scrape-runs/report",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-Notification-Ingest-Token": token,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp.read()
        except Exception as exc:  # noqa: BLE001
            ok = False
            LOGGER.warning("Panel bildirimi başarısız (%s): %s", r.target, exc)
    return ok



# ── Teşhis: kurulum bu makinede hazır mı? ───────────────────────────────────

def doctor(*, with_browser: bool = True) -> int:
    """Yeni bir Mac'te kurulumu tek komutla doğrula.

    Keychain makineye özeldir; her makinede ayrı kurulur. Bu mod hangi parçanın
    eksik olduğunu tek bakışta gösterir.
    """
    import platform
    import socket

    from backend.services.scrape_credentials import credentials_status

    ok = True
    print(f"Makine: {platform.node()} ({socket.gethostname()})")
    pw_state = "var" if _has_playwright() else "YOK"
    print(f"Yorumlayıcı: {sys.executable} · playwright {pw_state}")
    if pw_state == "YOK":
        ok = False
        cands = interpreter_candidates()
        if cands:
            print("   Denenen diğer yorumlayıcılar:")
            for c in cands:
                print(f"     playwright {'var' if _interpreter_has_playwright(c) else 'YOK'} · {c}")
        else:
            print("   Başka yorumlayıcı bulunamadı (venv yok, köprü plist'i okunamadı).")
        target = next((c for c in cands), ROOT / ".venv" / "bin" / "python")
        print(f"   Çare: {target} -m pip install playwright "
              f"&& {target} -m playwright install firefox")
    print()

    print("1) Keychain kimlikleri")
    for key in ("asc", "google"):
        st = credentials_status(key)
        good = bool(st["ready"])
        ok = ok and good
        mark = "OK  " if good else "EKSİK"
        detail = "e-posta+parola var" if good else (
            "keychain yok" if not st["keychain_available"]
            else f"e-posta:{'var' if st['has_email'] else 'YOK'} parola:{'var' if st['has_password'] else 'YOK'}"
        )
        print(f"   {mark} {key:<8} {st['keychain_service']:<20} {detail}")

    print()
    print("2) Firefox profilleri")
    for name, spec in TARGETS.items():
        path = spec["profile"]()
        exists = path.exists()
        cookies = (path / "cookies.sqlite").exists()
        state = "profil+çerez" if cookies else ("profil var, çerez yok" if exists else "YOK")
        print(f"   {'OK  ' if cookies else 'NOT '} {name:<8} {state:<22} {path}")

    print()
    print("3) Panel bağlantısı (boşluk doldurma için)")
    import os

    base = (os.environ.get("PROJECT_CONTROL_BASE_URL")
            or "https://projectcontrol.up.railway.app")
    # Köprü .env'i kendi yüklüyor (bkz. bridge._load_dotenv); bu yüzden token
    # etkileşimli kabukta görünmeyebilir ama koşarken mevcut olur. İkisine de bak.
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    where = "ortam"
    if not token:
        env_file = ROOT / ".env"
        try:
            for line in env_file.read_text(encoding="utf-8").splitlines():
                if line.strip().startswith("NOTIFICATION_INGEST_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    where = ".env"
        except Exception:  # noqa: BLE001
            pass
    has_token = bool(token)
    ok = ok and has_token
    print(f"   {'OK  ' if has_token else 'EKSİK'} ingest token "
          f"{'var (' + where + ')' if has_token else 'YOK — .env kontrol et'} · {base}")

    if with_browser:
        print()
        print("4) Oturumlar (tarayıcı açılır, giriş DENENMEZ)")
        for name in TARGETS:
            r = warm_target(name, check_only=True, headed=True)
            good = not r.needs_action
            ok = ok and good
            print(f"   {'OK  ' if good else 'DİKKAT'} {name:<8} {r.message}")
    else:
        print()
        print("4) Oturum kontrolü atlandı (--no-browser)")

    print()
    print("SONUÇ: " + ("kurulum hazır" if ok else "eksik var — yukarıdaki EKSİK/DİKKAT satırlarına bak"))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scrape login warm-up")
    parser.add_argument("--only", choices=sorted(TARGETS), help="tek hedef")
    parser.add_argument("--check", action="store_true", help="yalnızca kontrol, login denemez")
    parser.add_argument("--headless", action="store_true", help="pencere açma (müdahale gerekirse işe yaramaz)")
    parser.add_argument("--no-report", action="store_true", help="panele bildirme")
    parser.add_argument("--doctor", action="store_true", help="kurulum teşhisi (yeni makine)")
    parser.add_argument("--no-browser", action="store_true", help="teşhiste tarayıcı açma")
    parser.add_argument("--takeover", action="store_true",
                        help="açık pencereyi devral (oturum kaybına yol açabilir)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.doctor:
        return doctor(with_browser=not args.no_browser)
    names = [args.only] if args.only else list(TARGETS)

    results: list[TargetResult] = []
    for name in names:
        LOGGER.info("warm-up başlıyor: %s", name)
        r = warm_target(name, check_only=args.check, headed=not args.headless,
                        takeover=args.takeover)
        LOGGER.info("%s → %s · %s", name, r.status, r.message)
        results.append(r)

    if not args.no_report:
        report_to_panel(results)
        send_alert_emails(results)

    print(json.dumps({"results": [r.as_dict() for r in results]}, ensure_ascii=False, indent=2))
    return 1 if any(r.needs_action for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())


def warm_for_job(job_id: str, *, headed: bool = True) -> list[TargetResult]:
    """Elle tetiklenen iş öncesi ilgili oturumu doğrula.

    Panel «yenile / güncelle» düğmeleri de planlı turlarla aynı yoldan geçsin
    diye ayrı tutuldu: oturum geçerliyse hiçbir maliyet yok, düşmüşse Keychain
    ile giriş denenir ve iş boşa gitmez.
    """
    targets = JOB_WARMUP_TARGETS.get((job_id or "").strip().lower(), ())
    return [warm_target(t, headed=headed) for t in targets]
