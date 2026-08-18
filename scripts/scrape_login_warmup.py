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

from backend.services.scrape_browser import (  # noqa: E402
    acquire_persistent_context,
    asc_profile_dir,
    firebase_profile_dir,
    google_profile_dir,
    release_persistent_context,
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
        "url": "https://play.google.com/console",
        "credential_key": "google",
        "login_host_hints": ("accounts.google.com", "signin/v2", "ServiceLogin"),
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


def _looks_like_login(page: Any, hints: tuple[str, ...]) -> bool:
    url = (getattr(page, "url", "") or "").lower()
    if any(h.lower() in url for h in hints):
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


def _fill_first(page: Any, selectors: tuple[str, ...], value: str) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=2000):
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

    blocker = _needs_human(page)
    if blocker:
        result.status = "intervention"
        result.needs_action = True
        result.message = f"{spec['label']}: ikinci faktör gerekiyor ({blocker}). Pencere açık."
        result.detail["blocker"] = blocker
        return

    if _looks_like_login(page, spec["login_host_hints"]):
        result.status = "intervention"
        result.needs_action = True
        result.message = (
            f"{spec['label']}: otomatik giriş tamamlanamadı, giriş ekranı duruyor. Pencere açık."
        )
        result.detail["reason"] = "still_on_login"
        return

    result.status = "logged_in"
    result.message = f"{spec['label']}: oturum yenilendi."


def warm_target(name: str, *, check_only: bool = False, headed: bool = True) -> TargetResult:
    spec = TARGETS[name]
    result = TargetResult(target=name, label=spec["label"])
    profile = spec["profile"]()
    pw = ctx = None
    try:
        pw, ctx, reused = acquire_persistent_context(
            spec["key"],
            profile=profile,
            headed=headed,
            env_key=spec["env_key"],
            label=f"warmup:{spec['label']}",
            locale="tr-TR",
        )
        result.detail["profile"] = str(profile)
        result.detail["reused_warm_session"] = bool(reused)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(spec["url"], wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        time.sleep(SETTLE_SEC)

        if not _looks_like_login(page, spec["login_host_hints"]):
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scrape login warm-up")
    parser.add_argument("--only", choices=sorted(TARGETS), help="tek hedef")
    parser.add_argument("--check", action="store_true", help="yalnızca kontrol, login denemez")
    parser.add_argument("--headless", action="store_true", help="pencere açma (müdahale gerekirse işe yaramaz)")
    parser.add_argument("--no-report", action="store_true", help="panele bildirme")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    names = [args.only] if args.only else list(TARGETS)

    results: list[TargetResult] = []
    for name in names:
        LOGGER.info("warm-up başlıyor: %s", name)
        r = warm_target(name, check_only=args.check, headed=not args.headless)
        LOGGER.info("%s → %s · %s", name, r.status, r.message)
        results.append(r)

    if not args.no_report:
        report_to_panel(results)

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
