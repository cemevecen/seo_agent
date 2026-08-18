"""Login warm-up + Keychain kimlik katmanı.

Sözleşme: parola asla sızmaz, 2FA/CAPTCHA otomatik aşılmaya çalışılmaz,
müdahale gerektiğinde pencere açık kalır ve panele bildirilir.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

from backend.services import scrape_credentials as C  # noqa: E402


def _warmup_module():
    spec = importlib.util.spec_from_file_location(
        "scrape_login_warmup", ROOT / "scripts/scrape_login_warmup.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclass çözümlemesi için şart
    spec.loader.exec_module(mod)
    return mod


# ── Kimlik bilgisi ──────────────────────────────────────────────────────────

def test_password_never_appears_in_repr():
    creds = C.Credentials(target="asc", email="a@b.com", password="çok-gizli")
    assert "çok-gizli" not in repr(creds)
    assert "dolu" in repr(creds)


def test_status_never_returns_the_password():
    st = C.credentials_status("asc")
    assert "password" not in st
    assert set(st) >= {"has_password", "has_email", "ready"}
    assert isinstance(st["has_password"], bool)


def test_unknown_target_is_empty_not_an_error():
    creds = C.load_credentials("bilinmeyen")
    assert creds.email == "" and creds.password == "" and creds.complete is False


def test_credentials_come_from_keychain_not_env_file(monkeypatch):
    """Parola .env'den okunmamalı — sadece Keychain."""
    monkeypatch.setattr(C, "keychain_available", lambda: True)
    monkeypatch.setattr(C, "_keychain_password", lambda service: "kc-parola")
    monkeypatch.setattr(C, "_keychain_account", lambda service: "kc@ornek.com")
    monkeypatch.setenv("ASC_LOGIN_PASSWORD", "env-parola-kullanilmamali")
    creds = C.load_credentials("asc")
    assert creds.password == "kc-parola"
    assert creds.email == "kc@ornek.com"
    src = (ROOT / "backend/services/scrape_credentials.py").read_text(encoding="utf-8")
    assert "ASC_LOGIN_PASSWORD" not in src


def test_email_env_overrides_keychain_account(monkeypatch):
    monkeypatch.setattr(C, "keychain_available", lambda: True)
    monkeypatch.setattr(C, "_keychain_password", lambda service: "p")
    monkeypatch.setenv("ASC_LOGIN_EMAIL", "env@ornek.com")
    assert C.load_credentials("asc").email == "env@ornek.com"


def test_missing_keychain_is_not_fatal(monkeypatch):
    monkeypatch.setattr(C, "keychain_available", lambda: False)
    creds = C.load_credentials("asc")
    assert creds.complete is False


# ── Warm-up davranışı ───────────────────────────────────────────────────────

class _Loc:
    def __init__(self, visible=False):
        self._v = visible
        self.filled = None

    @property
    def first(self):
        return self

    def is_visible(self, timeout=0):
        return self._v

    def click(self):
        pass

    def fill(self, v):
        self.filled = v


class _Page:
    def __init__(self, url="", text="", visible=()):
        self.url = url
        self._text = text
        self._visible = set(visible)
        self.keyboard = self

    def inner_text(self, sel):
        return self._text

    def content(self):
        return self._text

    def locator(self, sel):
        return _Loc(sel in self._visible)

    def press(self, key):
        pass


def test_two_factor_stops_the_attempt():
    """2FA otomatik aşılmaya çalışılmamalı — kilitlenme riski ve yanlış."""
    m = _warmup_module()
    page = _Page(url="https://idmsa.apple.com/x", text="Two-factor authentication required")
    spec = m.TARGETS["asc"]
    result = m.TargetResult(target="asc", label="ASC")
    m.load_credentials = lambda k: C.Credentials(k, "a@b.com", "p")  # type: ignore[assignment]
    m._attempt_login(page, spec, result)
    assert result.status == "intervention"
    assert result.needs_action is True
    assert "two-factor" in result.detail.get("blocker", "")


def test_no_credentials_asks_for_help_instead_of_guessing():
    m = _warmup_module()
    m.load_credentials = lambda k: C.Credentials(k, "", "")  # type: ignore[assignment]
    result = m.TargetResult(target="asc", label="ASC")
    m._attempt_login(_Page(url="https://idmsa.apple.com"), m.TARGETS["asc"], result)
    assert result.status == "intervention"
    assert result.detail["reason"] == "no_credentials"


def test_valid_session_is_detected_without_login():
    m = _warmup_module()
    page = _Page(url="https://appstoreconnect.apple.com/apps", text="my apps")
    assert m._looks_like_login(page, m.TARGETS["asc"]["login_host_hints"]) is False


def test_login_page_is_detected_by_host_and_by_field():
    m = _warmup_module()
    hints = m.TARGETS["asc"]["login_host_hints"]
    assert m._looks_like_login(_Page(url="https://idmsa.apple.com/signin"), hints) is True
    assert m._looks_like_login(
        _Page(url="https://x", visible=('input[type="password"]',)), hints) is True


def test_intervention_markers_cover_captcha_and_2fa():
    m = _warmup_module()
    for text in ("please complete the reCAPTCHA", "iki faktörlü doğrulama",
                 "2-step verification", "unusual activity detected"):
        assert m._needs_human(_Page(text=text)) != ""
    assert m._needs_human(_Page(text="app analytics dashboard")) == ""


def test_warmup_shares_the_same_warm_session_keys_as_the_scrapers():
    """Farklı key kullanılsaydı warm-up ayrı pencere açar, oturum paylaşılmazdı."""
    m = _warmup_module()
    assert m.TARGETS["asc"]["key"] == "asc"
    assert m.TARGETS["asc"]["env_key"] == "ASC_CONSOLE_KEEP_OPEN"
    assert m.TARGETS["firebase"]["key"] == "firebase"
    assert m.TARGETS["firebase"]["env_key"] == "FIREBASE_CONSOLE_KEEP_OPEN"


def test_bridge_runs_warmup_before_the_morning_scrapes():
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    assert "LOGIN_WARMUP_SLOT_HOURS = (5,)" in src
    assert "run_login_warmup_bridge_once" in src
    assert '"login_warmup"' in src
    # Warm-up ASC (06:11) ve Firebase (06:46) slotlarından önce olmalı
    assert 'LOGIN_WARMUP_BRIDGE_MINUTE") or "45"' in src


def test_report_is_sent_to_project_control():
    m = _warmup_module()
    src = (ROOT / "scripts/scrape_login_warmup.py").read_text(encoding="utf-8")
    assert "/api/scrape-runs/report" in src
    assert "X-Notification-Ingest-Token" in src
    assert hasattr(m, "report_to_panel")
