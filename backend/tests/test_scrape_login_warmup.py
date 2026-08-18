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


def _bridge():
    import importlib.util as iu
    import os

    os.environ.setdefault("NOTIFICATION_INGEST_TOKEN", "x")
    spec = iu.spec_from_file_location(
        "bridge_sched", ROOT / "scripts/doviz_admin_notification_bridge.py"
    )
    mod = iu.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


def test_asc_and_firebase_run_four_times_a_day():
    m = _bridge()
    assert [f"{h:02d}:{mi:02d}" for h, mi in m.ASC_SLOTS] == ["06:30", "11:15", "14:00", "20:00"]
    assert len(m.FIREBASE_SLOTS) == 4


def test_firebase_follows_asc_by_three_minutes():
    """Köprünün tarayıcı kuyruğu da 3 dk aralık istiyor; slotlar buna uymalı."""
    m = _bridge()
    gap_min = max(3, int(m.BRIDGE_SCRAPE_MIN_GAP_SEC) // 60)
    for (ah, am), (fh, fm) in zip(m.ASC_SLOTS, m.FIREBASE_SLOTS):
        delta = (fh * 60 + fm) - (ah * 60 + am)
        assert delta == 3, f"ASC {ah:02d}:{am:02d} → Firebase {fh:02d}:{fm:02d} = {delta} dk"
        assert delta >= gap_min


def test_warmup_runs_before_every_asc_slot():
    m = _bridge()
    assert len(m.LOGIN_WARMUP_SLOTS) == len(m.ASC_SLOTS)
    for (wh, wm), (ah, am) in zip(m.LOGIN_WARMUP_SLOTS, m.ASC_SLOTS):
        lead = (ah * 60 + am) - (wh * 60 + wm)
        assert lead == 10, f"warm-up {wh:02d}:{wm:02d} → ASC {ah:02d}:{am:02d} = {lead} dk"


def test_slot_pairs_parser_ignores_junk():
    m = _bridge()
    assert m._parse_slot_pairs("7:05, 12:40 ,bozuk, 19:00") == ((7, 5), (12, 40), (19, 0))
    assert m._parse_slot_pairs("") == ()


def test_history_seal_no_longer_caps_asc_frequency():
    """Mühür geçmiş derinliği içindi; tur sıklığını kısmamalı (PLAY'de kalır)."""
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    seal = src.split("from backend.services.history_seal import", 1)[1].split("except Exception", 1)[0]
    assert "PLAY_SLOT_HOURS = (6,)" in seal
    assert "ASC_SLOTS = ((6, 30),)" not in seal
    assert "mark_all_expensive_pipelines_sealed()" in seal


def test_bridge_wires_the_warmup_runner():
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    assert "run_login_warmup_bridge_once" in src
    assert '"login_warmup"' in src
    assert "slots=LOGIN_WARMUP_SLOTS" in src
    assert "slots=ASC_SLOTS" in src
    assert "slots=FIREBASE_SLOTS" in src


def test_report_is_sent_to_project_control():
    m = _warmup_module()
    src = (ROOT / "scripts/scrape_login_warmup.py").read_text(encoding="utf-8")
    assert "/api/scrape-runs/report" in src
    assert "X-Notification-Ingest-Token" in src
    assert hasattr(m, "report_to_panel")


# ── 3 alan: ASC + Firebase + Play ───────────────────────────────────────────

def test_all_three_login_areas_are_covered():
    m = _warmup_module()
    assert set(m.TARGETS) == {"asc", "firebase", "play"}
    assert m.TARGETS["play"]["credential_key"] == "google"
    assert m.TARGETS["firebase"]["credential_key"] == "google"
    assert m.TARGETS["asc"]["credential_key"] == "asc"


def test_play_shares_the_google_profile_and_its_own_key():
    m = _warmup_module()
    from backend.services.scrape_browser import firebase_profile_dir, google_profile_dir

    assert m.TARGETS["play"]["profile"]() == google_profile_dir()
    assert m.TARGETS["firebase"]["profile"]() == firebase_profile_dir()
    # Warm-session anahtarları scrape'lerle birebir
    assert m.TARGETS["play"]["key"] == "play"
    assert m.TARGETS["play"]["env_key"] == "PLAY_CONSOLE_KEEP_OPEN"


def test_job_warmup_mapping_covers_manual_buttons():
    m = _warmup_module()
    assert set(m.JOB_WARMUP_TARGETS) == {"asc", "firebase", "play"}
    assert hasattr(m, "warm_for_job")


def test_manual_trigger_runs_warmup_first():
    """Panel «yenile» düğmesi de planlı turla aynı yoldan geçmeli."""
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    assert "_warmup_before_job" in src
    assert "_WARMUP_JOB_IDS" in src
    # İş başlamadan önce çağrılmalı
    body = src.split("def _run_claimed_job", 1)[1]
    assert body.index("_warmup_before_job(job_id)") < body.index("meta[\"runner\"]")


def test_manual_warmup_only_for_login_kinds():
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    block = src.split("_WARMUP_JOB_IDS = frozenset(", 1)[1].split(")", 1)[0]
    for kind in ("asc", "firebase", "play"):
        assert kind in block
    # Login gerektirmeyen işler warm-up maliyeti ödemesin
    assert "market" not in block and "news" not in block


def test_data_window_is_last_day_and_persists_yesterday():
    """“Tüm geçmişi çekme, dünü kaydet, boşluk bırakma” — pencere sözleşmesi."""
    from backend.services.history_seal import scheduled_fetch_window

    w = scheduled_fetch_window("asc", force_full=False)
    assert w["mode"] in ("yesterday_only", "gap_fill")
    assert w["store_end"] == w["yesterday"]      # bugün kalıcı kaydedilmez
    assert w["days"] <= 3                         # tüm geçmiş çekilmez


# ── Yorumlayıcı kendini düzeltmeli (ofis Mac: sistem python3'te playwright yok)

def test_reexecs_into_venv_when_playwright_is_missing(monkeypatch):
    m = _warmup_module()
    calls = {}

    monkeypatch.setattr(m, "_has_playwright", lambda: False)
    monkeypatch.delenv("_WARMUP_REEXEC", raising=False)
    monkeypatch.setattr(m.os, "execv", lambda p, a: calls.setdefault("exec", (p, a)))
    monkeypatch.setattr(m.sys, "executable", "/usr/bin/python3")
    monkeypatch.setattr(m, "sys", m.sys)

    venv = m.ROOT / ".venv" / "bin" / "python"
    if not venv.exists():
        return  # venv yoksa davranış zaten "hiçbir şey yapma"
    m._ensure_playwright_interpreter()
    assert "exec" in calls, "venv'e geçilmedi"
    assert calls["exec"][0] == str(venv)


def test_no_reexec_when_playwright_is_available(monkeypatch):
    m = _warmup_module()
    monkeypatch.setattr(m, "_has_playwright", lambda: True)
    monkeypatch.setattr(m.os, "execv", lambda p, a: (_ for _ in ()).throw(AssertionError("exec olmamalı")))
    m._ensure_playwright_interpreter()   # patlamamalı


def test_reexec_guard_prevents_infinite_loop(monkeypatch):
    """venv'de de playwright yoksa tekrar tekrar exec edilmemeli."""
    m = _warmup_module()
    monkeypatch.setattr(m, "_has_playwright", lambda: False)
    monkeypatch.setenv("_WARMUP_REEXEC", "1")
    monkeypatch.setattr(m.os, "execv", lambda p, a: (_ for _ in ()).throw(AssertionError("exec olmamalı")))
    m._ensure_playwright_interpreter()


def test_doctor_reports_interpreter_and_the_fix():
    src = (ROOT / "scripts/scrape_login_warmup.py").read_text(encoding="utf-8")
    assert "Yorumlayıcı:" in src
    assert "pip install playwright" in src   # çaresi yazılı


# ── E-posta uyarısı: iki makine için de, spam yapmadan ──────────────────────

def _res(target, needs_action, msg="mesaj"):
    m = _warmup_module()
    return m.TargetResult(target=target, label=target.upper(),
                          status="intervention" if needs_action else "ok",
                          message=msg, needs_action=needs_action)


def _mail_spy(m, monkeypatch, tmp_path):
    sent = []
    monkeypatch.setattr(m, "_alert_state_path", lambda: tmp_path / "alerts.json")
    monkeypatch.setattr("backend.services.mailer.send_email",
                        lambda s, h, r=None: sent.append((s, h)) or True)
    return sent


def test_failure_sends_one_email_with_machine_name(monkeypatch, tmp_path):
    m = _warmup_module()
    sent = _mail_spy(m, monkeypatch, tmp_path)
    monkeypatch.setattr(m, "_machine_name", lambda: "Ofis-Mac")

    m.send_alert_emails([_res("asc", True, "2FA gerekiyor")])
    assert len(sent) == 1
    subject, html = sent[0]
    assert "Ofis-Mac" in subject and "asc" in subject
    assert "2FA gerekiyor" in html


def test_repeat_failure_is_throttled(monkeypatch, tmp_path):
    m = _warmup_module()
    sent = _mail_spy(m, monkeypatch, tmp_path)
    fail = [_res("asc", True)]
    m.send_alert_emails(fail)
    m.send_alert_emails(fail)      # hemen ardından
    assert len(sent) == 1, "aynı arıza tekrar mail atmamalı"


def test_alert_repeats_after_the_window(monkeypatch, tmp_path):
    m = _warmup_module()
    sent = _mail_spy(m, monkeypatch, tmp_path)
    m.send_alert_emails([_res("asc", True)])
    # Durumu geriye al: pencere dolmuş gibi
    import json
    path = tmp_path / "alerts.json"
    state = json.loads(path.read_text())
    for k in state:
        state[k]["last_sent"] = 0
    path.write_text(json.dumps(state))
    m.send_alert_emails([_res("asc", True)])
    assert len(sent) == 2


def test_recovery_email_is_sent_once(monkeypatch, tmp_path):
    m = _warmup_module()
    sent = _mail_spy(m, monkeypatch, tmp_path)
    m.send_alert_emails([_res("asc", True)])          # arıza
    m.send_alert_emails([_res("asc", False)])         # düzeldi
    m.send_alert_emails([_res("asc", False)])         # hâlâ iyi → sessiz
    assert len(sent) == 2
    assert "düzeldi" in sent[1][0]


def test_healthy_from_the_start_sends_nothing(monkeypatch, tmp_path):
    m = _warmup_module()
    sent = _mail_spy(m, monkeypatch, tmp_path)
    m.send_alert_emails([_res("asc", False), _res("firebase", False)])
    assert sent == []


def test_targets_are_tracked_independently(monkeypatch, tmp_path):
    m = _warmup_module()
    sent = _mail_spy(m, monkeypatch, tmp_path)
    m.send_alert_emails([_res("asc", True)])
    m.send_alert_emails([_res("asc", True), _res("firebase", True)])
    # asc throttle'da, firebase yeni → ikinci mail yalnızca firebase için
    assert len(sent) == 2
    assert "firebase" in sent[1][0] and "asc" not in sent[1][0]


def test_mailer_failure_does_not_break_warmup(monkeypatch, tmp_path):
    m = _warmup_module()
    monkeypatch.setattr(m, "_alert_state_path", lambda: tmp_path / "alerts.json")
    monkeypatch.setattr("backend.services.mailer.send_email",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("smtp yok")))
    try:
        m.send_alert_emails([_res("asc", True)])
    except Exception as exc:  # noqa: BLE001
        raise AssertionError(f"warm-up mail hatasında patlamamalı: {exc}") from exc


def test_bridge_sends_alerts_too():
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    assert "send_alert_emails" in src


# ── Yorumlayıcı adayları ────────────────────────────────────────────────────

def test_bridge_interpreter_is_the_first_candidate():
    """Köprünün python'u çalıştığı bilinen tek yorumlayıcı — önce o denenmeli."""
    src = (ROOT / "scripts/scrape_login_warmup.py").read_text(encoding="utf-8")
    block = src.split("def interpreter_candidates", 1)[1].split("\ndef ", 1)[0]
    assert block.index("_bridge_interpreter()") < block.index('".venv"')


def test_candidates_skip_the_current_interpreter():
    m = _warmup_module()
    import sys as _sys

    for c in m.interpreter_candidates():
        assert str(c.resolve()) != str(pathlib_resolve(_sys.executable))


def pathlib_resolve(p):
    from pathlib import Path as _P

    return _P(p).resolve()


def test_warmup_is_exempt_from_the_shared_lease():
    """Oturum makineye özel: kira uygulanırsa kaybeden Mac'in oturumu yenilenmez.

    Veri taramaları (asc/firebase) kirayla tekilleştirilmeli — iki kez koşarsa
    boşa gider. Warm-up tam tersi: her makine kendi Firefox profilini kontrol
    etmeli, yoksa kirayı kaybeden Mac ölü oturumla scrape'e girer.
    """
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")

    # warm-up çağrısı shared=False olmalı
    warm_call = src.split('"login_warmup", "Login warm-up"', 1)[1].split(")", 1)[0]
    assert "shared=False" in warm_call

    # veri taramaları kirada kalmalı
    for kind in ('"asc", "ASC"', '"firebase", "Firebase"'):
        call = src.split(kind, 1)[1].split(")", 1)[0]
        assert "shared=False" not in call, f"{kind} kirada kalmalı"

    # kira yalnızca shared işlerde sorulmalı
    body = src.split("def _slot_job(", 1)[1].split("\n        def ", 1)[0]
    assert "if shared:" in body
    assert body.index("if shared:") < body.index("_auto_lease_state(kind, slot)")
