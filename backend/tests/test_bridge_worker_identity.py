"""Bridge worker kimliği, kabiliyet raporu, otomatik iş kirası ve oturum açma ucu."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_bridge():
    path = Path(__file__).resolve().parents[2] / "scripts" / "doviz_admin_notification_bridge.py"
    spec = importlib.util.spec_from_file_location("doviz_admin_notification_bridge", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_worker_name_prefers_env_and_is_slugified(monkeypatch):
    b = _load_bridge()
    monkeypatch.setenv("BRIDGE_WORKER_NAME", "cem-office-mac")
    b._worker_name_cache = ""
    assert b._worker_name() == "cem-office-mac"


def test_auto_derived_worker_name_has_machine_suffix(monkeypatch):
    """İki Mac'in bilgisayar adı aynı olsa bile kuyrukta ayrışsın."""
    b = _load_bridge()
    monkeypatch.delenv("BRIDGE_WORKER_NAME", raising=False)
    b._worker_name_cache = ""
    name = b._worker_name()
    assert name == name.lower()
    suffix = name.rsplit("-", 1)[-1]
    assert len(suffix) == 4 and suffix == b._machine_fingerprint()


def test_readiness_marks_missing_credentials(monkeypatch):
    b = _load_bridge()
    monkeypatch.setattr(b, "_job_session_ok", lambda jid: True)
    monkeypatch.delenv("VIRGUL_EMAIL", raising=False)
    monkeypatch.delenv("VIRGUL_PASSWORD", raising=False)
    monkeypatch.setenv("DOVIZ_ADMIN_EMAIL", "a@b.c")
    monkeypatch.setenv("DOVIZ_ADMIN_PASSWORD", "x")
    monkeypatch.setattr(b, "_playwright_firefox_ready", lambda: True)
    b._readiness_cache = (0.0, {})
    ready = b._worker_readiness()
    assert ready["virgul"] == "no_creds"
    assert ready["revenue_targets"] == "no_creds"
    assert ready["notification"] == "ready"
    assert ready["moderation"] == "ready"


def test_readiness_marks_missing_playwright_browser(monkeypatch):
    b = _load_bridge()
    monkeypatch.setattr(b, "_playwright_firefox_ready", lambda: False)
    monkeypatch.setattr(b, "_job_session_ok", lambda jid: True)  # oturumdan bağımsız ölç
    b._readiness_cache = (0.0, {})
    ready = b._worker_readiness()
    assert ready["asc"] == "no_browser"
    assert ready["moderation"] == "no_browser"
    # Firebase sistem Firefox'a düşebiliyor — engellenmemeli
    assert ready["firebase"] == "ready"


def test_ready_param_is_parsed_back_by_the_api():
    from backend.api.page_tarama import _parse_ready

    b = _load_bridge()
    b._readiness_cache = (0.0, {})
    parsed = _parse_ready(b._ready_param())
    assert parsed is not None
    assert set(parsed) == set(b._remote_claim_job_registry())


def test_job_id_aliases_reach_the_needs_login_classifier():
    b = _load_bridge()
    for job_id in ("cwv", "policy", "moderation", "noads", "links"):
        kind = b.JOB_ID_KIND_ALIASES.get(job_id, job_id)
        assert b._result_needs_login(kind, None, "GSC oturumu yok — giriş gerekli")


def _lease_resp(monkeypatch, bridge, status: int, payload: dict | None = None):
    class _Resp:
        status_code = status

        def json(self):
            return payload or {}

    monkeypatch.setattr(bridge, "_ingest_token", lambda: "t")
    monkeypatch.setattr(bridge.requests, "post", lambda *a, **k: _Resp())


def test_auto_lease_held_by_other_mac(monkeypatch):
    b = _load_bridge()
    _lease_resp(monkeypatch, b, 200, {"ok": True, "granted": False, "holder": "cem-home-mac"})
    assert b._auto_lease_state("sinemalar_moderation", "2026-08-17T14:17") == b.LEASE_HELD


def test_auto_lease_granted(monkeypatch):
    b = _load_bridge()
    _lease_resp(monkeypatch, b, 200, {"ok": True, "granted": True, "holder": "cem-office-mac"})
    assert b._auto_lease_state("play", "slot") == b.LEASE_GRANTED


def test_auto_lease_falls_back_when_endpoint_missing_or_unauthorized(monkeypatch):
    """Deploy penceresi / eski Railway: kira yüzünden zamanlı taramalar durmasın."""
    for status in (401, 403, 404):
        b = _load_bridge()
        _lease_resp(monkeypatch, b, status, {})
        assert b._auto_lease_state("play", "slot") == b.LEASE_GRANTED, status


def test_auto_lease_unavailable_when_railway_unreachable(monkeypatch):
    """Slot 'yapıldı' diye işaretlenmemeli — geçici hata kalıcı atlamaya dönüşmesin."""
    b = _load_bridge()
    monkeypatch.setattr(b, "_ingest_token", lambda: "t")

    def _boom(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(b.requests, "post", _boom)
    assert b._auto_lease_state("play", "slot") == b.LEASE_UNAVAILABLE


def test_slot_job_marks_done_only_when_another_mac_holds_the_lease():
    """held → slot işaretlenir; unavailable → işaretlenmez (kaynak koda dair sözleşme)."""
    src = (
        Path(__file__).resolve().parents[2] / "scripts" / "doviz_admin_notification_bridge.py"
    ).read_text(encoding="utf-8")
    chunk = src.split("lease = _auto_lease_state(kind, slot)", 1)[1][:400]
    assert "if lease == LEASE_HELD:" in chunk
    assert "globals()[last_attr] = slot" in chunk
    unavailable = chunk.split("if lease == LEASE_UNAVAILABLE:", 1)[1][:120]
    assert "globals()[last_attr]" not in unavailable


def test_readiness_marks_missing_session_as_login_required(monkeypatch):
    """Oturumu olmayan Mac işi kapmasın — boşuna deneme ve hata olmasın."""
    b = _load_bridge()
    monkeypatch.setattr(b, "_playwright_firefox_ready", lambda: True)
    monkeypatch.setattr(b, "_job_session_ok", lambda jid: False if jid == "asc" else True)
    b._readiness_cache = (0.0, {})
    ready = b._worker_readiness()
    assert ready["asc"] == "login_required"
    assert ready["moderation"] == "ready"


def test_unknown_session_state_does_not_block_the_job(monkeypatch):
    """Çerez veritabanı okunamıyorsa iş engellenmemeli."""
    b = _load_bridge()
    monkeypatch.setattr(b, "_playwright_firefox_ready", lambda: True)
    monkeypatch.setattr(b, "_job_session_ok", lambda jid: None)
    b._readiness_cache = (0.0, {})
    assert set(b._worker_readiness().values()) <= {"ready", "no_creds"}


def test_session_cookie_probe_reads_firefox_profile(tmp_path):
    import sqlite3

    from backend.services.system_firefox_driver import profile_has_session_cookie

    profile = tmp_path / "fx-sinemalar"
    profile.mkdir()
    # Profil var ama çerez veritabanı yok → oturum yok
    assert profile_has_session_cookie(profile, "sinemalar.com", ("PHPSESSID",)) is False
    # Profil hiç yok → bilinmiyor
    assert profile_has_session_cookie(tmp_path / "yok", "sinemalar.com", ("PHPSESSID",)) is None

    con = sqlite3.connect(profile / "cookies.sqlite")
    con.execute("CREATE TABLE moz_cookies (host TEXT, name TEXT, expiry INTEGER)")
    con.execute("INSERT INTO moz_cookies VALUES ('.sinemalar.com', 'PHPSESSID', 0)")
    con.execute("INSERT INTO moz_cookies VALUES ('.other.com', 'PHPSESSID', 0)")
    con.commit()
    con.close()
    assert profile_has_session_cookie(profile, "sinemalar.com", ("PHPSESSID",)) is True
    assert profile_has_session_cookie(profile, "sinemalar.com", ("myacinfo",)) is False


def test_expired_session_cookie_counts_as_logged_out(tmp_path):
    import sqlite3

    from backend.services.system_firefox_driver import profile_has_session_cookie

    profile = tmp_path / "fx-asc"
    profile.mkdir()
    con = sqlite3.connect(profile / "cookies.sqlite")
    con.execute("CREATE TABLE moz_cookies (host TEXT, name TEXT, expiry INTEGER)")
    con.execute("INSERT INTO moz_cookies VALUES ('.apple.com', 'myacinfo', 1000)")
    con.commit()
    con.close()
    assert profile_has_session_cookie(profile, "apple.com", ("myacinfo",)) is False


def test_whoami_endpoint_is_served_for_the_panel_probe():
    """Panel 127.0.0.1/whoami ile bastığın Mac'i tanır; CORS + PNA başlıkları şart."""
    src = (
        Path(__file__).resolve().parents[2] / "scripts" / "doviz_admin_notification_bridge.py"
    ).read_text(encoding="utf-8")
    assert 'if path in ("/whoami", "/worker"):' in src
    assert '"Access-Control-Allow-Private-Network": "true"' in src
    assert "https://projectcontrol.up.railway.app" in src


def test_panel_sends_local_worker_as_preference():
    js = (
        Path(__file__).resolve().parents[2] / "static" / "js" / "page_tarama.js"
    ).read_text(encoding="utf-8")
    assert "function probeLocalWorker()" in js
    assert 'BRIDGE + "/whoami"' in js
    assert "claimManual(key, localWorker)" in js
    assert "prefer: prefer" in js


def test_login_endpoint_targets_cover_every_session():
    b = _load_bridge()
    assert set(b.LOGIN_TARGETS) == {"google", "asc", "sinemalar", "empower"}
    assert b.run_open_login("")[0] == 400
    assert "google" in b.run_open_login("")[1]["targets"]


def test_login_helper_keeps_long_interactive_wait():
    """Daemon kısa login beklerken elle oturum açma 15 dk kalmalı."""
    from backend.services import scrape_browser

    src = Path(scrape_browser.__file__).read_text(encoding="utf-8")
    assert "timeout_sec = max(LOGIN_WAIT_SEC, int(timeout_sec or LOGIN_WAIT_SEC))" in src
    assert scrape_browser.login_wait_sec(default=150) == 150


def test_google_jobs_are_not_pre_blocked_by_a_cookie_check():
    """Google oturumu çerez tablosunda görünmeyebiliyor (session-restore deposu).

    Ön kontrol yanlış negatif verirse çalışan Mac devre dışı kalır; bu işlerde
    oturum sorunu taramanın needs_login sonucuyla ve devirle çözülür.
    """
    b = _load_bridge()
    assert b.GOOGLE_SESSION_JOB_IDS == frozenset()
    for jid in ("play", "cwv", "links", "policy", "firebase"):
        assert b._job_session_ok(jid) is None, jid
    # Profil tabanlı kesin kontroller duruyor
    assert set(b.SESSION_JOB_PROFILES) == {"moderation", "noads", "asc"}
