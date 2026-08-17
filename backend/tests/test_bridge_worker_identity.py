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


def test_auto_lease_denied_skips_the_scheduled_run(monkeypatch):
    b = _load_bridge()
    monkeypatch.setattr(b, "_ingest_token", lambda: "t")

    class _Resp:
        status_code = 200

        def json(self):
            return {"ok": True, "granted": False, "holder": "cem-home-mac"}

    monkeypatch.setattr(b.requests, "post", lambda *a, **k: _Resp())
    assert b._auto_lease_ok("sinemalar_moderation", "2026-08-17T14:17") is False


def test_auto_lease_allows_when_railway_is_old(monkeypatch):
    """Uç henüz deploy edilmediyse (404) mevcut davranış korunur."""
    b = _load_bridge()
    monkeypatch.setattr(b, "_ingest_token", lambda: "t")

    class _Resp:
        status_code = 404

        def json(self):
            return {}

    monkeypatch.setattr(b.requests, "post", lambda *a, **k: _Resp())
    assert b._auto_lease_ok("play", "slot") is True


def test_auto_lease_skips_when_railway_unreachable(monkeypatch):
    b = _load_bridge()
    monkeypatch.setattr(b, "_ingest_token", lambda: "t")

    def _boom(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(b.requests, "post", _boom)
    assert b._auto_lease_ok("play", "slot") is False


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
