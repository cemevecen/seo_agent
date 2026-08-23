"""Bridge needs_login / auto-failure: e-posta kalıcı kapalı."""

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


def test_bridge_alert_email_disabled():
    b = _load_bridge()
    assert b.BRIDGE_EMAIL_ALERTS_ENABLED is False
    assert b._send_bridge_alert_email(kind="login:asc", subject="x", body_text="y") is False


def test_needs_login_never_mails(monkeypatch):
    b = _load_bridge()
    sent: list[str] = []

    def fake_send(*, kind: str, subject: str, body_text: str) -> bool:
        sent.append(subject)
        return True

    monkeypatch.setattr(b, "_send_bridge_alert_email", fake_send)
    b._login_alert_open.clear()
    b._last_login_email_at.clear()
    b._fail_streak.clear()

    b._notify_auto_failure("asc", {"ok": False, "needs_login": True, "message": "login gerekli"})
    assert sent == []
    assert b._login_alert_open.get("asc") is True

    b._note_auto_success("asc")
    assert sent == []
    assert not b._login_alert_open.get("asc")


def test_non_login_failure_never_mails(monkeypatch):
    b = _load_bridge()
    sent: list[str] = []

    def fake_send(*, kind: str, subject: str, body_text: str) -> bool:
        sent.append(subject)
        return True

    monkeypatch.setattr(b, "_send_bridge_alert_email", fake_send)
    b._login_alert_open.clear()
    b._last_login_email_at.clear()
    b._fail_streak.clear()
    b._last_fail_email_at.clear()

    b._notify_auto_failure("news", {"ok": False, "message": "ingest 500"})
    assert sent == []
