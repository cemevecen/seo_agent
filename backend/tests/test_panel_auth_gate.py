from unittest.mock import MagicMock

from backend.services.panel_auth import auth_gate_enabled, panel_session_granted


def test_panel_session_granted_member_without_admin_password():
    assert panel_session_granted(
        password_ready=False,
        admin_authenticated=False,
        member_authenticated=True,
    )


def test_panel_session_granted_admin_password_no_longer_opens_panel():
    assert not panel_session_granted(
        password_ready=True,
        admin_authenticated=True,
        member_authenticated=False,
    )


def test_auth_gate_enabled_railway(monkeypatch):
    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    req = MagicMock()
    req.headers = {"host": "127.0.0.1"}
    req.client = MagicMock(host="127.0.0.1")
    assert auth_gate_enabled(req) is True


def test_auth_gate_local_loopback_can_disable(monkeypatch):
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_PROJECT_ID", raising=False)
    monkeypatch.setattr("backend.services.panel_auth.settings.admin_auth_enforced", False)
    req = MagicMock()
    req.headers = {"host": "127.0.0.1"}
    req.client = MagicMock(host="127.0.0.1")
    assert auth_gate_enabled(req) is False
