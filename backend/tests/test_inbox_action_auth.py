"""Inbox aksiyon şifresi — panel oturumu (admin veya Google üye) bypass."""

from unittest.mock import MagicMock, patch

from backend.services import inbox_action_auth as iaa


def test_inbox_action_open_when_password_unset():
    req = MagicMock()
    with patch.object(iaa.settings, "inbox_action_password", ""):
        assert iaa.is_inbox_action_authenticated(req) is True


def test_google_member_bypasses_inbox_action_password():
    req = MagicMock()
    with patch.object(iaa.settings, "inbox_action_password", "secret-inbox"):
        with patch("backend.main._is_app_panel_authenticated", return_value=True):
            assert iaa.is_inbox_action_authenticated(req) is True


def test_anonymous_needs_inbox_cookie_when_password_set():
    req = MagicMock()
    req.cookies = {}
    with patch.object(iaa.settings, "inbox_action_password", "secret-inbox"):
        with patch("backend.main._is_app_panel_authenticated", return_value=False):
            assert iaa.is_inbox_action_authenticated(req) is False
