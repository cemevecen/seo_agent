"""AI Talk geçmişi şifre koruması testleri."""

from unittest.mock import MagicMock, patch

from backend.services import ai_talk_history_auth as auth


def test_password_not_configured_allows_access():
    with patch.object(auth.settings, "settings_password", ""), patch.object(
        auth.settings, "inbox_action_password", ""
    ):
        assert auth.ai_talk_history_password_configured() is False
        req = MagicMock()
        assert auth.is_ai_talk_history_authenticated(req) is True


def test_verify_accepts_settings_or_inbox_password():
    with patch.object(auth.settings, "settings_password", "secret-a"), patch.object(
        auth.settings, "inbox_action_password", "secret-b"
    ):
        assert auth.verify_ai_talk_history_password("secret-a") is True
        assert auth.verify_ai_talk_history_password("secret-b") is True
        assert auth.verify_ai_talk_history_password("wrong") is False


def test_cookie_token_valid_when_matching():
    with patch.object(auth.settings, "settings_password", "secret-a"), patch.object(
        auth.settings, "inbox_action_password", ""
    ), patch.object(auth.settings, "secret_key", "test-secret"):
        token = auth.issue_ai_talk_history_cookie_token()
        req = MagicMock()
        req.cookies = {auth.AI_TALK_HISTORY_AUTH_COOKIE: token}
        assert auth.is_ai_talk_history_authenticated(req) is True
