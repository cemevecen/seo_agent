from backend.services import app_member_auth as ama


def test_nokta_email_allowed():
    assert ama.is_email_eligible_for_membership("user@nokta.com") is True


def test_gmail_exception_allowed():
    assert ama.is_email_eligible_for_membership("cemevecen@gmail.com") is True


def test_other_gmail_rejected():
    assert ama.is_email_eligible_for_membership("other@gmail.com") is False


def test_redirect_mismatch_message():
    msg = ama.format_member_oauth_login_error("redirect_uri_mismatch", request=None)
    assert "redirect_uri_mismatch" in msg
    assert "/auth/google/callback" in msg


def test_oauth_prompt_first_visit():
    from unittest.mock import MagicMock

    req = MagicMock()
    req.cookies = {}
    assert ama.member_oauth_authorization_extra_params(req) == {"prompt": "select_account"}


def test_oauth_prompt_returning_browser():
    from unittest.mock import MagicMock

    req = MagicMock()
    req.cookies = {ama.PANEL_MEMBER_SEEN_COOKIE: "1"}
    assert ama.member_oauth_authorization_extra_params(req) == {}


def test_online_presence_visible_only_for_cem_accounts():
    from unittest.mock import MagicMock

    from backend.models import AppMember

    req = MagicMock()
    req.cookies = {}
    assert ama.can_view_online_presence(req) is False

    with __import__("unittest").mock.patch.object(
        ama, "member_from_request", return_value=AppMember(email="cemevecen@nokta.com")
    ):
        assert ama.can_view_online_presence(req) is True

    with __import__("unittest").mock.patch.object(
        ama, "member_from_request", return_value=AppMember(email="onurtorun@nokta.com")
    ):
        assert ama.can_view_online_presence(req) is False
