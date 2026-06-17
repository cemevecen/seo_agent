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
