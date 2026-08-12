from backend.services.system_firefox_driver import google_profile_has_session


def test_google_profile_has_session_false_on_empty(tmp_path):
    assert google_profile_has_session(tmp_path) is False
