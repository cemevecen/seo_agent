from backend.services.search_console_auth import (
    SearchConsoleOAuthError,
    _is_oauth_revoked_error,
    format_search_console_error_for_ui,
)


def test_oauth_revoked_detection():
    assert _is_oauth_revoked_error("invalid_grant: Token has been expired or revoked.")


def test_format_revoked_for_ui():
    msg = format_search_console_error_for_ui("invalid_grant: Token has been expired or revoked.")
    assert "Bağlantıyı Kaldır" in msg
    assert "Google ile Bağlan" in msg


def test_search_console_oauth_error_is_exception():
    err = SearchConsoleOAuthError("test")
    assert isinstance(err, Exception)
