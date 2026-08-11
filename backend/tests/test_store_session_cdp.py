from backend.services.store_session_cdp import (
    cdp_port,
    cdp_url,
    _url_looks_logged_out,
)


def test_cdp_ports_and_urls():
    assert cdp_port("play") == 9222
    assert cdp_port("asc") == 9223
    assert cdp_url("play").startswith("http://127.0.0.1:9222")
    assert cdp_url("asc").startswith("http://127.0.0.1:9223")


def test_login_url_hints():
    assert _url_looks_logged_out("play", "https://accounts.google.com/v3/signin")
    assert not _url_looks_logged_out("play", "https://play.google.com/console/u/0")
    assert _url_looks_logged_out("asc", "https://idmsa.apple.com/appleauth/auth")
    assert not _url_looks_logged_out("asc", "https://appstoreconnect.apple.com/apps/1")
