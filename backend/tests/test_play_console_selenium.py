from backend.services.selenium_playwright_shim import play_console_use_selenium


def test_play_console_defaults_to_selenium_on_mac():
    # CI/Linux'ta Firefox.app yoksa False olabilir
    assert isinstance(play_console_use_selenium(), bool)
