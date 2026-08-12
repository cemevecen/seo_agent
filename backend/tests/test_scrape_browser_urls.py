from backend.services.scrape_browser import google_blocks_automation_text, normalize_nav_url


def test_normalize_nav_url_strips_accidental_email_prefix():
    raw = (
        "cemhttps://accounts.google.com/v3/signin/identifier?"
        "continue=https://play.google.com/console/u/0/developers/1/app/2/app-dashboard"
    )
    out = normalize_nav_url(raw)
    assert out.startswith("https://accounts.google.com/")
    assert "cemhttps" not in out


def test_normalize_nav_url_adds_scheme():
    assert normalize_nav_url("play.google.com/console").startswith("https://")


def test_google_blocks_automation_text():
    assert google_blocks_automation_text("This browser or app may not be secure.")
    assert google_blocks_automation_text("Couldn't sign you in")
    assert not google_blocks_automation_text("Play Console dashboard")
