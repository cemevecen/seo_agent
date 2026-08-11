from backend.services.scrape_browser import (
    STATE_DIR,
    google_profile_dir,
    asc_profile_dir,
    firebase_profile_dir,
    sinemalar_profile_dir,
)


def test_default_profiles_are_firefox():
    assert google_profile_dir() == STATE_DIR / "fx-google"
    assert asc_profile_dir() == STATE_DIR / "fx-asc"
    assert firebase_profile_dir() == STATE_DIR / "fx-google"
    assert sinemalar_profile_dir() == STATE_DIR / "fx-sinemalar"


def test_chrome_env_paths_remap_to_firefox(monkeypatch):
    monkeypatch.setenv("PLAY_CONSOLE_PROFILE_DIR", str(STATE_DIR / "play-console-profile"))
    monkeypatch.setenv("ASC_CONSOLE_PROFILE_DIR", str(STATE_DIR / "asc-console-profile"))
    assert google_profile_dir() == STATE_DIR / "fx-google"
    assert asc_profile_dir() == STATE_DIR / "fx-asc"


def test_explicit_fx_env_kept(monkeypatch, tmp_path):
    custom = tmp_path / "fx-google"
    monkeypatch.setenv("PLAY_CONSOLE_PROFILE_DIR", str(custom))
    assert google_profile_dir() == custom
