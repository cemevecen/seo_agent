from pathlib import Path

from backend.services.scrape_browser import (
    STATE_DIR,
    google_profile_dir,
    asc_profile_dir,
    firebase_profile_dir,
    sinemalar_profile_dir,
    resolve_firefox_executable,
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


def test_resolve_firefox_executable_prefers_env(monkeypatch, tmp_path):
    fake = tmp_path / "firefox"
    fake.write_text("x", encoding="utf-8")
    monkeypatch.setenv("PLAYWRIGHT_FIREFOX_EXECUTABLE", str(fake))
    assert resolve_firefox_executable() == str(fake)


def test_resolve_firefox_executable_default_none_without_env(monkeypatch):
    monkeypatch.delenv("PLAYWRIGHT_FIREFOX_EXECUTABLE", raising=False)
    # Default: Playwright'ın kendi binary'si (juggler uyumu)
    assert resolve_firefox_executable() is None


def test_align_firefox_profile_compatibility(tmp_path):
    from backend.services.scrape_browser import align_firefox_profile_compatibility

    ini = tmp_path / "compatibility.ini"
    ini.write_text(
        "[Compatibility]\nLastVersion=146.0.1_x\nLastOSABI=Darwin_aarch64-gcc3\n",
        encoding="utf-8",
    )
    align_firefox_profile_compatibility(tmp_path)
    text = ini.read_text(encoding="utf-8")
    assert "LastVersion=0" in text


def test_deploy_installs_firefox_not_chromium():
    root = Path(__file__).resolve().parents[2]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    nix = (root / "nixpacks.toml").read_text(encoding="utf-8")
    assert "playwright install firefox" in dockerfile
    assert "playwright install chromium" not in dockerfile
    assert "playwright install firefox" in nix
    assert "playwright install chromium" not in nix


def test_launch_helpers_use_firefox_only():
    src = (Path(__file__).resolve().parents[1] / "services" / "scrape_browser.py").read_text(
        encoding="utf-8"
    )
    assert "pw.firefox.launch" in src
    assert "pw.chromium" not in src
