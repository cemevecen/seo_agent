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


def test_deploy_skips_firefox_on_railway_image():
    """Railway/Docker app imajında Playwright Firefox yok — Mac bridge kurar."""
    root = Path(__file__).resolve().parents[2]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    nix = (root / "nixpacks.toml").read_text(encoding="utf-8")
    assert "RUN playwright install firefox" not in dockerfile
    assert "playwright install chromium" not in dockerfile
    assert "playwright install firefox" not in nix
    assert "playwright install chromium" not in nix
    assert "Mac bridge" in dockerfile


def test_browser_scrape_forbidden_on_railway(monkeypatch):
    from backend.services import scrape_browser as sb

    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    monkeypatch.delenv("ALLOW_BROWSER_SCRAPE_ON_RAILWAY", raising=False)
    assert sb.browser_scrape_forbidden() is True
    try:
        sb.assert_browser_scrape_allowed(context="test")
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "Mac bridge" in str(exc)


def test_browser_scrape_allowed_off_railway(monkeypatch):
    from backend.services import scrape_browser as sb

    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    monkeypatch.delenv("RAILWAY_PROJECT_ID", raising=False)
    monkeypatch.delenv("ALLOW_BROWSER_SCRAPE_ON_RAILWAY", raising=False)
    assert sb.browser_scrape_forbidden() is False
    sb.assert_browser_scrape_allowed(context="test")


def test_clear_stale_locks_skips_when_pids(monkeypatch, tmp_path):
    from backend.services import scrape_browser as sb

    (tmp_path / "SingletonLock").write_text("x", encoding="utf-8")
    (tmp_path / "cookies.sqlite").write_text("keep", encoding="utf-8")
    monkeypatch.setattr(sb, "list_profile_browser_pids", lambda _p: [12345])
    assert sb.clear_stale_profile_locks(tmp_path) == []
    assert (tmp_path / "SingletonLock").is_file()
    assert (tmp_path / "cookies.sqlite").read_text(encoding="utf-8") == "keep"


def test_clear_stale_locks_removes_only_locks(monkeypatch, tmp_path):
    from backend.services import scrape_browser as sb

    (tmp_path / "SingletonLock").write_text("x", encoding="utf-8")
    (tmp_path / ".parentlock").write_text("x", encoding="utf-8")
    (tmp_path / "cookies.sqlite").write_text("keep", encoding="utf-8")
    monkeypatch.setattr(sb, "list_profile_browser_pids", lambda _p: [])
    removed = sb.clear_stale_profile_locks(tmp_path)
    assert "SingletonLock" in removed
    assert not (tmp_path / "SingletonLock").exists()
    assert (tmp_path / "cookies.sqlite").read_text(encoding="utf-8") == "keep"


def test_assert_profile_auth_untouched(tmp_path):
    from backend.services.scrape_browser import assert_profile_auth_untouched
    import pytest

    with pytest.raises(RuntimeError, match="auth state"):
        assert_profile_auth_untouched(tmp_path / "cookies.sqlite")


def test_release_profile_browsers_graceful_then_force(monkeypatch, tmp_path):
    from backend.services import scrape_browser as sb

    monkeypatch.setattr(sb, "profile_login_lock_active", lambda _p: False)
    alive = {99: True}

    def fake_list(_p):
        return [99] if alive.get(99) else []

    kills: list[tuple[int, int]] = []

    def fake_kill(pid, sig):
        kills.append((pid, sig))
        if sig == sb.signal.SIGKILL:
            alive[99] = False
        elif sig == 0:
            if not alive.get(pid):
                raise ProcessLookupError()
            return None
        return None

    monkeypatch.setattr(sb, "list_profile_browser_pids", fake_list)
    monkeypatch.setattr(sb.os, "kill", fake_kill)
    monkeypatch.setattr(sb.time, "sleep", lambda _s: None)
    # force=False: SIGTERM only, process may remain
    soft = sb.release_profile_browsers(tmp_path, force=False, wait_sec=0.01, reason="t")
    assert soft["term"] == 1
    assert soft["kill"] == 0
    hard = sb.release_profile_browsers(tmp_path, force=True, wait_sec=0.01, reason="t")
    assert hard["kill"] == 1
    assert any(sig == sb.signal.SIGKILL for _, sig in kills)


def test_no_profile_rmtree_in_scrape_browser():
    src = (Path(__file__).resolve().parents[1] / "services" / "scrape_browser.py").read_text(
        encoding="utf-8"
    )
    assert "rmtree" not in src
    assert "ensure_profile_free_for_launch" in src
    assert "SIGKILL" in src


def test_launch_helpers_use_firefox_only():
    src = (Path(__file__).resolve().parents[1] / "services" / "scrape_browser.py").read_text(
        encoding="utf-8"
    )
    assert "pw.firefox.launch" in src
    assert "pw.chromium" not in src
