"""Selenium modunda ağ yanıtları yakalanmalı.

Firebase Console (Google, Playwright Nightly'yi reddediyor) yalnızca Selenium ile
taranıyor. Scrape, crash-free RPC'lerini `page.context.on("response", ...)` ile
topluyordu; shim'de bu çağrı sessiz no-op olduğu için hiçbir yanıt gelmiyor ve
ana sayfa / ios / android crash-free hücreleri boş kalıyordu.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.selenium_playwright_shim import (
    _CAPTURE_JS,
    SeleniumContext,
    _CapturedResponse,
)

RELEASEMON_URL = "https://firebase.corp.google.com/firebasereleasemon/v1/crashFreeStats"


class FakeSwitchTo:
    def __init__(self, driver: FakeDriver) -> None:
        self._d = driver

    def default_content(self) -> None:
        self._d.path = []

    def parent_frame(self) -> None:
        if self._d.path:
            self._d.path.pop()

    def frame(self, el: Any) -> None:
        self._d.path.append(el)

    def window(self, _handle: Any) -> None:
        return None


class FakeDriver:
    """execute_script çağrılarını taklit eder; her frame'in kendi tamponu var."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        frames: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        # frames: {"iframe-0": [...satırlar...]} — üst belge "" anahtarı
        self.buffers: dict[str, list[dict[str, Any]]] = {"": list(rows or [])}
        self.buffers.update({k: list(v) for k, v in (frames or {}).items()})
        self.path: list[str] = []
        self.installed: list[str] = []
        self.navigations: list[str] = []
        self.current_window_handle = "w1"
        self.switch_to = FakeSwitchTo(self)

    @property
    def _key(self) -> str:
        return self.path[-1] if self.path else ""

    def find_elements(self, by: str, value: str) -> list[str]:
        assert (by, value) == ("tag name", "iframe")
        if self.path:  # yalnızca üst belgede iframe var (tek seviye)
            return []
        return [k for k in self.buffers if k]

    def execute_script(self, script: str, *args: Any) -> Any:
        if "__pcNetCaptureInstalled" in script:
            self.installed.append(self._key)
            return None
        if "__pcNetCapture = []" in script:
            key = self._key
            out = self.buffers.get(key, [])
            self.buffers[key] = []
            return out
        return None

    def set_page_load_timeout(self, _sec: int) -> None:
        return None

    def get(self, url: str) -> None:
        self.navigations.append(url)


def _context(
    rows: list[dict[str, Any]] | None = None,
    frames: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[SeleniumContext, FakeDriver]:
    driver = FakeDriver(rows, frames)
    return SeleniumContext(driver, download_dir=Path("/tmp")), driver


def test_context_level_listener_reaches_the_page():
    ctx, _ = _context()
    seen: list[Any] = []
    ctx.on("response", seen.append)
    assert ctx.pages[0]._response_handlers, "context dinleyicisi sayfaya bağlanmadı"


def test_page_exposes_its_context_like_playwright():
    ctx, _ = _context()
    assert ctx.pages[0].context is ctx


def test_captured_responses_are_dispatched_to_handlers():
    rows = [
        {
            "url": RELEASEMON_URL,
            "status": 200,
            "content_type": "application/json",
            "body": '{"crashFreeUsers": 99.81}',
        }
    ]
    ctx, driver = _context(rows)
    seen: list[Any] = []
    ctx.on("response", seen.append)
    page = ctx.pages[0]
    page.goto("https://console.firebase.google.com/x/releasemonitoring")
    page.wait_for_timeout(1100)
    assert len(seen) == 1
    resp = seen[0]
    assert resp.url == RELEASEMON_URL
    assert resp.status == 200
    assert resp.headers.get("content-type") == "application/json"
    assert resp.text() == '{"crashFreeUsers": 99.81}'
    assert resp.json()["crashFreeUsers"] == 99.81


def test_capture_script_is_installed_after_navigation():
    ctx, driver = _context()
    ctx.on("response", lambda _r: None)
    ctx.pages[0].goto("https://console.firebase.google.com/")
    assert driver.installed, "yakalayıcı sayfaya kurulmadı"
    assert driver.navigations == ["https://console.firebase.google.com/"]


def test_no_listener_means_no_injection():
    """Dinleyici yoksa sayfaya kod enjekte etmeyelim — diğer scrape'ler etkilenmesin."""
    ctx, driver = _context()
    ctx.pages[0].goto("https://example.com/")
    assert driver.installed == []


def test_new_pages_inherit_context_listeners():
    ctx, _ = _context()
    ctx.on("response", lambda _r: None)
    page = ctx.new_page()
    assert page._response_handlers
    assert page.context is ctx


def test_capture_js_wraps_fetch_and_xhr_and_is_idempotent():
    assert "window.__pcNetCaptureInstalled" in _CAPTURE_JS
    assert "window.fetch = function" in _CAPTURE_JS
    assert "XHR.prototype.send" in _CAPTURE_JS
    # POST RPC gövdeleri de yakalanmalı (releasemon/batchexecute POST kullanıyor)
    assert "resp.clone().text()" in _CAPTURE_JS


def test_captured_response_tolerates_missing_fields():
    resp = _CapturedResponse({})
    assert resp.url == ""
    assert resp.status == 0
    assert resp.text() == ""


VENUS_URL = "https://analytics.google.com/analytics/app/data/v2/venus?dashboard_card_1"


def test_analytics_iframe_responses_are_captured():
    """DAU/MAU verisi çapraz-köken iframe'den geliyor; JS oraya erişemez, Selenium erişir."""
    ctx, driver = _context(
        frames={
            "iframe-analytics": [
                {
                    "url": VENUS_URL,
                    "status": 200,
                    "content_type": "application/json",
                    "body": '{"active_users_1": 1234}',
                }
            ]
        }
    )
    seen: list[Any] = []
    ctx.on("response", seen.append)
    page = ctx.pages[0]
    page.goto("https://console.firebase.google.com/project/x/analytics")
    page.wait_for_timeout(1100)
    assert [r.url for r in seen] == [VENUS_URL]
    assert seen[0].json()["active_users_1"] == 1234


def test_capture_is_installed_inside_frames_too():
    ctx, driver = _context(frames={"iframe-analytics": []})
    ctx.on("response", lambda _r: None)
    ctx.pages[0].goto("https://console.firebase.google.com/")
    assert "iframe-analytics" in driver.installed
    assert "" in driver.installed


def test_frame_walk_returns_to_the_top_document():
    """Frame'de kalırsak scrape'in sonraki seçicileri bozulur."""
    ctx, driver = _context(frames={"iframe-analytics": []})
    ctx.on("response", lambda _r: None)
    page = ctx.pages[0]
    page.goto("https://console.firebase.google.com/")
    page.wait_for_timeout(1100)
    assert driver.path == []


def test_late_loading_frames_are_instrumented_on_the_next_drain():
    """Analytics iframe'i sayfa yüklendikten sonra geliyor — sonraki turda yakalanmalı."""
    ctx, driver = _context()
    seen: list[Any] = []
    ctx.on("response", seen.append)
    page = ctx.pages[0]
    page.goto("https://console.firebase.google.com/project/x/analytics")
    # iframe gezinmeden sonra belirdi
    driver.buffers["iframe-late"] = [
        {"url": VENUS_URL, "status": 200, "content_type": "application/json", "body": "{}"}
    ]
    page.wait_for_timeout(1100)
    assert [r.url for r in seen] == [VENUS_URL]
    assert "iframe-late" in driver.installed
