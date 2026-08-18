"""ASC → Selenium göçü: shim'in ASC'nin kullandığı API'leri karşılaması.

Neden göç: Playwright kalıcı bağlamına başka bir süreçten bağlanılamıyor,
bu yüzden köprü her yeniden başladığında ASC oturumu ölüyor ve Apple tekrar
2FA istiyordu. Selenium yolunda pencere köprüden bağımsız yaşıyor.
"""

from __future__ import annotations

import json
from pathlib import Path

from backend.services.selenium_playwright_shim import SeleniumContext

ROOT = Path(__file__).resolve().parents[2]


class _FakeDriver:
    def __init__(self, *, cookies=None, script_result=None) -> None:
        self._cookies = cookies if cookies is not None else []
        self._script_result = script_result
        self.async_calls: list[tuple] = []

    def get_cookies(self):
        if self._cookies == "boom":
            raise RuntimeError("çerez okunamadı")
        return self._cookies

    def execute_async_script(self, script, *args):
        self.async_calls.append((script, args))
        return self._script_result

    def execute_script(self, script, *args):
        return None

    @property
    def current_url(self):
        return "https://appstoreconnect.apple.com/apps"


def _ctx(driver) -> SeleniumContext:
    return SeleniumContext(driver, download_dir=Path("/tmp/x"))


def test_context_exposes_cookies_for_the_session_check():
    """ASC oturum kontrolü itctx/dqsid/wosid çerezlerine bakıyor."""
    driver = _FakeDriver(cookies=[{"name": "itctx", "value": "v"}])
    assert _ctx(driver).cookies() == [{"name": "itctx", "value": "v"}]


def test_cookies_failure_is_not_fatal():
    """Çerez okunamazsa oturum kontrolü çökmemeli, 'yok' saymalı."""
    assert _ctx(_FakeDriver(cookies="boom")).cookies() == []


def test_request_get_runs_in_page_with_credentials():
    """context.request'in Selenium karşılığı: sayfa içinden credentials'lı fetch."""
    driver = _FakeDriver(script_result={
        "status": 200,
        "headers": {"Content-Type": "application/json"},
        "body": '{"results": [1, 2]}',
    })
    resp = _ctx(driver).request.get(
        "https://appstoreconnect.apple.com/analytics/api/v1/settings/all",
        headers={"X-Requested-By": "appstoreconnect.apple.com"},
        timeout=30_000,
    )
    assert resp.status == 200 and resp.ok is True
    assert resp.json() == {"results": [1, 2]}
    # Başlık aramaları büyük/küçük harfe takılmamalı
    assert resp.headers.get("content-type") == "application/json"

    script, args = driver.async_calls[0]
    assert "credentials: 'include'" in script
    # `window.` olmadan Firefox: "'fetch' called on an object that does not
    # implement interface Window" — canlı ASC probe'u tam burada düşmüştü.
    assert "window.fetch(" in script
    payload = args[0]
    assert payload["method"] == "GET"
    assert payload["body"] is None
    assert payload["headers"]["X-Requested-By"] == "appstoreconnect.apple.com"


def test_request_post_serializes_the_body():
    driver = _FakeDriver(script_result={"status": 200, "headers": {}, "body": "{}"})
    ctx = _ctx(driver)
    ctx.request.post("https://x.test/api", data=json.dumps({"a": 1}))
    assert ctx._driver.async_calls[0][1][0]["body"] == '{"a": 1}'
    # dict verilirse de JSON'a çevrilmeli
    ctx.request.post("https://x.test/api", data={"b": 2})
    assert json.loads(ctx._driver.async_calls[1][1][0]["body"]) == {"b": 2}


def test_request_failure_is_reported_not_raised():
    """Ağ hatası istisna atmamalı; ASC zincirinde bir sonraki yola düşülür."""
    resp = _ctx(_FakeDriver(script_result=None)).request.get("https://x.test/")
    assert resp.status == 0 and resp.ok is False
    assert resp.json() is None


def test_asc_uses_selenium_by_default_and_can_fall_back():
    src = (ROOT / "scripts" / "asc_console_scrape.py").read_text(encoding="utf-8")
    body = src.split("def _asc_use_selenium(", 1)[1].split("\ndef ", 1)[0]
    # Varsayılan açık; tek değişkenle eski yola dönülebilmeli
    assert 'ASC_CONSOLE_USE_SELENIUM"' in body and '"1"' in body
    assert '("0", "false", "no", "off")' in body
    # Sistem Firefox yoksa Selenium'a zorlanmamalı
    assert "resolve_system_firefox_executable" in body


def test_asc_release_routes_by_context_type():
    """Selenium bağlamı Playwright release'ine gitmemeli (ve tersi)."""
    src = (ROOT / "scripts" / "asc_console_scrape.py").read_text(encoding="utf-8")
    body = src.split("def _release_context(", 1)[1].split("\ndef ", 1)[0]
    assert '_selenium_mode' in body
    assert "release_selenium_context(pw, ctx)" in body
    assert "release_persistent_context(" in body
