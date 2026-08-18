"""Selenium WebDriver → Playwright Page API uyumluluk katmanı.

Google Play Console, Playwright Nightly'de oturumu reddeder; gerçek Firefox.app
(Selenium) aynı fx-google profilinde çalışır. play_console_scrape.py page.* çağrıları
bu shim üzerinden gider.
"""

from __future__ import annotations

import re
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Pattern

from backend.services.scrape_browser import STATE_DIR
from backend.services.system_firefox_driver import (
    launch_system_firefox_driver,
    quit_system_firefox_driver,
)


def _norm_timeout_ms(timeout: int | float | None, default: int = 5000) -> float:
    if timeout is None:
        return default / 1000.0
    return max(0.1, float(timeout) / 1000.0)


class _DownloadHandle:
    def __init__(self, path: Path) -> None:
        self._path = path

    def save_as(self, dest: str) -> None:
        shutil.move(str(self._path), dest)


class SeleniumLocator:
    def __init__(
        self,
        page: "SeleniumPage",
        *,
        css: str = "*",
        elements: list[Any] | None = None,
        role: str | None = None,
        name: str | Pattern[str] | None = None,
        exact: bool = False,
        label: str | None = None,
    ) -> None:
        self._page = page
        self._css = css
        self._cached = elements
        self._role = role
        self._name = name
        self._exact = exact
        self._label = label

    def _resolve(self) -> list[Any]:
        if self._cached is not None:
            return self._cached
        driver = self._page._driver
        out: list[Any] = []
        if self._label:
            needles = [self._label.lower()]
            for el in driver.find_elements("css selector", "label, [aria-label]"):
                try:
                    txt = (el.text or el.get_attribute("aria-label") or "").strip().lower()
                    if any(n in txt for n in needles):
                        fid = el.get_attribute("for")
                        if fid:
                            try:
                                out.append(driver.find_element("id", fid))
                                continue
                            except Exception:
                                pass
                        out.append(el)
                except Exception:
                    continue
            self._cached = out
            return out

        if self._role:
            sel = f"[role='{self._role}']"
            candidates = driver.find_elements("css selector", sel)
            if self._name is not None:
                filtered = []
                for el in candidates:
                    try:
                        txt = (el.text or el.get_attribute("aria-label") or "").strip()
                        if isinstance(self._name, Pattern):
                            if self._name.search(txt):
                                filtered.append(el)
                        elif self._exact:
                            if txt == self._name:
                                filtered.append(el)
                        elif self._name.lower() in txt.lower():
                            filtered.append(el)
                    except Exception:
                        continue
                candidates = filtered
            self._cached = candidates
            return candidates

        if self._css != "*":
            self._cached = driver.find_elements("css selector", self._css)
            return self._cached

        self._cached = driver.find_elements("css selector", "*")
        return self._cached

    def filter(self, *, has_text: str | Pattern[str] | None = None, **_: Any) -> SeleniumLocator:
        base = self._resolve()
        if has_text is None:
            return SeleniumLocator(self._page, elements=list(base))
        filtered: list[Any] = []
        for el in base:
            try:
                txt = (el.text or "").strip()
                if isinstance(has_text, Pattern):
                    if has_text.search(txt):
                        filtered.append(el)
                elif has_text in txt:
                    filtered.append(el)
            except Exception:
                continue
        return SeleniumLocator(self._page, elements=filtered)

    def count(self) -> int:
        return len(self._resolve())

    @property
    def first(self) -> SeleniumLocator:
        els = self._resolve()
        return SeleniumLocator(self._page, elements=els[:1] if els else [])

    def nth(self, index: int) -> SeleniumLocator:
        els = self._resolve()
        if 0 <= index < len(els):
            return SeleniumLocator(self._page, elements=[els[index]])
        return SeleniumLocator(self._page, elements=[])

    def locator(self, css: str) -> SeleniumLocator:
        nested: list[Any] = []
        for el in self._resolve():
            try:
                nested.extend(el.find_elements("css selector", css))
            except Exception:
                continue
        return SeleniumLocator(self._page, elements=nested)

    def click(self, *, timeout: int | float | None = None) -> None:
        _ = timeout
        els = self._resolve()
        if not els:
            raise RuntimeError("locator.click: element yok")
        els[0].click()

    def inner_text(self, *, timeout: int | float | None = None) -> str:
        _ = timeout
        els = self._resolve()
        if not els:
            return ""
        return (els[0].text or "").strip()

    def is_visible(self) -> bool:
        els = self._resolve()
        if not els:
            return False
        try:
            return els[0].is_displayed()
        except Exception:
            return False

    def scroll_into_view_if_needed(self, *, timeout: int | float | None = None) -> None:
        _ = timeout
        els = self._resolve()
        if not els:
            return
        self._page._driver.execute_script(
            "arguments[0].scrollIntoView({block:'center',inline:'nearest'});", els[0]
        )

    def element_handle(self) -> Any:
        els = self._resolve()
        return els[0] if els else None


class _Keyboard:
    def __init__(self, page: SeleniumPage) -> None:
        self._page = page

    def press(self, key: str) -> None:
        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.webdriver.common.keys import Keys

        mapping = {"Escape": Keys.ESCAPE, "Enter": Keys.ENTER, "Tab": Keys.TAB}
        ActionChains(self._page._driver).send_keys(mapping.get(key, key)).perform()


class _Mouse:
    def __init__(self, page: SeleniumPage) -> None:
        self._page = page

    def move(self, x: float, y: float) -> None:
        from selenium.webdriver.common.action_chains import ActionChains

        ActionChains(self._page._driver).move_by_offset(int(x), int(y)).perform()


# Sayfa içi ağ yakalayıcı: fetch + XHR sarmalanır, yanıt gövdeleri tampona yazılır.
# Idempotent — her goto sonrası tekrar çalıştırmak güvenli.
_CAPTURE_MAX_ENTRIES = 240
_CAPTURE_MAX_BODY = 600_000
_CAPTURE_JS = """
(function () {
  if (window.__pcNetCaptureInstalled) return;
  window.__pcNetCaptureInstalled = true;
  window.__pcNetCapture = window.__pcNetCapture || [];
  var MAX = %(max_entries)d, MAXB = %(max_body)d;
  function push(url, status, ctype, body) {
    try {
      if (!body || body.length > MAXB) return;
      var buf = window.__pcNetCapture;
      buf.push({ url: String(url || '').slice(0, 500), status: status || 0,
                 content_type: ctype || '', body: body });
      while (buf.length > MAX) buf.shift();
    } catch (e) {}
  }
  var of = window.fetch;
  if (typeof of === 'function') {
    window.fetch = function () {
      var args = arguments;
      return of.apply(this, args).then(function (resp) {
        try {
          var u = (resp && resp.url) || (args[0] && args[0].url) || args[0];
          resp.clone().text().then(function (t) {
            push(u, resp.status, resp.headers && resp.headers.get('content-type'), t);
          }).catch(function () {});
        } catch (e) {}
        return resp;
      });
    };
  }
  var XHR = window.XMLHttpRequest;
  if (XHR && XHR.prototype) {
    var oo = XHR.prototype.open, os = XHR.prototype.send;
    XHR.prototype.open = function (m, u) { this.__pcUrl = u; return oo.apply(this, arguments); };
    XHR.prototype.send = function () {
      var self = this;
      this.addEventListener('load', function () {
        try {
          push(self.__pcUrl || self.responseURL, self.status,
               self.getResponseHeader && self.getResponseHeader('content-type'),
               typeof self.responseText === 'string' ? self.responseText : '');
        } catch (e) {}
      });
      return os.apply(this, arguments);
    };
  }
})();
""" % {"max_entries": _CAPTURE_MAX_ENTRIES, "max_body": _CAPTURE_MAX_BODY}


class _CapturedResponse:
    """Playwright Response uyumu — scrape handler'ları url/status/headers/text() bekler."""

    def __init__(self, row: dict[str, Any]) -> None:
        self.url = str(row.get("url") or "")
        try:
            self.status = int(row.get("status") or 0)
        except (TypeError, ValueError):
            self.status = 0
        self.headers = {"content-type": str(row.get("content_type") or "")}
        self._body = row.get("body") or ""

    def text(self) -> str:
        return self._body

    def json(self) -> Any:
        import json as _json

        return _json.loads(self._body)


class SeleniumPage:
    _selenium_mode = True

    def __init__(self, driver: Any, *, download_dir: Path) -> None:
        self._driver = driver
        self._download_dir = download_dir
        self.context: Any = None  # SeleniumContext bağlar (page.context.on(...) uyumu)
        self.keyboard = _Keyboard(self)
        self.mouse = _Mouse(self)
        self._response_handlers: list[Callable[..., Any]] = []

    @property
    def url(self) -> str:
        try:
            return self._driver.current_url or ""
        except Exception:
            return ""

    def title(self) -> str:
        try:
            return self._driver.title or ""
        except Exception:
            return ""

    def is_closed(self) -> bool:
        try:
            _ = self._driver.current_url
            return False
        except Exception:
            return True

    def bring_to_front(self) -> None:
        try:
            self._driver.switch_to.window(self._driver.current_window_handle)
        except Exception:
            pass

    def goto(
        self,
        url: str,
        *,
        wait_until: str = "load",
        timeout: int | float = 30_000,
    ) -> None:
        _ = wait_until
        # Sayfadan ayrılmadan önce birikeni boşalt — son RPC'ler kaybolmasın
        self._drain_capture()
        self._driver.set_page_load_timeout(max(5, int(timeout / 1000)))
        self._driver.get(url)
        self._install_capture()

    def _walk_frames(self, fn: Callable[[], None], *, depth: int = 2) -> None:
        """fn'i üst belgede ve (çapraz-köken dahil) iframe'lerde çalıştır.

        Firebase Analytics kartları analytics.google.com iframe'inden geliyor;
        JS oraya erişemez ama Selenium frame'e geçebilir.
        """
        driver = self._driver

        def _descend(level: int) -> None:
            try:
                fn()
            except Exception:
                pass
            if level <= 0:
                return
            try:
                count = len(driver.find_elements("tag name", "iframe"))
            except Exception:
                return
            for idx in range(min(count, 8)):
                try:
                    frames = driver.find_elements("tag name", "iframe")
                    if idx >= len(frames):
                        break
                    driver.switch_to.frame(frames[idx])
                except Exception:
                    continue
                try:
                    _descend(level - 1)
                finally:
                    restored = True
                    try:
                        driver.switch_to.parent_frame()
                    except Exception:
                        restored = False
                if not restored:
                    # Üst çerçeveye dönemedik: kökten başla, yarım gezinme bırakma
                    try:
                        driver.switch_to.default_content()
                    except Exception:
                        pass
                    break

        try:
            driver.switch_to.default_content()
        except Exception:
            pass
        _descend(depth)
        try:
            driver.switch_to.default_content()
        except Exception:
            pass

    def _install_capture(self) -> None:
        """fetch/XHR sarmalayıcısını sayfaya ve iframe'lere kur.

        Selenium'da Playwright'ın pasif `page.on("response")` olayı yok; Firefox
        için CDP de yok. RPC gövdelerini yakalamanın tek güvenilir yolu sayfa
        içinde fetch/XHR'yi sarmalamak (POST RPC'ler de böyle yakalanır).
        """
        if not self._response_handlers:
            return
        self._walk_frames(lambda: self._driver.execute_script(_CAPTURE_JS))

    def _drain_capture(self) -> None:
        """Sayfada ve iframe'lerde biriken yanıtları kayıtlı handler'lara dağıt."""
        if not self._response_handlers:
            return
        collected: list[Any] = []

        def _take() -> None:
            # Kurulum idempotent: sonradan yüklenen iframe'ler (Analytics) de yakalansın
            self._driver.execute_script(_CAPTURE_JS)
            rows = self._driver.execute_script(
                "try { const b = window.__pcNetCapture || []; "
                "window.__pcNetCapture = []; return b; } catch (e) { return []; }"
            )
            if rows:
                collected.extend(rows)

        self._walk_frames(_take)
        for row in collected:
            if not isinstance(row, dict):
                continue
            resp = _CapturedResponse(row)
            for handler in list(self._response_handlers):
                try:
                    handler(resp)
                except Exception:
                    pass

    def evaluate(self, expression: str, *args: Any) -> Any:
        script = (expression or "").strip()
        if script.startswith("async"):
            # Playwright: page.evaluate(async () => ..., arg) — argümanları fn'e ilet
            payload = f"""
            const done = arguments[arguments.length - 1];
            const fnArgs = Array.prototype.slice.call(arguments, 0, arguments.length - 1);
            const fn = {script};
            (async () => {{
              try {{ done(await fn.apply(null, fnArgs)); }}
              catch (e) {{ done(null); }}
            }})();
            """
            return self._driver.execute_async_script(payload, *args)
        if "=>" in script and not script.startswith("return"):
            # page.evaluate("(args) => ...", arg) — Playwright arg geçirir; Selenium da geçirmeli
            return self._driver.execute_script(
                f"return ({script}).apply(null, arguments);",
                *args,
            )
        if not script.startswith("return"):
            script = f"return {script}"
        return self._driver.execute_script(script, *args)

    def inner_text(self, selector: str) -> str:
        if selector in ("body", "html"):
            return (self._driver.find_element("tag name", selector).text or "").strip()
        el = self._driver.find_element("css selector", selector)
        return (el.text or "").strip()

    def wait_for_timeout(self, ms: int | float) -> None:
        """Beklerken yakalananları periyodik boşalt — tampon taşmasın."""
        total = max(0.0, float(ms) / 1000.0)
        if not self._response_handlers:
            time.sleep(total)
            return
        deadline = time.time() + total
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            time.sleep(min(1.0, remaining))
            self._drain_capture()

    def wait_for_load_state(self, state: str = "load", *, timeout: int | float = 30_000) -> None:
        _ = state
        deadline = time.time() + _norm_timeout_ms(timeout, default=30_000)
        while time.time() < deadline:
            try:
                ready = self._driver.execute_script("return document.readyState")
                if ready in ("interactive", "complete"):
                    return
            except Exception:
                pass
            time.sleep(0.15)

    def wait_for_selector(self, selector: str, *, timeout: int | float = 30_000) -> None:
        deadline = time.time() + _norm_timeout_ms(timeout, default=30_000)
        while time.time() < deadline:
            try:
                if self._driver.find_elements("css selector", selector):
                    return
            except Exception:
                pass
            time.sleep(0.2)
        raise TimeoutError(f"wait_for_selector timeout: {selector}")

    def locator(self, css: str) -> SeleniumLocator:
        return SeleniumLocator(self, css=css)

    def get_by_role(
        self,
        role: str,
        *,
        name: str | Pattern[str] | None = None,
        exact: bool = False,
    ) -> SeleniumLocator:
        return SeleniumLocator(self, role=role, name=name, exact=exact)

    def get_by_label(self, text: str, *, exact: bool = False) -> SeleniumLocator:
        _ = exact
        return SeleniumLocator(self, label=text)

    def get_by_text(
        self,
        text: str | Pattern[str],
        *,
        exact: bool = False,
    ) -> SeleniumLocator:
        loc = SeleniumLocator(self, css="body *")
        if isinstance(text, Pattern):
            return loc.filter(has_text=text)
        if exact:
            pat = re.compile(rf"^{re.escape(text)}$", re.I)
            return loc.filter(has_text=pat)
        return loc.filter(has_text=text)

    def on(self, event: str, handler: Callable[..., Any]) -> None:
        if event == "response" and callable(handler):
            self._response_handlers.append(handler)

    @contextmanager
    def expect_download(self, *, timeout: int | float = 30_000):
        dl_dir = self._download_dir
        dl_dir.mkdir(parents=True, exist_ok=True)
        before = {p.name for p in dl_dir.iterdir() if p.is_file()}

        class _DI:
            value: _DownloadHandle | None = None

        di = _DI()
        yield di

        deadline = time.time() + _norm_timeout_ms(timeout, default=30_000)
        while time.time() < deadline:
            for p in dl_dir.iterdir():
                if not p.is_file() or p.name in before:
                    continue
                if p.name.endswith(".part") or p.name.endswith(".crdownload"):
                    continue
                # Firefox indirmesi tamamlanana kadar bekle
                time.sleep(0.3)
                if p.stat().st_size > 0:
                    di.value = _DownloadHandle(p)
                    return
            time.sleep(0.25)

    @contextmanager
    def expect_response(self, predicate: Callable[[Any], bool], *, timeout: int | float = 30_000):
        """Playwright uyumu — Selenium’da XHR’yi performance+fetch ile yakala."""

        class _Resp:
            def __init__(self, url: str = "", status: int = 200, body: Any = None) -> None:
                self.url = url
                self.status = status
                self.headers: dict[str, str] = {"content-type": "application/json"}
                self._body = body

            def json(self) -> Any:
                return self._body if self._body is not None else {}

            def text(self) -> str:
                if self._body is None:
                    return ""
                if isinstance(self._body, (dict, list)):
                    import json as _json

                    return _json.dumps(self._body)
                return str(self._body)

        class _RI:
            value: _Resp | None = None

        ri = _RI()
        yield ri

        deadline = time.time() + _norm_timeout_ms(timeout, default=30_000)
        while time.time() < deadline and ri.value is None:
            try:
                found = self.evaluate(
                    """async () => {
                      const urls = [...new Set(
                        (performance.getEntriesByType('resource') || [])
                          .map((e) => e.name || '')
                          .filter((u) => /statsfrontend|statspage|playconsole|clients6\\.google/i.test(u))
                      )];
                      for (const url of urls.slice(0, 12)) {
                        try {
                          const r = await fetch(url, { credentials: 'include' });
                          if (!r.ok) continue;
                          const j = await r.json();
                          return { url, status: r.status, body: j };
                        } catch (e) {}
                      }
                      return null;
                    }"""
                )
            except Exception:
                found = None
            if isinstance(found, dict) and found.get("url"):
                cand = _Resp(
                    url=str(found.get("url") or ""),
                    status=int(found.get("status") or 200),
                    body=found.get("body"),
                )
                try:
                    if predicate(cand):
                        ri.value = cand
                        for handler in list(self._response_handlers):
                            try:
                                handler(cand)
                            except Exception:
                                pass
                        break
                except Exception:
                    pass
            time.sleep(0.4)


class SeleniumContext:
    _selenium_mode = True

    def __init__(self, driver: Any, *, download_dir: Path) -> None:
        self._driver = driver
        self._download_dir = download_dir
        self._response_handlers: list[Callable[..., Any]] = []
        first = SeleniumPage(driver, download_dir=download_dir)
        first.context = self
        self.pages: list[SeleniumPage] = [first]

    def new_page(self) -> SeleniumPage:
        page = SeleniumPage(self._driver, download_dir=self._download_dir)
        page.context = self
        for handler in self._response_handlers:
            page.on("response", handler)
        self.pages.append(page)
        return page

    def on(self, event: str, handler: Callable[..., Any]) -> None:
        """Context düzeyi dinleyiciyi sayfalara dağıt.

        Eskiden burası sessiz no-op'tu: `page.context.on("response", ...)` ile
        bağlanan yakalayıcılar hiç çalışmıyor, Firebase crash-free RPC'leri
        toplanmadığı için paneldeki hücreler boş kalıyordu.
        """
        if event != "response" or not callable(handler):
            return
        self._response_handlers.append(handler)
        for page in self.pages:
            page.on(event, handler)

    def close(self) -> None:
        quit_system_firefox_driver(self._driver)


def play_console_use_selenium() -> bool:
    import os

    raw = (os.environ.get("PLAY_CONSOLE_USE_SELENIUM") or "1").strip().lower()
    if raw in ("0", "false", "no"):
        return False
    from backend.services.scrape_browser import resolve_system_firefox_executable

    return resolve_system_firefox_executable() is not None


# Profil → açık Selenium context. Playwright tarafındaki «sıcak pencere»
# davranışının Selenium karşılığı: pencere tur bitince kapanmaz, bir sonraki
# tarama aynı pencereden devam eder. Böylece kullanıcının girdiği oturum ve
# yarım kalan iki adımlı doğrulama ekranı ayakta kalır.
_SELENIUM_WARM: dict[str, SeleniumContext] = {}


def _selenium_profile_key(profile: Path) -> str:
    try:
        return str(Path(str(profile)).expanduser().resolve())
    except Exception:  # noqa: BLE001
        return str(profile)


def _selenium_alive(ctx: SeleniumContext | None) -> bool:
    """Sürücüye gerçek bir çağrı yap — kapanmış pencere canlı sayılmasın."""
    if ctx is None:
        return False
    try:
        _ = ctx._driver.current_url
        return True
    except Exception:  # noqa: BLE001
        return False


def selenium_keep_window_open() -> bool:
    from backend.services.scrape_browser import scrape_keep_window_open

    return scrape_keep_window_open(env_key="SELENIUM_KEEP_OPEN")


def launch_selenium_context(profile: Path, *, headed: bool) -> tuple[None, SeleniumContext, bool]:
    dl = STATE_DIR / "cache" / "play-downloads"
    key = _selenium_profile_key(profile)

    warm = _SELENIUM_WARM.get(key)
    if selenium_keep_window_open() and _selenium_alive(warm):
        print("Selenium: mevcut Firefox penceresi yeniden kullanılıyor (kapatılmadı)", flush=True)
        return None, warm, True
    if warm is not None:
        _SELENIUM_WARM.pop(key, None)  # ölü kayıt

    driver = launch_system_firefox_driver(profile, headed=headed, download_dir=dl)
    ctx = SeleniumContext(driver, download_dir=dl)
    if selenium_keep_window_open():
        _SELENIUM_WARM[key] = ctx
    print("Play: sistem Firefox.app (Selenium) · profil oturumu korunur", flush=True)
    return None, ctx, False


def release_selenium_context(_pw: Any, context: SeleniumContext) -> None:
    """Pencereyi kapatma — sıcak tut. Yalnızca KEEP_OPEN kapalıysa kapatılır."""
    if selenium_keep_window_open() and _selenium_alive(context):
        print(
            "Selenium: Firefox penceresi açık bırakıldı (sonraki tarama buradan "
            "devam eder; kapatmak için SELENIUM_KEEP_OPEN=0)",
            flush=True,
        )
        return
    for k, v in list(_SELENIUM_WARM.items()):
        if v is context:
            _SELENIUM_WARM.pop(k, None)
    try:
        context.close()
    except Exception:
        pass
