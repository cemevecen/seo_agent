#!/usr/bin/env python3
"""Sinemalar management/noAds tarama (Mac bridge).

https://www.sinemalar.com/management/noAds
Admin oturumu gerekir (kalıcı Chromium profili veya form login).

Örnek:
  .venv/bin/python scripts/sinemalar_noads_scrape.py --login
  .venv/bin/python scripts/sinemalar_noads_scrape.py --sync --ingest

Env:
  SINEMALAR_NOADS_PROFILE_DIR  (default: ~/.seo-agent/sinemalar-admin-profile)
  SINEMALAR_ADMIN_EMAIL / SINEMALAR_ADMIN_PASSWORD  (opsiyonel form login)
  SINEMALAR_NOADS_URL          (default: https://www.sinemalar.com/management/noAds)
  SINEMALAR_NOADS_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

PROFILE_DIR = Path(
    os.environ.get("SINEMALAR_NOADS_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "sinemalar-admin-profile")
).expanduser()

NOADS_URL = (
    os.environ.get("SINEMALAR_NOADS_URL") or "https://www.sinemalar.com/management/noAds"
).strip()

INGEST_URL = (
    os.environ.get("SINEMALAR_NOADS_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/policy/noads/ingest"
).strip()

ADMIN_EMAIL = (os.environ.get("SINEMALAR_ADMIN_EMAIL") or "").strip()
ADMIN_PASSWORD = (os.environ.get("SINEMALAR_ADMIN_PASSWORD") or "").strip()

_EXTRACT_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  const push = (obj) => {
    const url = (obj.url || obj.href || "").trim();
    const eid = String(obj.entity_id || obj.id || "").trim();
    const label = (obj.label || obj.title || obj.text || "").trim();
    const key = (url || "") + "|" + eid + "|" + label;
    if (!url && !eid && !label) return;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      url: url || null,
      entity_id: eid || null,
      label: label || null,
    });
  };

  const idFromHref = (href) => {
    if (!href) return "";
    const m = String(href).match(
      /(?:movieInfo|personMovies|person|\/film\/[^/]+|\/sanatci\/[^/]+|management\/(?:movie|person))\/(\d+)/i
    );
    return m ? m[1] : "";
  };

  document.querySelectorAll("a[href]").forEach((a) => {
    const href = a.href || a.getAttribute("href") || "";
    if (!href || href.startsWith("javascript:")) return;
    if (!/sinemalar\.com|management\/|movieInfo|\/film\/|\/sanatci\//i.test(href)
        && !/^\//.test(href)) return;
    push({
      url: href,
      entity_id: idFromHref(href),
      label: (a.textContent || "").replace(/\s+/g, " ").trim().slice(0, 200),
    });
  });

  document.querySelectorAll("[data-url],[data-href],[data-id],[data-movie-id],[data-film-id]").forEach((el) => {
    push({
      url: el.getAttribute("data-url") || el.getAttribute("data-href") || "",
      entity_id: el.getAttribute("data-id") || el.getAttribute("data-movie-id") || el.getAttribute("data-film-id") || "",
      label: (el.textContent || "").replace(/\s+/g, " ").trim().slice(0, 200),
    });
  });

  document.querySelectorAll("input[type='text'], input[type='url'], textarea").forEach((el) => {
    const v = (el.value || "").trim();
    if (!v) return;
    if (/sinemalar\.com|https?:\/\//i.test(v) || /^\d{4,}$/.test(v)) {
      push({ url: /https?:\/\//i.test(v) ? v : "", entity_id: /^\d+$/.test(v) ? v : idFromHref(v), label: v.slice(0, 200) });
    }
  });

  document.querySelectorAll("table tr, .table tr, tbody tr").forEach((tr) => {
    const tds = Array.from(tr.querySelectorAll("td"));
    if (!tds.length) return;
    const text = tds.map((td) => (td.textContent || "").replace(/\s+/g, " ").trim()).filter(Boolean);
    const joined = text.join(" | ");
    const a = tr.querySelector("a[href]");
    const href = a ? (a.href || "") : "";
    let eid = idFromHref(href);
    if (!eid) {
      for (const t of text) {
        const m = t.match(/^(\d{4,})$/);
        if (m) { eid = m[1]; break; }
      }
    }
    if (href || eid || /sinemalar\.com/.test(joined)) {
      push({ url: href, entity_id: eid, label: joined.slice(0, 240) });
    }
  });

  // Fallback: page text URLs
  if (out.length < 3) {
    const html = document.body ? document.body.innerText : "";
    const re = /https?:\/\/[^\s"'<>]+sinemalar\.com[^\s"'<>]*/gi;
    let m;
    while ((m = re.exec(html)) !== null) {
      push({ url: m[0].replace(/[.,);]+$/, ""), entity_id: idFromHref(m[0]), label: "" });
    }
  }

  return {
    url: location.href,
    title: document.title || "",
    count: out.length,
    entries: out,
  };
}
"""


def _looks_logged_in(page) -> bool:
    try:
        url = (page.url or "").lower()
        if "login" in url or "giris" in url or "uye" in url:
            return False
        if "management" in url:
            return True
        html = page.content() or ""
        if re.search(r"management/noAds|reklam\s*çıkmayacak|noAds", html, re.I):
            return True
        if re.search(r"üye girişi|uye girisi|şifre|password", html, re.I) and "management" not in url:
            return False
    except Exception:
        return False
    return "management" in (page.url or "").lower()


def _try_form_login(page) -> bool:
    if not ADMIN_EMAIL or not ADMIN_PASSWORD:
        return False
    try:
        page.goto("https://www.sinemalar.com/", wait_until="domcontentloaded", timeout=60000)
        time.sleep(1.2)
        # Open login modal if present
        for sel in (
            "text=Giriş Yap",
            "a:has-text('Giriş')",
            "button:has-text('Giriş')",
            ".login",
            "#login",
        ):
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible():
                    loc.click(timeout=2000)
                    break
            except Exception:
                continue
        time.sleep(0.6)
        filled = False
        for user_sel in (
            "input[name='username']",
            "input[name='email']",
            "input[type='email']",
            "input[placeholder*='E-Posta']",
            "input[placeholder*='Kullanıcı']",
        ):
            try:
                loc = page.locator(user_sel).first
                if loc.count() and loc.is_visible():
                    loc.fill(ADMIN_EMAIL)
                    filled = True
                    break
            except Exception:
                continue
        for pass_sel in ("input[name='password']", "input[type='password']"):
            try:
                loc = page.locator(pass_sel).first
                if loc.count() and loc.is_visible():
                    loc.fill(ADMIN_PASSWORD)
                    break
            except Exception:
                continue
        if not filled:
            return False
        for btn in (
            "button:has-text('Giriş Yap')",
            "button[type='submit']",
            "input[type='submit']",
        ):
            try:
                loc = page.locator(btn).first
                if loc.count() and loc.is_visible():
                    loc.click(timeout=3000)
                    break
            except Exception:
                continue
        time.sleep(2.5)
        return True
    except Exception as exc:
        print(f"Form login denemesi: {exc}", flush=True)
        return False


def scrape_sinemalar_noads(*, headed: bool = True) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    channel = (os.environ.get("SINEMALAR_NOADS_BROWSER_CHANNEL") or os.environ.get("PLAY_CONSOLE_BROWSER_CHANNEL") or "").strip() or None

    with sync_playwright() as p:
        browser_kwargs: dict[str, Any] = {
            "user_data_dir": str(PROFILE_DIR),
            "headless": not headed,
            "viewport": {"width": 1400, "height": 900},
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if channel:
            browser_kwargs["channel"] = channel
        context = p.chromium.launch_persistent_context(**browser_kwargs)
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(NOADS_URL, wait_until="domcontentloaded", timeout=90000)
            time.sleep(2.0)
            if not _looks_logged_in(page):
                _try_form_login(page)
                page.goto(NOADS_URL, wait_until="domcontentloaded", timeout=90000)
                time.sleep(2.0)
            if not _looks_logged_in(page):
                return {
                    "ok": False,
                    "needs_login": True,
                    "message": "Sinemalar admin oturumu yok. --login ile giriş yapın.",
                    "entries": [],
                }

            # Scroll to load lazy rows
            for _ in range(12):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(0.45)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(0.4)

            data = page.evaluate(_EXTRACT_JS) or {}
            entries = data.get("entries") or []
            # Absolute URLs
            fixed = []
            for e in entries:
                if not isinstance(e, dict):
                    continue
                url = (e.get("url") or "").strip()
                if url and url.startswith("/"):
                    url = urljoin("https://www.sinemalar.com", url)
                fixed.append(
                    {
                        "url": url or None,
                        "entity_id": e.get("entity_id") or None,
                        "label": e.get("label") or None,
                    }
                )
            return {
                "ok": True,
                "needs_login": False,
                "source": "sinemalar_noads",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "page_url": data.get("url") or NOADS_URL,
                "page_title": data.get("title") or "",
                "entries": fixed,
                "message": f"{len(fixed)} noAds kaydı",
            }
        finally:
            context.close()


def ingest_noads_result(result: dict[str, Any]) -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    body = {
        "source": result.get("source") or "sinemalar_noads",
        "scraped_at": result.get("scraped_at") or "",
        "message": result.get("message") or "",
        "entries": result.get("entries") or [],
    }
    req = urllib.request.Request(
        INGEST_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Notification-Ingest-Token": token,
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"ok": False, "message": raw[:500]}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:800]
        return {"ok": False, "message": f"HTTP {exc.code}: {detail}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}


def run_login_interactive() -> int:
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Profil: {PROFILE_DIR}", flush=True)
    print(f"Açılacak: {NOADS_URL}", flush=True)
    print("Tarayıcıda Sinemalar admin girişi yapın; noAds sayfasını görünce Enter.", flush=True)
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            viewport={"width": 1400, "height": 900},
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(NOADS_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            input("Giriş tamam → Enter… ")
        except EOFError:
            time.sleep(120)
        context.close()
    print("Profil kaydedildi.", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sinemalar noAds tarama")
    parser.add_argument("--login", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args(argv)
    if args.login:
        return run_login_interactive()
    if not args.sync and not args.ingest:
        parser.print_help()
        return 2
    env_hl = (os.environ.get("SINEMALAR_NOADS_HEADLESS") or "").strip().lower()
    headed = True
    if args.headless or env_hl in ("1", "true", "yes"):
        headed = False
    if args.headed:
        headed = True
    result = scrape_sinemalar_noads(headed=headed)
    print(json.dumps({k: result.get(k) for k in ("ok", "needs_login", "message", "page_url")}, ensure_ascii=False), flush=True)
    print(f"entries={len(result.get('entries') or [])}", flush=True)
    if not result.get("ok"):
        return 1
    if args.ingest:
        ing = ingest_noads_result(result)
        print(json.dumps(ing, ensure_ascii=False)[:2000], flush=True)
        return 0 if ing.get("ok") else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
