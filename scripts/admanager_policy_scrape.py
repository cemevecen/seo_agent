#!/usr/bin/env python3
"""Google Ad Manager Policy Center scrape (Mac bridge).

https://admanager.google.com/{NETWORK}#admin/policy_center/issues
Site filtresi: sinemalar.com → CSV veya DOM → Railway ingest.

Örnek:
  .venv/bin/python scripts/admanager_policy_scrape.py --login
  .venv/bin/python scripts/admanager_policy_scrape.py --sync --ingest

Env:
  ADMANAGER_POLICY_PROFILE_DIR  (default: ~/.seo-agent/fx-google)
  ADMANAGER_NETWORK_ID          (default: 21728129623)
  ADMANAGER_POLICY_INGEST_URL
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
from urllib.parse import urlparse

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

from backend.services.scrape_browser import google_profile_dir

PROFILE_DIR = google_profile_dir()

NETWORK_ID = (os.environ.get("ADMANAGER_NETWORK_ID") or "21728129623").strip()
SITE_FILTER = (os.environ.get("ADMANAGER_POLICY_SITE_FILTER") or "sinemalar.com").strip()

INGEST_URL = (
    os.environ.get("ADMANAGER_POLICY_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/policy/ingest"
).strip()

ISSUES_URL = f"https://admanager.google.com/{NETWORK_ID}#admin/policy_center/issues"

ALLOWED_HOST_SUFFIXES = ("sinemalar.com",)


def _policy_url() -> str:
    return ISSUES_URL


def _looks_signed_in(page) -> bool:
    try:
        url = (page.url or "").lower()
        if "accounts.google.com" in url or "signin" in url:
            return False
        title = (page.title() or "").lower()
        if "sign in" in title or "oturum aç" in title:
            return False
        body = ""
        try:
            body = (page.inner_text("body") or "")[:1500].lower()
        except Exception:
            pass
        if "email or phone" in body or "e-posta veya telefon" in body:
            return False
        return "admanager.google.com" in url
    except Exception:
        return False


def _policy_use_selenium() -> bool:
    """Headed Google: Playwright Nightly reddedilir — sistem Firefox (Selenium)."""
    raw = (os.environ.get("ADMANAGER_POLICY_USE_SELENIUM") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _launch_context(*, headed: bool):
    """(pw, context, selenium_mode). Headed → Selenium Firefox.app (fx-google)."""
    if headed and _policy_use_selenium():
        from backend.services.scrape_browser import (
            resolve_system_firefox_executable,
            warm_session_forget_profile,
        )
        from backend.services.selenium_playwright_shim import launch_selenium_context

        if not resolve_system_firefox_executable():
            raise RuntimeError(
                "Ad Manager Policy headed için /Applications/Firefox.app gerekli "
                "(Playwright Nightly Google oturumunu reddeder)."
            )
        try:
            warm_session_forget_profile(PROFILE_DIR)
        except Exception:
            pass
        pw, context, _attached = launch_selenium_context(PROFILE_DIR, headed=True)
        print(
            "Ad Manager Policy: sistem Firefox.app (Selenium) · Playwright Nightly YOK",
            flush=True,
        )
        return pw, context, True

    from backend.services.scrape_browser import acquire_persistent_context

    pw, context, _reused = acquire_persistent_context(
        "admanager-policy",
        profile=PROFILE_DIR,
        headed=headed,
        env_key="ADMANAGER_POLICY_KEEP_OPEN",
        label="Ad Manager Policy",
        locale="tr-TR",
        viewport={"width": 1500, "height": 1100},
    )
    return pw, context, False


def _release_context(pw, context, *, headed: bool = True, selenium: bool = False) -> None:
    if selenium or getattr(context, "_selenium_mode", False):
        from backend.services.selenium_playwright_shim import release_selenium_context

        release_selenium_context(pw, context)
        print(
            "Ad Manager Policy: Selenium turu bitti (pencere yönetimi shim'de)",
            flush=True,
        )
        return
    from backend.services.scrape_browser import release_persistent_context

    release_persistent_context(
        "admanager-policy",
        pw,
        context,
        headed=headed,
        env_key="ADMANAGER_POLICY_KEEP_OPEN",
        label="Ad Manager Policy",
        profile=PROFILE_DIR,
    )


def run_login_interactive(timeout_sec: int | None = None) -> dict[str, Any]:
    """Gerçek Firefox.app ile Google girişi (Selenium warm paylaşımına bağlı kalma).

    Playwright Nightly / paylaşılan warm geckodriver login sürecini düşürüyordu.
    Kullanıcı Policy Center görününce pencereyi KAPATIR → çerezler diske yazılır.
    """
    from backend.services.scrape_browser import (
        LOGIN_WAIT_SEC,
        launch_system_firefox_login,
        login_wait_sec,
        warm_session_forget_profile,
    )

    timeout_sec = login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))
    try:
        warm_session_forget_profile(PROFILE_DIR)
    except Exception:
        pass
    # Warm/detached Selenium penceresini kapat — profili login'e bırak
    try:
        from backend.services.system_firefox_driver import shutdown_detached_firefox

        shutdown_detached_firefox(PROFILE_DIR)
    except Exception:
        pass
    try:
        from backend.services.scrape_browser import ensure_profile_free_for_launch

        ensure_profile_free_for_launch(
            PROFILE_DIR,
            takeover=True,
            busy_wait_sec=1.0,
            release_wait_sec=3.0,
            reason="admanager-policy-login",
        )
    except Exception as exc:
        print(f"  · Login öncesi profil uyarısı: {exc}", flush=True)

    return launch_system_firefox_login(
        PROFILE_DIR,
        _policy_url(),
        timeout_sec=timeout_sec,
        success_hint=(
            "Açılan Firefox'ta cemevecen@nokta.com ile Google girişi + 2FA yapın. "
            "Ad Manager Policy Center (sorun listesi) görününce Firefox'u KAPATIN "
            f"(en fazla {timeout_sec // 60} dk; çerezler diske yazılır)."
        ),
        verify_session=True,
    )


def _clear_global_top_search(page) -> None:
    """GAM üst arama (material 'search') Policy içeriğini boşaltabiliyor — temizle."""
    try:
        cleared = page.evaluate(
            """() => {
              const inputs = Array.from(document.querySelectorAll(
                'input[type="search"], input[aria-label*="search" i], input[aria-label*="Ara" i], header input, [role="banner"] input'
              ));
              let n = 0;
              for (const inp of inputs) {
                if (!inp || inp.offsetParent === null) continue;
                const ph = (inp.getAttribute('placeholder') || '').toLowerCase();
                const al = (inp.getAttribute('aria-label') || '').toLowerCase();
                // Policy filtre kutusu değil — üst global arama
                if (ph.includes('filtre') || ph.includes('filter') || al.includes('filtre') || al.includes('filter')) {
                  continue;
                }
                if ((inp.value || '').trim()) {
                  inp.focus();
                  inp.value = '';
                  inp.dispatchEvent(new Event('input', { bubbles: true }));
                  inp.dispatchEvent(new Event('change', { bubbles: true }));
                  n++;
                }
              }
              return n;
            }"""
        )
        if cleared:
            print(f"  · Üst arama temizlendi ({cleared})", flush=True)
            time.sleep(1.2)
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
    except Exception as exc:
        print(f"  · Üst arama temizleme uyarısı: {exc}", flush=True)


def _apply_site_filter(page, site: str = SITE_FILTER) -> None:
    """Politika merkezi İÇİ filtre: Site = sinemalar.com (üst GAM aramaya yazma)."""
    _clear_global_top_search(page)

    # Açık chip varsa temizleme — yeniden uygula
    try:
        clear = page.locator(
            "button:has-text('Temizle'), button:has-text('Clear'), "
            "[aria-label*='filtreyi temizle' i], [aria-label*='clear filter' i]"
        ).first
        if clear.count() and clear.is_visible(timeout=1500):
            clear.click(timeout=3000)
            time.sleep(0.8)
    except Exception:
        pass

    # Yalnız Policy filtre — üst search/banner input YOK
    candidates = [
        "input[placeholder*='Filtre' i]",
        "input[placeholder*='Filter' i]",
        "input[aria-label*='Filtre' i]",
        "input[aria-label*='Filter' i]",
        "input[placeholder*='Site' i]",
        "[aria-label*='Add filter' i]",
        "[aria-label*='Filtre ekle' i]",
    ]
    box = None
    for sel in candidates:
        loc = page.locator(sel).first
        try:
            if loc.count() and loc.is_visible(timeout=1200):
                box = loc
                break
        except Exception:
            continue
    if box is None:
        # Funnel / filter chip — üst arama değil
        try:
            page.locator(
                "button[aria-label*='Filtre' i], button[aria-label*='Filter' i], "
                "[data-test-id*='filter' i]"
            ).first.click(timeout=4000)
            time.sleep(0.5)
            for sel in candidates:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible(timeout=1000):
                    box = loc
                    break
        except Exception:
            pass
    if box is None:
        # Son çare: main içerik alanındaki text input (header hariç)
        try:
            found = page.evaluate(
                """() => {
                  const main = document.querySelector('main, [role="main"], .main-content') || document.body;
                  const inputs = Array.from(main.querySelectorAll('input[type="text"], input:not([type])'));
                  for (const inp of inputs) {
                    if (!inp.offsetParent) continue;
                    if (inp.closest('header, [role="banner"]')) continue;
                    const ph = (inp.getAttribute('placeholder') || '').toLowerCase();
                    const al = (inp.getAttribute('aria-label') || '').toLowerCase();
                    if (ph.includes('search') || al.includes('search') || al.includes('ara')) continue;
                    return true;
                  }
                  return false;
                }"""
            )
            if found:
                # click via JS focus first matching
                page.evaluate(
                    """() => {
                      const main = document.querySelector('main, [role="main"], .main-content') || document.body;
                      const inputs = Array.from(main.querySelectorAll('input[type="text"], input:not([type])'));
                      for (const inp of inputs) {
                        if (!inp.offsetParent) continue;
                        if (inp.closest('header, [role="banner"]')) continue;
                        const ph = (inp.getAttribute('placeholder') || '').toLowerCase();
                        const al = (inp.getAttribute('aria-label') || '').toLowerCase();
                        if (ph.includes('search') || al.includes('search') || al.includes('ara')) continue;
                        inp.focus();
                        inp.click();
                        return true;
                      }
                      return false;
                    }"""
                )
                box = page.locator("input:focus").first
        except Exception:
            pass
    if box is None:
        raise RuntimeError("Policy Center filtre kutusu bulunamadı (üst arama atlandı)")

    box.click(timeout=5000)
    box.fill("")
    box.type(site, delay=40)
    time.sleep(0.8)

    # Dropdown: "Site: sinemalar.com"
    option_clicked = False
    for label in (
        f"Site: {site}",
        f"Site = {site}",
        f"Site:{site}",
        site,
    ):
        opt = page.get_by_role("option", name=re.compile(re.escape(label), re.I))
        try:
            if opt.count():
                opt.first.click(timeout=4000)
                option_clicked = True
                break
        except Exception:
            pass
        text_opt = page.locator(f"text={label}").first
        try:
            if text_opt.count() and text_opt.is_visible(timeout=800):
                text_opt.click(timeout=4000)
                option_clicked = True
                break
        except Exception:
            pass
    if not option_clicked:
        box.press("Enter")
    time.sleep(2.5)


def _download_csv(page) -> bytes | None:
    """'CSV dosyasını indir' / Download / Export ile CSV al."""
    selectors = [
        "button:has-text('CSV dosyasını indir')",
        "button:has-text('Download CSV')",
        "button:has-text('CSV indir')",
        "a:has-text('CSV dosyasını indir')",
        "a:has-text('Download CSV')",
        "[aria-label*='CSV' i]",
    ]
    btn = None
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() and loc.is_visible(timeout=1200):
                btn = loc
                break
        except Exception:
            continue
    if btn is None:
        # metin ile
        try:
            loc = page.get_by_text(re.compile(r"CSV dosyasını indir|Download CSV", re.I))
            if loc.count():
                btn = loc.first
        except Exception:
            pass
    if btn is None:
        return None
    try:
        with page.expect_download(timeout=45_000) as dl_info:
            btn.click(timeout=8000)
            time.sleep(0.6)
            menu = page.locator(
                "[role='menuitem']:has-text('CSV'), button:has-text('CSV')"
            ).first
            try:
                if menu.count() and menu.is_visible(timeout=2000):
                    menu.click(timeout=4000)
            except Exception:
                pass
        download = getattr(dl_info, "value", None)
        if download is None:
            print("  · CSV indirme: dosya oluşmadı (timeout)", flush=True)
            return None
        path = None
        try:
            path = download.path()
        except Exception:
            path = None
        if not path:
            tmp = Path("/tmp") / f"admanager-policy-{int(time.time())}.csv"
            download.save_as(str(tmp))
            path = str(tmp)
        data = Path(path).read_bytes()
        if data and len(data) > 40:
            return data
    except Exception as exc:
        print(f"  · CSV indirme başarısız: {exc}", flush=True)
    return None


def _open_policy_center_ui(page) -> None:
    """Hash URL tek başına boş panel bırakabiliyor — Yönetici → Politika merkezi."""
    try:
        page.goto(
            f"https://admanager.google.com/{NETWORK_ID}",
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(2.5)
    except Exception as exc:
        print(f"  · Ana sayfa uyarısı: {exc}", flush=True)
    for label in ("Yönetici", "Admin"):
        try:
            loc = page.get_by_text(label, exact=True)
            if loc.count():
                loc.first.click(timeout=4000)
                time.sleep(0.8)
                break
        except Exception:
            continue
    clicked = False
    for label in ("Politika merkezi", "Policy center", "Policy Center"):
        try:
            locs = page.get_by_text(label)
            n = locs.count()
            if n:
                locs.nth(max(0, n - 1)).click(timeout=5000)
                clicked = True
                time.sleep(4.0)
                break
        except Exception:
            continue
    if not clicked:
        page.goto(_policy_url(), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3.0)
    else:
        # issues hash'e otur
        try:
            page.goto(_policy_url(), wait_until="domcontentloaded", timeout=120_000)
            time.sleep(2.0)
        except Exception:
            pass
    try:
        tab = page.get_by_text(re.compile(r"^Sorunlar$|^Issues$", re.I))
        if tab.count():
            tab.first.click(timeout=4000)
            time.sleep(3.0)
    except Exception:
        pass
    _clear_global_top_search(page)
    # İçerik paneli (CSV / özet kartları) gelene kadar bekle
    deadline = time.time() + 50.0
    while time.time() < deadline:
        stats = _dom_table_stats(page)
        if _policy_shell_ready(stats):
            print(
                f"  · Policy içerik hazır · role_rows={stats.get('role_rows')} "
                f"links={stats.get('all_links')}",
                flush=True,
            )
            break
        time.sleep(1.5)
    else:
        print("  · Policy içerik bekleme doldu — devam", flush=True)


_DOM_PROBE_JS = """() => {
  function walk(node, visit) {
    if (!node) return;
    visit(node);
    const kids = node.querySelectorAll ? node.querySelectorAll('*') : [];
    for (const el of kids) {
      if (el.shadowRoot) walk(el.shadowRoot, visit);
    }
  }
  let tables = 0, maxRows = 0, links = 0, roleRows = 0;
  const roots = [document];
  walk(document, (node) => {
    if (!node.querySelectorAll) return;
    const ts = node.querySelectorAll('table');
    tables += ts.length;
    for (const t of ts) {
      maxRows = Math.max(maxRows, t.querySelectorAll('tbody tr, tr').length);
    }
    links += node.querySelectorAll('a[href*="sinemalar"]').length;
    roleRows += node.querySelectorAll('[role="row"]').length;
  });
  const body = (document.body && document.body.innerText || '').replace(/\\s+/g, ' ').trim();
  return {
    tables,
    max_rows: maxRows,
    all_links: links,
    role_rows: roleRows,
    url: location.href || '',
    title: document.title || '',
    body_snip: body.slice(0, 360),
  };
}"""


def _dom_table_stats(page) -> dict[str, Any]:
    try:
        return page.evaluate(_DOM_PROBE_JS)
    except Exception:
        return {
            "tables": 0,
            "max_rows": 0,
            "all_links": 0,
            "role_rows": 0,
            "url": "",
            "title": "",
            "body_snip": "",
        }


def _policy_shell_ready(stats: dict[str, Any]) -> bool:
    """Policy UI gerçekten yüklendi mi — nav tek başına yetmez, tablo/CSV şart."""
    if int(stats.get("all_links") or 0) > 0:
        return True
    if int(stats.get("max_rows") or 0) > 0:
        return True
    # role_rows nav'da da var; asıl içerik ~15+ satır veya CSV butonu
    if int(stats.get("role_rows") or 0) >= 12:
        return True
    low = f"{stats.get('body_snip') or ''} {stats.get('title') or ''}".lower()
    content_markers = (
        "csv dosyasını indir",
        "download csv",
        "etkilenen öğeler",
        "affected items",
        "sorunun konumu",
        "issue location",
        "reklam istekleri",
        "ad requests",
    )
    if any(m in low for m in content_markers):
        return True
    empty_ok = (
        "no issues" in low
        or "sorun yok" in low
        or "no results" in low
        or "sonuç yok" in low
        or "hiç sorun" in low
    )
    return empty_ok


def _open_policy_center(page) -> None:
    """Hash SPA bazen warm pencerede yenilenmiyor — blank → ana → issues."""
    try:
        page.goto("about:blank", wait_until="domcontentloaded", timeout=30_000)
    except Exception:
        pass
    time.sleep(0.4)
    home = f"https://admanager.google.com/{NETWORK_ID}"
    try:
        page.goto(home, wait_until="domcontentloaded", timeout=120_000)
        time.sleep(1.5)
    except Exception as exc:
        print(f"  · Ad Manager ana sayfa uyarısı: {exc}", flush=True)
    page.goto(_policy_url(), wait_until="domcontentloaded", timeout=120_000)
    time.sleep(2.5)
    # Hash değişimini zorla (warm reuse)
    try:
        page.evaluate(
            """() => {
              const want = '#admin/policy_center/issues';
              if ((location.hash || '') !== want) {
                location.hash = want;
              }
            }"""
        )
        time.sleep(1.5)
    except Exception:
        pass


def _wait_policy_content(page, *, timeout_sec: float = 75.0) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = _dom_table_stats(page)
        if _policy_shell_ready(last):
            return last
        try:
            page.mouse.wheel(0, 600)
        except Exception:
            pass
        time.sleep(1.6)
    return last


def _closed_target_error(exc: BaseException) -> bool:
    msg = str(exc or "").lower()
    return any(
        s in msg
        for s in (
            "target page, context or browser has been closed",
            "target closed",
            "browser has been closed",
            "connection closed",
        )
    )


def _scrape_dom_rows(page) -> list[dict[str, Any]]:
    """Tablo satırlarını DOM'dan oku (CSV yoksa) — shadow root dahil."""
    # Scroll to load more (virtualized tables)
    for _ in range(40):
        try:
            page.mouse.wheel(0, 2400)
            time.sleep(0.35)
        except Exception:
            break
    try:
        page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass
    time.sleep(0.5)

    raw = page.evaluate(
        """() => {
      function walk(node, visit) {
        if (!node) return;
        visit(node);
        const kids = node.querySelectorAll ? node.querySelectorAll('*') : [];
        for (const el of kids) {
          if (el.shadowRoot) walk(el.shadowRoot, visit);
        }
      }
      const tables = [];
      const gridRows = [];
      walk(document, (node) => {
        if (!node.querySelectorAll) return;
        for (const t of node.querySelectorAll('table')) tables.push(t);
        for (const r of node.querySelectorAll('[role="row"]')) {
          if (r.querySelectorAll('[role="gridcell"], [role="cell"], td').length >= 2) {
            gridRows.push(r);
          }
        }
      });
      let best = null, bestN = 0;
      for (const t of tables) {
        const rows = t.querySelectorAll('tbody tr, tr');
        if (rows.length > bestN) { best = t; bestN = rows.length; }
      }
      if (!best || bestN < 1) {
        if (gridRows.length > bestN) {
          return gridRows.map(tr => {
            const cells = Array.from(tr.querySelectorAll('[role="gridcell"], [role="cell"], td'));
            const vals = cells.map(td => (td.innerText || '').trim().replace(/\\s+/g, ' '));
            const links = Array.from(tr.querySelectorAll('a[href]'))
              .map(a => a.href)
              .filter(h => /sinemalar\\.com/i.test(h));
            return { cells: vals, headers: [], links };
          });
        }
      }
      if (!best) return [];
      const headers = Array.from(best.querySelectorAll('thead th, thead td'))
        .map(el => (el.innerText || '').trim());
      const out = [];
      for (const tr of best.querySelectorAll('tbody tr, tr')) {
        const cells = Array.from(tr.querySelectorAll('td'));
        if (!cells.length) continue;
        const vals = cells.map(td => (td.innerText || '').trim().replace(/\\s+/g, ' '));
        const links = Array.from(tr.querySelectorAll('a[href]'))
          .map(a => a.href)
          .filter(h => /sinemalar\\.com/i.test(h));
        if (!links.length) {
          for (const v of vals) {
            const m = String(v || '').match(/https?:\\/\\/[^\\s]*sinemalar\\.com[^\\s]*/i)
              || String(v || '').match(/(?:www\\.)?(?:m\\.)?sinemalar\\.com\\/[^\\s]*/i);
            if (m) {
              let u = m[0];
              if (!/^https?:/i.test(u)) u = 'https://' + u;
              links.push(u);
              break;
            }
          }
        }
        out.push({ cells: vals, headers, links });
      }
      return out;
    }"""
    )
    rows: list[dict[str, Any]] = []
    for item in raw or []:
        headers = [str(h or "").strip().lower() for h in (item.get("headers") or [])]
        cells = item.get("cells") or []
        links = item.get("links") or []

        def col(*names: str) -> str:
            for name in names:
                n = name.lower()
                for i, h in enumerate(headers):
                    if n in h and i < len(cells):
                        return str(cells[i] or "").strip()
            return ""

        url = ""
        if links:
            url = str(links[0])
        if not url:
            # Sorunun konumu hücresi
            loc = col("sorunun konumu", "issue location", "location")
            if loc:
                url = loc.split()[0] if loc else ""
        if not url:
            # Site + path birleşimi
            site = col("site veya uygulama", "site or app", "site")
            path = col("sorunun konumu", "issue location")
            if site and path:
                path_clean = path.split()[0]
                if not path_clean.startswith("http"):
                    url = f"https://{site.rstrip('/')}/{path_clean.lstrip('./')}"
                else:
                    url = path_clean
        if not url:
            continue
        rows.append(
            {
                "url": url,
                "issue_type": col("sorunlar", "issues", "issue") or "Politika sorunu",
                "enforcement": col("durum", "status"),
                "ad_requests_7d": col(
                    "reklam istekleri", "ad requests", "ad requests: son 7 gün"
                ),
                "first_reported": col("bildirim tarihi", "notification date"),
                "last_reported": col("bildirim tarihi", "notification date"),
                "site_host": col("site veya uygulama", "site or app", "site"),
            }
        )
    return rows


def _host_allowed(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    if not host:
        return False
    return any(host == s or host.endswith("." + s) for s in ALLOWED_HOST_SUFFIXES)


def _normalize_row_dict(r: dict[str, Any]) -> dict[str, Any] | None:
    url = str(r.get("url") or "").strip()
    if not url:
        # Site sütunu bazen çıplak host (donanimhaber.com / sinemalar.com)
        site = str(r.get("site_host") or r.get("site") or "").strip()
        if site and "." in site and " " not in site:
            url = "https://" + site.lstrip("/")
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url.lstrip("/")
    if not _host_allowed(url):
        return None
    issue = str(r.get("issue_type") or r.get("sorunlar") or "").strip()
    if not issue:
        issue = "Politika sorunu"
    return {
        "url": url,
        "issue_type": issue,
        "enforcement": str(r.get("enforcement") or r.get("durum") or "").strip(),
        "ad_requests_7d": r.get("ad_requests_7d") or r.get("reklam_istekleri") or 0,
        "first_reported": r.get("first_reported") or r.get("bildirim_tarihi"),
        "last_reported": r.get("last_reported") or r.get("first_reported") or r.get("bildirim_tarihi"),
        "site_host": str(r.get("site_host") or urlparse(url).hostname or "").strip().lower(),
        "source": "admanager_policy_scrape",
    }


def _pull_csv_or_dom(page) -> tuple[list[dict[str, Any]], str, str | None]:
    """CSV veya DOM → normalize edilmiş sinemalar satırları."""
    csv_bytes = _download_csv(page)
    rows: list[dict[str, Any]] = []
    csv_b64 = None
    method = "dom"

    if csv_bytes:
        method = "csv"
        from backend.services import policy_csv as pcsv
        import base64

        parsed, _headers, err = pcsv.parse_csv(csv_bytes)
        if err:
            print(f"  · CSV parse: {err}", flush=True)
        for pr in parsed:
            nr = _normalize_row_dict(
                {
                    "url": pr.get("url"),
                    "issue_type": pr.get("issue_type"),
                    "enforcement": pr.get("enforcement"),
                    "ad_requests_7d": pr.get("ad_requests_7d"),
                    "first_reported": (
                        pr["first_reported"].isoformat()
                        if pr.get("first_reported")
                        else None
                    ),
                    "last_reported": (
                        pr["last_reported"].isoformat()
                        if pr.get("last_reported")
                        else None
                    ),
                }
            )
            if nr:
                rows.append(nr)
        csv_b64 = base64.b64encode(csv_bytes).decode("ascii")
        print(f"  · CSV · {len(csv_bytes)} byte · parse={len(rows)} satır", flush=True)
    else:
        print("  · CSV yok — DOM satırları…", flush=True)
        raw_dom = _scrape_dom_rows(page)
        print(f"  · DOM ham satır: {len(raw_dom)}", flush=True)
        for raw in raw_dom:
            nr = _normalize_row_dict(raw)
            if nr:
                rows.append(nr)
    return rows, method, csv_b64


def _collect_rows_from_page(page) -> tuple[list[dict[str, Any]], str, str | None, dict[str, Any]]:
    """(rows, method, csv_b64, stats) — önce filtresiz (üst arama temiz), gerekirse site filtresi."""
    _clear_global_top_search(page)
    time.sleep(1.0)
    stats_before = _wait_policy_content(page, timeout_sec=45.0)
    print(
        f"  · DOM (filtresiz) · tables={stats_before.get('tables')} "
        f"max_rows={stats_before.get('max_rows')} "
        f"sinemalar_links={stats_before.get('all_links')} "
        f"role_rows={stats_before.get('role_rows')} "
        f"snip={(stats_before.get('body_snip') or '')[:100]!r}",
        flush=True,
    )

    rows, method, csv_b64 = _pull_csv_or_dom(page)
    stats = stats_before

    # Filtresiz boşsa Policy site filtresini dene (üst aramaya değil)
    if not rows:
        print(f"  · Site filtresi deneniyor: {SITE_FILTER}", flush=True)
        try:
            _apply_site_filter(page, SITE_FILTER)
            time.sleep(2.0)
            stats = _wait_policy_content(page, timeout_sec=40.0)
            print(
                f"  · DOM (filtreli) · tables={stats.get('tables')} "
                f"max_rows={stats.get('max_rows')} "
                f"sinemalar_links={stats.get('all_links')} "
                f"role_rows={stats.get('role_rows')}",
                flush=True,
            )
            rows, method, csv_b64 = _pull_csv_or_dom(page)
        except Exception as exc:
            print(f"  · Filtre uyarısı: {exc}", flush=True)

    return rows, method, csv_b64, stats


def _scrape_once(*, headed: bool, force_fresh: bool = False) -> dict[str, Any]:
    scraped_at = datetime.now(timezone.utc).isoformat()
    if force_fresh:
        try:
            from backend.services.scrape_browser import (
                ensure_profile_free_for_launch,
                warm_session_forget_profile,
            )

            warm_session_forget_profile(PROFILE_DIR)
            ensure_profile_free_for_launch(
                PROFILE_DIR,
                takeover=True,
                busy_wait_sec=2.0,
                release_wait_sec=4.0,
                reason="admanager-policy:fresh",
            )
            time.sleep(1.5)
        except Exception as exc:
            print(f"  · Fresh profil uyarısı: {exc}", flush=True)

    pw, context, selenium = _launch_context(headed=headed)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        print(f"  · Policy Center açılıyor… {NETWORK_ID}", flush=True)
        # Selenium warm pencerede about:blank oturumu bozabiliyor — yumuşak aç
        if selenium:
            # Hash URL bazen boş panel; menü ile aç
            _open_policy_center_ui(page)
            # fx-google oturumu düşmüşse günlük Firefox Google çerezlerini dene
            if not _looks_signed_in(page):
                try:
                    from backend.services.system_firefox_driver import (
                        inject_google_cookies_from_default,
                    )

                    drv = getattr(context, "_driver", None) or getattr(page, "_driver", None)
                    if drv is not None:
                        n = inject_google_cookies_from_default(drv)
                        print(f"  · Varsayılan Firefox Google çerez enjekte: {n}", flush=True)
                        if n:
                            page.goto(
                                _policy_url(),
                                wait_until="domcontentloaded",
                                timeout=120_000,
                            )
                            time.sleep(3.0)
                except Exception as exc:
                    print(f"  · Çerez enjekte uyarısı: {exc}", flush=True)
        else:
            _open_policy_center(page)
        # Google hesap seçici / yavaş redirect için kısa bekleme
        signed = _looks_signed_in(page)
        if not signed:
            for _ in range(12):
                time.sleep(1.5)
                signed = _looks_signed_in(page)
                if signed:
                    break
                try:
                    u = (page.url or "").lower()
                    if "admanager.google.com" in u and "accounts.google.com" not in u:
                        # ara sayfa — hash'e zorla
                        page.goto(_policy_url(), wait_until="domcontentloaded", timeout=60_000)
                        time.sleep(1.5)
                except Exception:
                    pass
        if not signed:
            return {
                "ok": False,
                "needs_login": True,
                "message": "Ad Manager oturumu yok — --login ile cemevecen@nokta.com gir",
                "scraped_at": scraped_at,
                "rows": [],
                "url": getattr(page, "url", None),
            }

        stats0 = _wait_policy_content(page, timeout_sec=75.0)
        print(
            f"  · Shell · url={(stats0.get('url') or '')[:80]} "
            f"links={stats0.get('all_links')} tables={stats0.get('tables')} "
            f"ready={_policy_shell_ready(stats0)}",
            flush=True,
        )
        if not _policy_shell_ready(stats0):
            print("  · Shell boş — hard reload…", flush=True)
            try:
                page.reload(wait_until="domcontentloaded", timeout=120_000)
            except Exception:
                _open_policy_center(page)
            stats0 = _wait_policy_content(page, timeout_sec=60.0)
            print(
                f"  · Shell reload · links={stats0.get('all_links')} "
                f"tables={stats0.get('tables')} snip={(stats0.get('body_snip') or '')[:120]!r}",
                flush=True,
            )
            if not _policy_shell_ready(stats0):
                return {
                    "ok": False,
                    "needs_login": False,
                    "message": (
                        "Policy Center UI yüklenmedi (boş shell). "
                        f"title={stats0.get('title')!r} snip={(stats0.get('body_snip') or '')[:160]!r}"
                    ),
                    "scraped_at": scraped_at,
                    "rows": [],
                    "stats": stats0,
                }

        rows, method, csv_b64, stats = _collect_rows_from_page(page)

        seen: set[tuple[str, str]] = set()
        uniq: list[dict[str, Any]] = []
        for r in rows:
            key = (r["url"], r["issue_type"])
            if key in seen:
                continue
            seen.add(key)
            uniq.append(r)

        msg = f"{len(uniq)} sinemalar satır · yöntem={method} · filtre={SITE_FILTER}"
        print(f"  · {msg}", flush=True)
        # 0 satır = başarısız (auto-retry + panel «çekilemedi» doğru işaretlensin)
        ok = len(uniq) > 0 or bool(csv_b64)
        if not ok:
            msg = (
                f"0 sinemalar satır · yöntem={method} · filtre={SITE_FILTER} "
                f"· links={stats.get('all_links')} · snip={(stats.get('body_snip') or '')[:120]!r}"
            )
        return {
            "ok": ok,
            "needs_login": False,
            "source": "admanager_policy_scrape",
            "scraped_at": scraped_at,
            "network_id": NETWORK_ID,
            "site_filter": SITE_FILTER,
            "method": method,
            "message": msg,
            "rows": uniq,
            "csv_base64": csv_b64,
            "profile": str(PROFILE_DIR),
            "stats": stats,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"Tarama hatası: {exc}",
            "scraped_at": scraped_at,
            "rows": [],
            "closed": _closed_target_error(exc),
        }
    finally:
        _release_context(pw, context, headed=headed, selenium=selenium)


def scrape_admanager_policy(*, headed: bool = True) -> dict[str, Any]:
    """Policy Center → satırlar. Kapalı tarayıcı / boş shell için 1 fresh retry."""
    result = _scrape_once(headed=headed, force_fresh=False)
    if result.get("ok") or result.get("needs_login"):
        return result
    retry_reason = ""
    if result.get("closed") or _closed_target_error(
        Exception(str(result.get("message") or ""))
    ):
        retry_reason = "kapalı tarayıcı"
    elif "yüklenmedi" in str(result.get("message") or "").lower():
        retry_reason = "boş shell"
    elif not (result.get("rows") or []):
        retry_reason = "0 satır"
    if not retry_reason:
        return result
    print(f"  · Policy yeniden deneme (fresh profil) · neden={retry_reason}", flush=True)
    time.sleep(2.0)
    return _scrape_once(headed=headed, force_fresh=True)


def ingest_scrape_result(result: dict[str, Any]) -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    body = {
        "source": result.get("source") or "admanager_policy_scrape",
        "scraped_at": result.get("scraped_at") or "",
        "message": result.get("message") or "",
        "network_id": result.get("network_id") or NETWORK_ID,
        "site_filter": result.get("site_filter") or SITE_FILTER,
        "method": result.get("method") or "",
        "rows": result.get("rows") or [],
        "csv_base64": result.get("csv_base64"),
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Notification-Ingest-Token": token,
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            status = int(getattr(resp, "status", 200) or 200)
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                low = raw[:400].lower()
                if "<html" in low or "giriş" in low or "<!doctype" in low:
                    return {
                        "ok": False,
                        "http_status": status,
                        "message": (
                            "Ingest HTML login sayfası döndü (auth/allowlist). "
                            "Railway deploy + /api/policy/ingest public olmalı. "
                            f"Gövde: {raw[:160]!r}"
                        ),
                    }
                return {
                    "ok": False,
                    "http_status": status,
                    "message": f"Ingest JSON değil: {raw[:200]!r}",
                }
            if not isinstance(parsed, dict):
                return {"ok": False, "http_status": status, "message": "Ingest beklenmeyen gövde"}
            parsed["http_status"] = status
            if parsed.get("ok") is False:
                return parsed
            # Boş scrape’i “başarı” diye yutma — satır yoksa uyarı
            if not (result.get("rows") or result.get("csv_base64")):
                parsed.setdefault(
                    "warning",
                    "0 satır ingest edildi — Policy Center’da CSV/DOM boş; filtre veya UI değişmiş olabilir.",
                )
            return parsed
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:400]
        return {"ok": False, "http_status": exc.code, "message": detail or str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ad Manager Policy Center scrape")
    parser.add_argument("--login", action="store_true")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headed", action="store_true", default=False)
    parser.add_argument("--headless", action="store_true", default=False)
    args = parser.parse_args(argv)

    if args.login:
        out = run_login_interactive()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    if not args.sync and not args.ingest:
        parser.print_help()
        return 2

    env_hl = (os.environ.get("ADMANAGER_POLICY_HEADLESS") or "").strip().lower()
    headed = True
    if args.headless or env_hl in ("1", "true", "yes"):
        headed = False
    if args.headed:
        headed = True

    result = scrape_admanager_policy(headed=headed)
    print(json.dumps({k: v for k, v in result.items() if k != "csv_base64"}, ensure_ascii=False, indent=2)[:4000])
    if not result.get("ok"):
        return 1
    if args.ingest:
        ing = ingest_scrape_result(result)
        print(json.dumps(ing, ensure_ascii=False, indent=2)[:2000])
        return 0 if ing.get("ok") else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
