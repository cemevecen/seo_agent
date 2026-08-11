#!/usr/bin/env python3
"""Google Ad Manager Policy Center scrape (Mac bridge).

https://admanager.google.com/{NETWORK}#admin/policy_center/issues
Site filtresi: sinemalar.com → CSV veya DOM → Railway ingest.

Örnek:
  .venv/bin/python scripts/admanager_policy_scrape.py --login
  .venv/bin/python scripts/admanager_policy_scrape.py --sync --ingest

Env:
  ADMANAGER_POLICY_PROFILE_DIR  (default: ~/.seo-agent/play-console-profile)
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

PROFILE_DIR = Path(
    os.environ.get("ADMANAGER_POLICY_PROFILE_DIR")
    or os.environ.get("PLAY_CONSOLE_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "play-console-profile")
).expanduser()

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


def _launch_context(*, headed: bool):
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (PROFILE_DIR / name).unlink(missing_ok=True)
        except Exception:
            pass
    pw = sync_playwright().start()
    channel = (
        os.environ.get("ADMANAGER_POLICY_BROWSER_CHANNEL")
        or os.environ.get("PLAY_CONSOLE_BROWSER_CHANNEL")
        or "chrome"
    ).strip()
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(PROFILE_DIR),
        "headless": not headed,
        "viewport": {"width": 1500, "height": 1100},
        "locale": "tr-TR",
        "accept_downloads": True,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    if channel and channel.lower() not in ("0", "none", "chromium"):
        launch_kwargs["channel"] = channel
    try:
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    except Exception:
        launch_kwargs.pop("channel", None)
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    return pw, context


def run_login_interactive(timeout_sec: int = 600) -> dict[str, Any]:
    pw, context = _launch_context(headed=True)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(_policy_url(), wait_until="domcontentloaded", timeout=120_000)
        print(
            f"Tarayıcıda Ad Manager giriş yap (cemevecen@nokta.com). "
            f"Policy Center açılınca {timeout_sec}s içinde otomatik kapanır.",
            flush=True,
        )
        deadline = time.time() + max(60, timeout_sec)
        while time.time() < deadline:
            if _looks_signed_in(page) and (
                "policy" in (page.url or "").lower()
                or "sorun" in (page.inner_text("body") or "")[:800].lower()
                or "issue" in (page.inner_text("body") or "")[:800].lower()
            ):
                time.sleep(2)
                print(f"Login OK · {page.url}", flush=True)
                return {"ok": True, "url": page.url, "profile": str(PROFILE_DIR)}
            time.sleep(2)
        return {
            "ok": False,
            "message": "Login zaman aşımı — tekrar --login dene",
            "url": page.url,
            "profile": str(PROFILE_DIR),
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _apply_site_filter(page, site: str = SITE_FILTER) -> None:
    """Filtre çubuğuna Site = sinemalar.com uygula."""
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

    # Filtre input
    candidates = [
        "input[placeholder*='Filtre' i]",
        "input[placeholder*='Filter' i]",
        "input[aria-label*='Filtre' i]",
        "input[aria-label*='Filter' i]",
        "[role='searchbox']",
        "input[type='search']",
        "input[type='text']",
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
        # Funnel ikonuna tıkla
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
        raise RuntimeError("Policy Center filtre kutusu bulunamadı")

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
        # Enter ile dene
        box.press("Enter")
    time.sleep(2.5)


def _download_csv(page) -> bytes | None:
    """'CSV dosyasını indir' / Download / Export ile CSV al."""
    selectors = [
        "button:has-text('CSV dosyasını indir')",
        "button:has-text('Download CSV')",
        "button:has-text('CSV indir')",
        "button:has-text('Export')",
        "button:has-text('Dışa aktar')",
        "a:has-text('CSV dosyasını indir')",
        "a:has-text('Download CSV')",
        "[aria-label*='CSV' i]",
        "[aria-label*='Download' i]",
        "[aria-label*='Export' i]",
        "[aria-label*='İndir' i]",
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
        return None
    try:
        with page.expect_download(timeout=90_000) as dl_info:
            btn.click(timeout=8000)
            time.sleep(0.5)
            menu = page.locator(
                "[role='menuitem']:has-text('CSV'), button:has-text('CSV'), "
                "text=CSV dosyasını indir, text=Download CSV, text=CSV"
            ).first
            try:
                if menu.count() and menu.is_visible(timeout=2000):
                    menu.click(timeout=4000)
            except Exception:
                pass
        download = dl_info.value
        path = download.path()
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


def _dom_table_stats(page) -> dict[str, int]:
    try:
        return page.evaluate(
            """() => {
              const tables = Array.from(document.querySelectorAll('table'));
              let maxRows = 0;
              for (const t of tables) {
                maxRows = Math.max(maxRows, t.querySelectorAll('tbody tr').length);
              }
              return {
                tables: tables.length,
                max_rows: maxRows,
                all_links: document.querySelectorAll('a[href*=\"sinemalar\"]').length,
              };
            }"""
        )
    except Exception:
        return {"tables": 0, "max_rows": 0, "all_links": 0}


def _scrape_dom_rows(page) -> list[dict[str, Any]]:
    """Tablo satırlarını DOM'dan oku (CSV yoksa)."""
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
      const tables = Array.from(document.querySelectorAll('table'));
      let best = null, bestN = 0;
      for (const t of tables) {
        const rows = t.querySelectorAll('tbody tr');
        if (rows.length > bestN) { best = t; bestN = rows.length; }
      }
      // Grid / role=row fallback
      if (!best || bestN < 1) {
        const gridRows = Array.from(document.querySelectorAll('[role=\"row\"]'))
          .filter(r => r.querySelectorAll('[role=\"gridcell\"], [role=\"cell\"], td').length >= 2);
        if (gridRows.length > bestN) {
          return gridRows.map(tr => {
            const cells = Array.from(tr.querySelectorAll('[role=\"gridcell\"], [role=\"cell\"], td'));
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
      for (const tr of best.querySelectorAll('tbody tr')) {
        const cells = Array.from(tr.querySelectorAll('td'));
        if (!cells.length) continue;
        const vals = cells.map(td => (td.innerText || '').trim().replace(/\\s+/g, ' '));
        const links = Array.from(tr.querySelectorAll('a[href]'))
          .map(a => a.href)
          .filter(h => /sinemalar\\.com/i.test(h));
        // href yoksa hücre metninde sinemalar ara
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
        const row = { cells: vals, headers, links };
        out.push(row);
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
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url.lstrip("/")
    if not _host_allowed(url):
        return None
    issue = str(r.get("issue_type") or r.get("sorunlar") or "").strip()
    if not issue:
        return None
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


def scrape_admanager_policy(*, headed: bool = True) -> dict[str, Any]:
    pw, context = _launch_context(headed=headed)
    scraped_at = datetime.now(timezone.utc).isoformat()
    try:
        page = context.pages[0] if context.pages else context.new_page()
        print(f"  · Policy Center açılıyor… {NETWORK_ID}", flush=True)
        page.goto(_policy_url(), wait_until="domcontentloaded", timeout=120_000)
        time.sleep(3)
        if not _looks_signed_in(page):
            return {
                "ok": False,
                "needs_login": True,
                "message": "Ad Manager oturumu yok — --login ile cemevecen@nokta.com gir",
                "scraped_at": scraped_at,
                "rows": [],
            }

        # SPA hash route bazen yeniden yüklenmeli
        if "policy_center" not in (page.url or ""):
            page.goto(_policy_url(), wait_until="domcontentloaded", timeout=120_000)
            time.sleep(2)

        print(f"  · Site filtresi: {SITE_FILTER}", flush=True)
        try:
            _apply_site_filter(page, SITE_FILTER)
        except Exception as exc:
            print(f"  · Filtre uyarısı: {exc}", flush=True)

        # Tablo/filtre yerleşsin
        time.sleep(2.5)
        try:
            page.wait_for_selector("table tbody tr, [role='row']", timeout=25_000)
        except Exception:
            pass
        stats = _dom_table_stats(page)
        print(
            f"  · DOM öncesi · tables={stats.get('tables')} max_rows={stats.get('max_rows')} "
            f"sinemalar_links={stats.get('all_links')}",
            flush=True,
        )

        csv_bytes = _download_csv(page)
        rows: list[dict[str, Any]] = []
        csv_b64 = None
        method = "dom"

        if csv_bytes:
            method = "csv"
            # Parse later on server — also parse locally for count
            from backend.services import policy_csv as pcsv

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
            import base64

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

        # Dedup
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
        return {
            "ok": True,
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
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"Tarama hatası: {exc}",
            "scraped_at": scraped_at,
            "rows": [],
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


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
