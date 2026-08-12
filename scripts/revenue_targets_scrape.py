#!/usr/bin/env python3
"""Ad-virgul aylık hedef sheet — sistem Firefox oturumu (Nightly yok).

Akış (manuel yok):
  1) Günlük Firefox.app profilinden Google çerezlerini oku
  2) Sheet CSV export'u HTTP ile çek
  3) İsteğe bağlı Railway ingest

Selenium yalnızca yedek (headed); Playwright Nightly hiç kullanılmaz.

  .venv/bin/python scripts/revenue_targets_scrape.py --sync
  .venv/bin/python scripts/revenue_targets_scrape.py --sync --ingest
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from http.cookiejar import Cookie, CookieJar
from pathlib import Path
from urllib.request import HTTPCookieProcessor, Request, build_opener

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.system_firefox_driver import (  # noqa: E402
    _read_google_cookies,
    ban_playwright_nightly_processes,
    default_firefox_profile_dir,
)
from backend.services.revenue_targets_sheet import (  # noqa: E402
    REVENUE_TARGETS_SHEET_URL,
    parse_revenue_targets_csv,
    save_ingested_revenue_targets,
)

SHEET_ID = "1ITl0rUlLylTspsztMtaaFGEdvT_gINoUHDPodspEa5Y"
GID = "244461752"
EXPORT_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
OUT_CSV = Path.home() / ".seo-agent" / "cache" / "revenue-targets.csv"
_SESSION_NAMES = {
    "SID",
    "HSID",
    "SSID",
    "APISID",
    "SAPISID",
    "__Secure-1PSID",
    "__Secure-3PSID",
    "LSID",
}


def _profiles_root() -> Path:
    return Path.home() / "Library/Application Support/Firefox/Profiles"


def best_firefox_google_profile() -> Path | None:
    """En zengin Google oturumlu sistem Firefox profili (Nightly cache değil)."""
    root = _profiles_root()
    if not root.is_dir():
        return default_firefox_profile_dir()
    best: Path | None = None
    best_score = -1
    for p in root.iterdir():
        if not p.is_dir():
            continue
        # Playwright / ms-playwright profili değil
        cookies = _read_google_cookies(p)
        names = {c["name"] for c in cookies}
        score = len(names & _SESSION_NAMES) * 100 + len(cookies)
        if score > best_score:
            best_score = score
            best = p
    if best_score < 100:
        # Session cookie yok
        return default_firefox_profile_dir()
    return best


def _cookie_jar_from_profile(profile: Path) -> CookieJar:
    jar = CookieJar()
    now = int(time.time())
    for c in _read_google_cookies(profile):
        if c.get("expiry") and int(c["expiry"]) < now:
            continue
        domain = c["domain"]
        try:
            jar.set_cookie(
                Cookie(
                    version=0,
                    name=c["name"],
                    value=c["value"],
                    port=None,
                    port_specified=False,
                    domain=domain,
                    domain_specified=True,
                    domain_initial_dot=domain.startswith("."),
                    path=c.get("path") or "/",
                    path_specified=True,
                    secure=bool(c.get("secure")),
                    expires=int(c["expiry"]) if c.get("expiry") else None,
                    discard=False,
                    comment=None,
                    comment_url=None,
                    rest={"HttpOnly": ""} if c.get("httpOnly") else {},
                    rfc2109=False,
                )
            )
        except Exception:
            continue
    return jar


def export_csv_via_firefox_cookies(profile: Path) -> str | None:
    jar = _cookie_jar_from_profile(profile)
    opener = build_opener(HTTPCookieProcessor(jar))
    req = Request(
        EXPORT_URL,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) "
                "Gecko/20100101 Firefox/147.0"
            ),
            "Accept": "text/csv,text/plain,*/*",
        },
    )
    try:
        with opener.open(req, timeout=90) as resp:
            data = resp.read()
            ctype = (resp.headers.get("Content-Type") or "").lower()
            print(f"export HTTP {getattr(resp, 'status', 200)} ctype={ctype} bytes={len(data)}", flush=True)
    except urllib.error.HTTPError as exc:
        print(f"export HTTP {exc.code}", flush=True)
        data = exc.read() if exc.fp else b""
    except Exception as exc:
        print(f"export fail: {exc}", flush=True)
        return None

    text = data.decode("utf-8", errors="replace")
    if not text.strip() or "<html" in text[:500].lower() or "accounts.google" in text[:2000].lower():
        return None
    return text


def _save_csv(text: str) -> Path:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUT_CSV.write_text(text, encoding="utf-8")
    return OUT_CSV


def _ingest_url() -> str:
    return (
        (os.environ.get("REVENUE_TARGETS_INGEST_URL") or "").strip()
        or "https://projectcontrol.up.railway.app/api/virgul-analytics/revenue-targets/ingest"
    )


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def post_ingest(csv_text: str) -> dict:
    token = _ingest_token()
    # Her zaman lokal disk cache (Railway erişilemese bile)
    try:
        local = save_ingested_revenue_targets(csv_text, source="mac_firefox_cookies")
    except Exception as exc:
        local = {"ok": False, "message": str(exc)[:200]}

    if not token:
        return {
            "ok": bool(local.get("ok")),
            "local": local,
            "message": "NOTIFICATION_INGEST_TOKEN yok — yalnız lokal cache",
        }

    payload = json.dumps({"csv": csv_text, "source": "mac_firefox_cookies"}).encode("utf-8")
    last_err = ""
    for attempt in range(3):
        req = Request(
            _ingest_url(),
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-Notification-Ingest-Token": token,
                "Content-Length": str(len(payload)),
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                return {
                    "ok": True,
                    "status": resp.status,
                    "body": body[:500],
                    "local": local,
                }
        except urllib.error.HTTPError as exc:
            last_err = exc.read()[:300].decode("utf-8", errors="replace")
            return {"ok": False, "status": exc.code, "message": last_err, "local": local}
        except Exception as exc:
            last_err = str(exc)[:240]
            time.sleep(1.5 * (attempt + 1))
    return {"ok": False, "message": last_err, "local": local}


def run_sync(*, ingest: bool = False, headed: bool = False) -> dict:
    """Sistem Firefox oturumu ile CSV — Nightly yok, manuel yok."""
    _ = headed  # Selenium yedek kaldırıldı; API uyumu
    ban_playwright_nightly_processes()
    profile = best_firefox_google_profile()
    if not profile:
        return {"ok": False, "message": "Sistem Firefox profili bulunamadı"}

    cookies = _read_google_cookies(profile)
    sess = {c["name"] for c in cookies} & _SESSION_NAMES
    print(
        f"Sistem Firefox profili · {profile.name} · google_cookies={len(cookies)} · session={sorted(sess)}",
        flush=True,
    )
    if not sess:
        return {
            "ok": False,
            "message": (
                "Firefox.app'de Google oturumu yok. Bir kez normal Firefox'ta "
                "docs.google.com'a cemevecen@nokta.com ile gir — sonra otomatik devam eder."
            ),
            "profile": str(profile),
        }

    text = export_csv_via_firefox_cookies(profile)
    if not text:
        return {
            "ok": False,
            "message": "CSV export 401/HTML — sheet erişimi yok veya oturum eksik",
            "profile": str(profile),
            "sheet": REVENUE_TARGETS_SHEET_URL,
        }

    _save_csv(text)
    rows = parse_revenue_targets_csv(text)
    print(f"OK · lines={len(text.splitlines())} parsed={len(rows)} → {OUT_CSV}", flush=True)
    for i, line in enumerate(text.splitlines(), 1):
        if i <= 4 or 22 <= i <= 30:
            print(f"{i:03d}|{line[:200]}", flush=True)

    out: dict = {
        "ok": True,
        "csv": str(OUT_CSV),
        "parsed": len(rows),
        "browser": "system_firefox_cookies",
        "profile": str(profile),
    }
    if ingest:
        ing = post_ingest(text)
        out["ingest"] = ing
        print(f"ingest={ing}", flush=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Revenue targets — sistem Firefox cookies (no Nightly)"
    )
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headless", action="store_true", help="(uyumluluk, yok sayılır)")
    parser.add_argument("--login", action="store_true", help="(kaldırıldı) --sync ile aynı")
    args = parser.parse_args()
    if not (args.sync or args.login or args.ingest):
        args.sync = True

    # Load .env token if present
    env_path = ROOT / ".env"
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, _, v = s.partition("=")
            k, v = k.strip(), v.strip().strip("'").strip('"')
            if k and k not in os.environ:
                os.environ[k] = v

    result = run_sync(ingest=bool(args.ingest or args.sync), headed=not args.headless)
    print(result, flush=True)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
