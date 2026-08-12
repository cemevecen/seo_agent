#!/usr/bin/env python3
"""Ad-virgul aylık hedef sheet scrape (Mac bridge / Firefox fx-google).

Kaynak:
  https://docs.google.com/spreadsheets/d/1ITl0rUlLylTspsztMtaaFGEdvT_gINoUHDPodspEa5Y/edit?gid=244461752

Örnek:
  .venv/bin/python scripts/revenue_targets_scrape.py --login
  .venv/bin/python scripts/revenue_targets_scrape.py --sync

Env:
  GOOGLE_PROFILE_DIR / PLAY_CONSOLE_PROFILE_DIR  (default ~/.seo-agent/fx-google)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.scrape_browser import (  # noqa: E402
    acquire_profile_login_lock,
    google_profile_dir,
    launch_persistent,
    release_profile_login_lock,
)
from backend.services.revenue_targets_sheet import (  # noqa: E402
    REVENUE_TARGETS_SHEET_URL,
    parse_revenue_targets_csv,
)

SHEET_ID = "1ITl0rUlLylTspsztMtaaFGEdvT_gINoUHDPodspEa5Y"
GID = "244461752"
EXPORT_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
OUT_CSV = Path.home() / ".seo-agent" / "cache" / "revenue-targets.csv"
PROFILE_DIR = google_profile_dir()


def _looks_on_sheet(page) -> bool:
    try:
        url = page.url or ""
    except Exception:
        return False
    if "accounts.google.com" in url or "signin" in url.lower():
        return False
    return "docs.google.com/spreadsheets" in url and SHEET_ID in url


def _launch(*, kill_existing: bool):
    from playwright.sync_api import sync_playwright

    pw = sync_playwright().start()
    context = launch_persistent(
        pw,
        PROFILE_DIR,
        headed=True,
        locale="en-US",
        viewport={"width": 1280, "height": 900},
        kill_existing=kill_existing,
    )
    return pw, context


def run_login(timeout_sec: int = 900) -> dict:
    """Firefox'u açık tut — giriş için kill yok, pencere timeout'a kadar kalır."""
    print(
        f"Firefox açılıyor (kill yok + login-lock) · profil={PROFILE_DIR}\n"
        f"cemevecen@nokta.com ile giriş yap — pencere ~{timeout_sec}s açık kalır.\n"
        f"Bridge/Play scrape bu sırada bu pencereyi öldüremez.",
        flush=True,
    )
    acquire_profile_login_lock(PROFILE_DIR, reason="revenue_targets_login")
    pw, context = _launch(kill_existing=False)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(REVENUE_TARGETS_SHEET_URL, wait_until="domcontentloaded", timeout=120_000)
        except Exception as exc:
            print(f"goto warn: {exc}", flush=True)
        print(f"url={page.url}", flush=True)

        deadline = time.time() + max(120, timeout_sec)
        while time.time() < deadline:
            try:
                if page.is_closed():
                    print("Pencere kapandı (dışarıdan kill?).", flush=True)
                    return {"ok": False, "message": "browser closed"}
            except Exception:
                pass
            if _looks_on_sheet(page):
                time.sleep(2)
                print(f"Login OK · {page.url}", flush=True)
                csv_text = _export_csv(page)
                if csv_text:
                    _save_csv(csv_text)
                    rows = parse_revenue_targets_csv(csv_text) if csv_text else []
                    print(f"CSV kaydedildi · satır≈{len(csv_text.splitlines())} → {OUT_CSV}", flush=True)
                    return {"ok": True, "url": page.url, "csv": str(OUT_CSV), "parsed": len(rows)}
                return {"ok": True, "url": page.url, "message": "login ok, export failed"}
            left = int(deadline - time.time())
            if left % 30 < 3:
                try:
                    u = page.url
                except Exception:
                    u = "?"
                print(f"… giriş bekleniyor ({left}s) · {u[:90]}", flush=True)
            time.sleep(2)

        return {"ok": False, "message": "login timeout", "url": getattr(page, "url", "")}
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass
        release_profile_login_lock(PROFILE_DIR)


def _export_csv(page) -> str | None:
    try:
        resp = page.request.get(EXPORT_URL)
        body = resp.body() or b""
        print(f"export HTTP {resp.status} · {len(body)} bytes", flush=True)
    except Exception as exc:
        print(f"export request fail: {exc}", flush=True)
        body = b""

    if not body or b"<html" in body[:400].lower():
        try:
            page.goto(EXPORT_URL, wait_until="domcontentloaded", timeout=120_000)
            time.sleep(1.5)
            body = (page.content() or "").encode("utf-8", errors="replace")
        except Exception as exc:
            print(f"export nav fail: {exc}", flush=True)
            return None

    text = body.decode("utf-8", errors="replace")
    if "<html" in text[:400].lower():
        print("EXPORT_HTML — oturum CSV vermedi", flush=True)
        return None
    return text


def _save_csv(text: str) -> Path:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUT_CSV.write_text(text, encoding="utf-8")
    return OUT_CSV


def run_sync() -> dict:
    """Mevcut fx-google oturumu ile CSV çek (kill ile temiz açılış)."""
    pw, context = _launch(kill_existing=True)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(REVENUE_TARGETS_SHEET_URL, wait_until="domcontentloaded", timeout=120_000)
        if not _looks_on_sheet(page):
            return {
                "ok": False,
                "message": "Giriş yok — önce: .venv/bin/python scripts/revenue_targets_scrape.py --login",
                "url": page.url,
            }
        text = _export_csv(page)
        if not text:
            return {"ok": False, "message": "export failed", "url": page.url}
        _save_csv(text)
        rows = parse_revenue_targets_csv(text)
        print(f"OK · {len(text.splitlines())} lines · parsed={len(rows)} → {OUT_CSV}", flush=True)
        for i, line in enumerate(text.splitlines(), 1):
            if i <= 4 or 22 <= i <= 30:
                print(f"{i:03d}|{line[:200]}", flush=True)
        return {"ok": True, "csv": str(OUT_CSV), "parsed": len(rows)}
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Revenue targets sheet (Firefox fx-google)")
    parser.add_argument("--login", action="store_true", help="Giriş penceresi (kill yok, uzun bekleme)")
    parser.add_argument("--sync", action="store_true", help="CSV çek → ~/.seo-agent/cache/")
    parser.add_argument("--timeout", type=int, default=900, help="Login bekleme (sn)")
    args = parser.parse_args()

    if args.login:
        result = run_login(timeout_sec=args.timeout)
    elif args.sync:
        result = run_sync()
    else:
        parser.print_help()
        return 2

    print(result, flush=True)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
