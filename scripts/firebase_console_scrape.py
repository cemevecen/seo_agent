#!/usr/bin/env python3
"""Firebase Console Crashlytics scrape — ASC/Play bridge modeli.

Mac'te Google oturumu (persistent Chrome profile) ile Firebase Console
(overview + Crashlytics issues + Release Monitoring) okunur → Railway ingest.

  .venv/bin/python scripts/firebase_console_scrape.py --login
  .venv/bin/python scripts/firebase_console_scrape.py --sync --ingest

Hedefler:
  Android: https://console.firebase.google.com/project/doviz-android/...
  iOS:     https://console.firebase.google.com/project/doviz-ios/...

Env:
  FIREBASE_CONSOLE_PROFILE_DIR  default ~/.seo-agent/firebase-console-profile
  FIREBASE_CONSOLE_INGEST_URL   default …/api/firebase-console/ingest
  NOTIFICATION_INGEST_TOKEN
  FIREBASE_CONSOLE_SCRAPE_DAYS  default 365 (1–365)
  FIREBASE_CONSOLE_HEADLESS=1
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


_load_dotenv()

PROFILE_DIR = Path(
    os.environ.get("FIREBASE_CONSOLE_PROFILE_DIR")
    or (Path.home() / ".seo-agent" / "firebase-console-profile")
).expanduser()
INGEST_URL = (
    os.environ.get("FIREBASE_CONSOLE_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/firebase-console/ingest"
).strip()

PLATFORMS: dict[str, dict[str, str]] = {
    "android": {
        "project": "doviz-android",
        "app": "android:com.Doviz",
        "package": "com.Doviz",
        "latest_version": "9.5.10 (290)",
    },
    "ios": {
        "project": "doviz-ios",
        "app": "ios:com.nokta.Finans-Takip",
        "package": "com.nokta.Finans.Takip",
        "latest_version": "9.0.2 (316)",
    },
}


def _scrape_days() -> int:
    raw = (os.environ.get("FIREBASE_CONSOLE_SCRAPE_DAYS") or "365").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 365
    return min(max(n, 1), 365)


def _time_param(days: int) -> str:
    if days <= 1:
        return "24h"
    if days <= 7:
        return "7d"
    if days <= 30:
        return "30d"
    if days <= 90:
        return "90d"
    return "90d"  # Console UI üst sınır; 365g panel filtre + BQ yedek


def _urls(plat: str, days: int) -> dict[str, str]:
    meta = PLATFORMS[plat]
    project = meta["project"]
    app = quote(meta["app"], safe=":")
    ver = quote(meta["latest_version"], safe="")
    t = _time_param(days)
    base = f"https://console.firebase.google.com/u/0/project/{project}"
    return {
        "overview": f"{base}/overview",
        "crashlytics": (
            f"{base}/crashlytics/app/{app}/issues"
            f"?state=open&time={t}&types=crash&tag=all&sort=eventCount"
        ),
        "release": (
            f"{base}/releasemonitoring/app/{app}/latest"
            f"?time={t}&version={ver}"
        ),
    }


def _page_needs_login(page) -> bool:
    """True only on Google Accounts / explicit login form — not Firebase dashboard HTML."""
    try:
        url = (page.url or "").lower()
    except Exception:
        url = ""
    if "accounts.google.com" in url:
        return True
    # Firebase overview/crashlytics already loaded → session OK
    if "console.firebase.google.com" in url and "/project/" in url:
        return False
    try:
        title = (page.title() or "").lower()
    except Exception:
        title = ""
    if "sign in" in title or "oturum aç" in title:
        return True
    try:
        # Prefer visible text; full HTML has many false "Sign in" strings in scripts.
        body = (page.inner_text("body") or "")[:1200].lower()
    except Exception:
        body = ""
    if "email or phone" in body or "e-posta veya telefon" in body:
        return True
    if "use your google account" in body or "google hesabınızı kullanın" in body:
        return True
    return False


def _wait_until_firebase(page, *, timeout_sec: int = 600) -> bool:
    """Oturum yoksa kullanıcı girene kadar poll et; Enter gerekmez."""
    deadline = time.time() + max(60, timeout_sec)
    printed = False
    while time.time() < deadline:
        try:
            url = (page.url or "").lower()
        except Exception:
            url = ""
        if not _page_needs_login(page) and "console.firebase.google.com" in url and "/project/" in url:
            page.wait_for_timeout(1500)
            return True
        if not printed:
            print(
                "Firebase oturumu yok — açılan Chromium’da Google ile giriş yapın. "
                f"Overview gelince otomatik devam ({timeout_sec}s). Enter’a basmaya gerek yok.",
                flush=True,
            )
            printed = True
        else:
            print(f"  … bekleniyor ({url[:80] or '—'})", flush=True)
        page.wait_for_timeout(2500)
    return False


def _extract_pcts(text: str) -> list[float]:
    out: list[float] = []
    for m in re.finditer(r"(\d{1,3}(?:[.,]\d{1,4})?)\s*%", text or ""):
        raw = m.group(1).replace(",", ".")
        try:
            v = float(raw)
        except ValueError:
            continue
        if 0 <= v <= 100:
            out.append(v)
    return out


def _fmt_pct(v: float | None) -> str | None:
    if v is None:
        return None
    if v >= 99.995:
        return f"{v:.4f}".replace(".", ",") + "%"
    return f"{v:.2f}".replace(".", ",") + "%"


def _parse_dom_metrics(page, *, plat: str, page_kind: str) -> dict[str, Any]:
    """DOM metininden crash-free / event sayıları çıkar (best-effort)."""
    try:
        text = page.inner_text("body") or ""
    except Exception:
        text = ""
    pcts = _extract_pcts(text)
    # Yüksek oranlar genelde crash-free
    crash_free = None
    for p in sorted(pcts, reverse=True):
        if p >= 90:
            crash_free = p
            break
    if crash_free is None and pcts:
        crash_free = max(pcts)

    issues: list[dict[str, Any]] = []
    # Satır benzeri: başlık + event sayısı
    for line in text.splitlines():
        s = line.strip()
        if len(s) < 8 or len(s) > 180:
            continue
        m = re.search(r"(\d[\d.\s]*)\s*(events?|olay|users?|kullanıcı)", s, re.I)
        if not m:
            continue
        title = s[:120]
        if any(k in title.lower() for k in ("crash-free", "anr-free", "overview", "firebase")):
            continue
        try:
            ev = int(re.sub(r"[^\d]", "", m.group(1)) or "0")
        except ValueError:
            ev = 0
        if ev <= 0:
            continue
        issues.append(
            {
                "title": title,
                "event_count": ev,
                "platform": plat,
                "page": page_kind,
            }
        )
        if len(issues) >= 40:
            break

    versions: list[dict[str, Any]] = []
    for m in re.finditer(r"\bv?(\d+\.\d+(?:\.\d+)?(?:\.\d+)?)\s*(?:\((\d+)\))?", text):
        ver = m.group(1)
        code = m.group(2)
        label = f"{ver} ({code})" if code else ver
        if not any(v.get("version") == ver for v in versions):
            versions.append({"version": ver, "label": label, "platform": plat})
        if len(versions) >= 12:
            break

    devices: list[dict[str, Any]] = []
    for brand in ("Samsung", "Xiaomi", "Huawei", "Oppo", "Pixel", "iPhone", "iPad"):
        if brand.lower() in text.lower():
            devices.append({"device": brand, "label": brand, "platform": plat})

    return {
        "crash_free_pct": crash_free,
        "crash_free_fmt": _fmt_pct(crash_free),
        "issues": issues,
        "by_version": versions,
        "by_device": devices,
        "text_len": len(text),
    }


def _capture_network(page) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []

    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            if "firebase" not in url and "crashlytics" not in url.lower():
                return
            ct = (resp.headers or {}).get("content-type", "")
            if "json" not in ct and "javascript" not in ct:
                return
            if resp.status != 200:
                return
            body = resp.text()
            if not body or len(body) > 2_000_000:
                return
            captured.append(
                {
                    "url": url[:400],
                    "status": resp.status,
                    "len": len(body),
                    "snippet": body[:400],
                }
            )
            if len(captured) > 80:
                captured.pop(0)
        except Exception:
            return

    page.on("response", on_response)
    return captured


def _scrape_platform(page, plat: str, days: int) -> dict[str, Any]:
    urls = _urls(plat, days)
    meta = PLATFORMS[plat]
    captured = _capture_network(page)
    blocks: dict[str, Any] = {}
    for kind, url in urls.items():
        print(f"  · {plat}/{kind} …", flush=True)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            page.wait_for_timeout(4500)
            if _page_needs_login(page):
                return {
                    "ok": False,
                    "error": "login_required",
                    "project": meta["project"],
                    "package": meta["package"],
                }
            # soft wait for SPA
            page.wait_for_timeout(2500)
            blocks[kind] = _parse_dom_metrics(page, plat=plat, page_kind=kind)
            blocks[kind]["url"] = url
        except Exception as exc:  # noqa: BLE001
            blocks[kind] = {"ok": False, "error": str(exc)[:160], "url": url}

    overview = blocks.get("overview") or {}
    crash = blocks.get("crashlytics") or {}
    release = blocks.get("release") or {}

    crash_free = (
        release.get("crash_free_pct")
        or crash.get("crash_free_pct")
        or overview.get("crash_free_pct")
    )
    issues = (crash.get("issues") or [])[:40]
    by_version = release.get("by_version") or crash.get("by_version") or overview.get("by_version") or []
    by_device = crash.get("by_device") or overview.get("by_device") or []

    # series placeholder — günlük noktalar scrape derinleşince doldurulur
    series: list[dict[str, Any]] = []
    if crash_free is not None:
        series.append(
            {
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "crash_free_pct": crash_free,
                "crash_free_fmt": _fmt_pct(crash_free),
                "platform": plat,
            }
        )

    return {
        "ok": True,
        "project": meta["project"],
        "package": meta["package"],
        "app": meta["app"],
        "latest_version": meta["latest_version"],
        "crash_free_pct": crash_free,
        "crash_free_fmt": _fmt_pct(crash_free) if crash_free is not None else None,
        "issues": issues,
        "by_version": by_version[:20],
        "by_device": by_device[:20],
        "series": series,
        "release_monitoring": {
            "version": meta["latest_version"],
            "crash_free_pct": release.get("crash_free_pct") or crash_free,
            "crash_free_fmt": _fmt_pct(release.get("crash_free_pct") or crash_free),
            "url": (release.get("url") or urls["release"]),
            "source_page": "releasemonitoring",
        },
        "pages": {
            "overview": overview.get("url") or urls["overview"],
            "crashlytics": crash.get("url") or urls["crashlytics"],
            "release": release.get("url") or urls["release"],
        },
        "network_hits": len(captured),
        "raw_hints": captured[-12:],
    }


def scrape_firebase_console(*, headed: bool | None = None) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    days = _scrape_days()
    if headed is None:
        headed = (os.environ.get("FIREBASE_CONSOLE_HEADLESS") or "").strip() != "1"

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    platforms_out: dict[str, Any] = {}
    metrics: list[dict[str, Any]] = []
    raw_network: list[dict[str, Any]] = []
    errors: list[str] = []

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=not headed,
            viewport={"width": 1440, "height": 960},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        try:
            # login probe — dashboard açıksa Enter beklemeden devam
            probe = _urls("android", days)["overview"]
            page.goto(probe, wait_until="domcontentloaded", timeout=90_000)
            page.wait_for_timeout(2000)
            if _page_needs_login(page) or "console.firebase.google.com" not in (page.url or "").lower():
                if not _wait_until_firebase(page, timeout_sec=600):
                    context.close()
                    return {
                        "sync_ok": False,
                        "sync_message": "Firebase Console login zaman aşımı (--login)",
                        "metrics": [],
                        "panels": {},
                        "scrape_days": days,
                    }
                try:
                    page.goto(probe, wait_until="domcontentloaded", timeout=90_000)
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
                if _page_needs_login(page):
                    context.close()
                    return {
                        "sync_ok": False,
                        "sync_message": "Firebase Console login gerekli (--login)",
                        "metrics": [],
                        "panels": {},
                        "scrape_days": days,
                    }

            for plat in ("android", "ios"):
                try:
                    block = _scrape_platform(page, plat, days)
                    platforms_out[plat] = block
                    if block.get("raw_hints"):
                        raw_network.extend(block.get("raw_hints") or [])
                    if block.get("crash_free_fmt"):
                        metrics.append(
                            {
                                "metric": f"{plat}_crash_free",
                                "platform": plat,
                                "value": block.get("crash_free_pct"),
                                "fmt": block.get("crash_free_fmt"),
                                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            }
                        )
                    if not block.get("ok"):
                        errors.append(f"{plat}:{block.get('error')}")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{plat}:{exc}")
                    platforms_out[plat] = {"ok": False, "error": str(exc)[:160]}
        finally:
            context.close()

    ok = any(isinstance(v, dict) and v.get("ok") for v in platforms_out.values())
    msg = "Firebase Console scrape OK" if ok else (" · ".join(errors) or "scrape başarısız")
    return {
        "sync_ok": ok,
        "sync_message": msg[:500],
        "sync_mode": "crashlytics_scrape",
        "source": "firebase_console_bridge",
        "source_url": "https://console.firebase.google.com/",
        "scrape_days": days,
        "metrics": metrics,
        "panels": {
            "platforms": platforms_out,
            "explorer_facts": metrics,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        },
        "raw_network": raw_network[:80],
    }


def ingest_scrape_result(payload: dict[str, Any]) -> dict[str, Any]:
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("NOTIFICATION_INGEST_TOKEN yok")
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Notification-Ingest-Token": token,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body) if body else {"ok": True}


def main() -> int:
    ap = argparse.ArgumentParser(description="Firebase Console Crashlytics scrape")
    ap.add_argument("--login", action="store_true", help="Google oturumu için headed tarayıcı")
    ap.add_argument("--sync", action="store_true", help="Scrape çalıştır")
    ap.add_argument("--ingest", action="store_true", help="Railway'e gönder")
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    if args.login:
        print(f"Profil: {PROFILE_DIR}", flush=True)
        result = scrape_firebase_console(headed=True)
        print(json.dumps({k: result.get(k) for k in ("sync_ok", "sync_message")}, ensure_ascii=False))
        return 0 if result.get("sync_ok") else 2

    if not args.sync:
        ap.print_help()
        return 1

    headed = not args.headless and (os.environ.get("FIREBASE_CONSOLE_HEADLESS") or "").strip() != "1"
    print(f"Firebase scrape days={_scrape_days()} headed={headed}", flush=True)
    result = scrape_firebase_console(headed=headed)
    print(
        f"sync_ok={result.get('sync_ok')} msg={result.get('sync_message')} "
        f"metrics={len(result.get('metrics') or [])}",
        flush=True,
    )
    if args.ingest:
        try:
            ing = ingest_scrape_result(result)
            print("ingest:", ing, flush=True)
        except Exception as exc:  # noqa: BLE001
            print("ingest FAILED:", exc, file=sys.stderr)
            return 3
    return 0 if result.get("sync_ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
