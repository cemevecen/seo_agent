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


def _rpc_name(url: str) -> str:
    return (url or "").rstrip("/").rsplit("/", 1)[-1]


def _safe_json(body: str) -> Any:
    try:
        return json.loads(body)
    except Exception:
        return None


def _ratio_to_pct(ratio: float | None) -> float | None:
    if ratio is None:
        return None
    try:
        r = float(ratio)
    except (TypeError, ValueError):
        return None
    if 0 <= r <= 1.0001:
        return min(r, 1.0) * 100.0
    if 1 < r <= 100:
        return r
    return None


def _parse_cf_timeline(body: str) -> tuple[float | None, list[dict[str, Any]]]:
    """ReleaseMon GetCrashFreeUsers/SessionsTimeline → overall % + günlük seri."""
    data = _safe_json(body)
    if not isinstance(data, list) or not data:
        return None, []
    buckets = data[0] if isinstance(data[0], list) else data
    if not isinstance(buckets, list):
        return None, []
    by_day: dict[str, list[tuple[float, float]]] = {}
    w_sum = 0.0
    u_sum = 0.0
    for b in buckets:
        if not isinstance(b, list) or len(b) < 3:
            continue
        try:
            users = float(str(b[1]).replace(",", "").replace(" ", "") or "0")
        except ValueError:
            users = 0.0
        ratio = None
        for x in b[2:]:
            if isinstance(x, bool) or x is None:
                continue
            if isinstance(x, (int, float)):
                if 0 < float(x) <= 1.0001:
                    ratio = float(x)
                    break
                if float(x) == 0:
                    continue
        if ratio is None:
            continue
        pct = _ratio_to_pct(ratio)
        if pct is None:
            continue
        day = None
        try:
            ts = int(b[0][0][0])
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            day = None
        if day:
            by_day.setdefault(day, []).append((users, pct))
        w_sum += users * (ratio if ratio <= 1 else ratio / 100.0)
        u_sum += users
    overall = (w_sum / u_sum * 100.0) if u_sum > 0 else None
    series: list[dict[str, Any]] = []
    for day in sorted(by_day):
        parts = by_day[day]
        tw = sum(u for u, _ in parts)
        if tw > 0:
            avg = sum(u * p for u, p in parts) / tw
        else:
            avg = sum(p for _, p in parts) / len(parts)
        series.append(
            {
                "date": day,
                "crash_free_pct": round(avg, 6),
                "crash_free_fmt": _fmt_pct(avg),
                "users": int(tw) if tw else None,
            }
        )
    return overall, series


def _parse_list_app_versions(body: str, *, plat: str) -> list[dict[str, Any]]:
    data = _safe_json(body)
    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
        return []
    out: list[dict[str, Any]] = []
    for row in data[1]:
        if not isinstance(row, list) or not row:
            continue
        ver_pair = row[0]
        if not isinstance(ver_pair, list) or not ver_pair:
            continue
        ver = str(ver_pair[0] or "").strip()
        code = str(ver_pair[1] or "").strip() if len(ver_pair) > 1 else ""
        if not ver or not re.match(r"^\d+\.\d+", ver):
            continue
        label = f"{ver} ({code})" if code else ver
        out.append({"version": ver, "build": code or None, "label": label, "platform": plat})
        if len(out) >= 40:
            break
    return out


def _parse_list_top_issues(body: str, *, plat: str) -> list[dict[str, Any]]:
    data = _safe_json(body)
    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
        return []
    out: list[dict[str, Any]] = []
    for row in data[1]:
        if not isinstance(row, list) or len(row) < 2:
            continue
        issue_id = str(row[0] or "")
        meta = row[1] if isinstance(row[1], list) else []
        title = str(meta[0] or "")[:200] if meta else ""
        detail = str(meta[1] or "")[:300] if len(meta) > 1 else ""
        events = 0
        if len(meta) > 3 and meta[3] is not None:
            try:
                events = int(re.sub(r"[^\d]", "", str(meta[3])) or "0")
            except ValueError:
                events = 0
        # timeline event sum fallback
        if events <= 0 and len(row) > 4 and isinstance(row[4], list):
            total = 0
            for b in row[4]:
                if isinstance(b, list) and len(b) >= 2:
                    try:
                        total += int(re.sub(r"[^\d]", "", str(b[1])) or "0")
                    except ValueError:
                        pass
            events = total
        if not title:
            continue
        out.append(
            {
                "id": issue_id,
                "title": title,
                "detail": detail,
                "event_count": events,
                "exception": str(meta[8] or meta[1] or "")[:120] if len(meta) > 1 else "",
                "platform": plat,
                "page": "releasemonitoring",
            }
        )
        if len(out) >= 50:
            break
    return out


def _parse_releasemon_network(captured: list[dict[str, Any]], *, plat: str) -> dict[str, Any]:
    """Release Monitoring RPC gövdelerinden yapılandırılmış metrikler."""
    by_version: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    users_cf: float | None = None
    sessions_cf: float | None = None
    series: list[dict[str, Any]] = []
    sessions_series: list[dict[str, Any]] = []
    for hit in captured:
        name = _rpc_name(hit.get("url") or "")
        body = hit.get("body") or ""
        if not body:
            continue
        if name == "ListAppVersions" and not by_version:
            by_version = _parse_list_app_versions(body, plat=plat)
        elif name == "ListTopIssues":
            parsed = _parse_list_top_issues(body, plat=plat)
            if len(parsed) > len(issues):
                issues = parsed
        elif name.startswith("GetCrashFreeUsers"):
            overall, ser = _parse_cf_timeline(body)
            if overall is not None:
                users_cf = overall
            if ser:
                series = ser
        elif name.startswith("GetCrashFreeSessions"):
            overall, ser = _parse_cf_timeline(body)
            if overall is not None:
                sessions_cf = overall
            if ser:
                sessions_series = ser
    if series:
        for s in series:
            s["platform"] = plat
            s["metric"] = "crash_free_users"
    if sessions_series:
        for s in sessions_series:
            s["platform"] = plat
            s["metric"] = "crash_free_sessions"
    return {
        "by_version": by_version,
        "issues": issues,
        "crash_free_users_pct": users_cf,
        "crash_free_sessions_pct": sessions_cf,
        "series": series,
        "sessions_series": sessions_series,
    }


def _parse_dom_metrics(page, *, plat: str, page_kind: str) -> dict[str, Any]:
    """DOM yedek — yalnızca network parse boşsa kullanılır."""
    try:
        text = page.inner_text("body") or ""
    except Exception:
        text = ""
    crash_free = None
    # "Crash-free users" / "Çökme yaşamayan" yakınındaki %
    for pat in (
        r"crash-free users[^0-9%]{0,40}(\d{1,3}(?:[.,]\d{1,4})?)\s*%",
        r"crash-free sessions[^0-9%]{0,40}(\d{1,3}(?:[.,]\d{1,4})?)\s*%",
        r"çökme yaşamayan[^0-9%]{0,40}(\d{1,3}(?:[.,]\d{1,4})?)\s*%",
    ):
        m = re.search(pat, text, re.I)
        if m:
            try:
                crash_free = float(m.group(1).replace(",", "."))
                break
            except ValueError:
                pass
    return {
        "crash_free_pct": crash_free,
        "crash_free_fmt": _fmt_pct(crash_free),
        "issues": [],
        "by_version": [],
        "by_device": [],
        "text_len": len(text),
    }


_INTERESTING_RPC = (
    "ListAppVersions",
    "ListTopIssues",
    "GetCrashFreeUsers",
    "GetCrashFreeSessions",
    "GetAdoptionTimeline",
)


def _capture_network(page) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []

    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            low = url.lower()
            interesting = (
                "firebasereleasemon" in low
                or "crashlytics" in low
                or any(k.lower() in url for k in _INTERESTING_RPC)
            )
            if not interesting and "firebase" not in low:
                return
            if resp.status != 200:
                return
            ct = (resp.headers or {}).get("content-type", "").lower()
            name = _rpc_name(url)
            keep_body = any(name.startswith(k) for k in _INTERESTING_RPC)
            if not keep_body and "json" not in ct and "javascript" not in ct and "text/plain" not in ct:
                return
            body = resp.text()
            if not body or len(body) > 2_000_000:
                return
            entry: dict[str, Any] = {
                "url": url[:400],
                "status": resp.status,
                "len": len(body),
                "snippet": body[:240],
            }
            if keep_body:
                entry["body"] = body[:500_000]
            captured.append(entry)
            if len(captured) > 120:
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
    # Release monitoring first — en zengin RPC (CF + issues + versions)
    order = ("release", "crashlytics", "overview")
    for kind in order:
        url = urls[kind]
        print(f"  · {plat}/{kind} …", flush=True)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            page.wait_for_timeout(5500)
            if _page_needs_login(page):
                return {
                    "ok": False,
                    "error": "login_required",
                    "project": meta["project"],
                    "package": meta["package"],
                }
            page.wait_for_timeout(2500)
            blocks[kind] = _parse_dom_metrics(page, plat=plat, page_kind=kind)
            blocks[kind]["url"] = url
        except Exception as exc:  # noqa: BLE001
            blocks[kind] = {"ok": False, "error": str(exc)[:160], "url": url}

    net = _parse_releasemon_network(captured, plat=plat)
    overview = blocks.get("overview") or {}
    crash = blocks.get("crashlytics") or {}
    release = blocks.get("release") or {}

    crash_free = (
        net.get("crash_free_users_pct")
        or release.get("crash_free_pct")
        or crash.get("crash_free_pct")
        or overview.get("crash_free_pct")
    )
    sessions_cf = net.get("crash_free_sessions_pct")
    issues = (net.get("issues") or crash.get("issues") or [])[:50]
    by_version = net.get("by_version") or []
    by_device = crash.get("by_device") or overview.get("by_device") or []
    series = net.get("series") or []
    if not series and crash_free is not None:
        series = [
            {
                "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "crash_free_pct": crash_free,
                "crash_free_fmt": _fmt_pct(crash_free),
                "platform": plat,
                "metric": "crash_free_users",
            }
        ]

    # ingest için body'leri düşür (snippet kalsın)
    raw_hints = []
    for h in captured[-20:]:
        raw_hints.append({k: v for k, v in h.items() if k != "body"})

    return {
        "ok": True,
        "project": meta["project"],
        "package": meta["package"],
        "app": meta["app"],
        "latest_version": meta["latest_version"],
        "crash_free_pct": crash_free,
        "crash_free_fmt": _fmt_pct(crash_free) if crash_free is not None else None,
        "crash_free_sessions_pct": sessions_cf,
        "crash_free_sessions_fmt": _fmt_pct(sessions_cf) if sessions_cf is not None else None,
        "issues": issues,
        "by_version": by_version[:40],
        "by_device": by_device[:20],
        "series": series,
        "sessions_series": net.get("sessions_series") or [],
        "release_monitoring": {
            "version": meta["latest_version"],
            "crash_free_pct": crash_free,
            "crash_free_fmt": _fmt_pct(crash_free) if crash_free is not None else None,
            "crash_free_sessions_pct": sessions_cf,
            "crash_free_sessions_fmt": _fmt_pct(sessions_cf) if sessions_cf is not None else None,
            "url": (release.get("url") or urls["release"]),
            "source_page": "releasemonitoring",
            "source": "releasemon_rpc",
        },
        "pages": {
            "overview": overview.get("url") or urls["overview"],
            "crashlytics": crash.get("url") or urls["crashlytics"],
            "release": release.get("url") or urls["release"],
        },
        "network_hits": len(captured),
        "raw_hints": raw_hints,
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
