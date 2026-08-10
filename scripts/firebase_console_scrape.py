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
        "fpn": "408735554583",
    },
    "ios": {
        "project": "doviz-ios",
        "app": "ios:com.nokta.Finans-Takip",
        "package": "com.nokta.Finans.Takip",
        "latest_version": "9.0.2 (316)",
        "fpn": "741318187155",
    },
}


SCRAPE_WINDOWS: tuple[str, ...] = ("24h", "7d", "30d", "90d")


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
    return "90d"  # Console UI üst sınır; panel 180/365 filtre biriken seriden


def _window_from_days(days: int) -> str:
    return _time_param(days)


def _urls(
    plat: str,
    *,
    time_param: str = "90d",
    version: str | None = None,
    issue_types: str = "crash",
) -> dict[str, str]:
    meta = PLATFORMS[plat]
    project = meta["project"]
    app = quote(meta["app"], safe=":")
    ver = quote(version or meta["latest_version"], safe="")
    t = time_param or "90d"
    base = f"https://console.firebase.google.com/u/0/project/{project}"
    fpn = meta.get("fpn") or ""
    analytics = (
        f"{base}/analytics/app/{app}/overview/"
        f"reports~2Fdashboard%3Fr%3Dfirebase-overview"
    )
    if fpn:
        analytics = f"{analytics}&fpn%3D{fpn}"
    return {
        "overview": f"{base}/overview",
        "analytics": analytics,
        "crashlytics": (
            f"{base}/crashlytics/app/{app}/issues"
            f"?state=open&time={t}&types={issue_types}&tag=all&sort=eventCount"
        ),
        "release": (
            f"{base}/releasemonitoring/app/{app}/latest"
            f"?time={t}&version={ver}"
        ),
        # Tüm sürümler — daha uzun CF / adoption serisi
        "release_all": (
            f"{base}/releasemonitoring/app/{app}"
            f"?time={t}"
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


def _strip_xssi_json(body: str) -> Any:
    s = (body or "").lstrip()
    if s.startswith(")]}'"):
        s = s.split("\n", 1)[-1] if "\n" in s else s[4:]
    return _safe_json(s)


def _fmt_int(n: float | int | None) -> str | None:
    if n is None:
        return None
    try:
        v = float(n)
    except (TypeError, ValueError):
        return None
    if abs(v) >= 1000:
        return f"{v:,.0f}".replace(",", ".")
    if abs(v - round(v)) < 1e-6:
        return str(int(round(v)))
    return f"{v:.2f}".replace(".", ",")


def _metric_values(row: dict[str, Any]) -> list[float]:
    out: list[float] = []
    for x in row.get("metricCompoundValues") or row.get("metricValues") or []:
        if isinstance(x, dict) and "value" in x:
            try:
                out.append(float(x["value"]))
            except (TypeError, ValueError):
                continue
        elif isinstance(x, (int, float)):
            out.append(float(x))
    return out


def _latest_metric_map(card: dict[str, Any]) -> dict[str, float]:
    """dashboard_card Venus cevabından son satır metric_id → value."""
    if not isinstance(card, dict):
        return {}
    responses = ((card.get("default") or {}).get("responses") or [])
    if not responses:
        return {}
    resp0 = responses[0] if isinstance(responses[0], dict) else {}
    metric_ids = [m.get("id") for m in (resp0.get("metrics") or []) if isinstance(m, dict)]
    rows = resp0.get("responseRows") or []
    if not rows or not metric_ids:
        return {}
    # son dolu satırı al
    vals: list[float] = []
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        vals = _metric_values(row)
        if vals:
            break
    out: dict[str, float] = {}
    for i, mid in enumerate(metric_ids):
        if mid and i < len(vals):
            # aynı id birden fazla olabilir (card_5) — ilkini koru, sonrakini _2
            key = str(mid)
            if key in out:
                key = f"{key}_{i}"
            out[key] = vals[i]
    return out


def _parse_analytics_cards(captured: list[dict[str, Any]], *, plat: str) -> dict[str, Any]:
    cards: dict[str, Any] = {}
    for hit in captured:
        url = hit.get("url") or ""
        if "data/v2/venus" not in url or "dashboard_card_" not in url:
            continue
        m = re.search(r"reportId=(dashboard_card_\d+)", url)
        if not m:
            continue
        rid = m.group(1)
        body = hit.get("body") or ""
        if not body:
            continue
        parsed = _strip_xssi_json(body)
        if isinstance(parsed, dict):
            cards[rid] = parsed

    c1 = _latest_metric_map(cards.get("dashboard_card_1") or {})
    c4 = _latest_metric_map(cards.get("dashboard_card_4") or {})
    c5 = _latest_metric_map(cards.get("dashboard_card_5") or {})
    c6 = _latest_metric_map(cards.get("dashboard_card_6") or {})
    c13 = _latest_metric_map(cards.get("dashboard_card_13") or {})

    dau = c1.get("active_users_1")
    wau = c1.get("active_users_7")
    mau = c1.get("active_users_30")
    active = c4.get("active_users") or mau
    total_users = c5.get("total_users")
    crash_affected = c5.get("crash_affected_users")
    crash_free_users_pct = None
    if total_users and total_users > 0 and crash_affected is not None:
        crash_free_users_pct = max(0.0, min(100.0, (1.0 - crash_affected / total_users) * 100.0))

    eng_per_user = c6.get("userEngagementDurationPerUser")  # seconds
    eng_sessions = c6.get("engagedSessionsPerUser")
    revenue = c13.get("combinedRevenue")
    ad_revenue = c13.get("total_ad_revenue")

    kpis = [
        {"key": "dau", "label": "Daily active users", "value": dau, "fmt": _fmt_int(dau)},
        {"key": "wau", "label": "Weekly active users", "value": wau, "fmt": _fmt_int(wau)},
        {"key": "mau", "label": "Monthly active users", "value": mau, "fmt": _fmt_int(mau)},
        {"key": "active_users", "label": "Active users", "value": active, "fmt": _fmt_int(active)},
        {
            "key": "crash_free_users",
            "label": "Crash-free users (Analytics)",
            "value": crash_free_users_pct,
            "fmt": _fmt_pct(crash_free_users_pct) if crash_free_users_pct is not None else None,
        },
        {
            "key": "engagement_sec",
            "label": "Engagement / user (s)",
            "value": eng_per_user,
            "fmt": _fmt_int(eng_per_user) if eng_per_user is not None else None,
        },
        {
            "key": "engaged_sessions",
            "label": "Engaged sessions / user",
            "value": eng_sessions,
            "fmt": f"{eng_sessions:.2f}".replace(".", ",") if isinstance(eng_sessions, float) else _fmt_int(eng_sessions),
        },
        {"key": "revenue", "label": "Revenue", "value": revenue, "fmt": _fmt_int(revenue)},
        {"key": "ad_revenue", "label": "Ad revenue", "value": ad_revenue, "fmt": _fmt_int(ad_revenue)},
    ]
    kpis = [k for k in kpis if k.get("fmt")]

    return {
        "ok": bool(kpis),
        "platform": plat,
        "source": "analytics_dashboard_venus",
        "cards_seen": sorted(cards.keys()),
        "dau": dau,
        "wau": wau,
        "mau": mau,
        "active_users": active,
        "crash_affected_users": crash_affected,
        "total_users": total_users,
        "crash_free_users_pct": crash_free_users_pct,
        "crash_free_users_fmt": _fmt_pct(crash_free_users_pct) if crash_free_users_pct is not None else None,
        "engagement_sec_per_user": eng_per_user,
        "engaged_sessions_per_user": eng_sessions,
        "revenue": revenue,
        "ad_revenue": ad_revenue,
        "kpis": kpis,
        "raw_metrics": {
            "card_1": c1,
            "card_4": c4,
            "card_5": c5,
            "card_6": c6,
            "card_13": c13,
        },
    }


def _capture_network(page) -> list[dict[str, Any]]:
    captured: list[dict[str, Any]] = []

    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            low = url.lower()
            is_analytics = "analytics.google.com/analytics/app/data/v2/venus" in low
            interesting = (
                is_analytics
                or "firebasereleasemon" in low
                or "crashlytics" in low
                or any(k.lower() in url for k in _INTERESTING_RPC)
            )
            if not interesting and "firebase" not in low:
                return
            if resp.status != 200:
                return
            ct = (resp.headers or {}).get("content-type", "").lower()
            name = _rpc_name(url)
            keep_body = (
                is_analytics
                or any(name.startswith(k) for k in _INTERESTING_RPC)
            )
            if not keep_body and "json" not in ct and "javascript" not in ct and "text/plain" not in ct:
                return
            body = resp.text()
            if not body or len(body) > 2_000_000:
                return
            entry: dict[str, Any] = {
                "url": url[:500],
                "status": resp.status,
                "len": len(body),
                "snippet": body[:240],
            }
            if keep_body:
                entry["body"] = body[:500_000]
            captured.append(entry)
            if len(captured) > 260:
                captured.pop(0)
        except Exception:
            return

    # iframe Analytics istekleri için context dinleyicisi
    try:
        page.context.on("response", on_response)
    except Exception:
        page.on("response", on_response)
    return captured


def _goto_collect(page, url: str, captured: list[dict[str, Any]], *, wait_ms: int = 7000) -> dict[str, Any]:
    """Sayfaya git; network capture zaten bağlı — DOM yedek döner."""
    before = len(captured)
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    page.wait_for_timeout(wait_ms)
    if _page_needs_login(page):
        return {"ok": False, "error": "login_required", "url": url}
    page.wait_for_timeout(1500)
    return {
        "ok": True,
        "url": url,
        "hits_delta": len(captured) - before,
    }


def _window_summary(net: dict[str, Any], *, version: str, time_param: str, plat: str) -> dict[str, Any]:
    users = net.get("crash_free_users_pct")
    sess = net.get("crash_free_sessions_pct")
    return {
        "time": time_param,
        "version": version,
        "platform": plat,
        "crash_free_pct": users,
        "crash_free_fmt": _fmt_pct(users) if users is not None else None,
        "crash_free_sessions_pct": sess,
        "crash_free_sessions_fmt": _fmt_pct(sess) if sess is not None else None,
        "issues": (net.get("issues") or [])[:40],
        "by_version": (net.get("by_version") or [])[:40],
        "series": net.get("series") or [],
        "sessions_series": net.get("sessions_series") or [],
        "source": "releasemon_rpc",
    }


def _merge_issues(*lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for lst in lists:
        for iss in lst or []:
            if not isinstance(iss, dict):
                continue
            key = str(iss.get("id") or iss.get("title") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(iss)
            if len(out) >= 80:
                return out
    return out


def _scrape_platform(page, plat: str, days: int) -> dict[str, Any]:
    meta = dict(PLATFORMS[plat])
    captured: list[dict[str, Any]] = _capture_network(page)
    version = meta["latest_version"]
    windows: dict[str, Any] = {}

    urls90 = _urls(plat, time_param="90d", version=version)
    print(f"  · {plat}/release@90d …", flush=True)
    go = _goto_collect(page, urls90["release"], captured)
    if go.get("error") == "login_required":
        return {
            "ok": False,
            "error": "login_required",
            "project": meta["project"],
            "package": meta["package"],
        }
    net90 = _parse_releasemon_network(captured, plat=plat)
    if net90.get("by_version"):
        top = net90["by_version"][0]
        label = str(top.get("label") or "").strip()
        if label:
            version = label
            meta["latest_version"] = label
    windows["90d"] = _window_summary(net90, version=version, time_param="90d", plat=plat)

    for t in ("24h", "7d", "30d"):
        urls = _urls(plat, time_param=t, version=version)
        print(f"  · {plat}/release@{t} …", flush=True)
        before = len(captured)
        _goto_collect(page, urls["release"], captured, wait_ms=6500)
        slice_hits = captured[before:]
        net_t = _parse_releasemon_network(slice_hits, plat=plat)
        if not net_t.get("by_version"):
            net_t["by_version"] = net90.get("by_version") or []
        windows[t] = _window_summary(net_t, version=version, time_param=t, plat=plat)

    print(f"  · {plat}/release_all@90d …", flush=True)
    before = len(captured)
    _goto_collect(page, urls90["release_all"], captured, wait_ms=6500)
    net_all = _parse_releasemon_network(captured[before:], plat=plat)
    release_all = _window_summary(net_all, version="all", time_param="90d", plat=plat)

    print(f"  · {plat}/crashlytics@90d …", flush=True)
    before = len(captured)
    crash_urls = _urls(plat, time_param="90d", version=version, issue_types="crash")
    _goto_collect(page, crash_urls["crashlytics"], captured, wait_ms=7000)
    crash_net = _parse_releasemon_network(captured[before:], plat=plat)

    anr_issues: list[dict[str, Any]] = []
    if plat == "android":
        print(f"  · {plat}/crashlytics_anr@90d …", flush=True)
        before = len(captured)
        anr_urls = _urls(plat, time_param="90d", version=version, issue_types="anr")
        _goto_collect(page, anr_urls["crashlytics"], captured, wait_ms=7000)
        anr_net = _parse_releasemon_network(captured[before:], plat=plat)
        anr_issues = anr_net.get("issues") or []

    print(f"  · {plat}/analytics …", flush=True)
    ov_urls = _urls(plat, time_param="90d", version=version)
    before_analytics = len(captured)
    _goto_collect(page, ov_urls["analytics"], captured, wait_ms=20000)
    # iframe kartları geç gelebilir
    try:
        page.wait_for_timeout(4000)
    except Exception:
        pass
    analytics = _parse_analytics_cards(captured[before_analytics:], plat=plat)
    if not analytics.get("ok"):
        # tüm buffer'da ara (listener çift kayıt / gecikme)
        analytics = _parse_analytics_cards(captured, plat=plat)
    print(
        f"    → analytics ok={analytics.get('ok')} "
        f"dau={analytics.get('dau')} mau={analytics.get('mau')} "
        f"cards={len(analytics.get('cards_seen') or [])}",
        flush=True,
    )

    print(f"  · {plat}/overview …", flush=True)
    _goto_collect(page, ov_urls["overview"], captured, wait_ms=5000)

    issues = _merge_issues(
        windows.get("90d", {}).get("issues") or [],
        windows.get("7d", {}).get("issues") or [],
        windows.get("30d", {}).get("issues") or [],
        crash_net.get("issues") or [],
        anr_issues,
    )
    by_version = windows.get("90d", {}).get("by_version") or net90.get("by_version") or []
    series = windows.get("90d", {}).get("series") or []
    if len(release_all.get("series") or []) > len(series):
        series = release_all.get("series") or []
    sessions_series = windows.get("90d", {}).get("sessions_series") or []

    w24 = windows.get("24h") or {}
    w7 = windows.get("7d") or {}
    crash_free = (
        w7.get("crash_free_pct")
        or windows.get("90d", {}).get("crash_free_pct")
        or w24.get("crash_free_pct")
    )
    sessions_cf = (
        w7.get("crash_free_sessions_pct")
        or windows.get("90d", {}).get("crash_free_sessions_pct")
        or w24.get("crash_free_sessions_pct")
    )

    raw_hints = [{k: v for k, v in h.items() if k != "body"} for h in captured[-30:]]

    return {
        "ok": True,
        "project": meta["project"],
        "package": meta["package"],
        "app": meta["app"],
        "latest_version": version,
        "crash_free_pct": crash_free,
        "crash_free_fmt": _fmt_pct(crash_free) if crash_free is not None else None,
        "crash_free_sessions_pct": sessions_cf,
        "crash_free_sessions_fmt": _fmt_pct(sessions_cf) if sessions_cf is not None else None,
        "latest_24h": w24,
        "latest_7d": w7,
        "windows": windows,
        "release_all": release_all,
        "issues": issues,
        "anr_issues": anr_issues[:40],
        "by_version": by_version[:40],
        "by_device": [],
        "series": series,
        "sessions_series": sessions_series,
        "release_monitoring": {
            "version": version,
            "crash_free_pct": crash_free,
            "crash_free_fmt": _fmt_pct(crash_free) if crash_free is not None else None,
            "crash_free_sessions_pct": sessions_cf,
            "crash_free_sessions_fmt": _fmt_pct(sessions_cf) if sessions_cf is not None else None,
            "latest_24h": w24,
            "latest_7d": w7,
            "url": urls90["release"],
            "source_page": "releasemonitoring",
            "source": "releasemon_rpc",
        },
        "analytics": analytics,
        "pages": {
            "overview": ov_urls["overview"],
            "analytics": ov_urls["analytics"],
            "crashlytics": crash_urls["crashlytics"],
            "release": urls90["release"],
            "release_all": urls90["release_all"],
        },
        "network_hits": len(captured),
        "raw_hints": raw_hints,
        "scrape_windows": list(windows.keys()),
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
            probe = _urls("android", time_param="90d")["overview"]
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
                    for win_key, metric_name in (
                        ("latest_24h", f"{plat}_crash_free_24h"),
                        ("latest_7d", f"{plat}_crash_free_7d"),
                    ):
                        w = block.get(win_key) if isinstance(block.get(win_key), dict) else {}
                        if w.get("crash_free_fmt"):
                            metrics.append(
                                {
                                    "metric": metric_name,
                                    "platform": plat,
                                    "value": w.get("crash_free_pct"),
                                    "fmt": w.get("crash_free_fmt"),
                                    "period": w.get("time"),
                                    "version": w.get("version") or block.get("latest_version"),
                                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                                }
                            )
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
                    an = block.get("analytics") if isinstance(block.get("analytics"), dict) else {}
                    for k in ("dau", "wau", "mau"):
                        if an.get(k) is not None:
                            metrics.append(
                                {
                                    "metric": f"{plat}_{k}",
                                    "platform": plat,
                                    "value": an.get(k),
                                    "fmt": _fmt_int(an.get(k)),
                                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                                    "source": "analytics_dashboard",
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
