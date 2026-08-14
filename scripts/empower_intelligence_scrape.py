#!/usr/bin/env python3
"""Empower Intelligence (intelligence.empower.net) scrape → Project Control.

doviz-report: Web / Mobile Web / iOS / Android günlük tablolar.

  # Bir kez Cognito girişi (fx-empower profili)
  .venv/bin/python scripts/empower_intelligence_scrape.py --login

  # Historical backfill (tüm sekmeler)
  .venv/bin/python scripts/empower_intelligence_scrape.py \\
    --backfill --start 2025-01-01 --end 2026-08-13 --ingest

  # Günlük Yesterday (Quick Select) — 02:12 + 13:18 TR
  .venv/bin/python scripts/empower_intelligence_scrape.py --yesterday --ingest

Env:
  EMPOWER_INTEL_PROFILE_DIR   default ~/.seo-agent/fx-empower
  EMPOWER_INTEL_INGEST_URL    default …/api/empower-intel/ingest
  NOTIFICATION_INGEST_TOKEN
  EMPOWER_INTEL_HEADLESS=1
  EMPOWER_INTEL_PROJECT=doviz
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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

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

from backend.services.scrape_browser import (  # noqa: E402
    STATE_DIR,
    acquire_profile_login_lock,
    empower_profile_dir,
    login_wait_sec,
    release_profile_login_lock,
)
from backend.services.system_firefox_driver import (  # noqa: E402
    launch_system_firefox_driver,
    quit_system_firefox_driver,
)

PROFILE_DIR = empower_profile_dir()
INGEST_URL = (
    os.environ.get("EMPOWER_INTEL_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/empower-intel/ingest"
).strip()
PROJECT = (os.environ.get("EMPOWER_INTEL_PROJECT") or "doviz").strip().lower() or "doviz"
BASE_REPORT = f"https://intelligence.empower.net/{PROJECT}-report/"

PLATFORMS: tuple[tuple[str, str], ...] = (
    ("web", "Web"),
    ("mweb", "Mobile Web"),
    ("ios", "iOS"),
    ("android", "Android"),
)

# Columns → visibleColumns (localStorage columnPrefs)
# Uygulamalar (iOS/Android) — kullanıcının ilk eklediği set
APP_VISIBLE_COLUMNS: tuple[str, ...] = (
    "date",
    "view",  # Impression
    "match",
    "usdSpent",  # Revenue ($)
    "usdEcpm",  # eCPM ($)
    "request",  # Requests
    "active1DayUsers",  # DAU (1 Day)
    "active7DayUsers",  # DAU (7 Days)
    "dauPerMau",
    "appVersion",
    "arpdauTry",  # ARPDAU (₺)
    "arpdauUsd",  # ARPDAU ($)
    "averageSessionDuration",
    "bounceRate",
    "crashAffectedUsers",
    "crashFreeUsersRate",
    "engagementRate",
    "impdau",
    "is_holiday",
    "newUsers",
    "rpiUsd",  # RPI ($)
    "rpmTry",  # RPM (₺)
    "rpsTry",  # RPS (₺)
    "sessions",
    "totalUsers",
    "tryEcpm",  # eCPM (₺)
    "trySpent",  # Revenue (₺)
    "userEngagementDuration",
    "avgEngagementTimePerUser",
    "avgEngagementTimePerSession",
)

# Web / Mobile Web — kullanıcının ikinci eklediği set
WEB_VISIBLE_COLUMNS: tuple[str, ...] = (
    "date",
    "view",
    "match",
    "usdSpent",
    "usdEcpm",
    "request",
    "active1DayUsers",
    "active7DayUsers",
    "dauPerMau",
    "avgEngagementTimePerUser",
    "avgEngagementTimePerSession",
    "arpdauTry",
    "arpdauUsd",
    "averageSessionDuration",
    "bounceRate",
    "engagementRate",
    "impdau",
    "newUsers",
    "organicGoogleSearchClicks",
    "organicGoogleSearchClickThroughRate",
    "rpmTry",
    "rpmUsd",
    "screenPageViews",
    "screenPageViewsPerSession",
    "screenPageViewsPerUser",
    "sessions",
    "totalUsers",
    "tryEcpm",
    "trySpent",
    "userEngagementDuration",
)

COLUMN_SETS: dict[str, tuple[str, ...]] = {
    "ios": APP_VISIBLE_COLUMNS,
    "android": APP_VISIBLE_COLUMNS,
    "web": WEB_VISIBLE_COLUMNS,
    "mweb": WEB_VISIBLE_COLUMNS,
}

# Empower report API (browser OIDC bearer ile)
FETCH_REPORT_API = (
    os.environ.get("EMPOWER_INTEL_FETCH_URL")
    or "https://lkusbybvt5.execute-api.eu-west-1.amazonaws.com/v1/report/fetch_report"
).strip()
DEFAULT_VIRGUL_ID = (os.environ.get("EMPOWER_INTEL_VIRGUL_ID") or "55af4685a503b0ad628b4567").strip()
# columnPrefs anahtarındaki property_id — locale başına sabit
DEFAULT_PROPERTY_IDS: dict[str, str] = {
    "android": "152168629",
    "ios": "163175967",
    "mweb": "329808608",
    "web": "376928120",
}

_HOOK_JS = r"""
window.__empowerNet = window.__empowerNet || [];
if (!window.__empowerHooked) {
  window.__empowerHooked = true;
  const push = (kind, url, body) => {
    try {
      const s = typeof body === 'string' ? body : JSON.stringify(body);
      window.__empowerNet.push({
        kind, url: String(url || ''),
        body: (s || '').slice(0, 2_000_000),
        ts: Date.now()
      });
      if (window.__empowerNet.length > 80) {
        window.__empowerNet = window.__empowerNet.slice(-60);
      }
    } catch (e) {}
  };
  const ofetch = window.fetch;
  window.fetch = async function(...args) {
    const res = await ofetch.apply(this, args);
    try {
      const clone = res.clone();
      const ct = (clone.headers.get('content-type') || '');
      const u = String(args[0] && args[0].url ? args[0].url : args[0] || '');
      if (ct.includes('json') || /api|graphql|report|query|doviz/i.test(u)) {
        push('fetch', u, await clone.text());
      }
    } catch (e) {}
    return res;
  };
  const XO = XMLHttpRequest.prototype.open;
  const XS = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.open = function(m, u, ...r) {
    this.__u = u;
    return XO.call(this, m, u, ...r);
  };
  XMLHttpRequest.prototype.send = function(...a) {
    this.addEventListener('load', function() {
      try { push('xhr', String(this.__u || ''), this.responseText || ''); } catch (e) {}
    });
    return XS.apply(this, a);
  };
}
return true;
"""

_TABLE_JS = r"""
const out = [];
const tables = [...document.querySelectorAll('table')];
for (const t of tables) {
  const rows = [...t.querySelectorAll('tr')].map(tr =>
    [...tr.querySelectorAll('th,td')].map(c => (c.innerText || '').trim())
  ).filter(r => r.some(Boolean));
  if (rows.length >= 2) out.push(rows);
}
return out;
"""


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _report_url(
    *,
    platform: str,
    start: date | None = None,
    end: date | None = None,
    day: bool = True,
) -> str:
    q: dict[str, str] = {
        "day": "true" if day else "false",
        "month": "false",
        "year": "false",
        "platform": platform,
    }
    if start:
        q["start_date"] = start.isoformat()
    if end:
        q["end_date"] = end.isoformat()
    return BASE_REPORT + "?" + urlencode(q)


def _needs_login(driver: Any) -> bool:
    try:
        url = (driver.current_url or "").lower()
    except Exception:
        url = ""
    if "cognito" in url or "amazoncognito" in url:
        return True
    if "/login" in url:
        return True
    try:
        body = (driver.find_element("tag name", "body").text or "")[:1200].lower()
    except Exception:
        body = ""
    if "welcome to empower" in body or "please sign in" in body:
        return True
    if "sign in to your account" in body:
        return True
    if "email address" in body and "next" in body and "sign in" in body:
        return True
    # Yalnızca "Loading…" / boş gövde → henüz oturum kanıtı yok
    stripped = re.sub(r"\s+", " ", body).strip()
    if not stripped or stripped in {"loading...", "loading", "loading…"}:
        return True
    return False


def _session_looks_ready(driver: Any) -> bool:
    """Pozitif dashboard sinyali — loading veya login false-negative olmasın."""
    if _needs_login(driver):
        return False
    try:
        url = (driver.current_url or "").lower()
    except Exception:
        url = ""
    if "intelligence.empower.net" not in url:
        return False
    try:
        body = (driver.find_element("tag name", "body").text or "")[:2500]
    except Exception:
        body = ""
    low = body.lower()
    markers = (
        "mobile web",
        "android",
        "quick select",
        "quick options",
        "yesterday",
        "start date",
        "platform",
        "web",
        "ios",
    )
    hits = sum(1 for m in markers if m in low)
    if hits >= 2:
        return True
    try:
        tables = driver.find_elements("css selector", "table")
        if tables:
            return True
    except Exception:
        pass
    return False


def _wait_logged_in(driver: Any, *, timeout_sec: int | None = None) -> bool:
    timeout_sec = login_wait_sec() if timeout_sec is None else int(timeout_sec)
    print(
        "Empower Cognito girişi gerekli.\n"
        "Açılan Firefox penceresinde Sign In → e-posta/şifre.\n"
        f"Rapor açılınca tarama devam eder (en fazla {timeout_sec // 60} dk).",
        flush=True,
    )
    deadline = time.time() + timeout_sec
    last = 0.0
    while time.time() < deadline:
        if _session_looks_ready(driver):
            time.sleep(1.5)
            if _session_looks_ready(driver):
                return True
        now = time.time()
        if now - last >= 12:
            try:
                u = (driver.current_url or "")[:120]
            except Exception:
                u = "—"
            print(f"  · login bekleniyor · kalan≈{int(deadline - now)}s · {u}", flush=True)
            last = now
        time.sleep(2)
    return False


def _install_hooks(driver: Any) -> None:
    try:
        driver.execute_script(_HOOK_JS)
    except Exception:
        pass


def _clear_net(driver: Any) -> None:
    try:
        driver.execute_script("window.__empowerNet = [];")
    except Exception:
        pass


def _net_captures(driver: Any) -> list[dict[str, Any]]:
    try:
        raw = driver.execute_script("return window.__empowerNet || [];")
    except Exception:
        return []
    return [x for x in (raw or []) if isinstance(x, dict)]


def _parse_num(raw: Any) -> float | None:
    if raw is None or raw is False:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s or s in {"—", "-", "n/a", "N/A", "null"}:
        return None
    s = s.replace("\u00a0", " ").replace("%", "").strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "")
    # TR format 1.234,56
    if re.search(r"^\d{1,3}(\.\d{3})+(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        v = float(s)
    except ValueError:
        return None
    return -v if neg else v


def _parse_date_cell(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y", "%b %d, %Y", "%d %b %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:20].strip(), fmt).date()
        except ValueError:
            continue
    return None


def _looks_like_metric_key(k: str) -> bool:
    low = (k or "").strip().lower()
    if not low or low in {"date", "day", "tarih", "platform", "project"}:
        return False
    return bool(re.search(r"[a-z]", low))


def rows_from_table_matrix(matrix: list[list[str]]) -> list[dict[str, Any]]:
    if not matrix or len(matrix) < 2:
        return []
    # header = first row with a date-like column
    header = None
    data_start = 1
    for i, row in enumerate(matrix[:5]):
        lows = [c.strip().lower() for c in row]
        if any(h in {"date", "day", "tarih", "gün", "gun"} for h in lows):
            header = row
            data_start = i + 1
            break
        # or first cell of next rows look like dates
        if i == 0 and any(_parse_date_cell(c) for c in matrix[1][:3] if matrix[1:]):
            header = row
            data_start = 1
            break
    if not header:
        return []

    date_idx = 0
    for i, h in enumerate(header):
        if h.strip().lower() in {"date", "day", "tarih", "gün", "gun"}:
            date_idx = i
            break

    out: list[dict[str, Any]] = []
    for row in matrix[data_start:]:
        if date_idx >= len(row):
            continue
        rd = _parse_date_cell(row[date_idx])
        if not rd:
            continue
        metrics: dict[str, Any] = {}
        for i, h in enumerate(header):
            if i == date_idx or i >= len(row):
                continue
            key = re.sub(r"[^a-z0-9]+", "_", h.strip().lower()).strip("_")
            if not key or not _looks_like_metric_key(key):
                continue
            num = _parse_num(row[i])
            metrics[key] = num if num is not None else row[i]
        out.append({"report_date": rd.isoformat(), "metrics": metrics})
    return out


def rows_from_json_payload(payload: Any, *, depth: int = 0) -> list[dict[str, Any]]:
    """JSON ağacında tarihli satır listeleri ara."""
    if depth > 8:
        return []
    found: list[dict[str, Any]] = []

    def as_row(obj: dict[str, Any]) -> dict[str, Any] | None:
        rd = None
        for k in ("date", "day", "report_date", "reportDate", "dt", "tarih"):
            if k in obj:
                rd = _parse_date_cell(obj.get(k))
                if rd:
                    break
        if not rd:
            return None
        metrics: dict[str, Any] = {}
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in {"date", "day", "report_date", "reportdate", "dt", "tarih", "platform", "project"}:
                continue
            if isinstance(v, (dict, list)):
                continue
            key = re.sub(r"[^a-z0-9]+", "_", str(k).strip().lower()).strip("_")
            if not key:
                continue
            num = _parse_num(v)
            metrics[key] = num if num is not None else v
        if not metrics:
            return None
        return {"report_date": rd.isoformat(), "metrics": metrics}

    if isinstance(payload, list):
        if payload and all(isinstance(x, dict) for x in payload[:3]):
            for item in payload:
                if isinstance(item, dict):
                    r = as_row(item)
                    if r:
                        found.append(r)
            if found:
                return found
        for item in payload:
            found.extend(rows_from_json_payload(item, depth=depth + 1))
            if len(found) >= 50:
                break
        return found

    if isinstance(payload, dict):
        # preferred keys
        for key in ("rows", "data", "items", "results", "records", "series", "days", "daily"):
            if key in payload:
                found.extend(rows_from_json_payload(payload[key], depth=depth + 1))
                if found:
                    return found
        r = as_row(payload)
        if r:
            return [r]
        for v in payload.values():
            if isinstance(v, (dict, list)):
                found.extend(rows_from_json_payload(v, depth=depth + 1))
                if len(found) >= 50:
                    break
    return found


def rows_from_net(captures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: list[dict[str, Any]] = []
    for cap in captures:
        body = cap.get("body") or ""
        if not isinstance(body, str) or len(body) < 10:
            continue
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            continue
        rows = rows_from_json_payload(payload)
        if len(rows) > len(best):
            best = rows
    return best


def _click_text(driver: Any, labels: list[str]) -> bool:
    for label in labels:
        xpaths = [
            f"//button[normalize-space()='{label}']",
            f"//a[normalize-space()='{label}']",
            f"//*[@role='tab' and normalize-space()='{label}']",
            f"//*[@role='button' and normalize-space()='{label}']",
            f"//div[normalize-space()='{label}']",
            f"//span[normalize-space()='{label}']",
        ]
        for xp in xpaths:
            try:
                els = driver.find_elements("xpath", xp)
            except Exception:
                continue
            for el in els[:3]:
                try:
                    if not el.is_displayed():
                        continue
                    el.click()
                    time.sleep(0.8)
                    return True
                except Exception:
                    try:
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.8)
                        return True
                    except Exception:
                        continue
    return False


def _apply_yesterday_quick_select(driver: Any) -> bool:
    """Quick options / Quick select → Yesterday."""
    opened = _click_text(
        driver,
        [
            "Quick options",
            "Quick Options",
            "Quick select",
            "Quick Select",
            "Quick Select Date",
            "Date range",
        ],
    )
    time.sleep(0.5)
    clicked = _click_text(driver, ["Yesterday", "Dün", "yesterday"])
    if clicked:
        time.sleep(2.5)
        return True
    # URL fallback: yesterday date
    y = date.today() - timedelta(days=1)
    try:
        cur = driver.current_url or ""
        plat = "mweb"
        m = re.search(r"[?&]platform=([a-zA-Z_]+)", cur)
        if m:
            plat = m.group(1)
        driver.get(_report_url(platform=plat, start=y, end=y))
        time.sleep(3)
        return True
    except Exception:
        return opened and False


def _read_oidc_access_token(driver: Any) -> str:
    try:
        tok = driver.execute_script(
            """
            for (let i = 0; i < localStorage.length; i++) {
              const k = localStorage.key(i);
              if (k && k.startsWith('oidc.user:')) {
                const u = JSON.parse(localStorage.getItem(k) || '{}');
                return u.access_token || u.id_token || '';
              }
            }
            return '';
            """
        )
    except Exception:
        return ""
    return str(tok or "").strip()


def _read_property_ids(driver: Any) -> dict[str, str]:
    out = dict(DEFAULT_PROPERTY_IDS)
    try:
        found = driver.execute_script(
            """
            const m = {};
            for (let i = 0; i < localStorage.length; i++) {
              const k = localStorage.key(i);
              if (!k || !k.startsWith('columnPrefs:')) continue;
              const parts = k.split(':');
              if (parts.length < 2) continue;
              m[parts[parts.length - 2]] = parts[parts.length - 1];
            }
            return m;
            """
        )
    except Exception:
        found = {}
    if isinstance(found, dict):
        for plat, pid in found.items():
            if plat in out and pid:
                out[str(plat)] = str(pid)
    return out


def _fetch_report_api(
    *,
    access_token: str,
    property_id: str,
    start: date,
    end: date,
    virgul_id: str = DEFAULT_VIRGUL_ID,
) -> list[dict[str, Any]]:
    q = urlencode(
        {
            "virgul_id": virgul_id,
            "property_id": property_id,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "day": "true",
            "month": "false",
            "year": "false",
        }
    )
    url = f"{FETCH_REPORT_API}?{q}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Origin": "https://intelligence.empower.net",
            "Referer": "https://intelligence.empower.net/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) "
                "Gecko/20100101 Firefox/147.0"
            ),
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    if not isinstance(payload, dict) or not payload.get("success"):
        msg = payload.get("message") if isinstance(payload, dict) else "bad response"
        raise RuntimeError(f"fetch_report failed: {msg}")
    data = payload.get("data") or {}
    rows = data.get("merged_data") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("fetch_report: merged_data yok")
    return [r for r in rows if isinstance(r, dict)]


def _rows_from_api_records(
    records: list[dict[str, Any]],
    *,
    visible: tuple[str, ...] | list[str],
) -> list[dict[str, Any]]:
    want = [c for c in visible if c != "date"]
    out: list[dict[str, Any]] = []
    for rec in records:
        rd = _parse_date_cell(rec.get("date") or rec.get("day"))
        if not rd:
            continue
        metrics: dict[str, Any] = {}
        for key in want:
            if key not in rec:
                continue
            val = rec.get(key)
            num = _parse_num(val)
            metrics[key] = num if num is not None else val
        out.append({"report_date": rd.isoformat(), "metrics": metrics})
    by_date: dict[str, dict[str, Any]] = {}
    for r in out:
        by_date[str(r["report_date"])] = r
    rows = list(by_date.values())
    rows.sort(key=lambda x: str(x.get("report_date") or ""))
    return rows


def _apply_column_prefs(driver: Any, platform: str) -> int:
    """Platforma göre Columns visible setini localStorage'a yazar.

    Returns: güncellenen pref anahtarı sayısı.
    """
    want = list(COLUMN_SETS.get(platform) or ())
    if not want:
        return 0
    try:
        n = driver.execute_script(
            """
            const platform = arguments[0];
            const want = arguments[1];
            let updated = 0;
            const prefix = 'columnPrefs:';
            for (let i = 0; i < localStorage.length; i++) {
              const k = localStorage.key(i);
              if (!k || !k.startsWith(prefix)) continue;
              // ...:/doviz-report/:android:HASH
              const parts = k.split(':');
              const plat = parts.length >= 2 ? parts[parts.length - 2] : '';
              if (plat !== platform) continue;
              let pref;
              try { pref = JSON.parse(localStorage.getItem(k) || '{}'); }
              catch (e) { continue; }
              const order = Array.isArray(pref.columnOrder) ? pref.columnOrder.slice() : want.slice();
              const known = new Set(order.concat(want));
              const visible = want.filter(c => known.has(c));
              if (!visible.includes('date') && known.has('date')) visible.unshift('date');
              const rest = order.filter(c => !visible.includes(c));
              pref.columnOrder = visible.concat(rest);
              pref.visibleColumns = visible;
              pref.version = pref.version || 1;
              localStorage.setItem(k, JSON.stringify(pref));
              updated += 1;
            }
            return updated;
            """,
            platform,
            want,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"    · columnPrefs yazılamadı: {exc}", flush=True)
        return 0
    print(f"    · columns set ({platform}): {len(want)} sütun · prefs={n}", flush=True)
    return int(n or 0)


def _click_generate_report(driver: Any) -> bool:
    return _click_text(driver, ["Generate Report", "Generate report", "Rapor Oluştur"])


def _scrape_platform(
    driver: Any,
    *,
    platform: str,
    start: date | None,
    end: date | None,
    yesterday: bool,
    access_token: str,
    property_ids: dict[str, str],
) -> list[dict[str, Any]]:
    label = dict(PLATFORMS).get(platform, platform)
    if yesterday:
        y = date.today() - timedelta(days=1)
        start = end = y
    if start is None or end is None:
        raise RuntimeError("start/end gerekli")

    visible = COLUMN_SETS.get(platform) or APP_VISIBLE_COLUMNS
    prop_id = (property_ids.get(platform) or DEFAULT_PROPERTY_IDS.get(platform) or "").strip()
    print(
        f"  · {platform} ({label}) · property={prop_id} · "
        f"{start.isoformat()}→{end.isoformat()} · {len(visible)} sütun",
        flush=True,
    )
    _apply_column_prefs(driver, platform)

    if not access_token:
        raise RuntimeError("OIDC access_token yok — --login")
    if not prop_id:
        raise RuntimeError(f"property_id yok: {platform}")

    try:
        records = _fetch_report_api(
            access_token=access_token,
            property_id=prop_id,
            start=start,
            end=end,
        )
    except Exception as exc:  # noqa: BLE001
        # token drop → sayfayı yenile, tekrar dene
        print(f"    · API hata, token yenile: {exc}", flush=True)
        driver.get(_report_url(platform=platform, start=start, end=end))
        time.sleep(3)
        if _needs_login(driver) or not _session_looks_ready(driver):
            raise
        access_token = _read_oidc_access_token(driver)
        records = _fetch_report_api(
            access_token=access_token,
            property_id=prop_id,
            start=start,
            end=end,
        )

    out = _rows_from_api_records(records, visible=visible)
    cols = len((out[0].get("metrics") or {})) if out else 0
    print(f"    → {len(out)} gün · {cols} metrik (API)", flush=True)
    return out


def scrape_empower(
    *,
    platforms: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
    yesterday: bool = False,
    headed: bool = True,
    login_only: bool = False,
) -> dict[str, Any]:
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    plats = [p for p, _ in PLATFORMS]
    if platforms:
        want = {p.strip().lower() for p in platforms}
        plats = [p for p in plats if p in want] or plats

    if yesterday:
        y = date.today() - timedelta(days=1)
        start = end = y
    if not yesterday and (start is None or end is None):
        # API için tarih zorunlu
        start = start or date(2025, 1, 1)
        end = end or date.today()

    driver = None
    lock_held = False
    try:
        driver = launch_system_firefox_driver(PROFILE_DIR, headed=headed, page_load_timeout=120)
        if login_only:
            acquire_profile_login_lock(PROFILE_DIR, reason="empower-login")
            lock_held = True
        driver.get(_report_url(platform=plats[0], start=start, end=end))
        time.sleep(3)
        ready = _session_looks_ready(driver)
        if not ready:
            if not headed:
                return {
                    "ok": False,
                    "sync_ok": False,
                    "sync_message": "Empower login gerekli (--login / headed)",
                    "platforms": [],
                }
            if not lock_held:
                acquire_profile_login_lock(PROFILE_DIR, reason="empower-login")
                lock_held = True
            if _needs_login(driver):
                _click_text(driver, ["Sign In", "Sign in", "Giriş"])
                time.sleep(1.5)
            if not _wait_logged_in(driver):
                return {
                    "ok": False,
                    "sync_ok": False,
                    "sync_message": "Empower login zaman aşımı",
                    "platforms": [],
                }
            if login_only:
                (STATE_DIR / "cache").mkdir(parents=True, exist_ok=True)
                (STATE_DIR / "cache" / "empower-login-ok.txt").write_text(
                    f"{driver.current_url}\n{time.time()}\n", encoding="utf-8"
                )
                return {"ok": True, "sync_ok": True, "sync_message": "login ok", "platforms": []}

        if login_only:
            return {"ok": True, "sync_ok": True, "sync_message": "zaten girişli", "platforms": []}

        # Tüm platform columnPrefs'lerini hedef setlere çek
        for p in plats:
            _apply_column_prefs(driver, p)

        access_token = _read_oidc_access_token(driver)
        property_ids = _read_property_ids(driver)
        if not access_token:
            return {
                "ok": False,
                "sync_ok": False,
                "sync_message": "OIDC token okunamadı — --login",
                "platforms": [],
            }

        blocks: list[dict[str, Any]] = []
        for plat in plats:
            try:
                rows = _scrape_platform(
                    driver,
                    platform=plat,
                    start=start,
                    end=end,
                    yesterday=yesterday,
                    access_token=access_token,
                    property_ids=property_ids,
                )
                # token uzun turda drop olursa güncelle
                maybe = _read_oidc_access_token(driver)
                if maybe:
                    access_token = maybe
            except Exception as exc:  # noqa: BLE001
                print(f"  · {plat} hata: {exc}", flush=True)
                blocks.append({"platform": plat, "rows": [], "error": str(exc)})
                continue
            blocks.append({"platform": plat, "rows": rows})

        total = sum(len(b.get("rows") or []) for b in blocks)
        return {
            "ok": total > 0,
            "sync_ok": total > 0,
            "sync_message": f"{PROJECT}: {total} satır / {len(blocks)} platform",
            "project": PROJECT,
            "source": "empower_intel_scrape",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "start": start.isoformat() if start else "",
            "end": end.isoformat() if end else "",
            "yesterday": yesterday,
            "column_sets": {
                p: list(COLUMN_SETS.get(p) or []) for p in plats
            },
            "platforms": blocks,
        }
    finally:
        if lock_held:
            release_profile_login_lock(PROFILE_DIR)
        if driver is not None:
            quit_system_firefox_driver(driver)


def post_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}

    def _post_one(body: dict[str, Any]) -> dict[str, Any]:
        data = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
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
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {"ok": True, "message": raw[:300]}
        except urllib.error.HTTPError as exc:
            err = exc.read().decode("utf-8", errors="replace")[:500]
            return {"ok": False, "message": f"HTTP {exc.code}: {err}"}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "message": str(exc)}

    platforms = payload.get("platforms") or []
    if isinstance(platforms, list) and len(platforms) > 1:
        # Büyük backfill: platform başına gönder (Broken pipe / body limit)
        totals = {"inserted": 0, "updated": 0, "skipped": 0, "row_count": 0}
        details: list[dict[str, Any]] = []
        base = {k: v for k, v in payload.items() if k != "platforms"}
        for block in platforms:
            if not isinstance(block, dict):
                continue
            chunk = dict(base)
            chunk["platforms"] = [block]
            res = _post_one(chunk)
            details.append({"platform": block.get("platform"), **res})
            if not res.get("ok"):
                return {
                    "ok": False,
                    "message": res.get("message") or "platform ingest failed",
                    "platforms": details,
                }
            for k in ("inserted", "updated", "skipped", "row_count"):
                totals[k] += int(res.get(k) or 0)
            print(
                f"  · ingest {block.get('platform')}: "
                f"+{res.get('inserted')} ~{res.get('updated')}",
                flush=True,
            )
        return {
            "ok": True,
            "inserted": totals["inserted"],
            "updated": totals["updated"],
            "skipped": totals["skipped"],
            "row_count": totals["row_count"],
            "platforms": details,
            "message": (
                f"chunked ingest +{totals['inserted']} ~{totals['updated']} "
                f"({len(details)} platform)"
            ),
        }

    return _post_one(payload)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Empower Intelligence scrape")
    ap.add_argument("--login", action="store_true")
    ap.add_argument("--backfill", action="store_true", help="Tarih aralığı (tüm sekmeler)")
    ap.add_argument("--yesterday", action="store_true", help="Quick Select → Yesterday")
    ap.add_argument("--sync", action="store_true", help="Alias: --yesterday")
    ap.add_argument("--start", default="", help="YYYY-MM-DD")
    ap.add_argument("--end", default="", help="YYYY-MM-DD")
    ap.add_argument("--platform", action="append", default=[], help="web|mweb|ios|android (tekrar)")
    ap.add_argument("--ingest", action="store_true")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--out", default="", help="JSON çıktı yolu")
    args = ap.parse_args(argv)

    env_hl = (os.environ.get("EMPOWER_INTEL_HEADLESS") or "").strip().lower() in {"1", "true", "yes"}
    headed = not (args.headless or env_hl)
    if args.login:
        headed = True

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    if args.backfill and not start:
        start = date(2025, 1, 1)
    if args.backfill and not end:
        end = date(2026, 8, 13)

    yesterday = bool(args.yesterday or args.sync)
    if args.backfill:
        yesterday = False

    result = scrape_empower(
        platforms=args.platform or None,
        start=start,
        end=end,
        yesterday=yesterday,
        headed=headed,
        login_only=bool(args.login),
    )

    out_path = Path(args.out) if args.out else (STATE_DIR / "cache" / "empower-intel-last.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"wrote {out_path}", flush=True)

    if args.ingest and not args.login:
        ing = post_ingest(result)
        print(f"ingest: {ing}", flush=True)
        if not ing.get("ok"):
            return 2

    if args.login:
        return 0 if result.get("ok") else 1
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
