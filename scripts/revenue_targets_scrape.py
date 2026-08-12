#!/usr/bin/env python3
"""Ad-virgul aylık hedef sheet — sistem Firefox oturumu (Nightly yok).

Akış (manuel yok):
  1) Günlük Firefox.app profilinden Google çerezlerini oku
  2) MCM workbook aylık sekmelerini (Şubat 2023+) CSV export
  3) Doviz/Sinemalar satırlarını birleştir → lokal + Railway ingest

  .venv/bin/python scripts/revenue_targets_scrape.py --sync
  .venv/bin/python scripts/revenue_targets_scrape.py --sync --ingest
  .venv/bin/python scripts/revenue_targets_scrape.py --sync --current-only
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
    REVENUE_TARGETS_HISTORY_FROM,
    REVENUE_TARGETS_SHEET_URL,
    parse_revenue_targets_csv,
    parse_sheet_tab_period,
    save_ingested_revenue_targets,
)

SHEET_ID = "1ITl0rUlLylTspsztMtaaFGEdvT_gINoUHDPodspEa5Y"
GID_CURRENT = "244461752"
OUT_CSV = Path.home() / ".seo-agent" / "cache" / "revenue-targets.csv"
OUT_ROWS = Path.home() / ".seo-agent" / "cache" / "revenue-targets-rows.json"
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
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) "
    "Gecko/20100101 Firefox/147.0"
)


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
        cookies = _read_google_cookies(p)
        names = {c["name"] for c in cookies}
        score = len(names & _SESSION_NAMES) * 100 + len(cookies)
        if score > best_score:
            best_score = score
            best = p
    if best_score < 100:
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


def _opener(profile: Path):
    return build_opener(HTTPCookieProcessor(_cookie_jar_from_profile(profile)))


def export_gid_csv(opener, gid: str) -> str | None:
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    req = Request(url, headers={"User-Agent": _UA, "Accept": "text/csv,text/plain,*/*"})
    try:
        with opener.open(req, timeout=90) as resp:
            data = resp.read()
    except urllib.error.HTTPError as exc:
        print(f"export gid={gid} HTTP {exc.code}", flush=True)
        return None
    except Exception as exc:
        print(f"export gid={gid} fail: {exc}", flush=True)
        return None
    text = data.decode("utf-8", errors="replace")
    if not text.strip() or "<html" in text[:500].lower() or "accounts.google" in text[:2000].lower():
        return None
    return text


def _decode_js_string(raw: str) -> str:
    """htmlview items.push name/url — UTF-8 bozmadan \\x27 / \\uXXXX çöz."""
    s = (raw or "").replace("\\/", "/")
    s = s.replace("\\x27", "'").replace("\\'", "'")

    def _hex(m: re.Match[str]) -> str:
        return chr(int(m.group(1), 16))

    s = re.sub(r"\\x([0-9a-fA-F]{2})", _hex, s)
    s = re.sub(r"\\u([0-9a-fA-F]{4})", _hex, s)
    return s


def discover_month_tabs(opener) -> list[tuple[str, str, str]]:
    """htmlview → [(tab_name, gid, period_key), ...] Şubat 2023+."""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/htmlview"
    req = Request(url, headers={"User-Agent": _UA})
    with opener.open(req, timeout=60) as resp:
        html = resp.read().decode("utf-8", errors="replace")

    items = re.findall(
        r'items\.push\(\{name:\s*"([^"]+)",\s*pageUrl:\s*"([^"]+)"',
        html,
    )
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for name_raw, url_raw in items:
        name = _decode_js_string(name_raw)
        page = _decode_js_string(url_raw)
        m = re.search(r"gid=(\d+)", page)
        if not m:
            continue
        gid = m.group(1)
        period = parse_sheet_tab_period(name)
        if not period:
            continue
        _label, _y, _mon, period_key = period
        if period_key < REVENUE_TARGETS_HISTORY_FROM:
            continue
        if period_key in seen:
            continue
        seen.add(period_key)
        out.append((name, gid, period_key))

    out.sort(key=lambda t: t[2])
    return out


def scrape_history_rows(
    profile: Path,
    *,
    current_only: bool = False,
) -> tuple[list[dict], str | None, list[str]]:
    """Tüm aylık sekmelerden Doviz/Sinemalar satırları."""
    opener = _opener(profile)
    current_csv: str | None = None
    errors: list[str] = []

    if current_only:
        tabs = [("Ağustos'26", GID_CURRENT, "current")]
        # period from CSV header
        text = export_gid_csv(opener, GID_CURRENT)
        if not text:
            return [], None, ["current month export failed"]
        current_csv = text
        rows = parse_revenue_targets_csv(text)
        return rows, current_csv, errors

    tabs = discover_month_tabs(opener)
    if not tabs:
        # Fallback: en azından güncel sekme
        tabs = [("current", GID_CURRENT, "9999-99")]
        errors.append("tab discovery empty — current gid only")

    print(f"month tabs={len(tabs)} from={REVENUE_TARGETS_HISTORY_FROM}", flush=True)
    merged: dict[tuple[str, str], dict] = {}
    for i, (name, gid, period_key) in enumerate(tabs, 1):
        t0 = time.time()
        text = export_gid_csv(opener, gid)
        if not text:
            errors.append(f"{name}/{gid}: export failed")
            print(f"[{i}/{len(tabs)}] FAIL {name} gid={gid}", flush=True)
            continue
        if gid == GID_CURRENT or period_key.endswith("-08") and "2026-08" == period_key:
            current_csv = text
        rows = parse_revenue_targets_csv(text, period_hint=name)
        for r in rows:
            pk = str(r.get("period_key") or "")
            proj = str(r.get("project") or "")
            if pk and proj:
                merged[(pk, proj)] = r
        print(
            f"[{i}/{len(tabs)}] OK {name} gid={gid} rows={len(rows)} "
            f"{round(time.time() - t0, 1)}s",
            flush=True,
        )
        time.sleep(0.15)

    if current_csv is None:
        text = export_gid_csv(opener, GID_CURRENT)
        if text:
            current_csv = text
            for r in parse_revenue_targets_csv(text):
                merged[(str(r.get("period_key")), str(r.get("project")))] = r

    rows_out = sorted(merged.values(), key=lambda r: (r.get("period_key") or "", r.get("project") or ""))
    return rows_out, current_csv, errors


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


def post_ingest(rows: list[dict], csv_text: str | None) -> dict:
    token = _ingest_token()
    try:
        local = save_ingested_revenue_targets(
            csv_text,
            rows=rows,
            source="mac_firefox_cookies",
            source_url=REVENUE_TARGETS_SHEET_URL,
        )
    except Exception as exc:
        local = {"ok": False, "message": str(exc)[:200]}

    OUT_ROWS.parent.mkdir(parents=True, exist_ok=True)
    OUT_ROWS.write_text(
        json.dumps({"rows": rows, "count": len(rows)}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if not token:
        return {
            "ok": bool(local.get("ok")),
            "local": local,
            "message": "NOTIFICATION_INGEST_TOKEN yok — yalnız lokal cache",
        }

    payload = json.dumps(
        {
            "rows": rows,
            "csv": csv_text,
            "source": "mac_firefox_cookies",
            "source_url": REVENUE_TARGETS_SHEET_URL,
        },
        ensure_ascii=False,
    ).encode("utf-8")
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
            with urllib.request.urlopen(req, timeout=120) as resp:
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


def run_sync(
    *,
    ingest: bool = False,
    headed: bool = False,
    current_only: bool = False,
) -> dict:
    """Sistem Firefox oturumu ile aylık sekmeler — Nightly yok, manuel yok."""
    _ = headed
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

    rows, current_csv, errors = scrape_history_rows(profile, current_only=current_only)
    if not rows:
        return {
            "ok": False,
            "message": "Hiç satır parse edilemedi",
            "errors": errors,
            "profile": str(profile),
            "sheet": REVENUE_TARGETS_SHEET_URL,
        }

    if current_csv:
        _save_csv(current_csv)

    period_keys = sorted({str(r.get("period_key") or "") for r in rows if r.get("period_key")})
    print(
        f"OK · parsed={len(rows)} periods={len(period_keys)} "
        f"from={period_keys[0] if period_keys else '?'} to={period_keys[-1] if period_keys else '?'}",
        flush=True,
    )
    if errors:
        print(f"warnings={len(errors)} sample={errors[:5]}", flush=True)

    out: dict = {
        "ok": True,
        "csv": str(OUT_CSV) if current_csv else None,
        "parsed": len(rows),
        "period_keys": period_keys,
        "errors": errors,
        "browser": "system_firefox_cookies",
        "profile": str(profile),
    }
    if ingest:
        ing = post_ingest(rows, current_csv)
        out["ingest"] = ing
        print(f"ingest={ing}", flush=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Revenue targets — MCM aylık sekmeler (system Firefox cookies)"
    )
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument(
        "--current-only",
        action="store_true",
        help="Sadece güncel ay sekmesi (hızlı KPI)",
    )
    parser.add_argument("--headless", action="store_true", help="(uyumluluk, yok sayılır)")
    parser.add_argument("--login", action="store_true", help="(kaldırıldı) --sync ile aynı")
    args = parser.parse_args()
    if not (args.sync or args.login or args.ingest):
        args.sync = True

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

    result = run_sync(
        ingest=bool(args.ingest or args.sync),
        headed=not args.headless,
        current_only=bool(args.current_only),
    )
    print(result, flush=True)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
