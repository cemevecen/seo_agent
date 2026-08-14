#!/usr/bin/env python3
"""GSC Core Web Vitals — screenshot capture (Mac bridge).

Döviz + Sinemalar için overview + mobile/desktop summary PNG alır,
KPI özetini JSON ingest'e yazar, görselleri shots-ingest'e yükler.

  .venv/bin/python scripts/gsc_cwv_shots.py --sync --ingest --site doviz
  .venv/bin/python scripts/gsc_cwv_shots.py --sync --ingest

Env: NOTIFICATION_INGEST_TOKEN, GSC_CWV_SHOTS_INGEST_URL, GSC_CWV_INGEST_URL
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

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
        key, val = key.strip(), val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

SHOTS_DIR = Path.home() / ".seo-agent" / "cache" / "cwv-shots"
DEVICE_MOBILE = 2
DEVICE_DESKTOP = 1


def _shots_ingest_url() -> str:
    return (
        os.environ.get("GSC_CWV_SHOTS_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/gsc-cwv/shots-ingest"
    ).strip()


def _kpi_ingest_url() -> str:
    return (
        os.environ.get("GSC_CWV_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/gsc-cwv/ingest"
    ).strip()


def _token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _png_of(page, path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(path), full_page=False, type="png")
    return path.read_bytes()


def _screenshot_main(page, path: Path) -> bytes:
    """Ana içerik alanını yakala; yoksa viewport."""
    path.parent.mkdir(parents=True, exist_ok=True)
    selectors = (
        "[role='main']",
        "main",
        "[data-is-app-shell-content]",
        "#fcxPrimaryContent",
        ".i3WFpf",
    )
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if loc.count() and loc.is_visible(timeout=1500):
                loc.screenshot(path=str(path), type="png")
                return path.read_bytes()
        except Exception:
            continue
    return _png_of(page, path)


def _load_cwv_scrape():
    import importlib.util

    path = ROOT / "scripts" / "gsc_cwv_scrape.py"
    spec = importlib.util.spec_from_file_location("gsc_cwv_scrape_mod", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _last_updated_from_body(body: str) -> str:
    import re

    text = body or ""
    m = re.search(
        r"(?:Last updated|Son güncelleme|Updated)\s*[:\-]?\s*([^\n\r·|]{4,40})",
        text,
        flags=re.I,
    )
    if m:
        return (m.group(1) or "").strip()[:80]
    return ""


def capture_property_shots(page, prop: dict[str, str], cwv_mod: Any) -> dict[str, Any]:
    _cwv_url = cwv_mod._cwv_url
    _ensure_signed_in = cwv_mod._ensure_signed_in
    _parse_overview_counts = cwv_mod._parse_overview_counts
    _extract_page_meta = cwv_mod._extract_page_meta

    rid = prop["resource_id"]
    site_key = prop.get("site_key") or "site"
    out_dir = SHOTS_DIR / site_key
    out_dir.mkdir(parents=True, exist_ok=True)
    shots: dict[str, Path] = {}

    print(f"CWV shots · {prop.get('label') or rid}", flush=True)
    page.goto(_cwv_url(rid), wait_until="domcontentloaded", timeout=120_000)
    _ensure_signed_in(page, headed=True)
    time.sleep(3.5)
    try:
        page.evaluate("window.scrollTo(0, 120)")
    except Exception:
        pass
    time.sleep(1.0)

    meta = _extract_page_meta(page)
    body = str((meta or {}).get("body_head") or "")
    overview = _parse_overview_counts(body)
    last_upd = _last_updated_from_body(body)

    shots["full"] = out_dir / "full.png"
    _screenshot_main(page, shots["full"])
    print(f"  · full overview → {shots['full'].name}", flush=True)

    # Panel grafikleri = GSC device summary (mobile web / desktop web)
    for device, variant, label in (
        (DEVICE_MOBILE, "mobile", "Mobile web summary"),
        (DEVICE_DESKTOP, "desktop", "Desktop web summary"),
    ):
        path = out_dir / f"{variant}-summary.png"
        page.goto(
            _cwv_url(rid, "/summary", device=device),
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(4.0)
        _screenshot_main(page, path)
        shots[variant] = path
        print(f"  · {label} → {path.name}", flush=True)

    mobile_k = dict(overview.get("mobile") or {})
    desktop_k = dict(overview.get("desktop") or {})
    poor = int(mobile_k.get("poor") or 0) + int(desktop_k.get("poor") or 0)
    ni = int(mobile_k.get("needs_improvement") or 0) + int(
        desktop_k.get("needs_improvement") or 0
    )
    good = int(mobile_k.get("good") or 0) + int(desktop_k.get("good") or 0)

    return {
        "site_key": site_key,
        "site_domain": prop.get("site_domain") or "",
        "resource_id": rid,
        "label": prop.get("label") or rid,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": last_upd,
        "overview": overview,
        "chart_series": {},
        "mobile": {
            "kpis": mobile_k,
            "last_updated": last_upd,
            "issues": [],
            "issue_drilldowns": [],
            "good_urls": [],
        },
        "desktop": {
            "kpis": desktop_k,
            "last_updated": last_upd,
            "issues": [],
            "issue_drilldowns": [],
            "good_urls": [],
        },
        "amp": {"issues": [], "url_row_count": 0, "skipped": True},
        "totals": {"poor": poor, "needs_improvement": ni, "good": good},
        "source": "gsc_cwv_shots",
        "charts_only": True,
        "paths": {k: str(v) for k, v in shots.items()},
    }


def post_kpi_snapshot(capture: dict[str, Any]) -> dict[str, Any]:
    """KPI kartları için hafif JSON snapshot (AMP/issue drilldown yok)."""
    token = _token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    snap = {
        "site_key": capture.get("site_key") or "",
        "site_domain": capture.get("site_domain") or "",
        "resource_id": capture.get("resource_id") or "",
        "label": capture.get("label") or "",
        "scraped_at": capture.get("scraped_at") or "",
        "last_updated": capture.get("last_updated") or "",
        "overview": capture.get("overview") or {},
        "chart_series": capture.get("chart_series") or {},
        "mobile": capture.get("mobile") or {},
        "desktop": capture.get("desktop") or {},
        "amp": capture.get("amp") or {"issues": [], "url_row_count": 0, "skipped": True},
        "totals": capture.get("totals") or {},
        "source": "gsc_cwv_shots",
        "charts_only": True,
    }
    body_payload = {
        "source": "gsc_cwv_shots",
        "scraped_at": capture.get("scraped_at") or "",
        "snapshots": [snap],
    }
    try:
        resp = requests.post(
            _kpi_ingest_url(),
            headers={
                "X-Notification-Ingest-Token": token,
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            data=json.dumps(body_payload, ensure_ascii=False).encode("utf-8"),
            timeout=120,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"ok": False, "message": resp.text[:300]}
        body["http_status"] = resp.status_code
        body["ok"] = bool(body.get("ok")) and resp.status_code < 400
        return body
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:300]}


def post_shots(capture: dict[str, Any]) -> dict[str, Any]:
    token = _token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    paths = capture.get("paths") or {}
    open_handles = []
    try:
        mapping = (
            ("full", "full"),
            ("mobile", "mobile"),
            ("desktop", "desktop"),
        )
        files = []
        data = [
            ("site_key", capture.get("site_key") or ""),
            ("site_domain", capture.get("site_domain") or ""),
            ("source", "gsc_cwv_shots"),
            ("scraped_at", capture.get("scraped_at") or ""),
        ]
        for key, variant in mapping:
            p = Path(str(paths.get(key) or ""))
            if not p.is_file():
                continue
            fh = p.open("rb")
            open_handles.append(fh)
            files.append(("files", (f"{variant}.png", fh, "image/png")))
            data.append(("variants", variant))
        if not files:
            return {"ok": False, "message": "Yüklenecek PNG yok"}
        resp = requests.post(
            _shots_ingest_url(),
            headers={"X-Notification-Ingest-Token": token},
            data=data,
            files=files,
            timeout=180,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"ok": False, "message": resp.text[:300]}
        body["http_status"] = resp.status_code
        body["ok"] = bool(body.get("ok")) and resp.status_code < 400
        return body
    finally:
        for fh in open_handles:
            try:
                fh.close()
            except Exception:
                pass


def run_shots(
    *,
    site_filter: str = "",
    ingest: bool = True,
    headed: bool | None = None,
) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    from backend.services.scrape_browser import google_profile_dir, launch_persistent

    cwv_mod = _load_cwv_scrape()
    PROPERTIES = cwv_mod.PROPERTIES

    sk = (site_filter or "").strip().lower()
    props = PROPERTIES
    if sk:
        props = [p for p in PROPERTIES if p["site_key"] == sk or sk in (p["site_domain"] or "")]
    if not props:
        return {"ok": False, "message": f"site bulunamadı: {site_filter}"}

    if headed is None:
        headed = True

    captures: list[dict[str, Any]] = []
    pw = sync_playwright().start()
    ctx = None
    try:
        ctx = launch_persistent(pw, google_profile_dir(), headed=headed, locale="en-US")
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        for prop in props:
            try:
                cap = capture_property_shots(page, prop, cwv_mod)
                if ingest:
                    cap["kpi_ingest"] = post_kpi_snapshot(cap)
                    print(
                        f"  · KPI ingest · {cap.get('site_key')}: "
                        f"{(cap.get('kpi_ingest') or {}).get('message') or (cap.get('kpi_ingest') or {}).get('ok')}",
                        flush=True,
                    )
                    cap["ingest"] = post_shots(cap)
                    print(
                        f"  · shots ingest · {cap.get('site_key')}: "
                        f"{(cap.get('ingest') or {}).get('message') or (cap.get('ingest') or {}).get('ok')}",
                        flush=True,
                    )
                captures.append(cap)
            except Exception as exc:  # noqa: BLE001
                captures.append(
                    {
                        "site_key": prop.get("site_key"),
                        "ok": False,
                        "error": str(exc)[:300],
                    }
                )
                print(f"CWV shots hata: {exc}", flush=True)
    finally:
        if ctx is not None:
            try:
                ctx.close()
            except Exception:
                pass
        try:
            pw.stop()
        except Exception:
            pass

    ok_n = sum(
        1
        for c in captures
        if c.get("paths") and (not ingest or (c.get("ingest") or {}).get("ok"))
    )
    return {
        "ok": ok_n > 0,
        "kind": "gsc_cwv_shots",
        "captures": len(captures),
        "ok_captures": ok_n,
        "message": f"{ok_n}/{len(captures)} CWV shot set OK",
        "results": captures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GSC CWV screenshot capture")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--site", default="")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args(argv)
    if not args.sync:
        parser.print_help()
        return 2
    headed: bool | None = None
    if args.headed:
        headed = True
    elif args.headless:
        headed = False
    out = run_shots(site_filter=args.site, ingest=args.ingest, headed=headed)
    print(json.dumps({k: v for k, v in out.items() if k != "results"}, ensure_ascii=False))
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
