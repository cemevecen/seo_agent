#!/usr/bin/env python3
"""SEO meta audit — Mac bridge Playwright scrape → Railway ingest.

GA4 top trafik URL listesini Railway'den alır, Playwright ile HTML çeker,
ortak skorlayıcıyla denetler, /api/seo-audit/ingest yazar.

  .venv/bin/python scripts/seo_audit_scrape.py --sync --ingest
  .venv/bin/python scripts/seo_audit_scrape.py --sync --ingest --site-id 1 --limit 500

Env:
  SEO_AUDIT_API_BASE          (default: https://projectcontrol.up.railway.app)
  SEO_AUDIT_INGEST_URL
  SEO_AUDIT_URLS_URL
  NOTIFICATION_INGEST_TOKEN
  SEO_AUDIT_TOP_LIMIT         (default 500)
  SEO_AUDIT_CONCURRENCY       (default 3)
  SEO_AUDIT_TIMEOUT_MS        (default 20000)
  SEO_AUDIT_SITE_IDS          (default 1,2 — site_id verilmezse)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

API_BASE = (
    os.environ.get("SEO_AUDIT_API_BASE")
    or "https://projectcontrol.up.railway.app"
).rstrip("/")
INGEST_URL = (
    os.environ.get("SEO_AUDIT_INGEST_URL") or f"{API_BASE}/api/seo-audit/ingest"
).strip()
URLS_URL = (
    os.environ.get("SEO_AUDIT_URLS_URL") or f"{API_BASE}/api/seo-audit/urls"
).strip()
PROGRESS_URL = (
    os.environ.get("SEO_AUDIT_PROGRESS_URL") or f"{API_BASE}/api/seo-audit/progress"
).strip()
TOKEN = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _auth_headers() -> dict[str, str]:
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if TOKEN:
        h["X-Notification-Ingest-Token"] = TOKEN
        h["Authorization"] = f"Bearer {TOKEN}"
    return h


def _http_json(method: str, url: str, payload: dict | None = None, *, timeout: int = 120) -> dict[str, Any]:
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=_auth_headers(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")[:400]
        raise RuntimeError(f"HTTP {exc.code}: {err}") from exc


def push_progress(site_id: int, **kwargs: Any) -> None:
    if not TOKEN:
        return
    try:
        payload = {"site_id": int(site_id), **kwargs}
        _http_json("POST", PROGRESS_URL, payload, timeout=30)
    except Exception as exc:  # noqa: BLE001
        print(f"progress push skip: {exc}", flush=True)


def fetch_url_list(site_id: int, *, limit: int) -> dict[str, Any]:
    q = f"?site_id={int(site_id)}&limit={int(limit)}"
    return _http_json("GET", URLS_URL + q, timeout=180)


def list_owned_site_ids() -> list[int]:
    raw = (os.environ.get("SEO_AUDIT_SITE_IDS") or "1,2").strip()
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out or [1]


def _user_agent() -> str:
    return (
        os.environ.get("SEO_AUDIT_USER_AGENT")
        or "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0"
    )


def scrape_chunk(
    urls: list[str],
    *,
    timeout_ms: int,
    on_row=None,
) -> list[dict[str, Any]]:
    """Tek worker: bir Firefox, sırayla URL (Playwright sync thread-safe değil)."""
    from playwright.sync_api import sync_playwright

    from backend.collectors.site_audit import build_url_audit_from_html

    out: list[dict[str, Any]] = []
    if not urls:
        return out
    with sync_playwright() as p:
        from backend.services.scrape_browser import launch_ephemeral

        browser, context = launch_ephemeral(
            p,
            headed=False,
            user_agent=_user_agent(),
            viewport={"width": 1280, "height": 900},
        )
        try:
            page = context.new_page()
            for url in urls:
                try:
                    resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    time.sleep(0.15)
                    status = int(resp.status) if resp else 0
                    final = page.url or url
                    html = page.content()
                    ctype = "text/html"
                    if resp:
                        try:
                            ctype = str(resp.headers.get("content-type") or "text/html")
                        except Exception:
                            pass
                    row = build_url_audit_from_html(
                        url,
                        html=html,
                        final_url=final,
                        status_code=status,
                        content_type=ctype,
                    )
                    row["source"] = "seo_audit_scrape"
                except Exception as exc:  # noqa: BLE001
                    row = build_url_audit_from_html(
                        url, html="", final_url=url, status_code=0, content_type=""
                    )
                    row["error"] = str(exc)[:200]
                    row["source"] = "seo_audit_scrape"
                out.append(row)
                if on_row is not None:
                    try:
                        on_row(row)
                    except Exception:
                        pass
            context.close()
        finally:
            browser.close()
    return out


def scrape_site(site_id: int, *, limit: int, concurrency: int, timeout_ms: int) -> dict[str, Any]:
    import threading

    print(f"SEO scrape · site_id={site_id} limit={limit}", flush=True)
    push_progress(site_id, running=True, total=0, done=0, ok=0, error=0, current="URL listesi…")
    listing = fetch_url_list(site_id, limit=limit)
    urls = list(listing.get("urls") or [])
    domain = str(listing.get("domain") or "")
    if not urls:
        push_progress(site_id, running=False, current="URL yok", total=0, done=0)
        return {"ok": False, "site_id": site_id, "message": "URL listesi boş", "domain": domain}

    total = len(urls)
    push_progress(site_id, running=True, total=total, done=0, ok=0, error=0, current="Tarama başladı")
    workers = max(1, min(int(concurrency), 6))
    chunks: list[list[str]] = [[] for _ in range(workers)]
    for i, u in enumerate(urls):
        chunks[i % workers].append(u)

    rows: list[dict[str, Any]] = []
    state = {"ok": 0, "err": 0, "done": 0}
    state_lock = threading.Lock()

    def _on_row(row: dict[str, Any]) -> None:
        with state_lock:
            state["done"] += 1
            if int(row.get("status_code") or 0) == 200 and row.get("has_title"):
                state["ok"] += 1
            else:
                state["err"] += 1
            done = state["done"]
            ok = state["ok"]
            err = state["err"]
            cur = str(row.get("url") or "")
        if done % 10 == 0 or done == total:
            push_progress(
                site_id,
                running=True,
                total=total,
                done=done,
                ok=ok,
                error=err,
                current=cur,
            )
            print(f"  {done}/{total} · ok={ok} err={err}", flush=True)

    def _on_chunk(chunk: list[str]) -> list[dict[str, Any]]:
        return scrape_chunk(chunk, timeout_ms=timeout_ms, on_row=_on_row)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_on_chunk, c) for c in chunks if c]
        for fut in as_completed(futs):
            try:
                part = fut.result()
            except Exception as exc:  # noqa: BLE001
                print(f"chunk fail: {exc}", flush=True)
                part = []
            rows.extend(part)

    ok = state["ok"]
    err = state["err"]
    done = state["done"]

    # Sıra trafik sırasına yakın olsun (chunk karıştı)
    by_url = {str(r.get("url") or ""): r for r in rows}
    ordered = [by_url[u] for u in urls if u in by_url]
    for r in rows:
        u = str(r.get("url") or "")
        if u and u not in {x.get("url") for x in ordered}:
            ordered.append(r)
    rows = ordered

    collected_at = datetime.now(timezone.utc).isoformat()
    # replace_all yalnızca “tam” top-N turunda: kısmi smoke eski kayıtları silmesin
    top_default = int(os.environ.get("SEO_AUDIT_TOP_LIMIT") or 500)
    full_refresh = bool(len(rows) >= min(limit, top_default) and len(rows) >= 50)
    batch_size = 80
    saved = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        replace = full_refresh and (i + batch_size >= len(rows))
        ing = _http_json(
            "POST",
            INGEST_URL,
            {
                "site_id": site_id,
                "domain": domain,
                "rows": chunk,
                "replace_all": replace,
                "collected_at": collected_at,
                "trigger_source": "seo_audit_scrape",
            },
            timeout=180,
        )
        saved += int(ing.get("saved") or 0)
        print(f"ingest batch {i // batch_size + 1}: {ing.get('message')}", flush=True)

    push_progress(
        site_id,
        running=False,
        total=total,
        done=done,
        ok=ok,
        error=err,
        current=f"Tamam · {saved} kayıt",
    )
    return {
        "ok": True,
        "site_id": site_id,
        "domain": domain,
        "total": total,
        "ok_count": ok,
        "error_count": err,
        "saved": saved,
        "message": f"tarama {total} URL · kaydedilen {saved}",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SEO audit Mac bridge scrape")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--ingest", action="store_true", help="(uyumluluk) her zaman ingest eder")
    parser.add_argument("--site-id", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=0)
    args = parser.parse_args(argv)
    if not args.sync and not args.ingest:
        parser.print_help()
        return 2
    if not TOKEN:
        print("NOTIFICATION_INGEST_TOKEN gerekli", file=sys.stderr)
        return 1
    limit = int(args.limit or os.environ.get("SEO_AUDIT_TOP_LIMIT") or 500)
    concurrency = int(args.concurrency or os.environ.get("SEO_AUDIT_CONCURRENCY") or 3)
    timeout_ms = int(os.environ.get("SEO_AUDIT_TIMEOUT_MS") or 20000)
    site_ids = [args.site_id] if args.site_id else list_owned_site_ids()
    overall_ok = True
    for sid in site_ids:
        try:
            out = scrape_site(sid, limit=limit, concurrency=concurrency, timeout_ms=timeout_ms)
            print(json.dumps(out, ensure_ascii=False), flush=True)
            if not out.get("ok"):
                overall_ok = False
        except Exception as exc:  # noqa: BLE001
            overall_ok = False
            try:
                push_progress(sid, running=False, current=f"Hata: {exc}"[:180])
            except Exception:
                pass
            print(json.dumps({"ok": False, "site_id": sid, "message": str(exc)}), flush=True)
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
