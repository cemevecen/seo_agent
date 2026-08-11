#!/usr/bin/env python3
"""doviz.com tarihsel tablo taraması → Project Control ingest.

Tablo görünümü + 01.01.2025 … bugün → Verileri Getir.
Playwright ile sayfayı açar, arşiv yanıtını veya tablo satırlarını okur.

Örnek:
  .venv/bin/python scripts/doviz_market_tarama.py --ingest
  .venv/bin/python scripts/doviz_market_tarama.py --key gram_altin --headed
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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

from backend.services.market_sheets_config import (  # noqa: E402
    MARKET_SHEET_SERIES,
    SERIES_BY_KEY,
    TARAMA_START_DATE,
)
from backend.services.market_sheets_sync import (  # noqa: E402
    parse_archive_payload,
    parse_historical_table_matrix,
)

INGEST_URL = (
    os.environ.get("MARKET_TARAMA_INGEST_URL")
    or os.environ.get("PLAY_CONSOLE_INGEST_URL", "").replace("play-console", "market-quotes")
    or "https://projectcontrol.up.railway.app/api/market-quotes/ingest"
).strip()

START_ISO = TARAMA_START_DATE
TR_TZ = ZoneInfo("Europe/Istanbul")


def _ingest_token() -> str:
    return (
        os.environ.get("MARKET_TARAMA_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("PLAY_CONSOLE_INGEST_TOKEN")
        or os.environ.get("BRIDGE_INGEST_TOKEN")
        or ""
    ).strip()


def _today_tr() -> date:
    return datetime.now(TR_TZ).date()


def _dismiss_overlays(page: Any) -> None:
    page.keyboard.press("Escape")
    selectors = (
        "#takeOverClose",
        "#takeOverCloseButtonInnerContainer",
        "button:has-text('Kabul')",
        "button:has-text('Tamam')",
        "button:has-text('Anladım')",
        "[aria-label='Kapat']",
        ".cookie-accept",
        ".modal .close",
    )
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible(timeout=400):
                loc.click(timeout=600)
        except Exception:
            continue
    try:
        page.evaluate(
            """() => {
              document.querySelectorAll('[class*="modal"], [class*="overlay"], [id*="login"]').forEach((el) => {
                const t = (el.innerText || '');
                if (t.includes('Hoş Geldin') || t.includes('Giriş Yap') || t.includes('Kayıt Ol')) {
                  el.style.display = 'none';
                  el.remove();
                }
              });
            }"""
        )
    except Exception:
        pass


def _set_date_inputs(page: Any, start_iso: str, end_iso: str) -> None:
    for cls, val in (
        ("historical-data-date-input-1", start_iso),
        ("historical-data-date-input-2", end_iso),
    ):
        loc = page.locator(f"input.{cls}, input[type='date'].{cls}, .{cls}").first
        try:
            loc.wait_for(state="attached", timeout=8000)
            loc.fill(val, timeout=4000)
            loc.evaluate(
                """(el, v) => {
                  el.value = v;
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                  el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                val,
            )
        except Exception:
            continue
    try:
        page.evaluate(
            """([start, end]) => {
              const a = document.querySelector('.historical-data-date-input-1, input.historical-data-date-input-1');
              const b = document.querySelector('.historical-data-date-input-2, input.historical-data-date-input-2');
              if (a) { a.value = start; a.dispatchEvent(new Event('input', { bubbles: true })); a.dispatchEvent(new Event('change', { bubbles: true })); }
              if (b) { b.value = end; b.dispatchEvent(new Event('input', { bubbles: true })); b.dispatchEvent(new Event('change', { bubbles: true })); }
            }""",
            [start_iso, end_iso],
        )
    except Exception:
        pass


def _click_tablo(page: Any) -> bool:
    locators = (
        "li.historical-data-filter",
        ".chart-time-filter.historical-data-filter",
        "li.chart-time-filter:has-text('Tablo')",
        "button:has-text('Tablo')",
        "a:has-text('Tablo')",
        "a:has-text('Tarihsel Veri')",
    )
    for sel in locators:
        try:
            loc = page.locator(sel).first
            if loc.count():
                loc.click(timeout=4000, force=True)
                return True
        except Exception:
            continue
    return False


def _read_table_matrix(page: Any) -> tuple[list[str], list[list[str]]]:
    try:
        data = page.evaluate(
            """() => {
              const table = document.querySelector('.historical-data table')
                || document.querySelector('table');
              if (!table) return { headers: [], rows: [] };
              const headers = [...table.querySelectorAll('thead th, thead td')].map(
                (el) => (el.innerText || '').trim()
              );
              const rows = [...table.querySelectorAll('tbody tr')].map((tr) =>
                [...tr.querySelectorAll('td')].map((td) => (td.innerText || '').trim())
              );
              return { headers, rows };
            }"""
        )
    except Exception:
        return [], []
    headers = list(data.get("headers") or [])
    rows = [list(r) for r in (data.get("rows") or []) if r]
    return headers, rows


def _rows_to_ingest(parsed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for item in parsed:
        d = item.get("report_date")
        if hasattr(d, "isoformat"):
            d = d.isoformat()
        close = item.get("close_price")
        if close is None or not d:
            continue
        row = {"report_date": str(d)[:10], "close_price": float(close)}
        open_p = item.get("open_price")
        if open_p is not None:
            row["open_price"] = float(open_p)
        out.append(row)
    return out


def tarama_series(page: Any, spec: Any, *, start_iso: str, end_iso: str) -> dict[str, Any]:
    archive_hits: list[dict[str, Any]] = []

    def _on_response(resp: Any) -> None:
        try:
            url = (resp.url or "").lower()
            if "/archive" not in url or "/assets/" not in url:
                return
            if resp.status != 200:
                return
            body = resp.json()
            if isinstance(body, dict):
                archive_hits.append(body)
        except Exception:
            return

    page.on("response", _on_response)
    try:
        page.goto(spec.source_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1800)
        _dismiss_overlays(page)
        page.wait_for_timeout(600)
        _click_tablo(page)
        page.wait_for_timeout(800)
        _dismiss_overlays(page)
        try:
            page.locator(".historical-data, .load-historical-data").first.wait_for(
                state="visible", timeout=10000
            )
        except Exception:
            pass
        _set_date_inputs(page, start_iso, end_iso)
        page.wait_for_timeout(400)
        before = len(archive_hits)
        clicked = False
        for sel in (".load-historical-data", "button:has-text('Verileri Getir')"):
            try:
                loc = page.locator(sel).first
                if loc.count():
                    loc.click(timeout=5000, force=True)
                    clicked = True
                    break
            except Exception:
                continue
        if clicked:
            deadline = time.time() + 25
            while time.time() < deadline:
                if len(archive_hits) > before:
                    break
                page.wait_for_timeout(400)
        page.wait_for_timeout(800)

        parsed: list[dict[str, Any]] = []
        source = ""
        for hit in reversed(archive_hits):
            parsed = parse_archive_payload(hit)
            if parsed:
                source = "archive"
                break
        if not parsed:
            headers, matrix = _read_table_matrix(page)
            parsed = parse_historical_table_matrix(headers, matrix)
            if parsed:
                source = "table"
        start_d = date.fromisoformat(start_iso)
        end_d = date.fromisoformat(end_iso)
        parsed = [r for r in parsed if start_d <= r["report_date"] <= end_d]
        rows = _rows_to_ingest(parsed)
        ok = bool(rows)
        return {
            "key": spec.key,
            "ok": ok,
            "source": source,
            "url": spec.source_url,
            "row_count": len(rows),
            "rows": rows,
            "min_date": rows[0]["report_date"] if rows else None,
            "max_date": rows[-1]["report_date"] if rows else None,
            "error": None if ok else "Tablo / arşiv boş",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "key": spec.key,
            "ok": False,
            "source": "",
            "url": spec.source_url,
            "row_count": 0,
            "rows": [],
            "error": str(exc),
        }
    finally:
        try:
            page.remove_listener("response", _on_response)
        except Exception:
            pass


def run_tarama(
    *,
    keys: list[str] | None = None,
    headed: bool = False,
    start_iso: str = START_ISO,
    end_iso: str | None = None,
) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    end_iso = end_iso or _today_tr().isoformat()
    specs = [s for s in MARKET_SHEET_SERIES if not keys or s.key in keys]
    results: list[dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context = browser.new_context(
            viewport={"width": 1440, "height": 1100},
            locale="tr-TR",
            timezone_id="Europe/Istanbul",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        try:
            for spec in specs:
                print(f"Piyasa tarama · {spec.key} · {spec.source_url}", flush=True)
                page = context.new_page()
                try:
                    out = tarama_series(page, spec, start_iso=start_iso, end_iso=end_iso)
                finally:
                    page.close()
                print(
                    f"  → {'ok' if out.get('ok') else 'hata'} "
                    f"{out.get('row_count') or 0} satır ({out.get('source') or out.get('error')})",
                    flush=True,
                )
                results.append(out)
        finally:
            context.close()
            browser.close()
    ok_count = sum(1 for r in results if r.get("ok"))
    return {
        "ok": ok_count == len(specs) and bool(specs),
        "ok_count": ok_count,
        "series_count": len(specs),
        "start": start_iso,
        "end": end_iso,
        "results": results,
    }


def post_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    series = []
    for item in payload.get("results") or []:
        if not item.get("ok") or not item.get("rows"):
            continue
        series.append({"key": item["key"], "rows": item["rows"]})
    if not series:
        return {"ok": False, "message": "Ingest için satır yok", "ingest": None}
    body = json.dumps({"series": series, "source": "doviz.com"}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Notification-Ingest-Token": token,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw else {}
            return {"ok": bool(data.get("ok") or data.get("ok_count")), "ingest": data}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:400]
        return {"ok": False, "message": f"ingest HTTP {exc.code}: {detail}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="doviz.com piyasa tablo taraması")
    parser.add_argument("--key", action="append", dest="keys", help="Seri anahtarı (tekrar edilebilir)")
    parser.add_argument("--ingest", action="store_true", help="Railway ingest")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--start", default=START_ISO)
    parser.add_argument("--end", default="")
    parser.add_argument("--dump", default="", help="JSON çıktı dosyası")
    args = parser.parse_args(argv)

    unknown = [k for k in (args.keys or []) if k not in SERIES_BY_KEY]
    if unknown:
        print(f"Bilinmeyen seri: {', '.join(unknown)}", file=sys.stderr)
        return 2

    payload = run_tarama(
        keys=args.keys,
        headed=args.headed,
        start_iso=args.start,
        end_iso=args.end or None,
    )
    if args.dump:
        Path(args.dump).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    if args.ingest:
        ing = post_ingest(payload)
        payload["ingest"] = ing
        print(
            f"Ingest · {'ok' if ing.get('ok') else 'hata'} · {ing.get('message') or ing.get('ingest')}",
            flush=True,
        )
        if not ing.get("ok"):
            return 1
    print(
        json.dumps(
            {
                "ok": payload.get("ok"),
                "ok_count": payload.get("ok_count"),
                "series_count": payload.get("series_count"),
                "results": [
                    {
                        "key": r.get("key"),
                        "ok": r.get("ok"),
                        "row_count": r.get("row_count"),
                        "source": r.get("source"),
                        "min_date": r.get("min_date"),
                        "max_date": r.get("max_date"),
                        "error": r.get("error"),
                    }
                    for r in payload.get("results") or []
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return 0 if int(payload.get("ok_count") or 0) > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
