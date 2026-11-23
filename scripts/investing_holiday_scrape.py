#!/usr/bin/env python3
"""Investing.com borsa tatil takvimi scrape → Project Control ingest.

Tüm Dünya + geniş tarih aralığı. Cloudflare bazen headed Firefox ister;
kayıt popup'ı otomatik kapatılır.

Örnek:
  .venv/bin/python scripts/investing_holiday_scrape.py --ingest
  .venv/bin/python scripts/investing_holiday_scrape.py --headed --ingest
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
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

SOURCE_URL = "https://tr.investing.com/holiday-calendar/"
INGEST_URL = (
    os.environ.get("HOLIDAY_CALENDAR_INGEST_URL")
    or os.environ.get("PLAY_CONSOLE_INGEST_URL", "").replace("play-console", "holiday-calendar")
    or "https://projectcontrol.up.railway.app/api/holiday-calendar/ingest"
).strip()
TR = ZoneInfo("Europe/Istanbul")


def _ingest_token() -> str:
    return (
        os.environ.get("HOLIDAY_CALENDAR_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("PLAY_CONSOLE_INGEST_TOKEN")
        or os.environ.get("BRIDGE_INGEST_TOKEN")
        or ""
    ).strip()


def _dismiss(page: Any) -> None:
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    for sel in (
        "i.popupCloseIcon",
        ".popupCloseIcon",
        "#PromoteSignUpPopUp .popupCloseIcon",
        "#onetrust-accept-btn-handler",
        "button:has-text('Kabul')",
        "button:has-text('Accept')",
        "button:has-text('Anladım')",
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible(timeout=350):
                loc.click(timeout=1200, force=True)
        except Exception:
            continue
    try:
        page.evaluate(
            """() => {
              const killers = [
                '#PromoteSignUpPopUp', '#registrationOverlay', '.blackOverlay',
                '#orangeRegisterWrap', '.js-registration-popup'
              ];
              for (const sel of killers) {
                document.querySelectorAll(sel).forEach((el) => el.remove());
              }
              document.querySelectorAll('[class*="SignUp"], [id*="SignUp"]').forEach((el) => {
                const t = el.innerText || '';
                if (/ücretsiz|kayıt ol|facebook ile|google ile/i.test(t)) el.remove();
              });
              document.body.style.overflow = 'auto';
            }"""
        )
    except Exception:
        pass


def _wait_ready(page: Any, timeout_sec: int = 120) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            if page.locator("#holidayCalendarData").count() > 0:
                return True
        except Exception:
            pass
        # Cloudflare turnstile — tıkla (başarısız olabilir)
        try:
            for fr in page.frames:
                if "challenges.cloudflare" in (fr.url or ""):
                    fr.locator("input[type=checkbox], label, body").first.click(timeout=1500)
        except Exception:
            pass
        page.wait_for_timeout(2000)
    return False


def _parse_rows(page: Any) -> list[dict[str, Any]]:
    return page.evaluate(
        """() => {
          const rows = [...document.querySelectorAll('#holidayCalendarData tbody tr')];
          let lastDate = '';
          const out = [];
          for (const tr of rows) {
            const tds = [...tr.querySelectorAll('td')];
            if (tds.length < 4) continue;
            let d = (tds[0].innerText || '').trim();
            if (d) lastDate = d; else d = lastDate;
            const flag = tds[1].querySelector('.ceFlags');
            const flagClass = flag
              ? [...flag.classList].filter((c) => c !== 'ceFlags' && c !== 'float_lang_base_1')[0] || ''
              : '';
            out.push({
              date_tr: d,
              country: (tds[1].innerText || '').trim(),
              country_flag: flagClass,
              country_href: tds[1].querySelector('a')?.getAttribute('href') || '',
              exchange: (tds[2].innerText || '').trim(),
              holiday: (tds[3].innerText || '').trim(),
            });
          }
          return out;
        }"""
    )


def _parse_fragment(page: Any, html: str) -> list[dict[str, Any]]:
    if not html or "<tr" not in html:
        return []
    return page.evaluate(
        """(html) => {
          const wrap = document.createElement('table');
          wrap.innerHTML = '<tbody>' + html + '</tbody>';
          const rows = [...wrap.querySelectorAll('tr')];
          let lastDate = '';
          const out = [];
          for (const tr of rows) {
            const tds = [...tr.querySelectorAll('td')];
            if (tds.length < 4) continue;
            let d = (tds[0].innerText || '').trim();
            if (d) lastDate = d; else d = lastDate;
            const flag = tds[1].querySelector('.ceFlags');
            const flagClass = flag
              ? [...flag.classList].filter((c) => c !== 'ceFlags' && c !== 'float_lang_base_1')[0] || ''
              : '';
            out.push({
              date_tr: d,
              country: (tds[1].innerText || '').trim(),
              country_flag: flagClass,
              country_href: tds[1].querySelector('a')?.getAttribute('href') || '',
              exchange: (tds[2].innerText || '').trim(),
              holiday: (tds[3].innerText || '').trim(),
            });
          }
          return out;
        }""",
        html,
    )


def _fetch_filter(page: Any, *, date_from: str, date_to: str, limit_from: int = 0) -> dict[str, Any]:
    return page.evaluate(
        """async ({dateFrom, dateTo, limitFrom}) => {
          const body = new URLSearchParams({
            country: '',
            currentTab: 'custom',
            limit_from: String(limitFrom),
            dateFrom,
            dateTo,
          });
          const resp = await fetch('/holiday-calendar/Service/getCalendarFilteredData', {
            method: 'POST',
            credentials: 'include',
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
              'X-Requested-With': 'XMLHttpRequest',
            },
            body: body.toString(),
          });
          const text = await resp.text();
          try {
            const j = JSON.parse(text);
            const data = j.data || '';
            if (limitFrom === 0) {
              const tbody = document.querySelector('#holidayCalendarData tbody');
              if (tbody && data) tbody.innerHTML = data;
            }
            return { status: resp.status, data, keys: Object.keys(j) };
          } catch (e) {
            if (limitFrom === 0 && text.includes('<tr')) {
              const tbody = document.querySelector('#holidayCalendarData tbody');
              if (tbody) tbody.innerHTML = text;
            }
            return { status: resp.status, data: text };
          }
        }""",
        {"dateFrom": date_from, "dateTo": date_to, "limitFrom": limit_from},
    )


def scrape(*, headed: bool = True, lookback_days: int = 14, ahead_days: int = 400) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    from backend.services.scrape_browser import launch_ephemeral

    today = datetime.now(TR).date()
    date_from = (today - timedelta(days=max(0, lookback_days))).isoformat()
    date_to = (today + timedelta(days=max(30, ahead_days))).isoformat()

    with sync_playwright() as p:
        browser, context = launch_ephemeral(
            p,
            headed=headed,
            viewport={"width": 1440, "height": 1100},
            locale="tr-TR",
            timezone_id="Europe/Istanbul",
        )
        try:
            page = context.new_page()
            print(f"Holiday scrape · {SOURCE_URL}", flush=True)
            page.goto(SOURCE_URL, wait_until="domcontentloaded", timeout=120000)
            if not _wait_ready(page, timeout_sec=120):
                return {
                    "ok": False,
                    "message": "Cloudflare / sayfa hazır değil (#holidayCalendarData yok)",
                    "rows": [],
                }
            _dismiss(page)
            page.wait_for_timeout(400)
            _dismiss(page)
            try:
                page.select_option("#countrySelectDropdown", value="")
            except Exception:
                pass

            first = _fetch_filter(page, date_from=date_from, date_to=date_to, limit_from=0)
            print(f"  filter status={first.get('status')} data_len={len(first.get('data') or '')}", flush=True)
            page.wait_for_timeout(600)
            _dismiss(page)

            all_rows: list[dict[str, Any]] = list(_parse_rows(page) or [])
            for lim in (200, 400, 600, 800, 1000, 1200):
                more = _fetch_filter(page, date_from=date_from, date_to=date_to, limit_from=lim)
                html = str(more.get("data") or "")
                frag = _parse_fragment(page, html)
                print(f"  limit_from={lim} +{len(frag)}", flush=True)
                if not frag:
                    break
                all_rows.extend(frag)
                if len(frag) < 40:
                    break

            # dedupe
            seen: set[tuple[str, str, str, str]] = set()
            rows: list[dict[str, Any]] = []
            for r in all_rows:
                key = (
                    str(r.get("date_tr") or ""),
                    str(r.get("country") or ""),
                    str(r.get("exchange") or ""),
                    str(r.get("holiday") or ""),
                )
                if key in seen or not key[0] or not key[1] or not key[3]:
                    continue
                seen.add(key)
                rows.append(r)

            return {
                "ok": bool(rows),
                "source": SOURCE_URL,
                "country": "all",
                "date_from": date_from,
                "date_to": date_to,
                "fetched_at": datetime.now(TR).isoformat(),
                "rows": rows,
                "row_count": len(rows),
                "message": f"{len(rows)} satır" if rows else "satır yok",
            }
        finally:
            context.close()
            browser.close()


def post_ingest(payload: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    rows = payload.get("rows") or []
    if not rows:
        return {"ok": False, "message": "Ingest için satır yok"}
    body = json.dumps(
        {
            "source": payload.get("source") or SOURCE_URL,
            "country": payload.get("country") or "all",
            "date_from": payload.get("date_from") or "",
            "date_to": payload.get("date_to") or "",
            "fetched_at": payload.get("fetched_at") or "",
            "rows": rows,
        },
        ensure_ascii=False,
    ).encode("utf-8")
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
            return {"ok": bool(data.get("ok") or data.get("row_count")), "ingest": data}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:400]
        return {"ok": False, "message": f"ingest HTTP {exc.code}: {detail}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Investing holiday calendar scrape")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--headed", action="store_true", default=True)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--ahead-days", type=int, default=400)
    parser.add_argument("--out", type=str, default="")
    args = parser.parse_args()
    headed = not bool(args.headless)
    payload = scrape(
        headed=headed,
        lookback_days=int(args.lookback_days),
        ahead_days=int(args.ahead_days),
    )
    print(payload.get("message") or payload, flush=True)
    if args.out:
        Path(args.out).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    if args.ingest:
        if not payload.get("ok"):
            print("ingest atlandı — scrape başarısız", flush=True)
            return 2
        ing = post_ingest(payload)
        print("ingest", ing, flush=True)
        return 0 if ing.get("ok") else 3
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
