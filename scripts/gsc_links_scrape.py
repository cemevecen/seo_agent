#!/usr/bin/env python3
"""Google Search Console Links scrape (Mac bridge).

GSC Links drilldown tablolarını Playwright ile çeker → Railway ingest.

Özellikler (döviz + sinemalar):
  sc-domain:doviz.com, sc-domain:m.doviz.com
  https://www.sinemalar.com/, https://m.sinemalar.com/

Türler: EXTERNAL · DOMAIN · ANCHOR_TEXT · INTERNAL

Örnek:
  .venv/bin/python scripts/gsc_links_scrape.py --login
  .venv/bin/python scripts/gsc_links_scrape.py --sync --ingest
  .venv/bin/python scripts/gsc_links_scrape.py --site doviz --type EXTERNAL --ingest

Env:
  GSC_LINKS_PROFILE_DIR  (default: ~/.seo-agent/play-console-profile — aynı Google oturumu)
  GSC_LINKS_INGEST_URL
  NOTIFICATION_INGEST_TOKEN
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
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_dotenv()

PROFILE_DIR = Path(
    os.environ.get("GSC_LINKS_PROFILE_DIR")
    or os.environ.get("PLAY_CONSOLE_PROFILE_DIR")
    or str(Path.home() / ".seo-agent" / "play-console-profile")
).expanduser()

INGEST_URL = (
    os.environ.get("GSC_LINKS_INGEST_URL")
    or os.environ.get("PLAY_CONSOLE_INGEST_URL", "").replace("play-console", "gsc-links")
    or "https://projectcontrol.up.railway.app/api/gsc-links/ingest"
).strip()

LINK_TYPES: tuple[str, ...] = ("EXTERNAL", "DOMAIN", "ANCHOR_TEXT", "INTERNAL")

# site_key → panel Site.domain eşlemesi
PROPERTIES: list[dict[str, str]] = [
    {
        "site_key": "doviz",
        "site_domain": "www.doviz.com",
        "resource_id": "sc-domain:doviz.com",
        "label": "doviz.com (domain)",
    },
    {
        "site_key": "doviz",
        "site_domain": "www.doviz.com",
        "resource_id": "sc-domain:m.doviz.com",
        "label": "m.doviz.com",
    },
    {
        "site_key": "sinemalar",
        "site_domain": "www.sinemalar.com",
        "resource_id": "https://www.sinemalar.com/",
        "label": "www.sinemalar.com",
    },
    {
        "site_key": "sinemalar",
        "site_domain": "www.sinemalar.com",
        "resource_id": "https://m.sinemalar.com/",
        "label": "m.sinemalar.com",
    },
]


def _ingest_token() -> str:
    return (
        os.environ.get("GSC_LINKS_INGEST_TOKEN")
        or os.environ.get("NOTIFICATION_INGEST_TOKEN")
        or os.environ.get("PLAY_CONSOLE_INGEST_TOKEN")
        or os.environ.get("BRIDGE_INGEST_TOKEN")
        or ""
    ).strip()


def _drilldown_url(resource_id: str, link_type: str) -> str:
    rid = quote(resource_id, safe="")
    return (
        "https://search.google.com/search-console/links/drilldown"
        f"?resource_id={rid}&type={link_type}&target=&domain=&hl=en"
    )


def _parse_tr_int(raw: str) -> int:
    s = (raw or "").strip().replace("\u00a0", " ").replace(" ", "")
    if not s or s in {"-", "—", "~", "N/A"}:
        return 0
    s = s.replace(".", "").replace(",", "")
    m = re.search(r"-?\d+", s)
    if not m:
        return 0
    try:
        return int(m.group(0))
    except ValueError:
        return 0


_PUA_RE = re.compile(r"[\ue000-\uf8ff\u0000-\u001f]")


def _clean_cell(raw: str) -> str:
    """Google Translate / material icon private-use chars strip."""
    s = _PUA_RE.sub("", raw or "")
    return re.sub(r"\s+", " ", s).strip()


def _normalize_rows(link_type: str, headers: list[str], rows: list[list[str]]) -> list[dict[str, Any]]:
    lt = (link_type or "").upper()
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        row = [_clean_cell(c) for c in row]
        if not row or not any((c or "").strip() for c in row):
            continue
        if lt == "EXTERNAL":
            target = (row[0] or "").strip()
            if not target:
                continue
            incoming = _parse_tr_int(row[1] if len(row) > 1 else "0")
            sites = _parse_tr_int(row[2] if len(row) > 2 else "0")
            out.append(
                {
                    "target_url": target,
                    "incoming_links": incoming,
                    "linking_sites": sites,
                }
            )
        elif lt == "INTERNAL":
            target = (row[0] or "").strip()
            if not target:
                continue
            incoming = _parse_tr_int(row[1] if len(row) > 1 else "0")
            out.append(
                {
                    "target_url": target,
                    "incoming_links": incoming,
                    "linking_sites": 0,
                }
            )
        elif lt == "DOMAIN":
            domain = (row[0] or "").strip().lower()
            if not domain:
                continue
            linking_pages = _parse_tr_int(row[1] if len(row) > 1 else "0")
            target_pages = _parse_tr_int(row[2] if len(row) > 2 else "0")
            out.append(
                {
                    "linking_site": domain,
                    "linking_pages": linking_pages,
                    "target_pages": target_pages,
                }
            )
        elif lt == "ANCHOR_TEXT":
            if len(row) >= 2 and re.match(r"^\d+$", (row[0] or "").strip()):
                rank = _parse_tr_int(row[0])
                text = (row[1] or "").strip()
            else:
                rank = i + 1
                text = (row[0] or "").strip()
            if not text:
                continue
            out.append({"rank": rank, "anchor_text": text})
        else:
            continue
    return out


def _looks_signed_in(page) -> bool:
    try:
        url = (page.url or "").lower()
        title = (page.title() or "").lower()
        body = ""
        try:
            body = (page.inner_text("body") or "")[:1200].lower()
        except Exception:
            pass
        if "accounts.google.com" in url or "signin" in url:
            return False
        if "sign in" in title or "oturum aç" in title:
            return False
        if "email or phone" in body or "e-posta veya telefon" in body:
            return False
        return "search.google.com/search-console" in url
    except Exception:
        return False


def _launch_context(*, headed: bool):
    from playwright.sync_api import sync_playwright

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (PROFILE_DIR / name).unlink(missing_ok=True)
        except Exception:
            pass
    pw = sync_playwright().start()
    channel = (os.environ.get("GSC_LINKS_BROWSER_CHANNEL") or os.environ.get("PLAY_CONSOLE_BROWSER_CHANNEL") or "chrome").strip()
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(PROFILE_DIR),
        "headless": not headed,
        "viewport": {"width": 1440, "height": 1100},
        "locale": "tr-TR",
        "accept_downloads": True,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    if channel and channel.lower() not in ("0", "none", "chromium"):
        launch_kwargs["channel"] = channel
    try:
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    except Exception:
        launch_kwargs.pop("channel", None)
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
    return pw, context


def run_login_interactive(timeout_sec: int = 600) -> dict[str, Any]:
    url = _drilldown_url("sc-domain:doviz.com", "EXTERNAL")
    pw, context = _launch_context(headed=True)
    try:
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        print(
            f"Tarayıcıda GSC giriş yap (cemevecen@nokta.com). "
            f"Links tablosu açılınca {timeout_sec}s içinde otomatik kapanır.",
            flush=True,
        )
        deadline = time.time() + max(60, timeout_sec)
        while time.time() < deadline:
            if _looks_signed_in(page):
                try:
                    page.wait_for_selector("table tbody tr", timeout=15_000)
                except Exception:
                    pass
                time.sleep(3)
                print(f"Login OK · {page.url}", flush=True)
                return {"ok": True, "url": page.url, "profile": str(PROFILE_DIR)}
            time.sleep(2)
        return {
            "ok": False,
            "message": "Login zaman aşımı — tekrar --login dene",
            "url": page.url,
            "profile": str(PROFILE_DIR),
        }
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _extract_page_payload(page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
      const out = { headers: [], rows: [], kpis: {}, title: '', row_count: 0 };
      const h = document.querySelector('h1, [role=heading]');
      out.title = (h && h.innerText || '').trim();
      const table = document.querySelector('table');
      if (!table) return out;
      out.headers = [...table.querySelectorAll('thead th, thead td')]
        .map(el => (el.innerText || '').trim().replace(/\\s+/g, ' '));
      const bodyRows = [...table.querySelectorAll('tbody tr')];
      out.row_count = bodyRows.length;
      out.rows = bodyRows.map(tr =>
        [...tr.querySelectorAll('td')].map(td => (td.innerText || '').trim().replace(/\\s+/g, ' '))
      );
      // KPI: label + büyük sayı yan yana
      const bodyText = (document.body && document.body.innerText) || '';
      const kpiMatch = bodyText.match(/Toplam[^\\n]{0,40}\\n\\s*([\\d.\\s]+)/i)
        || bodyText.match(/Total[^\\n]{0,40}\\n\\s*([\\d.\\s]+)/i);
      if (kpiMatch) out.kpis.total_links = (kpiMatch[1] || '').trim();
      const topMatch = bodyText.match(/En önemli hedef sayfalar[^\\n]{0,20}\\n\\s*([\\d.\\s]+)/i)
        || bodyText.match(/Top linked pages[^\\n]{0,20}\\n\\s*([\\d.\\s]+)/i);
      if (topMatch) out.kpis.top_target_pages = (topMatch[1] || '').trim();
      return out;
    }"""
    )


def scrape_one(
    page,
    *,
    resource_id: str,
    link_type: str,
    wait_ms: int = 5500,
) -> dict[str, Any]:
    url = _drilldown_url(resource_id, link_type)
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    try:
        page.wait_for_selector("table tbody tr", timeout=45_000)
    except Exception:
        pass
    page.wait_for_timeout(max(2000, wait_ms))
    if not _looks_signed_in(page):
        return {
            "ok": False,
            "needs_login": True,
            "message": "GSC oturumu yok — scripts/gsc_links_scrape.py --login",
            "resource_id": resource_id,
            "link_type": link_type,
            "url": page.url,
        }
    raw = _extract_page_payload(page)
    rows = _normalize_rows(link_type, raw.get("headers") or [], raw.get("rows") or [])
    kpis_raw = raw.get("kpis") or {}
    kpis = {
        "total_links": _parse_tr_int(str(kpis_raw.get("total_links") or "")),
        "top_target_pages": _parse_tr_int(str(kpis_raw.get("top_target_pages") or "")),
    }
    return {
        "ok": bool(rows),
        "needs_login": False,
        "message": f"{link_type} · {len(rows)} satır" if rows else f"{link_type} · tablo boş",
        "resource_id": resource_id,
        "link_type": link_type,
        "url": url,
        "title": raw.get("title") or "",
        "headers": raw.get("headers") or [],
        "row_count": len(rows),
        "rows": rows,
        "kpis": kpis,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def scrape_gsc_links(
    *,
    headed: bool | None = None,
    site_filter: str | None = None,
    type_filter: str | None = None,
    resource_filter: str | None = None,
) -> dict[str, Any]:
    env_hl = (os.environ.get("GSC_LINKS_HEADLESS") or os.environ.get("PLAY_CONSOLE_HEADLESS") or "").strip().lower()
    if headed is None:
        headed = env_hl not in ("1", "true", "yes")

    props = list(PROPERTIES)
    if site_filter:
        sk = site_filter.strip().lower()
        props = [p for p in props if p["site_key"] == sk or sk in p["site_domain"]]
    if resource_filter:
        rf = resource_filter.strip()
        props = [p for p in props if p["resource_id"] == rf]

    types = list(LINK_TYPES)
    if type_filter:
        tf = type_filter.strip().upper()
        types = [t for t in LINK_TYPES if t == tf]
        if not types:
            return {"ok": False, "message": f"Geçersiz type: {type_filter}"}

    if not props:
        return {"ok": False, "message": "Filtreye uyan property yok"}

    pw, context = _launch_context(headed=headed)
    snapshots: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        page = context.pages[0] if context.pages else context.new_page()
        first = props[0]
        warm = scrape_one(page, resource_id=first["resource_id"], link_type=types[0], wait_ms=4000)
        if warm.get("needs_login"):
            return {
                "ok": False,
                "needs_login": True,
                "message": warm.get("message") or "Login gerekli",
                "snapshots": [],
                "profile": str(PROFILE_DIR),
            }
        warm_key = (first["resource_id"], types[0])
        for prop in props:
            for lt in types:
                key = (prop["resource_id"], lt)
                if key == warm_key:
                    one = warm
                else:
                    try:
                        one = scrape_one(page, resource_id=prop["resource_id"], link_type=lt)
                    except Exception as exc:  # noqa: BLE001
                        msg = f"{prop['label']} · {lt}: {exc}"
                        errors.append(msg)
                        print(f"ERR {msg}", flush=True)
                        continue
                if one.get("needs_login"):
                    return {
                        "ok": False,
                        "needs_login": True,
                        "message": one.get("message"),
                        "snapshots": snapshots,
                        "profile": str(PROFILE_DIR),
                    }
                snap = {
                    **one,
                    "site_key": prop["site_key"],
                    "site_domain": prop["site_domain"],
                    "property_label": prop["label"],
                }
                snapshots.append(snap)
                flag = "OK" if one.get("ok") else "EMPTY"
                print(f"{flag} {prop['label']} · {lt} · {one.get('row_count', 0)}", flush=True)
                if key != warm_key:
                    time.sleep(0.8)
    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass

    ok_n = sum(1 for s in snapshots if s.get("ok"))
    return {
        "ok": ok_n > 0 and not (errors and ok_n == 0),
        "needs_login": False,
        "message": f"GSC Links scrape · {ok_n}/{len(snapshots)} snapshot"
        + (f" · {len(errors)} hata" if errors else ""),
        "snapshots": snapshots,
        "errors": errors,
        "profile": str(PROFILE_DIR),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "gsc_links_bridge",
    }


def ingest_scrape_result(result: dict[str, Any]) -> dict[str, Any]:
    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    payload = {
        "source": result.get("source") or "gsc_links_bridge",
        "scraped_at": result.get("scraped_at") or datetime.now(timezone.utc).isoformat(),
        "snapshots": result.get("snapshots") or [],
        "message": result.get("message") or "",
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        INGEST_URL,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "X-Notification-Ingest-Token": token,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                parsed = {"ok": True, "raw": body[:500]}
            parsed["http_status"] = getattr(resp, "status", 200)
            return parsed
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:800]
        return {"ok": False, "http_status": exc.code, "message": detail or str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="GSC Links scrape")
    parser.add_argument("--login", action="store_true")
    parser.add_argument("--sync", action="store_true", help="Tüm property × type scrape")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--site", default="", help="doviz | sinemalar")
    parser.add_argument("--type", default="", dest="link_type", help="EXTERNAL|DOMAIN|ANCHOR_TEXT|INTERNAL")
    parser.add_argument("--resource", default="", help="Tam resource_id")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args(argv)

    if args.login:
        out = run_login_interactive()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if out.get("ok") else 1

    headed: bool | None = None
    if args.headed:
        headed = True
    elif args.headless:
        headed = False

    result = scrape_gsc_links(
        headed=headed,
        site_filter=args.site or None,
        type_filter=args.link_type or None,
        resource_filter=args.resource or None,
    )
    if args.ingest and result.get("ok"):
        ing = ingest_scrape_result(result)
        result["ingest"] = ing
        if not ing.get("ok"):
            result["ok"] = False
            result["message"] = (result.get("message") or "") + " · ingest: " + (ing.get("message") or "fail")
    # compact stdout
    compact = {
        "ok": result.get("ok"),
        "needs_login": result.get("needs_login"),
        "message": result.get("message"),
        "snapshot_count": len(result.get("snapshots") or []),
        "errors": result.get("errors") or [],
        "ingest": result.get("ingest"),
    }
    print(json.dumps(compact, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
