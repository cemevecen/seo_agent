#!/usr/bin/env python3
"""App Store Connect Analytics scrape — Play Console bridge ile aynı model.

Mac'te Apple ID oturumu (persistent Chrome profile) ile ASC SPA network JSON
yakalanır → explorer_facts → Railway /api/asc-console/ingest.

  .venv/bin/python scripts/asc_console_scrape.py --login
  .venv/bin/python scripts/asc_console_scrape.py --sync --ingest

Env:
  ASC_CONSOLE_PROFILE_DIR   default ~/.seo-agent/asc-console-profile
  ASC_CONSOLE_INGEST_URL    default …/api/asc-console/ingest
  NOTIFICATION_INGEST_TOKEN
  ASC_CONSOLE_APP_ID        default 465599322
  ASC_CONSOLE_HEADLESS=1    (varsayılan headed — Apple oturumu için)
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

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

APP_ID = (os.environ.get("ASC_CONSOLE_APP_ID") or "465599322").strip()
BUNDLE_ID = (os.environ.get("ASC_CONSOLE_BUNDLE_ID") or "com.nokta.Finans.Takip").strip()
PROFILE_DIR = Path(
    os.environ.get("ASC_CONSOLE_PROFILE_DIR")
    or (Path.home() / ".seo-agent" / "asc-console-profile")
).expanduser()
INGEST_URL = (
    os.environ.get("ASC_CONSOLE_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/asc-console/ingest"
).strip()

# ASC web private API measureKey → warehouse metric
MEASURE_MAP: dict[str, str] = {
    "units": "units",
    "redownloads": "redownloads",
    "conversionRate": "conversion_rate",
    "pageViewCount": "page_views",
    "impressionsTotal": "impressions",
    "iap": "iap",
    "payingUsers": "paying_users",
    "proceeds": "proceeds",
    "subscription-state-plans-active": "active_subscriptions",
}
# Batch grupları (tek POST’ta birden fazla measure)
MEASURE_BATCHES: list[list[str]] = [
    ["units", "redownloads", "conversionRate", "impressionsTotal", "pageViewCount"],
    ["iap", "payingUsers", "proceeds"],
    ["subscription-state-plans-active"],
]
ANALYTICS_MEASURES_URL = (
    "https://appstoreconnect.apple.com/analytics/api/v1/data/app/detail/measures"
)


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _need_login(page_url: str, title: str, body_sample: str) -> bool:
    u = (page_url or "").lower()
    t = (title or "").lower()
    b = (body_sample or "").lower()
    if "idmsa.apple.com" in u or "appleid.apple.com" in u:
        return True
    if "sign in" in t or "oturum aç" in t or "sign-in" in u:
        return True
    if "account name" in b or "apple id" in b and "password" in b:
        return True
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
    channel = (os.environ.get("ASC_CONSOLE_BROWSER_CHANNEL") or "chrome").strip()
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
    ctx = pw.chromium.launch_persistent_context(**launch_kwargs)
    return pw, ctx


def run_login_interactive() -> None:
    print("ASC login — tarayıcıda Apple ID ile giriş yapın…", flush=True)
    pw, ctx = _launch_context(headed=True)
    try:
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(
            f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        deadline = time.time() + 600
        while time.time() < deadline:
            url = page.url or ""
            title = ""
            try:
                title = page.title()
            except Exception:
                pass
            body = ""
            try:
                body = page.locator("body").inner_text(timeout=2000)[:800]
            except Exception:
                pass
            if "appstoreconnect.apple.com" in url and not _need_login(url, title, body):
                print("ASC oturumu OK — profil kaydedildi.", flush=True)
                time.sleep(2)
                return
            time.sleep(2)
        print("Login zaman aşımı — tekrar --login deneyin.", flush=True)
    finally:
        try:
            ctx.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def _normalize_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        # epoch ms/s
        n = float(v)
        if n > 1e12:
            n /= 1000.0
        if 1e9 < n < 2e9:
            try:
                return datetime.utcfromtimestamp(n).date().isoformat()
            except Exception:
                return None
    s = str(v).strip()
    if not s:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
        except ValueError:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
            except ValueError:
                return None
    return None


def _as_float(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("%", "")
    if not s or s in ("-", "—", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _walk_series(obj: Any, out: list[tuple[str, float]], *, depth: int = 0) -> None:
    """JSON ağacından (date, value) çiftlerini topla."""
    if depth > 12 or obj is None:
        return
    if isinstance(obj, dict):
        keys_l = {str(k).lower(): k for k in obj.keys()}
        date_k = None
        for cand in (
            "date",
            "day",
            "reportdate",
            "startdate",
            "enddate",
            "period",
            "key",
            "x",
        ):
            if cand in keys_l:
                date_k = keys_l[cand]
                break
        val_k = None
        for cand in (
            "value",
            "total",
            "count",
            "y",
            "metricvalue",
            "unique",
            "units",
            "percentage",
        ):
            if cand in keys_l:
                val_k = keys_l[cand]
                break
        if date_k is not None and val_k is not None:
            ds = _normalize_date(obj.get(date_k))
            fv = _as_float(obj.get(val_k))
            if ds and fv is not None:
                out.append((ds, fv))
        # ASC: dataPoints / points / series
        for nest in ("data", "datapoints", "points", "series", "results", "values", "items"):
            if nest in keys_l:
                _walk_series(obj.get(keys_l[nest]), out, depth=depth + 1)
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _walk_series(v, out, depth=depth + 1)
    elif isinstance(obj, list):
        # [[date, value], ...] veya [{...}]
        if obj and not isinstance(obj[0], (dict, list)):
            return
        for item in obj:
            if (
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and not isinstance(item[0], (dict, list))
            ):
                ds = _normalize_date(item[0])
                fv = _as_float(item[1])
                if ds and fv is not None:
                    out.append((ds, fv))
            else:
                _walk_series(item, out, depth=depth + 1)


def _facts_from_payload(
    payload: Any, *, metric: str, measure_key: str
) -> list[dict[str, Any]]:
    pairs: list[tuple[str, float]] = []
    _walk_series(payload, pairs)
    by_date: dict[str, float] = {}
    for ds, fv in pairs:
        by_date[ds] = by_date.get(ds, 0.0) + fv if metric != "conversion_rate" else fv
    # conversion: aynı gün birden fazla gelirse ortalama
    if metric == "conversion_rate" and pairs:
        sums: dict[str, list[float]] = {}
        for ds, fv in pairs:
            sums.setdefault(ds, []).append(fv)
        by_date = {ds: sum(vs) / len(vs) for ds, vs in sums.items()}
    facts = []
    for ds in sorted(by_date.keys()):
        facts.append(
            {
                "metric": metric,
                "view_id": measure_key,
                "dim": "overview",
                "segment": "OVERALL",
                "date": ds,
                "value": round(float(by_date[ds]), 4),
                "label": f"{metric}:OVERALL",
                "source": "asc_network",
            }
        )
    return facts


def _facts_from_measures_response(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """ASC /data/app/detail/measures → explorer_facts."""
    facts: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return facts
    results = payload.get("results")
    if not isinstance(results, list):
        # fallback: genel walker
        for measure_key, metric in MEASURE_MAP.items():
            facts.extend(
                _facts_from_payload(payload, metric=metric, measure_key=measure_key)
            )
        return facts
    for row in results:
        if not isinstance(row, dict):
            continue
        measure_key = str(
            row.get("measure") or row.get("measureKey") or row.get("key") or ""
        ).strip()
        metric = MEASURE_MAP.get(measure_key)
        if not metric:
            # bilinmeyen measure — yine de walker ile dene
            continue
        points = row.get("data") or row.get("points") or row.get("values") or []
        if not isinstance(points, list):
            points = []
        for pt in points:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                ds = _normalize_date(pt[0])
                fv = _as_float(pt[1])
            elif isinstance(pt, dict):
                ds = _normalize_date(pt.get("date") or pt.get("day") or pt.get("key"))
                fv = _as_float(
                    pt.get("value")
                    if pt.get("value") is not None
                    else pt.get("total")
                )
            else:
                continue
            if not ds or fv is None:
                continue
            facts.append(
                {
                    "metric": metric,
                    "view_id": measure_key,
                    "dim": "overview",
                    "segment": "OVERALL",
                    "date": ds,
                    "value": round(fv, 4),
                    "label": f"{metric}:OVERALL",
                    "source": "asc_measures_api",
                }
            )
    if not facts:
        # results var ama parse edilemedi — walker
        for measure_key, metric in MEASURE_MAP.items():
            facts.extend(
                _facts_from_payload(payload, metric=metric, measure_key=measure_key)
            )
    return facts


def _parse_measures_text(text: str) -> Any:
    s = (text or "").strip()
    if not s:
        return None
    # bazı proxy/prefix’ler
    if s.startswith(")]}',"):
        s = s[5:].lstrip()
    try:
        return json.loads(s)
    except Exception:
        pass
    # ilk { veya [ bloğu
    for i, ch in enumerate(s):
        if ch in "{[":
            try:
                return json.loads(s[i:])
            except Exception:
                break
    return None


def _unregister_service_workers(page) -> None:
    """ASC SW bazen /analytics/api/* isteğini index.html’e düşürüyor."""
    try:
        page.evaluate(
            """async () => {
              if (!('serviceWorker' in navigator)) return 0;
              const regs = await navigator.serviceWorker.getRegistrations();
              for (const r of regs) { try { await r.unregister(); } catch (e) {} }
              return regs.length;
            }"""
        )
    except Exception:
        pass


def _metrics_page_url(measure_key: str) -> str:
    base = f"https://appstoreconnect.apple.com/apps/{APP_ID}"
    q = f"chartType=singleaxis&dateSpec=d90&frequency=day&measureKey={measure_key}"
    if measure_key in ("iap", "payingUsers", "proceeds"):
        return (
            f"{base}/analytics/monetization/sales/metrics?{q}"
            "&dimensionFilters=NobwRA5mBcYA4FcBOBjAFgQwM4FMtgBowA3GYAXQF9yg"
        )
    if measure_key.startswith("subscription"):
        return f"{base}/analytics/monetization/subscriptions/metrics?{q}"
    return f"{base}/analytics/metrics?{q}"


def _post_measures(page, measures: list[str], *, start: date, end: date) -> dict[str, Any]:
    """Private measures API — önce context.request (SW bypass), sonra page fetch."""
    payload = {
        "adamId": [str(APP_ID)],
        "startTime": f"{start.isoformat()}T00:00:00Z",
        "endTime": f"{end.isoformat()}T00:00:00Z",
        "measures": measures,
        "frequency": "day",
    }
    referer = f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "X-Requested-By": "appstoreconnect.apple.com",
        "Origin": "https://appstoreconnect.apple.com",
        "Referer": referer,
    }

    # 1) Playwright APIRequestContext — service worker’ı atlar, cookie paylaşır
    try:
        api_resp = page.context.request.post(
            ANALYTICS_MEASURES_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=60_000,
        )
        status = api_resp.status
        text = api_resp.text()
        body = _parse_measures_text(text)
        if status == 200 and isinstance(body, dict) and not str(text).lstrip().startswith("<!"):
            return {
                "ok": True,
                "status": status,
                "message": f"ok · request · results={len(body.get('results') or [])}",
                "body": body,
            }
        preview = (text or "")[:200].replace("\n", " ")
        req_msg = f"request HTTP {status} · preview={preview}"
    except Exception as exc:  # noqa: BLE001
        req_msg = f"request exc: {exc}"
        status = 0
        body = None

    # 2) page.fetch (SW unregister sonrası)
    _unregister_service_workers(page)
    result = page.evaluate(
        """async ({url, payload, referer}) => {
          try {
            const r = await fetch(url, {
              method: 'POST',
              credentials: 'include',
              headers: {
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json',
                'X-Requested-By': 'appstoreconnect.apple.com',
                'Origin': 'https://appstoreconnect.apple.com',
                'Referer': referer,
              },
              body: JSON.stringify(payload),
            });
            const text = await r.text();
            let data = null;
            let parseError = null;
            try { data = text ? JSON.parse(text) : null; }
            catch (e) { parseError = String(e); }
            return {
              status: r.status,
              ok: r.ok,
              data,
              parseError,
              preview: text.slice(0, 400),
              resultCount: data && data.results ? data.results.length : null,
            };
          } catch (e) {
            return { status: 0, ok: false, data: null, parseError: String(e), preview: '' };
          }
        }""",
        {"url": ANALYTICS_MEASURES_URL, "payload": payload, "referer": referer},
    )
    if isinstance(result, dict):
        status2 = int(result.get("status") or 0)
        body2 = result.get("data")
        if body2 is None:
            body2 = _parse_measures_text(str(result.get("preview") or ""))
        if status2 == 200 and isinstance(body2, dict):
            return {
                "ok": True,
                "status": status2,
                "message": f"ok · fetch · results={result.get('resultCount')}",
                "body": body2,
            }
        return {
            "ok": False,
            "status": status2 or status,
            "message": (
                f"{req_msg} | fetch: {result.get('parseError') or ''} "
                f"preview={str(result.get('preview') or '')[:120]}"
            )[:300],
            "body": body2 if isinstance(body2, dict) else body,
        }
    return {"ok": False, "status": status, "message": req_msg[:300], "body": body}


def _capture_measures_via_ui(page, measure_keys: list[str]) -> list[dict[str, Any]]:
    """UI sayfasına gidip gerçek SPA XHR/POST yanıtlarını yakala."""
    captured_bodies: list[Any] = []

    def on_response(resp) -> None:
        try:
            url = (resp.url or "").lower()
            if "analytics/api" not in url:
                return
            if resp.status != 200:
                return
            # body() binary; text JSON olabilir
            try:
                txt = resp.text()
            except Exception:
                return
            if not txt or txt.lstrip().startswith("<!"):
                return
            data = _parse_measures_text(txt)
            if data is not None:
                captured_bodies.append(data)
                print(f"  captured XHR · {resp.url[:80]}…", flush=True)
        except Exception:
            return

    page.on("response", on_response)
    try:
        for mk in measure_keys:
            url = _metrics_page_url(mk)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            except Exception as exc:
                print(f"  UI goto fail {mk}: {exc}", flush=True)
                continue
            for _ in range(16):
                time.sleep(0.5)
                try:
                    page.wait_for_load_state("networkidle", timeout=2000)
                except Exception:
                    pass
                if captured_bodies:
                    break
            time.sleep(1.2)
    finally:
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass
    return captured_bodies


def scrape_asc_console(*, headed: bool | None = None) -> dict[str, Any]:
    env_hl = (os.environ.get("ASC_CONSOLE_HEADLESS") or "").strip().lower()
    if headed is None:
        headed = env_hl not in ("1", "true", "yes")

    pw, ctx = _launch_context(headed=headed)
    explorer_facts: list[dict[str, Any]] = []
    pages_meta: dict[str, Any] = {}
    raw_network: list[dict[str, Any]] = []

    try:
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(
            f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(3)
        url0 = page.url or ""
        title0 = ""
        try:
            title0 = page.title()
        except Exception:
            pass
        body0 = ""
        try:
            body0 = page.locator("body").inner_text(timeout=3000)[:1200]
        except Exception:
            pass
        if _need_login(url0, title0, body0):
            return {
                "ok": False,
                "needs_login": True,
                "message": "ASC login gerekli — scripts/asc_console_scrape.py --login",
                "panels": {"explorer_facts": []},
                "raw_network": [],
            }

        _unregister_service_workers(page)
        time.sleep(0.5)

        end_d = date.today() - timedelta(days=1)
        start_d = end_d - timedelta(days=89)
        for batch in MEASURE_BATCHES:
            resp = _post_measures(page, batch, start=start_d, end=end_d)
            raw_network.append(
                {
                    "url": ANALYTICS_MEASURES_URL,
                    "status": resp.get("status"),
                    "ts": datetime.now().isoformat(),
                    "measures": batch,
                    "ok": resp.get("ok"),
                    "message": str(resp.get("message") or "")[:200],
                }
            )
            if not resp.get("ok"):
                print(
                    f"ASC measures POST fail · {batch} · HTTP {resp.get('status')} · "
                    f"{str(resp.get('message') or '')[:200]}",
                    flush=True,
                )
                for mk in batch:
                    pages_meta[mk] = {
                        "ok": False,
                        "error": f"HTTP {resp.get('status')}",
                        "message": str(resp.get("message") or "")[:160],
                    }
                continue
            facts_batch = _facts_from_measures_response(resp.get("body") or {})
            counts: dict[str, int] = {}
            for f in facts_batch:
                mk = str(f.get("view_id") or "")
                counts[mk] = counts.get(mk, 0) + 1
            explorer_facts.extend(facts_batch)
            for mk in batch:
                n = counts.get(mk, 0)
                pages_meta[mk] = {"ok": n > 0, "fact_count": n}
                metric = MEASURE_MAP.get(mk, mk)
                print(f"ASC scrape · {mk} → {metric}: {n} gün (measures API)", flush=True)
            time.sleep(0.6)

        # API HTML/boş dönerse: UI XHR yakala
        if not explorer_facts:
            print("ASC measures API boş/HTML — UI network yakalama…", flush=True)
            ui_bodies = _capture_measures_via_ui(page, list(MEASURE_MAP.keys()))
            for body in ui_bodies:
                if isinstance(body, dict):
                    facts_batch = _facts_from_measures_response(body)
                else:
                    facts_batch = []
                    for mk, metric in MEASURE_MAP.items():
                        facts_batch.extend(
                            _facts_from_payload(body, metric=metric, measure_key=mk)
                        )
                explorer_facts.extend(facts_batch)
            counts: dict[str, int] = {}
            for f in explorer_facts:
                mk = str(f.get("view_id") or f.get("metric") or "")
                counts[mk] = counts.get(mk, 0) + 1
            for mk, metric in MEASURE_MAP.items():
                n = counts.get(mk, 0) or counts.get(metric, 0)
                pages_meta[mk] = {"ok": n > 0, "fact_count": n, "source": "ui_xhr"}
                print(f"ASC scrape · {mk} → {metric}: {n} gün (UI XHR)", flush=True)

        # tekilleştir
        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for f in explorer_facts:
            key = (str(f.get("metric")), str(f.get("date") or "")[:10])
            if key[1]:
                by_key[key] = f
        explorer_facts = [by_key[k] for k in sorted(by_key.keys())]

        ok_metrics = sum(1 for v in pages_meta.values() if v.get("ok"))
        msg = (
            f"ASC scrape · {len(explorer_facts)} fact · "
            f"{ok_metrics}/{len(MEASURE_MAP)} measure"
        )
        return {
            "ok": bool(explorer_facts),
            "needs_login": False,
            "message": msg,
            "sync_mode": "analytics_scrape",
            "package_name": BUNDLE_ID,
            "bundle_id": BUNDLE_ID,
            "app_id": APP_ID,
            "source": "asc_console_bridge",
            "source_url": f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            "metrics": [],
            "panels": {
                "version": 1,
                "explorer_facts": explorer_facts[:50000],
                "explorer_fact_count": len(explorer_facts),
                "pages": pages_meta,
                "measure_keys": list(MEASURE_MAP.keys()),
                "scrape_meta": {
                    "start": start_d.isoformat(),
                    "end": end_d.isoformat(),
                    "api": ANALYTICS_MEASURES_URL,
                },
            },
            "raw_network": raw_network[-40:],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"ASC scrape hata: {exc}",
            "panels": {"explorer_facts": []},
            "raw_network": [],
        }
    finally:
        try:
            ctx.close()
        except Exception:
            pass
        try:
            pw.stop()
        except Exception:
            pass


def ingest_scrape_result(result: dict[str, Any]) -> dict[str, Any]:
    import requests

    token = _ingest_token()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN gerekli"}
    payload = {
        "metrics": result.get("metrics") or [],
        "panels": result.get("panels") or {},
        "raw_network": result.get("raw_network") or [],
        "source": result.get("source") or "asc_console_bridge",
        "source_url": result.get("source_url"),
        "bundle_id": result.get("bundle_id") or BUNDLE_ID,
        "app_id": result.get("app_id") or APP_ID,
        "sync_ok": bool(result.get("ok")),
        "sync_message": result.get("message"),
        "sync_mode": result.get("sync_mode") or "analytics_scrape",
    }
    r = requests.post(
        INGEST_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "message": r.text[:300]}
    data["http_status"] = r.status_code
    return data


def main() -> int:
    args = sys.argv[1:]
    if "--login" in args:
        run_login_interactive()
        return 0
    do_ingest = "--ingest" in args or "--sync" in args
    headed = "--headless" not in args
    if "--headed" in args:
        headed = True
    result = scrape_asc_console(headed=headed)
    print(result.get("message") or result, flush=True)
    if result.get("needs_login"):
        return 2
    if do_ingest:
        if not result.get("ok") and not (result.get("panels") or {}).get("explorer_facts"):
            print("Ingest atlandı — fact yok", flush=True)
            return 1
        ing = ingest_scrape_result(result)
        print(
            f"Ingest HTTP {ing.get('http_status')} · {ing.get('message') or ing}",
            flush=True,
        )
        return 0 if ing.get("ok") or ing.get("http_status") == 200 else 1
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
