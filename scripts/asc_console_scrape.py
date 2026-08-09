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

# measureKey (ASC URL) → warehouse metric
MEASURE_VIEWS: list[tuple[str, str, str]] = [
    ("units", "units", "analytics"),
    ("redownloads", "redownloads", "analytics"),
    ("conversionRate", "conversion_rate", "analytics"),
    ("pageViewCount", "page_views", "analytics"),
    ("impressionsTotal", "impressions", "analytics"),
    ("iap", "iap", "sales"),
    ("payingUsers", "paying_users", "sales"),
    ("subscription-state-plans-active", "active_subscriptions", "subscriptions"),
]


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _metrics_url(measure_key: str, *, kind: str = "analytics") -> str:
    base = f"https://appstoreconnect.apple.com/apps/{APP_ID}"
    q = (
        "chartType=singleaxis&dateSpec=d90&frequency=day&measureKey="
        + measure_key
    )
    if kind == "sales":
        return (
            f"{base}/analytics/monetization/sales/metrics?{q}"
            "&dimensionFilters=NobwRA5mBcYA4FcBOBjAFgQwM4FMtgBowA3GYAXQF9yg"
        )
    if kind == "subscriptions":
        return f"{base}/analytics/monetization/subscriptions/metrics?{q}"
    return f"{base}/analytics/metrics?{q}"


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


def _is_asc_api_url(url: str) -> bool:
    u = (url or "").lower()
    if "appstoreconnect.apple.com" not in u and "itunesconnect.apple.com" not in u:
        if "amp-api" not in u and "analytics" not in u:
            return False
    needles = (
        "analytics",
        "measure",
        "metrics",
        "timeseries",
        "time-series",
        "sales",
        "subscription",
        "graphql",
        "/api/",
    )
    return any(n in u for n in needles)


def scrape_asc_console(*, headed: bool | None = None) -> dict[str, Any]:
    env_hl = (os.environ.get("ASC_CONSOLE_HEADLESS") or "").strip().lower()
    if headed is None:
        headed = env_hl not in ("1", "true", "yes")

    pw, ctx = _launch_context(headed=headed)
    captured: list[dict[str, Any]] = []
    explorer_facts: list[dict[str, Any]] = []
    pages_meta: dict[str, Any] = {}

    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            if resp.status != 200:
                return
            if not _is_asc_api_url(url):
                return
            ctype = (resp.headers.get("content-type") or "").lower()
            if "json" not in ctype and "javascript" not in ctype and "text/" not in ctype:
                return
            body = resp.text()
            if not body or len(body) < 8 or len(body) > 8_000_000:
                return
            try:
                data = json.loads(body)
            except Exception:
                return
            captured.append(
                {
                    "url": url[:500],
                    "status": resp.status,
                    "ts": datetime.utcnow().isoformat(),
                    "keys": list(data.keys())[:40] if isinstance(data, dict) else ["__list__"],
                    "body": data,
                }
            )
        except Exception:
            return

    try:
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.on("response", on_response)
        page.goto(
            f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(2)
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

        for measure_key, metric, kind in MEASURE_VIEWS:
            before = len(captured)
            target = _metrics_url(measure_key, kind=kind)
            try:
                page.goto(target, wait_until="domcontentloaded", timeout=90_000)
            except Exception as exc:
                pages_meta[measure_key] = {"ok": False, "error": str(exc)[:160]}
                continue
            # SPA XHR bitsin
            for _ in range(12):
                time.sleep(0.75)
                try:
                    page.wait_for_load_state("networkidle", timeout=2500)
                    break
                except Exception:
                    pass
            time.sleep(1.0)
            batch = captured[before:]
            facts_m: list[dict[str, Any]] = []
            for cap in batch:
                facts_m.extend(
                    _facts_from_payload(
                        cap.get("body"), metric=metric, measure_key=measure_key
                    )
                )
            # Aynı günü tekilleştir
            by_d: dict[str, dict[str, Any]] = {}
            for f in facts_m:
                ds = str(f.get("date") or "")[:10]
                if ds:
                    by_d[ds] = f
            facts_m = [by_d[k] for k in sorted(by_d.keys())]
            # son 400 gün
            cutoff = (date.today() - timedelta(days=400)).isoformat()
            facts_m = [f for f in facts_m if str(f.get("date") or "") >= cutoff]
            explorer_facts.extend(facts_m)
            pages_meta[measure_key] = {
                "ok": bool(facts_m),
                "fact_count": len(facts_m),
                "captures": len(batch),
                "url": target,
            }
            print(
                f"ASC scrape · {measure_key} → {metric}: {len(facts_m)} gün "
                f"({len(batch)} network)",
                flush=True,
            )

        ok_metrics = sum(1 for v in pages_meta.values() if v.get("ok"))
        msg = (
            f"ASC scrape · {len(explorer_facts)} fact · "
            f"{ok_metrics}/{len(MEASURE_VIEWS)} measure"
        )
        return {
            "ok": bool(explorer_facts) or ok_metrics > 0,
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
                "measure_keys": [m[0] for m in MEASURE_VIEWS],
            },
            "raw_network": [
                {k: c.get(k) for k in ("url", "status", "ts", "keys")}
                for c in captured[-40:]
            ],
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
