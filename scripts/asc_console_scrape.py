#!/usr/bin/env python3
"""App Store Connect Analytics scrape — Play Console bridge ile aynı model.

Mac'te Apple ID oturumu (kalıcı Firefox profili) ile ASC SPA network JSON
yakalanır → explorer_facts → Railway /api/asc-console/ingest.

  .venv/bin/python scripts/asc_console_scrape.py --login
  .venv/bin/python scripts/asc_console_scrape.py --sync --ingest

Env:
  ASC_CONSOLE_PROFILE_DIR   default ~/.seo-agent/fx-asc
  ASC_CONSOLE_INGEST_URL    default …/api/asc-console/ingest
  NOTIFICATION_INGEST_TOKEN
  ASC_CONSOLE_APP_ID        default 465599322
  ASC_CONSOLE_SCRAPE_DAYS   yalnız HISTORY_SEALED=0 / FORCE_FULL iken (aksi halde dün)
  ASC_CONSOLE_FORCE_FULL=1  HISTORY_START → seal tek seferlik
  ASC_CONSOLE_HEADLESS=1    (varsayılan headed — Apple oturumu için)
  ASC_CONSOLE_KEEP_OPEN=0   (varsayılan açık bırak; 0=scrape bitince kapat)
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
from backend.services.empower_intel_config import (
    asc_console_skip_measure_keys,
    asc_console_skip_warehouse_metrics,
)
from backend.services.scrape_browser import asc_profile_dir

PROFILE_DIR = asc_profile_dir()
INGEST_URL = (
    os.environ.get("ASC_CONSOLE_INGEST_URL")
    or "https://projectcontrol.up.railway.app/api/asc-console/ingest"
).strip()


def _scrape_window() -> dict:
    """Mühürlü: yalnız dün. FORCE_FULL / unsealed: HISTORY_START → seal."""
    from backend.services.history_seal import (
        calendar_yesterday,
        force_full_history,
        history_start,
        is_pipeline_sealed,
        scheduled_fetch_window,
    )

    # Explicit day count only when forcing full / unsealed backfill
    raw = (os.environ.get("ASC_CONSOLE_SCRAPE_DAYS") or "").strip()
    if raw and (force_full_history("asc") or not is_pipeline_sealed("asc")):
        try:
            n = int(raw)
        except ValueError:
            n = 365
        n = min(max(n, 1), 400)
        end = calendar_yesterday()
        start = max(history_start(), end - timedelta(days=n - 1))
        return {
            "mode": "explicit_days",
            "start": start,
            "end": end,
            "days": (end - start).days + 1,
        }
    return scheduled_fetch_window("asc")


def _scrape_days() -> int:
    """Geriye uyum — pencere gün sayısı (mühürlüde 1)."""
    return int(_scrape_window().get("days") or 1)

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
    "sales": "sales",
    "activeDevices": "active_devices",
    "sessions": "sessions",
    "installs": "installs",
    "crashes": "crashes",
    "uninstalls": "uninstalls",
    "subscription-state-plans-active": "active_subscriptions",
    "subscription-state-churned": "subscription_churned",
    "subscription-events-renewals": "subscription_renewals",
    # Free Trials ≈ ASC “Subscription Offers” state (ayrı freeTrials key 400)
    "subscription-state-offers": "free_trials",
}
# Batch grupları (tek POST’ta birden fazla measure) — 365g günlük
MEASURE_BATCHES: list[list[str]] = [
    ["units", "redownloads", "conversionRate", "impressionsTotal", "pageViewCount"],
    ["iap", "payingUsers", "proceeds"],
    ["sales"],
    ["activeDevices", "sessions"],
    ["installs", "crashes", "uninstalls"],
    [
        "subscription-state-plans-active",
        "subscription-state-churned",
        "subscription-events-renewals",
    ],
    ["subscription-state-offers"],
]
# Warehouse’ta istediğimiz metrikler — scrape sonrası eksikler tek tek doldurulur
REQUIRED_WAREHOUSE_METRICS: list[str] = [
    "units",
    "redownloads",
    "impressions",
    "page_views",
    "conversion_rate",
    "iap",
    "paying_users",
    "proceeds",
    "sales",
    "active_devices",
    "sessions",
    "installs",
    "crashes",
    "uninstalls",
    "active_subscriptions",
    "subscription_churned",
    "subscription_renewals",
    "free_trials",
]


def _asc_measure_map() -> dict[str, str]:
    skip = asc_console_skip_measure_keys()
    if not skip:
        return MEASURE_MAP
    return {k: v for k, v in MEASURE_MAP.items() if k not in skip}


def _asc_measure_batches() -> list[list[str]]:
    skip = asc_console_skip_measure_keys()
    if not skip:
        return [list(b) for b in MEASURE_BATCHES]
    out: list[list[str]] = []
    for batch in MEASURE_BATCHES:
        kept = [k for k in batch if k not in skip]
        if kept:
            out.append(kept)
    return out


def _asc_required_metrics() -> list[str]:
    skip = asc_console_skip_warehouse_metrics()
    if not skip:
        return list(REQUIRED_WAREHOUSE_METRICS)
    return [m for m in REQUIRED_WAREHOUSE_METRICS if m not in skip]


def _asc_drop_overlap_facts(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    skip_m = asc_console_skip_warehouse_metrics()
    skip_k = asc_console_skip_measure_keys()
    if not skip_m and not skip_k:
        return facts
    kept: list[dict[str, Any]] = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        if str(f.get("metric") or "") in skip_m:
            continue
        if str(f.get("view_id") or "") in skip_k:
            continue
        kept.append(f)
    return kept


ANALYTICS_MEASURES_URL = (
    "https://appstoreconnect.apple.com/analytics/api/v1/data/app/detail/measures"
)
RATINGS_URL = (
    f"https://appstoreconnect.apple.com/apps/{APP_ID}/distribution/ratings/ios"
)


def _ingest_token() -> str:
    return (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()


def _need_login(page_url: str, title: str, body_sample: str) -> bool:
    u = (page_url or "").lower()
    t = (title or "").lower()
    b = (body_sample or "").lower()
    # URL kesin login
    if "idmsa.apple.com" in u or "appleid.apple.com" in u:
        return True
    if "appstoreconnect.apple.com/login" in u or "authresult=failed" in u:
        return True
    # Body yalnızca login host’unda (analytics SPA yanlış pozitif üretmesin)
    on_login_host = (
        "appstoreconnect.apple.com/login" in u
        or "idmsa.apple.com" in u
        or "appleid.apple.com" in u
    )
    if on_login_host:
        if "e-posta veya telefon" in b or "email or phone" in b:
            return True
        if "geçiş anahtarı ile giriş" in b or "sign in with passkey" in b:
            return True
    if "sign in" in t or "oturum aç" in t:
        return True
    return False


def _page_needs_login(page) -> bool:
    url = ""
    title = ""
    body = ""
    try:
        url = page.url or ""
    except Exception:
        pass
    try:
        title = page.title() or ""
    except Exception:
        pass
    try:
        body = page.locator("body").inner_text(timeout=2500)[:1200]
    except Exception:
        pass
    return _need_login(url, title, body)


def _probe_analytics_session(page, ctx=None) -> dict[str, Any]:
    """settings/all JSON + Apple oturum çerezleri.

    Dikkat: settings/all bazen oturumsuz HTTP 200 + measures kataloğu döner
    (myacinfo/itctx yok). Bu yüzden yalnız JSON yetmez — auth cookie şart.
    """
    cookie_info: dict[str, Any] = {}
    try:
        cookie_info = _cookie_debug(ctx or page.context)
    except Exception:
        cookie_info = {}
    has_auth = bool(cookie_info.get("has_myacinfo") or cookie_info.get("has_itctx"))
    try:
        warm = page.context.request.get(
            "https://appstoreconnect.apple.com/analytics/api/v1/settings/all",
            headers=_analytics_headers(
                f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics"
            ),
            timeout=30_000,
        )
        text = warm.text() or ""
        ctype = (warm.headers.get("content-type") or "")[:60]
        json_ok = (
            warm.status == 200
            and "json" in ctype.lower()
            and not text.lstrip().startswith("<!")
            and ('"measures"' in text or '"results"' in text or text.lstrip().startswith("{"))
        )
        # Auth cookie olmadan JSON 200 → sahte oturum (login ekranı açıkken görüldü)
        ok = bool(json_ok and has_auth)
        return {
            "ok": ok,
            "status": warm.status,
            "ctype": ctype,
            "preview": text[:120].replace("\n", " "),
            "has_auth_cookie": has_auth,
            "json_ok": json_ok,
            "cookies": cookie_info,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "status": 0,
            "ctype": "",
            "preview": str(exc)[:120],
            "has_auth_cookie": has_auth,
            "json_ok": False,
            "cookies": cookie_info,
        }


def _url_looks_like_login(page_url: str) -> bool:
    u = (page_url or "").lower()
    return (
        "appstoreconnect.apple.com/login" in u
        or "idmsa.apple.com" in u
        or "appleid.apple.com" in u
        or "authresult=failed" in u
        or "/sign-in" in u
        or "signin" in u and "apple" in u
    )


def _page_url_safe(page) -> str:
    try:
        return page.url or ""
    except Exception:
        return ""


def _focus_apple_login_once(page) -> None:
    """Tek sefer odak — bekleme döngüsünde DOM okuma yok (odak çalınmasın)."""
    try:
        page.bring_to_front()
    except Exception:
        pass
    # authResult=FAILED kalıntısı: temiz login URL
    cur = _page_url_safe(page).lower()
    if "authresult=failed" in cur:
        try:
            page.goto(
                f"https://appstoreconnect.apple.com/login?"
                f"targetUrl=%2Fapps%2F{APP_ID}%2Fanalytics%2Fmetrics",
                wait_until="domcontentloaded",
                timeout=90_000,
            )
            time.sleep(1.5)
        except Exception:
            pass
    selectors = (
        'input[type="email"]',
        'input[name="accountName"]',
        'input[id="account_name_text_field"]',
        'input[placeholder*="E-posta"]',
        'input[placeholder*="Email"]',
        'input[type="text"]',
    )
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=2500, force=True)
            time.sleep(0.3)
            return
        except Exception:
            continue


def _wait_for_asc_session(page, ctx, *, timeout_sec: int | None = None) -> bool:
    """Kullanıcı giriş yapana kadar bekle — sayfa DOM’una dokunma (odak bozulmasın).

    Giriş OK olunca True döner; çağıran aynı page/ctx ile taramaya devam etmeli
    (tarayıcı burada kapatılmaz).
    """
    from backend.services.scrape_browser import LOGIN_WAIT_SEC, login_wait_sec

    timeout_sec = login_wait_sec() if timeout_sec is None else max(LOGIN_WAIT_SEC, int(timeout_sec))
    print(
        "ASC oturumu yok / düşmüş — açılan Firefox penceresinde Apple ID ile giriş yapın.\n"
        "ÖNEMLİ: Önce pencereye bir kez tıklayın, sonra e-posta alanına yazın.\n"
        "Tarama arka planda bekler; giriş sırasında sayfayı yenilemez / odak çalmaz.\n"
        f"Girişten sonra tarayıcı KAPANMAZ — aynı pencerede kazıma devam eder "
        f"(en fazla {timeout_sec // 60} dk).",
        flush=True,
    )
    _focus_apple_login_once(page)
    deadline = time.time() + timeout_sec
    last_status = 0.0
    while time.time() < deadline:
        # Yalnızca network API — page.inner_text / locator yok
        probe = _probe_analytics_session(page)
        if probe.get("ok"):
            try:
                page.goto(
                    f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
                    wait_until="domcontentloaded",
                    timeout=90_000,
                )
            except Exception:
                pass
            time.sleep(2)
            # Sahte 200'e düşmemek için tekrar doğrula (cookie + API)
            probe2 = _probe_analytics_session(page, ctx)
            if not probe2.get("ok"):
                time.sleep(2)
                continue
            info = _cookie_debug(ctx)
            print(
                f"ASC oturumu OK · api=settings/all · myacinfo={info.get('has_myacinfo')} · "
                f"itctx={info.get('has_itctx')} — profil kaydedildi.\n"
                "→ Aynı pencerede ASC tarama devam ediyor (tarayıcı açık kalır).",
                flush=True,
            )
            time.sleep(5)  # cookie’lerin diske yazılması
            return True
        url = _page_url_safe(page)
        now = time.time()
        if now - last_status >= 15:
            print(
                f"  · ASC login bekleniyor · kalan≈{max(0, int(deadline - now))}s · "
                f"url={url[:120]}",
                flush=True,
            )
            last_status = now
        # URL login’den çıktıysa bir kez daha API+cookie dene (DOM okuma yok)
        if url and not _url_looks_like_login(url):
            probe2 = _probe_analytics_session(page, ctx)
            if probe2.get("ok"):
                info = _cookie_debug(ctx)
                print(
                    f"ASC oturumu OK · myacinfo={info.get('has_myacinfo')} · "
                    f"itctx={info.get('has_itctx')} — profil kaydedildi.\n"
                    "→ Aynı pencerede ASC tarama devam ediyor (tarayıcı açık kalır).",
                    flush=True,
                )
                time.sleep(5)
                return True
        time.sleep(3)
    print("Login zaman aşımı (15 dk) — tekrar Update page veya --login deneyin.", flush=True)
    return False


_CDP_ATTACHED: set[int] = set()


def _asc_keep_window_open() -> bool:
    """Varsayılan: ASC penceresi açık kalsın. Kapatmak için ASC_CONSOLE_KEEP_OPEN=0 veya SCRAPE_KEEP_OPEN=0."""
    from backend.services.scrape_browser import scrape_keep_window_open

    return scrape_keep_window_open(env_key="ASC_CONSOLE_KEEP_OPEN")


def _launch_context(*, headed: bool):
    from backend.services.scrape_browser import acquire_persistent_context

    pw, ctx, reused = acquire_persistent_context(
        "asc",
        profile=PROFILE_DIR,
        headed=headed,
        env_key="ASC_CONSOLE_KEEP_OPEN",
        label="ASC",
        locale="tr-TR",
        extra={"service_workers": "block"},
    )
    if reused:
        print("ASC: kalıcı Firefox profili (warm)", flush=True)
    return pw, ctx


def _release_context(pw, ctx) -> None:
    """Headed ASC'te pencereyi kapatma — oturum/şifre ekranı açık kalsın."""
    from backend.services.scrape_browser import release_persistent_context

    release_persistent_context(
        "asc",
        pw,
        ctx,
        headed=True,
        env_key="ASC_CONSOLE_KEEP_OPEN",
        label="ASC",
        profile=PROFILE_DIR,
    )


def run_login_interactive() -> None:
    print(
        "ASC login — açılan Firefox penceresinde giriş yapın "
        f"(profil: {PROFILE_DIR}).\n"
        "Önce pencereye tıklayın, sonra e-posta alanına yazın.\n"
        "Girişten sonra pencere açık kalır (ASC_CONSOLE_KEEP_OPEN).",
        flush=True,
    )
    pw, ctx = _launch_context(headed=True)
    try:
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(
            f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        ok = _wait_for_asc_session(page, ctx)
        if not ok:
            return
        print(
            "Login kaydedildi. Pencere açık bırakıldı — Update page aynı pencerede devam eder.",
            flush=True,
        )
    finally:
        _release_context(pw, ctx)


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
        for measure_key, metric in _asc_measure_map().items():
            facts.extend(
                _facts_from_payload(payload, metric=metric, measure_key=measure_key)
            )
        return facts
    skip_m = asc_console_skip_warehouse_metrics()
    skip_k = asc_console_skip_measure_keys()
    for row in results:
        if not isinstance(row, dict):
            continue
        measure_key = str(
            row.get("measure") or row.get("measureKey") or row.get("key") or ""
        ).strip()
        metric = MEASURE_MAP.get(measure_key)
        if not metric or metric in skip_m or measure_key in skip_k:
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
        for measure_key, metric in _asc_measure_map().items():
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
    days = _scrape_days()
    q = f"chartType=singleaxis&dateSpec=d{days}&frequency=day&measureKey={measure_key}"
    if measure_key in ("iap", "payingUsers", "proceeds", "sales"):
        return f"{base}/analytics/monetization/sales/metrics?{q}"
    if measure_key.startswith("subscription"):
        return f"{base}/analytics/monetization/subscriptions/metrics?{q}"
    return f"{base}/analytics/metrics?{q}"


def _synthesize_total_downloads(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """units + redownloads → total_downloads (ASC’de ayrı measure yok)."""
    by_date: dict[str, float] = {}
    for f in facts:
        m = str(f.get("metric") or "")
        if m not in ("units", "redownloads"):
            continue
        ds = str(f.get("date") or "")[:10]
        if not ds:
            continue
        try:
            by_date[ds] = by_date.get(ds, 0.0) + float(f.get("value") or 0)
        except (TypeError, ValueError):
            continue
    out: list[dict[str, Any]] = []
    for ds in sorted(by_date.keys()):
        out.append(
            {
                "metric": "total_downloads",
                "view_id": "total_downloads",
                "dim": "overview",
                "segment": "OVERALL",
                "date": ds,
                "value": round(by_date[ds], 4),
                "label": "total_downloads:OVERALL",
                "source": "asc_derived",
            }
        )
    return out


def _scrape_ratings_summary(page) -> dict[str, Any]:
    """ASC /distribution/ratings/ios — anlık puan / dağılım özeti."""
    out: dict[str, Any] = {
        "ok": False,
        "url": RATINGS_URL,
        "rating": None,
        "ratings_count": None,
        "stars": {},
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }
    try:
        page.goto(RATINGS_URL, wait_until="domcontentloaded", timeout=90_000)
        time.sleep(2.5)
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)[:160]
        return out
    try:
        raw = page.evaluate(
            """() => {
              const text = (document.body && document.body.innerText) || '';
              const html = document.documentElement ? document.documentElement.innerHTML : '';
              // "4,7" / "4.7" + ratings count nearby
              const ratingRe = /(?:\\b|^)([1-5][.,]\\d)\\s*(?:\\/\\s*5)?/;
              const countRe = /([\\d.\\s]+)\\s*(?:oy|rating|ratings|değerlendirme)/i;
              let rating = null, count = null;
              const m1 = text.match(ratingRe);
              if (m1) rating = m1[1];
              const m2 = text.match(countRe);
              if (m2) count = m2[1];
              // star histogram rows e.g. "5★ 12.345"
              const stars = {};
              for (let s = 5; s >= 1; s--) {
                const re = new RegExp(s + '\\\\s*[★*]\\\\s*([\\\\d.]+[\\\\s]?[KkMm]?)', 'i');
                const m = text.match(re);
                if (m) stars[String(s)] = m[1];
              }
              return { rating, count, stars, textSample: text.slice(0, 800), htmlHasIris: /iris|ratings/i.test(html) };
            }"""
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)[:160]
        return out
    if not isinstance(raw, dict):
        return out

    def _num(v: Any) -> float | None:
        if v is None:
            return None
        s = str(v).strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return None
        mult = 1.0
        if s[-1:].lower() == "k":
            mult = 1_000.0
            s = s[:-1]
        elif s[-1:].lower() == "m":
            mult = 1_000_000.0
            s = s[:-1]
        try:
            return float(s) * mult
        except ValueError:
            return None

    rating = _num(raw.get("rating"))
    count = _num(raw.get("count"))
    stars: dict[str, float] = {}
    for k, v in (raw.get("stars") or {}).items():
        nv = _num(v)
        if nv is not None:
            stars[str(k)] = nv
    out.update(
        {
            "ok": rating is not None or count is not None or bool(stars),
            "rating": rating,
            "ratings_count": int(count) if count is not None else None,
            "stars": stars,
            "text_sample": str(raw.get("textSample") or "")[:240],
        }
    )
    return out


def _analytics_headers(referer: str) -> dict[str, str]:
    # Strict Accept — */* Apple CDN’de SPA HTML shell tetikleyebiliyor
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Requested-By": "appstoreconnect.apple.com",
        "Origin": "https://appstoreconnect.apple.com",
        "Referer": referer,
    }


def _cookie_debug(ctx) -> dict[str, Any]:
    try:
        cookies = ctx.cookies()
    except Exception:
        cookies = []
    names = sorted({str(c.get("name") or "") for c in cookies if c.get("name")})
    return {
        "count": len(cookies),
        "names": names[:40],
        "has_myacinfo": "myacinfo" in names,
        "has_itctx": "itctx" in names,
        "has_dqsid": "dqsid" in names,
    }


def _interesting_analytics_url(url: str) -> bool:
    u = (url or "").lower()
    if "appstoreconnect.apple.com" not in u and "itunes.apple.com" not in u:
        return False
    return any(
        p in u
        for p in (
            "/analytics/api/",
            "/iris/",
            "/power/",
            "measures",
            "timeseries",
            "dimension",
            "app-info",
            "settings/all",
        )
    )


def _attach_network_bag(ctx, bag: list[dict[str, Any]], url_log: list[str]) -> None:
    """Context-level XHR capture — page listener SPA navigasyonunda kaçırabiliyor."""

    def on_response(resp) -> None:
        try:
            url = resp.url or ""
            status = int(resp.status or 0)
            if "appstoreconnect.apple.com" in url.lower() or "itunes.apple.com" in url.lower():
                if status and status < 400:
                    url_log.append(f"{status} {url[:220]}")
                    if len(url_log) > 400:
                        del url_log[:80]
            if not _interesting_analytics_url(url):
                return
            if status != 200:
                return
            ctype = (resp.headers or {}).get("content-type", "")
            text = ""
            try:
                text = resp.text()
            except Exception:
                return
            if not text or text.lstrip().startswith("<!"):
                return
            data = _parse_measures_text(text)
            if data is None:
                return
            bag.append(
                {
                    "url": url[:500],
                    "status": status,
                    "ctype": str(ctype)[:80],
                    "body": data,
                }
            )
            if len(bag) > 200:
                del bag[:40]
            print(f"  captured XHR · {url[:90]}", flush=True)
        except Exception:
            return

    ctx.on("response", on_response)


def _post_measures_via_requests(ctx, measures: list[str], *, start: date, end: date) -> dict[str, Any]:
    """Browser cookie’leriyle doğrudan HTTP — Playwright/SW stack’ini tamamen atla."""
    import requests

    payload = {
        "adamId": [str(APP_ID)],
        "startTime": f"{start.isoformat()}T00:00:00Z",
        "endTime": f"{end.isoformat()}T00:00:00Z",
        "measures": measures,
        "frequency": "day",
    }
    referer = f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics"
    try:
        jar = {
            str(c["name"]): str(c["value"])
            for c in ctx.cookies()
            if c.get("name") and c.get("value") is not None
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "status": 0, "message": f"cookie read: {exc}", "body": None}
    try:
        r = requests.post(
            ANALYTICS_MEASURES_URL,
            headers=_analytics_headers(referer),
            cookies=jar,
            json=payload,
            timeout=60,
            allow_redirects=False,
        )
        text = r.text or ""
        body = _parse_measures_text(text)
        ctype = (r.headers.get("content-type") or "")[:60]
        if r.status_code == 200 and isinstance(body, dict) and not text.lstrip().startswith("<!"):
            return {
                "ok": True,
                "status": r.status_code,
                "message": f"ok · http · ctype={ctype} · results={len(body.get('results') or [])}",
                "body": body,
            }
        return {
            "ok": False,
            "status": r.status_code,
            "message": (
                f"http HTTP {r.status_code} · ctype={ctype} · "
                f"preview={text[:160].replace(chr(10), ' ')}"
            )[:300],
            "body": body if isinstance(body, dict) else None,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "status": 0, "message": f"http exc: {exc}", "body": None}


def _post_measures(page, measures: list[str], *, start: date, end: date) -> dict[str, Any]:
    """Private measures API — http cookies → context.request → page fetch."""
    payload = {
        "adamId": [str(APP_ID)],
        "startTime": f"{start.isoformat()}T00:00:00Z",
        "endTime": f"{end.isoformat()}T00:00:00Z",
        "measures": measures,
        "frequency": "day",
    }
    referer = f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics"
    headers = _analytics_headers(referer)

    # 0) requests + browser cookies (en güvenilir SW bypass)
    http_resp = _post_measures_via_requests(page.context, measures, start=start, end=end)
    if http_resp.get("ok"):
        return http_resp
    msgs = [str(http_resp.get("message") or "")]

    # 1) Playwright APIRequestContext
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
        ctype = (api_resp.headers.get("content-type") or "")[:60]
        if status == 200 and isinstance(body, dict) and not str(text).lstrip().startswith("<!"):
            return {
                "ok": True,
                "status": status,
                "message": f"ok · request · ctype={ctype} · results={len(body.get('results') or [])}",
                "body": body,
            }
        msgs.append(f"request HTTP {status} · ctype={ctype} · preview={(text or '')[:120].replace(chr(10), ' ')}")
    except Exception as exc:  # noqa: BLE001
        msgs.append(f"request exc: {exc}")
        status = 0
        body = None

    # 2) page.fetch
    _unregister_service_workers(page)
    result = page.evaluate(
        """async ({url, payload, referer}) => {
          try {
            const r = await fetch(url, {
              method: 'POST',
              credentials: 'include',
              headers: {
                'Accept': 'application/json',
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
              ctype: r.headers.get('content-type') || '',
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
        if status2 == 200 and isinstance(body2, dict) and not str(result.get("preview") or "").lstrip().startswith("<!"):
            return {
                "ok": True,
                "status": status2,
                "message": f"ok · fetch · results={result.get('resultCount')}",
                "body": body2,
            }
        msgs.append(
            f"fetch HTTP {status2} · {result.get('parseError') or ''} "
            f"preview={str(result.get('preview') or '')[:100]}"
        )
        return {
            "ok": False,
            "status": status2 or status,
            "message": " | ".join(m for m in msgs if m)[:300],
            "body": body2 if isinstance(body2, dict) else body,
        }
    return {
        "ok": False,
        "status": status,
        "message": " | ".join(m for m in msgs if m)[:300],
        "body": body,
    }


def _capture_measures_via_ui(
    page,
    measure_keys: list[str],
    *,
    bag: list[dict[str, Any]],
    url_log: list[str],
) -> list[Any]:
    """UI sayfalarını gez — context bag’e düşen analytics JSON’ları kullan."""
    before_total = len(bag)
    for mk in measure_keys:
        url = _metrics_page_url(mk)
        before = len(bag)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        except Exception as exc:
            print(f"  UI goto fail {mk}: {exc}", flush=True)
            continue
        if _page_needs_login(page):
            print(
                f"  UI {mk}: login ekranı (auth düşmüş) — scrape durdu",
                flush=True,
            )
            break
        _unregister_service_workers(page)
        try:
            page.evaluate(
                """async () => {
                  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
                  for (let y = 0; y < 2400; y += 600) {
                    window.scrollTo(0, y);
                    await sleep(250);
                  }
                  window.scrollTo(0, 0);
                }"""
            )
        except Exception:
            pass
        for _ in range(24):
            time.sleep(0.5)
            try:
                page.wait_for_load_state("networkidle", timeout=1500)
            except Exception:
                pass
            if len(bag) > before:
                break
        got = len(bag) - before
        print(f"  UI {mk}: +{got} JSON · last_urls={len(url_log)}", flush=True)
        time.sleep(0.6)

    bodies = [row.get("body") for row in bag[before_total:] if isinstance(row.get("body"), (dict, list))]
    # debug dump
    try:
        dump = {
            "bag_n": len(bag),
            "new_bodies": len(bodies),
            "url_sample": url_log[-60:],
            "bag_urls": [r.get("url") for r in bag[-30:]],
        }
        Path("/tmp/asc_ui_capture.json").write_text(
            json.dumps(dump, ensure_ascii=False, indent=2, default=str)[:200000],
            encoding="utf-8",
        )
        print("  debug → /tmp/asc_ui_capture.json", flush=True)
    except Exception:
        pass
    return bodies


def scrape_asc_console(*, headed: bool | None = None) -> dict[str, Any]:
    env_hl = (os.environ.get("ASC_CONSOLE_HEADLESS") or "").strip().lower()
    if headed is None:
        headed = env_hl not in ("1", "true", "yes")

    pw, ctx = _launch_context(headed=headed)
    explorer_facts: list[dict[str, Any]] = []
    pages_meta: dict[str, Any] = {}
    raw_network: list[dict[str, Any]] = []
    net_bag: list[dict[str, Any]] = []
    url_log: list[str] = []

    try:
        _attach_network_bag(ctx, net_bag, url_log)
        page = ctx.pages[0] if ctx.pages else ctx.new_page()
        page.goto(
            f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            wait_until="domcontentloaded",
            timeout=120_000,
        )
        time.sleep(3)
        _unregister_service_workers(page)
        cookie_info = _cookie_debug(ctx)
        print(
            f"ASC cookies · n={cookie_info.get('count')} · "
            f"myacinfo={cookie_info.get('has_myacinfo')} · "
            f"itctx={cookie_info.get('has_itctx')} · "
            f"dqsid={cookie_info.get('has_dqsid')}",
            flush=True,
        )
        probe = _probe_analytics_session(page, ctx)
        print(
            f"ASC settings/all · HTTP {probe.get('status')} · "
            f"ctype={probe.get('ctype')} · auth_cookie={probe.get('has_auth_cookie')} · "
            f"session_ok={probe.get('ok')} · {probe.get('preview')}",
            flush=True,
        )
        ui_login = False
        try:
            ui_login = _page_needs_login(page) or _url_looks_like_login(_page_url_safe(page))
        except Exception:
            ui_login = False
        # Gerçek oturum: API + auth cookie + UI login değil
        session_ok = bool(probe.get("ok")) and not ui_login
        if probe.get("json_ok") and not probe.get("has_auth_cookie"):
            print(
                "ASC: settings/all JSON geldi ama myacinfo/itctx yok — oturum yok sayılıyor, "
                "giriş bekleniyor.",
                flush=True,
            )
            session_ok = False
        if ui_login:
            print(
                "ASC: tarayıcıda Apple giriş ekranı görünüyor — 15 dk bekleniyor "
                "(girişten sonra aynı pencerede devam).",
                flush=True,
            )
            session_ok = False

        if not session_ok:
            if headed:
                if not _wait_for_asc_session(page, ctx):
                    return {
                        "ok": False,
                        "needs_login": True,
                        "message": "ASC girişi gerekli — Mac köprüde oturum açın (15 dk doldu)",
                        "panels": {"explorer_facts": []},
                        "raw_network": [],
                    }
                probe = _probe_analytics_session(page, ctx)
                session_ok = bool(probe.get("ok"))
                if not session_ok:
                    try:
                        page.goto(
                            f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
                            wait_until="domcontentloaded",
                            timeout=90_000,
                        )
                        time.sleep(3)
                    except Exception:
                        pass
                    probe = _probe_analytics_session(page, ctx)
                    session_ok = bool(probe.get("ok"))
                if not session_ok:
                    return {
                        "ok": False,
                        "needs_login": True,
                        "message": "ASC analytics oturumu doğrulanamadı — tekrar Update page",
                        "panels": {"explorer_facts": []},
                        "raw_network": [],
                    }
            else:
                return {
                    "ok": False,
                    "needs_login": True,
                    "message": "ASC girişi gerekli — Mac köprüde oturum açın",
                    "panels": {"explorer_facts": []},
                    "raw_network": [],
                }

        win = _scrape_window()
        end_d = win["end"]
        start_d = win["start"]
        scrape_days = int(win.get("days") or ((end_d - start_d).days + 1))
        print(
            f"ASC scrape aralık · {win.get('mode')} · {start_d} → {end_d} ({scrape_days} gün)",
            flush=True,
        )
        measure_map = _asc_measure_map()
        measure_batches = _asc_measure_batches()
        required_metrics = _asc_required_metrics()
        skipped_mk = sorted(asc_console_skip_measure_keys())
        if skipped_mk:
            print(
                "ASC scrape · atlandı (Metrik/Empower örtüşme): " + ", ".join(skipped_mk),
                flush=True,
            )
        for batch in measure_batches:
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
                    f"{str(resp.get('message') or '')[:220]}",
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
                metric = measure_map.get(mk, mk)
                print(f"ASC scrape · {mk} → {metric}: {n} gün (measures API)", flush=True)
            time.sleep(0.6)

        # Eksik warehouse metrikleri — tek tek POST (batch bazen sessizce atlar / boş döner)
        covered = {
            str(f.get("metric") or "")
            for f in explorer_facts
            if f.get("metric")
        }
        missing = [m for m in required_metrics if m not in covered]
        if missing:
            print(f"ASC scrape · eksik metrikler tek tek: {missing}", flush=True)
            # warehouse metric → tercih edilen measureKey(ler)
            prefer: dict[str, list[str]] = {}
            for mk, metric in measure_map.items():
                prefer.setdefault(metric, []).append(mk)
            for metric in missing:
                keys = prefer.get(metric) or [metric]
                got_any = False
                for mk in keys:
                    resp = _post_measures(page, [mk], start=start_d, end=end_d)
                    raw_network.append(
                        {
                            "url": ANALYTICS_MEASURES_URL,
                            "status": resp.get("status"),
                            "ts": datetime.now().isoformat(),
                            "measures": [mk],
                            "ok": resp.get("ok"),
                            "message": str(resp.get("message") or "")[:200],
                            "retry_for": metric,
                        }
                    )
                    if not resp.get("ok"):
                        print(
                            f"ASC measures retry fail · {mk} ({metric}) · "
                            f"HTTP {resp.get('status')} · {str(resp.get('message') or '')[:160]}",
                            flush=True,
                        )
                        pages_meta[mk] = {
                            "ok": False,
                            "error": f"HTTP {resp.get('status')}",
                            "message": str(resp.get("message") or "")[:160],
                        }
                        continue
                    facts_batch = _facts_from_measures_response(resp.get("body") or {})
                    # yalnızca hedef metriği al
                    facts_batch = [
                        f for f in facts_batch if str(f.get("metric") or "") == metric
                    ]
                    n = len(facts_batch)
                    pages_meta[mk] = {
                        "ok": n > 0,
                        "fact_count": n,
                        "source": "retry_single",
                    }
                    print(
                        f"ASC scrape · retry {mk} → {metric}: {n} gün",
                        flush=True,
                    )
                    if n:
                        explorer_facts.extend(facts_batch)
                        got_any = True
                        break
                    time.sleep(0.4)
                if not got_any:
                    print(f"ASC scrape · {metric} hâlâ boş", flush=True)
                time.sleep(0.5)

        # API HTML/boş dönerse veya hâlâ eksik varsa: UI XHR yakala
        covered_after = {
            str(f.get("metric") or "")
            for f in explorer_facts
            if f.get("metric")
        }
        still_missing = [m for m in required_metrics if m not in covered_after]
        if not explorer_facts or still_missing:
            print(
                "ASC measures API boş/eksik — UI network yakalama… "
                f"missing={still_missing[:12]}",
                flush=True,
            )
            # Önce eksik metriklerin measure key’lerini gez
            ui_keys: list[str] = []
            for metric in (still_missing or required_metrics):
                for mk, m in measure_map.items():
                    if m == metric and mk not in ui_keys:
                        ui_keys.append(mk)
            if not ui_keys:
                ui_keys = list(measure_map.keys())
            ui_bodies = _capture_measures_via_ui(
                page, ui_keys, bag=net_bag, url_log=url_log
            )
            for body in ui_bodies:
                if isinstance(body, dict):
                    facts_batch = _facts_from_measures_response(body)
                else:
                    facts_batch = []
                    for mk, metric in measure_map.items():
                        facts_batch.extend(
                            _facts_from_payload(body, metric=metric, measure_key=mk)
                        )
                explorer_facts.extend(facts_batch)
            # bag’de kalanları da dene
            for row in net_bag:
                body = row.get("body")
                if isinstance(body, dict):
                    explorer_facts.extend(_facts_from_measures_response(body))
            counts: dict[str, int] = {}
            for f in explorer_facts:
                mk = str(f.get("view_id") or f.get("metric") or "")
                counts[mk] = counts.get(mk, 0) + 1
            for mk, metric in measure_map.items():
                n = counts.get(mk, 0) or counts.get(metric, 0)
                pages_meta[mk] = {"ok": n > 0, "fact_count": n, "source": "ui_xhr"}
                print(f"ASC scrape · {mk} → {metric}: {n} gün (UI XHR)", flush=True)

        explorer_facts = _asc_drop_overlap_facts(explorer_facts)

        # tekilleştir
        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for f in explorer_facts:
            key = (str(f.get("metric")), str(f.get("date") or "")[:10])
            if key[1]:
                by_key[key] = f
        # units+redownloads → total_downloads
        for f in _synthesize_total_downloads(list(by_key.values())):
            key = (str(f.get("metric")), str(f.get("date") or "")[:10])
            if key[1]:
                by_key[key] = f
        explorer_facts = [by_key[k] for k in sorted(by_key.keys())]

        ratings = _scrape_ratings_summary(page)
        print(
            f"ASC ratings · ok={ratings.get('ok')} · rating={ratings.get('rating')} · "
            f"count={ratings.get('ratings_count')}",
            flush=True,
        )
        rating_metrics: list[dict[str, Any]] = []
        if ratings.get("rating") is not None:
            rating_metrics.append(
                {
                    "title": "App Store puanı",
                    "value": f"{ratings['rating']:.2f}".replace(".", ","),
                    "delta": "",
                    "kind": "ratings",
                    "page": "ratings",
                }
            )
        if ratings.get("ratings_count") is not None:
            rating_metrics.append(
                {
                    "title": "Değerlendirme sayısı",
                    "value": f"{int(ratings['ratings_count']):,}".replace(",", "."),
                    "delta": "",
                    "kind": "ratings",
                    "page": "ratings",
                }
            )

        ok_metrics = sum(1 for v in pages_meta.values() if v.get("ok"))
        msg = (
            f"ASC tarama · {len(explorer_facts)} fact · "
            f"{ok_metrics}/{len(measure_map)} measure"
            + (" · ratings OK" if ratings.get("ok") else "")
        )
        return {
            "ok": bool(explorer_facts) or bool(ratings.get("ok")),
            "needs_login": False,
            "message": msg,
            "sync_mode": "analytics_scrape",
            "package_name": BUNDLE_ID,
            "bundle_id": BUNDLE_ID,
            "app_id": APP_ID,
            "source": "asc_console_bridge",
            "source_url": f"https://appstoreconnect.apple.com/apps/{APP_ID}/analytics/metrics",
            "metrics": rating_metrics,
            "panels": {
                "version": 1,
                "explorer_facts": explorer_facts[:50000],
                "explorer_fact_count": len(explorer_facts),
                "pages": pages_meta,
                "measure_keys": list(measure_map.keys()),
                "ratings": ratings,
                "scrape_meta": {
                    "mode": win.get("mode"),
                    "start": start_d.isoformat(),
                    "end": end_d.isoformat(),
                    "days": scrape_days,
                    "sealed": bool(win.get("sealed")),
                    "api": ANALYTICS_MEASURES_URL,
                    "ratings_url": RATINGS_URL,
                },
            },
            "raw_network": raw_network[-40:],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "needs_login": False,
            "message": f"ASC tarama hatası: {exc}",
            "panels": {"explorer_facts": []},
            "raw_network": [],
        }
    finally:
        _release_context(pw, ctx)


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
    text = r.text or ""
    if text.lstrip().lower().startswith("<!doctype") or text.lstrip().startswith("<html"):
        return {
            "ok": False,
            "http_status": r.status_code,
            "message": (
                "Ingest HTML login sayfası döndü — Railway’de /api/asc-console/ingest "
                "public allowlist + NOTIFICATION_INGEST_TOKEN kontrol et"
            ),
        }
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "message": text[:300]}
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
