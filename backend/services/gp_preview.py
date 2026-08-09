"""
Google Play Store özet — /android Play Console scrape (explorer_facts + vitals).

Reporting API / GCS CSV yalnızca scrape bu alanı hiç dolduramıyorsa kullanılır.
Sentetik / seed demo üretmez. Veri yoksa alanlar "—" kalır.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any

from backend.services.app_intel import APP_PRODUCTS

logger = logging.getLogger(__name__)

_PERIODS: tuple[int, ...] = (1, 7, 14, 30, 90, 365)

_COUNTRIES: list[dict[str, str]] = [
    {"code": "all", "name": "Tüm ülkeler"},
    {"code": "tr", "name": "Türkiye"},
    {"code": "us", "name": "ABD"},
    {"code": "de", "name": "Almanya"},
    {"code": "gb", "name": "Birleşik Krallık"},
    {"code": "in", "name": "Hindistan"},
    {"code": "br", "name": "Brezilya"},
    {"code": "ru", "name": "Rusya"},
    {"code": "az", "name": "Azerbaycan"},
    {"code": "kz", "name": "Kazakistan"},
]

_DEVICES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm cihazlar"},
    {"id": "phone", "name": "Telefon"},
    {"id": "tablet", "name": "Tablet"},
]

_ANDROID_SOURCES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm kaynaklar"},
    {"id": "google_search", "name": "Google Arama"},
    {"id": "play_search", "name": "Google Play Arama"},
    {"id": "third_party_referral", "name": "Üçüncü taraf yönlendirme"},
    {"id": "play_store_browse", "name": "Google Play Keşfet"},
    {"id": "direct", "name": "Doğrudan"},
]

_PREVIEW_CACHE_TTL_S = 8 * 60
_PREVIEW_CACHE_LOCK = threading.Lock()
_PREVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _fmt_compact(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rstrip("0").rstrip(".")
    if n >= 1000:
        return f"{n / 1000:.1f}K".rstrip("0").rstrip(".")
    return f"{n:.0f}"


def _empty_kpi() -> dict[str, Any]:
    return {
        "value": None,
        "value_label": "—",
        "delta_pct": None,
        "series": [],
        "is_unavailable": True,
    }


def _kpi(value: float | None, *, series: list[float] | None = None) -> dict[str, Any]:
    if value is None:
        return _empty_kpi()
    s = [float(x) for x in (series or [])]
    return {
        "value": round(float(value), 2),
        "value_label": _fmt_compact(float(value)),
        "delta_pct": None,
        "series": s,
        "is_unavailable": False,
    }


def _empty_payload(pid: str, label: str, p: int, cc: str, dev: str) -> dict[str, Any]:
    empty = _empty_kpi()
    return {
        "source": "empty",
        "source_note": "Play Console verisi henüz yok — /android sync bekleniyor.",
        "product": pid,
        "product_label": label,
        "period_days": p,
        "filters": {"country": cc, "device": dev},
        "available_filters": {
            "periods": list(_PERIODS),
            "countries": _COUNTRIES,
            "devices": _DEVICES,
            "sources": _ANDROID_SOURCES,
        },
        "kpis": {
            "installs": dict(empty),
            "uninstalls": dict(empty),
            "net_installs": dict(empty),
        },
        "ratings": {
            "average": None,
            "total": None,
            "distribution": {},
            "delta_avg": None,
        },
        "vitals": {
            "crash_rate": None,
            "crash_rate_label": "—",
            "crash_series": [],
            "anr_rate": None,
            "anr_rate_label": "—",
            "anr_series": [],
            "slow_render_rate": None,
            "slow_render_label": "—",
            "crash_free_pct": None,
            "anr_free_pct": None,
            "crash_free_label": "—",
            "anr_free_label": "—",
        },
        "top_countries": [],
        "top_sources": [],
        "trend_daily": [],
        "trend_daily_is_demo": False,
        "top_countries_is_demo": False,
        "top_sources_is_demo": False,
    }


def _analytics_from_scrape(days: int) -> dict[str, Any]:
    """explorer_facts + vitals overview — birincil kaynak."""
    from backend.database import SessionLocal
    from backend.services.play_console_store import play_console_payload
    from backend.services.play_scrape_warehouse import load_scrape_facts, query_scrape_analytics
    from backend.services.stability_free import free_rates_from_vitals_overview

    end = date.today()
    start = end - timedelta(days=max(int(days or 30), 1) - 1)
    start_s, end_s = start.isoformat(), end.isoformat()
    facts, meta = load_scrape_facts()

    inst = query_scrape_analytics(
        start=start_s,
        end=end_s,
        metric="device_acquisition",
        breakdown="date",
        dim="overview",
        compare=None,
        facts=facts,
        meta=meta,
    )
    lost = query_scrape_analytics(
        start=start_s,
        end=end_s,
        metric="user_lost",
        breakdown="date",
        dim="overview",
        compare=None,
        facts=facts,
        meta=meta,
    )
    i_map = {
        str(r.get("key")): float(r.get("value") or 0)
        for r in (inst.get("series") or [])
        if isinstance(r, dict) and r.get("key")
    }
    u_map = {
        str(r.get("key")): float(r.get("value") or 0)
        for r in (lost.get("series") or [])
        if isinstance(r, dict) and r.get("key")
    }
    keys = sorted(set(i_map) | set(u_map))
    i_series = [i_map.get(k, 0.0) for k in keys]
    u_series = [u_map.get(k, 0.0) for k in keys]

    install_stats: dict[str, Any] = {}
    if i_series or u_series:
        install_stats = {
            "dates": keys,
            "installs_series": i_series,
            "uninstalls_series": u_series,
            "total_installs": int(round(sum(i_series))),
            "total_uninstalls": int(round(sum(u_series))),
        }

    vitals: dict[str, Any] = {}
    try:
        with SessionLocal() as db:
            snap = play_console_payload(db) or {}
        panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
        vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}
    except Exception:
        logger.debug("GP preview vitals snapshot failed", exc_info=True)

    rates = free_rates_from_vitals_overview(vitals)
    crash_latest = None
    anr_latest = None
    if rates.get("crash_rate_pct") is not None:
        crash_latest = float(rates["crash_rate_pct"]) / 100.0
    if rates.get("anr_rate_pct") is not None:
        anr_latest = float(rates["anr_rate_pct"]) / 100.0

    return {
        "source": "play_scrape",
        "crash_rate_series": [],
        "crash_rate_latest": crash_latest,
        "anr_rate_series": [],
        "anr_rate_latest": anr_latest,
        "dates": keys,
        "install_stats": install_stats,
        "scrape_rates": rates,
        "vitals": vitals,
    }


def _ratings_from_app_intel(pid: str) -> tuple[float | None, int | None, dict[str, int]]:
    try:
        from backend.services.app_intel import get_raw_product_data

        raw = get_raw_product_data(pid, cache_only=True)
        meta = ((raw or {}).get("android") or {}).get("meta") or {}
        score = meta.get("score")
        total = meta.get("ratings")
        hist = meta.get("histogram") or {}
        dist: dict[str, int] = {}
        if isinstance(hist, dict):
            dist = {str(k): int(v or 0) for k, v in hist.items()}
        elif isinstance(hist, (list, tuple)) and len(hist) >= 5:
            dist = {str(i + 1): int(hist[i] or 0) for i in range(5)}
        return (
            float(score) if score is not None else None,
            int(total) if total is not None else None,
            dist,
        )
    except Exception:
        return None, None, {}


def _overlay_live_ratings(payload: dict[str, Any], package_name: str, product_id: str) -> dict[str, Any]:
    """Önce /android scrape + intel cache; public store scrape yalnızca boşsa."""
    score = None
    total = None
    dist: dict[str, int] = {}

    score, total, dist = _ratings_from_app_intel(product_id)

    try:
        from backend.database import SessionLocal
        from backend.services.play_console_store import play_console_payload

        with SessionLocal() as db:
            snap = play_console_payload(db) or {}
        rs = snap.get("rating_summary") if isinstance(snap.get("rating_summary"), dict) else {}
        raw = rs.get("default_rating")
        users_raw = rs.get("users")
        users_ok = users_raw not in (None, "", "—")
        if users_ok and raw not in (None, "", "—"):
            try:
                score = float(str(raw).replace(",", "."))
            except (TypeError, ValueError):
                pass
    except Exception:
        logger.debug("GP preview play-console rating failed", exc_info=True)

    if score is None or not dist:
        try:
            from google_play_scraper import app as gp_app

            meta = gp_app(package_name, lang="tr", country="tr")
            if score is None and meta.get("score") is not None:
                score = float(meta["score"])
            if total is None and meta.get("ratings") is not None:
                total = int(meta["ratings"])
            histogram = meta.get("histogram")
            if not dist and isinstance(histogram, (list, tuple)) and len(histogram) >= 5:
                dist = {str(i + 1): int(histogram[i] or 0) for i in range(5)}
            elif not dist and isinstance(histogram, dict):
                dist = {str(k): int(v or 0) for k, v in histogram.items()}
            if total is None and dist:
                total = sum(dist.values())
        except Exception as exc:
            logger.warning("GP store meta alınamadı (%s): %s", package_name, exc)

    if score is None and not dist:
        return payload

    payload["ratings"] = {
        "average": round(float(score), 3) if score is not None else None,
        "total": int(total) if total is not None else (sum(dist.values()) if dist else None),
        "distribution": dist,
        "delta_avg": None,
    }
    if payload.get("source") == "empty":
        payload["source"] = "play_scrape"
        payload["source_note"] = "Mağaza puanı / histogram — /android + public store."
    return payload


def _overlay_scrape_vitals(payload: dict[str, Any], live: dict[str, Any]) -> dict[str, Any]:
    crash = live.get("crash_rate_latest")
    anr = live.get("anr_rate_latest")
    c_series = live.get("crash_rate_series") or []
    a_series = live.get("anr_rate_series") or []
    if crash is not None:
        payload["vitals"]["crash_rate"] = round(crash * 100, 3)
        payload["vitals"]["crash_rate_label"] = f"{crash * 100:.2f}%"
        payload["vitals"]["crash_series"] = [round(v * 100, 4) for v in c_series]
    if anr is not None:
        payload["vitals"]["anr_rate"] = round(anr * 100, 3)
        payload["vitals"]["anr_rate_label"] = f"{anr * 100:.2f}%"
        payload["vitals"]["anr_series"] = [round(v * 100, 4) for v in a_series]

    rates = live.get("scrape_rates") if isinstance(live.get("scrape_rates"), dict) else {}
    cf = rates.get("crash_free_pct")
    af = rates.get("anr_free_pct")
    if cf is not None:
        payload["vitals"]["crash_free_pct"] = float(cf)
        payload["vitals"]["crash_free_label"] = rates.get("crash_free_fmt") or f"{cf:.2f}%"
    if af is not None:
        payload["vitals"]["anr_free_pct"] = float(af)
        payload["vitals"]["anr_free_label"] = rates.get("anr_free_fmt") or f"{af:.2f}%"

    inst = live.get("install_stats") or {}
    has_installs = bool(inst and (inst.get("installs_series") or inst.get("dates")))
    if has_installs:
        i_series = [float(x) for x in (inst.get("installs_series") or [])]
        u_series = [float(x) for x in (inst.get("uninstalls_series") or [])]
        total_i = int(inst.get("total_installs") or round(sum(i_series)))
        total_u = int(inst.get("total_uninstalls") or round(sum(u_series)))
        net = max(0, total_i - total_u)
        payload["kpis"]["installs"] = _kpi(float(total_i), series=i_series)
        payload["kpis"]["uninstalls"] = _kpi(float(total_u), series=u_series)
        payload["kpis"]["net_installs"] = _kpi(
            float(net),
            series=[
                max(0.0, i_series[k] - u_series[k])
                for k in range(min(len(i_series), len(u_series)))
            ],
        )
        payload["trend_daily"] = [
            {
                "i": k,
                "installs": round(i_series[k]),
                "uninstalls": round(u_series[k] if k < len(u_series) else 0),
            }
            for k in range(len(i_series))
        ]
        payload["trend_daily_is_demo"] = False

    has_vitals = (crash is not None) or (anr is not None) or cf is not None or af is not None
    if has_vitals or has_installs:
        payload["source"] = "play_scrape"
        payload["source_note"] = (
            "Play Console — /android explorer_facts + vitals overview. "
            "Sentetik veri yok."
        )
    return payload


def _overlay_stability_free(payload: dict[str, Any], product_id: str, package_name: str) -> dict[str, Any]:
    """Crash/ANR-free — scrape vitals boşsa stability-free (scrape öncelikli)."""
    if payload["vitals"].get("crash_rate") is not None and payload["vitals"].get("anr_rate") is not None:
        return payload
    try:
        from backend.database import SessionLocal
        from backend.services.play_console_store import play_console_payload
        from backend.services.stability_free import build_stability_free_payload

        vitals: dict = {}
        try:
            with SessionLocal() as db:
                snap = play_console_payload(db) or {}
            panels = snap.get("panels") if isinstance(snap.get("panels"), dict) else {}
            vitals = panels.get("vitals") if isinstance(panels.get("vitals"), dict) else {}
        except Exception:
            vitals = {}

        sf = build_stability_free_payload(
            package_name=package_name,
            product_id=product_id,
            vitals=vitals,
        )
        if not sf or not sf.get("ok"):
            return payload
        play = sf.get("play_overall") if isinstance(sf.get("play_overall"), dict) else {}
        cf = play.get("crash_free_pct")
        af = play.get("anr_free_pct")
        if cf is not None:
            payload["vitals"]["crash_free_pct"] = float(cf)
            payload["vitals"]["crash_free_label"] = play.get("crash_free_fmt") or f"{cf:.2f}%"
            if payload["vitals"].get("crash_rate") is None:
                rate = max(0.0, 100.0 - float(cf))
                payload["vitals"]["crash_rate"] = round(rate, 3)
                payload["vitals"]["crash_rate_label"] = f"{rate:.2f}%"
        if af is not None:
            payload["vitals"]["anr_free_pct"] = float(af)
            payload["vitals"]["anr_free_label"] = play.get("anr_free_fmt") or f"{af:.2f}%"
            if payload["vitals"].get("anr_rate") is None:
                rate = max(0.0, 100.0 - float(af))
                payload["vitals"]["anr_rate"] = round(rate, 3)
                payload["vitals"]["anr_rate_label"] = f"{rate:.2f}%"
        if payload.get("source") == "empty" and (cf is not None or af is not None):
            payload["source"] = "play_scrape"
            payload["source_note"] = "Vitals crash/ANR-free — /android."
    except Exception:
        logger.debug("GP preview stability-free overlay failed", exc_info=True)
    return payload


def build_gp_preview_payload(
    product_id: str,
    period_days: int,
    country: str = "all",
    device: str = "all",
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    pid = (product_id or "doviz").strip().lower()
    if pid not in APP_PRODUCTS:
        return {"error": "unknown_product"}

    try:
        p = int(period_days)
    except (TypeError, ValueError):
        p = 30
    if p not in _PERIODS:
        p = 30

    cc = (country or "all").strip().lower()
    dev = (device or "all").strip().lower()
    label = APP_PRODUCTS[pid]["label"]
    pkg = APP_PRODUCTS[pid].get("android_package") or ""
    cache_key = f"gp|{pid}|{p}|{cc}|{dev}|scrape"

    if not force_refresh:
        with _PREVIEW_CACHE_LOCK:
            hit = _PREVIEW_CACHE.get(cache_key)
            if hit and (time.time() - hit[0]) < _PREVIEW_CACHE_TTL_S:
                return hit[1]

    payload = _empty_payload(pid, label, p, cc, dev)

    def _fetch_scrape() -> dict[str, Any] | None:
        try:
            return _analytics_from_scrape(p)
        except Exception as exc:  # noqa: BLE001
            logger.warning("GP scrape analytics başarısız: %s", exc)
        return None

    def _fetch_ratings_branch() -> dict[str, Any]:
        local = dict(payload)
        if pkg:
            return _overlay_live_ratings(local, pkg, pid)
        return local

    live = None
    ratings_payload = None
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_scr = pool.submit(_fetch_scrape)
        f_rat = pool.submit(_fetch_ratings_branch)
        for fut in as_completed((f_scr, f_rat), timeout=25):
            try:
                if fut is f_scr:
                    live = fut.result()
                else:
                    ratings_payload = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("GP preview parallel step: %s", exc)

    if live:
        payload = _overlay_scrape_vitals(payload, live)
    if isinstance(ratings_payload, dict) and ratings_payload.get("ratings"):
        payload["ratings"] = ratings_payload["ratings"]
        if payload.get("source") == "empty" and ratings_payload.get("source") != "empty":
            payload["source"] = ratings_payload.get("source") or "play_scrape"
            payload["source_note"] = ratings_payload.get("source_note") or payload.get("source_note")

    if pkg:
        payload = _overlay_stability_free(payload, pid, pkg)

    # Reporting/GCS: yalnızca scrape kurulum+vitals tamamen boşsa
    needs_api = (
        payload["kpis"]["installs"].get("is_unavailable")
        and payload["vitals"].get("crash_rate") is None
        and payload["vitals"].get("anr_rate") is None
    )
    if needs_api and pkg:
        try:
            from backend.services import gp_client

            if gp_client.is_configured():
                api_live = gp_client.build_gp_analytics_payload(pkg, days=p)
                if api_live:
                    crash = api_live.get("crash_rate_latest")
                    anr = api_live.get("anr_rate_latest")
                    if crash is not None and payload["vitals"].get("crash_rate") is None:
                        payload["vitals"]["crash_rate"] = round(crash * 100, 3)
                        payload["vitals"]["crash_rate_label"] = f"{crash * 100:.2f}%"
                    if anr is not None and payload["vitals"].get("anr_rate") is None:
                        payload["vitals"]["anr_rate"] = round(anr * 100, 3)
                        payload["vitals"]["anr_rate_label"] = f"{anr * 100:.2f}%"
                    inst = api_live.get("install_stats") or {}
                    if inst.get("installs_series") and payload["kpis"]["installs"].get("is_unavailable"):
                        i_series = [float(x) for x in (inst.get("installs_series") or [])]
                        u_series = [float(x) for x in (inst.get("uninstalls_series") or [])]
                        total_i = int(inst.get("total_installs") or 0)
                        total_u = int(inst.get("total_uninstalls") or 0)
                        payload["kpis"]["installs"] = _kpi(float(total_i), series=i_series)
                        payload["kpis"]["uninstalls"] = _kpi(float(total_u), series=u_series)
                        payload["kpis"]["net_installs"] = _kpi(float(max(0, total_i - total_u)))
                        payload["trend_daily"] = [
                            {
                                "i": k,
                                "installs": round(i_series[k]),
                                "uninstalls": round(u_series[k] if k < len(u_series) else 0),
                            }
                            for k in range(len(i_series))
                        ]
                        payload["trend_daily_is_demo"] = False
                    payload["source"] = "api_fallback"
                    payload["source_note"] = (
                        "Konsol verisi yoktu — Play Reporting/GCS yedek (sentetik değil)."
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("GP API fallback başarısız: %s", exc)

    with _PREVIEW_CACHE_LOCK:
        _PREVIEW_CACHE[cache_key] = (time.time(), payload)
        if len(_PREVIEW_CACHE) > 48:
            cutoff = time.time() - _PREVIEW_CACHE_TTL_S
            for k, (ts, _) in list(_PREVIEW_CACHE.items()):
                if ts < cutoff:
                    del _PREVIEW_CACHE[k]
    return payload
