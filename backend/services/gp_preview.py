"""
Google Play Store özet — /android sekmesiyle aynı scrape + Reporting kaynakları.

Sentetik / seed demo üretmez. Veri yoksa alanlar "—" kalır.
Hız: bellek cache + paralel overlay; google-play-scraper yalnızca gerekirse.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        "source_note": "Play scrape / Reporting verisi henüz yok — /android sekmesinden scrape bekleniyor.",
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
    """Önce cache/DB; google-play-scraper yalnızca boşsa (yavaş ağ)."""
    score = None
    total = None
    dist: dict[str, int] = {}

    score, total, dist = _ratings_from_app_intel(product_id)

    # Play Console scrape (Özet) — users doluysa tercih
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
        "delta_avg": 0.0 if score is not None else None,
    }
    if payload.get("source") == "empty":
        payload["source"] = "play_scrape"
        payload["source_note"] = "Mağaza puanı / histogram scrape — /android kaynaklarıyla uyumlu."
    return payload


def _overlay_live_vitals(payload: dict[str, Any], live: dict[str, Any]) -> dict[str, Any]:
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

    inst = live.get("install_stats") or {}
    has_installs = bool(inst and inst.get("dates"))
    if has_installs:
        i_series = [float(x) for x in (inst.get("installs_series") or [])]
        u_series = [float(x) for x in (inst.get("uninstalls_series") or [])]
        total_i = int(inst.get("total_installs") or 0)
        total_u = int(inst.get("total_uninstalls") or 0)
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

    has_vitals = (crash is not None) or (anr is not None)
    if has_vitals and has_installs:
        payload["source"] = "live"
        payload["source_note"] = (
            "Canlı veri — Vitals + kurulum CSV (Play Console Reporting). "
            "/android ile aynı Reporting kaynağı."
        )
    elif has_vitals:
        payload["source"] = "live_partial"
        payload["source_note"] = (
            "Kısmi canlı — Android Vitals gerçek. Kurulum CSV için bucket yetkisi gerekir."
        )
    elif has_installs:
        payload["source"] = "live_partial"
        payload["source_note"] = "Kısmi canlı — kurulum CSV gerçek."
    return payload


def _overlay_stability_free(payload: dict[str, Any], product_id: str, package_name: str) -> dict[str, Any]:
    """Crash/ANR-free — yalnızca vitals boşsa (Reporting zaten doldurduysa atla)."""
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
            payload["source_note"] = "Vitals crash/ANR-free — /android stability-free."
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
    cache_key = f"gp|{pid}|{p}|{cc}|{dev}"

    if not force_refresh:
        with _PREVIEW_CACHE_LOCK:
            hit = _PREVIEW_CACHE.get(cache_key)
            if hit and (time.time() - hit[0]) < _PREVIEW_CACHE_TTL_S:
                return hit[1]

    payload = _empty_payload(pid, label, p, cc, dev)

    def _fetch_reporting() -> dict[str, Any] | None:
        try:
            from backend.services import gp_client

            if gp_client.is_configured() and pkg:
                return gp_client.build_gp_analytics_payload(pkg, days=p)
        except Exception as exc:  # noqa: BLE001
            logger.warning("GP live overlay başarısız: %s", exc)
        return None

    def _fetch_ratings_branch() -> dict[str, Any]:
        local = dict(payload)
        if pkg:
            return _overlay_live_ratings(local, pkg, pid)
        return local

    live = None
    ratings_payload = None
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_rep = pool.submit(_fetch_reporting)
        f_rat = pool.submit(_fetch_ratings_branch)
        for fut in as_completed((f_rep, f_rat), timeout=25):
            try:
                if fut is f_rep:
                    live = fut.result()
                else:
                    ratings_payload = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("GP preview parallel step: %s", exc)

    if live:
        payload = _overlay_live_vitals(payload, live)
    if isinstance(ratings_payload, dict) and ratings_payload.get("ratings"):
        payload["ratings"] = ratings_payload["ratings"]
        if payload.get("source") == "empty" and ratings_payload.get("source") != "empty":
            payload["source"] = ratings_payload.get("source") or "play_scrape"
            payload["source_note"] = ratings_payload.get("source_note") or payload.get("source_note")

    if pkg:
        payload = _overlay_stability_free(payload, pid, pkg)

    with _PREVIEW_CACHE_LOCK:
        _PREVIEW_CACHE[cache_key] = (time.time(), payload)
        if len(_PREVIEW_CACHE) > 48:
            cutoff = time.time() - _PREVIEW_CACHE_TTL_S
            for k, (ts, _) in list(_PREVIEW_CACHE.items()):
                if ts < cutoff:
                    del _PREVIEW_CACHE[k]
    return payload
