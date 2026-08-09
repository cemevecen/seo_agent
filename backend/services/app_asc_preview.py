"""
App Store Connect özet — /ios sekmesiyle aynı ASC scrape (+ opsiyonel Sales API).

Sentetik / seed demo üretmez. Scrape veya API yoksa alanlar "—" kalır.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, timedelta
from functools import lru_cache
from typing import Any

import httpx

from backend.services.app_intel import APP_PRODUCTS

logger = logging.getLogger(__name__)

_PERIODS: tuple[int, ...] = (0, 1, 7, 14, 30, 90, 365)

_PREVIEW_CACHE_TTL_S = 8 * 60
_PREVIEW_CACHE_LOCK = threading.Lock()
_PREVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

_COUNTRIES: list[dict[str, str]] = [
    {"code": "all", "name": "Tüm ülkeler"},
    {"code": "tr", "name": "Türkiye"},
    {"code": "us", "name": "ABD"},
    {"code": "de", "name": "Almanya"},
    {"code": "gb", "name": "Birleşik Krallık"},
    {"code": "fr", "name": "Fransa"},
    {"code": "nl", "name": "Hollanda"},
    {"code": "az", "name": "Azerbaycan"},
    {"code": "kz", "name": "Kazakistan"},
]

_SOURCES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm kaynaklar"},
    {"id": "search", "name": "App Store Arama"},
    {"id": "browse", "name": "App Store Keşfet"},
    {"id": "referrer_web", "name": "Web yönlendirme"},
    {"id": "referrer_app", "name": "Uygulama yönlendirme"},
    {"id": "institutional", "name": "Kurumsal"},
    {"id": "unavailable", "name": "Bilinmiyor"},
]

_DEVICES: list[dict[str, str]] = [
    {"id": "all", "name": "Tüm cihazlar"},
    {"id": "iphone", "name": "iPhone"},
    {"id": "ipad", "name": "iPad"},
    {"id": "ipod", "name": "iPod"},
]


@lru_cache(maxsize=32)
def _itunes_rating(app_id: str, country: str = "tr") -> dict[str, Any] | None:
    try:
        url = f"https://itunes.apple.com/lookup?id={app_id}&country={country}"
        with httpx.Client(timeout=10) as cli:
            resp = cli.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        data = resp.json().get("results", [])
        if not data:
            return None
        r = data[0]
        avg = r.get("averageUserRating")
        cnt = r.get("userRatingCount")
        if avg is None:
            return None
        return {"average": round(float(avg), 2), "total": int(cnt or 0)}
    except Exception as exc:
        logger.debug("iTunes lookup hatası (app_id=%s): %s", app_id, exc)
        return None


def _best_itunes_rating(app_id: str) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    for cc in ("tr", "us", "de", "gb"):
        r = _itunes_rating(app_id, cc)
        if r and (best is None or r["total"] > best["total"]):
            best = r
    return best


def _series_delta(series: list[float]) -> float | None:
    n = len(series)
    if n < 4:
        return None
    mid = n // 2
    first_avg = sum(series[:mid]) / mid
    second_avg = sum(series[mid:]) / (n - mid)
    if first_avg == 0:
        return None
    return round((second_avg - first_avg) / first_avg * 100, 1)


def _fmt_compact(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M".rstrip("0").rstrip(".")
    if n >= 1000:
        return f"{n / 1000:.1f}K".rstrip("0").rstrip(".")
    return f"{n:.0f}"


def _fmt_money(n: float) -> str:
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M".replace(".00M", "M")
    if n >= 1000:
        return f"${n / 1000:.1f}K".replace(".0K", "K")
    return f"${n:.2f}"


def _fmt_pct(n: float) -> str:
    return f"{n:.2f}%"


def _empty_kpi(*, unavailable: bool = True) -> dict[str, Any]:
    return {
        "value": None,
        "value_label": "—",
        "delta_pct": None,
        "series": [],
        "is_unavailable": unavailable,
    }


def _kpi(
    value: float | None,
    *,
    series: list[float] | None = None,
    money: bool = False,
    pct: bool = False,
) -> dict[str, Any]:
    if value is None:
        return _empty_kpi()
    s = [float(x) for x in (series or [])]
    if money:
        label = _fmt_money(float(value))
    elif pct:
        label = _fmt_pct(float(value))
    else:
        label = _fmt_compact(float(value))
    return {
        "value": round(float(value), 3 if pct else 2),
        "value_label": label,
        "delta_pct": _series_delta(s),
        "series": s,
        "is_unavailable": False,
    }


def _series_values(bundle: dict[str, Any]) -> list[float]:
    out: list[float] = []
    for row in bundle.get("series") or []:
        if isinstance(row, dict):
            try:
                out.append(float(row.get("value") or 0))
            except (TypeError, ValueError):
                out.append(0.0)
    return out


def _empty_payload(pid: str, label: str, p: int, cc: str, src: str, dev: str) -> dict[str, Any]:
    empty = _empty_kpi()
    return {
        "source": "empty",
        "source_note": "ASC / Sales verisi henüz yok — /ios sekmesinden sync bekleniyor.",
        "product": pid,
        "product_label": label,
        "period_days": p,
        "filters": {"country": cc, "source": src, "device": dev},
        "available_filters": {
            "periods": list(_PERIODS),
            "countries": _COUNTRIES,
            "sources": _SOURCES,
            "devices": _DEVICES,
        },
        "kpis": {
            "impressions": dict(empty),
            "product_page_views": dict(empty),
            "total_downloads": dict(empty),
            "conversion_rate": dict(empty),
            "proceeds": dict(empty),
            "active_devices": dict(empty),
        },
        "acquisition": {
            "impressions": dict(empty),
            "product_page_views": dict(empty),
            "first_time_downloads": dict(empty),
            "redownloads": dict(empty),
            "total_downloads": dict(empty),
            "updates": dict(empty),
            "conversion_rate": dict(empty),
        },
        "sales": {
            "proceeds": dict(empty),
            "paying_users": dict(empty),
            "in_app_purchases": dict(empty),
            "d1_download_to_paid": None,
            "d7_download_to_paid": None,
            "d35_download_to_paid": None,
            "refund_rate_pct": None,
            "arpu": dict(empty),
        },
        "subscriptions": {
            "active_plans": dict(empty),
            "paid_plans": dict(empty),
            "free_trials": dict(empty),
            "mrr": dict(empty),
            "net_paid_plans": {"value": None, "value_label": "—", "daily_bars": []},
            "plan_starts": None,
            "churned": None,
            "trial_conversion_pct": None,
        },
        "engagement": {
            "sessions": dict(empty),
            "active_devices": dict(empty),
            "crashes": dict(empty),
            "crash_rate_pct": None,
            "sessions_per_device": None,
            "retention_d1": None,
            "retention_d7": None,
            "retention_d28": None,
            "avg_session_seconds": None,
        },
        "ratings": {
            "average": None,
            "total": None,
            "distribution": {},
            "delta_avg": None,
            "source": None,
        },
        "top_countries": [],
        "top_sources": [],
        "top_versions": [],
        "trend_daily": [],
        "trend_daily_is_demo": False,
        "top_countries_is_demo": False,
        "top_sources_is_demo": False,
    }


def _overlay_asc_scrape(payload: dict[str, Any], overview: dict[str, Any]) -> dict[str, Any]:
    """ /ios asc-metrics/overview scrape paketini /app ASC paneline map et."""
    bundles = overview.get("bundles") or []
    by_metric: dict[str, dict[str, Any]] = {}
    for b in bundles:
        if not isinstance(b, dict):
            continue
        m = (b.get("metric") or "").strip()
        if m and b.get("ok"):
            by_metric[m] = b

    if not by_metric:
        return payload

    def _from_metric(key: str, *, money: bool = False, pct: bool = False) -> dict[str, Any] | None:
        b = by_metric.get(key)
        if not b or b.get("total") is None:
            return None
        return _kpi(float(b["total"]), series=_series_values(b), money=money, pct=pct)

    units = _from_metric("units")
    redownloads = _from_metric("redownloads")
    impressions = _from_metric("impressions")
    page_views = _from_metric("page_views")
    conversion = _from_metric("conversion_rate", pct=True)
    proceeds = _from_metric("proceeds", money=True)
    iap = _from_metric("iap")
    paying = _from_metric("paying_users")
    active_subs = _from_metric("active_subscriptions")

    if units:
        payload["acquisition"]["first_time_downloads"] = units
    if redownloads:
        payload["acquisition"]["redownloads"] = redownloads

    total_dl = None
    if units or redownloads:
        u_v = float((units or {}).get("value") or 0)
        r_v = float((redownloads or {}).get("value") or 0)
        u_s = list((units or {}).get("series") or [])
        r_s = list((redownloads or {}).get("series") or [])
        n = max(len(u_s), len(r_s))
        merged = [
            (u_s[i] if i < len(u_s) else 0.0) + (r_s[i] if i < len(r_s) else 0.0)
            for i in range(n)
        ]
        total_dl = _kpi(u_v + r_v, series=merged)
        payload["acquisition"]["total_downloads"] = total_dl
        payload["kpis"]["total_downloads"] = total_dl

    if impressions:
        payload["acquisition"]["impressions"] = impressions
        payload["kpis"]["impressions"] = impressions
    if page_views:
        payload["acquisition"]["product_page_views"] = page_views
        payload["kpis"]["product_page_views"] = page_views
    if conversion:
        payload["acquisition"]["conversion_rate"] = conversion
        payload["kpis"]["conversion_rate"] = conversion
    if proceeds:
        payload["sales"]["proceeds"] = proceeds
        payload["kpis"]["proceeds"] = proceeds
    if iap:
        payload["sales"]["in_app_purchases"] = iap
    if paying:
        payload["sales"]["paying_users"] = paying
        pv = float(paying.get("value") or 0)
        pr = float((proceeds or {}).get("value") or 0)
        if pv > 0 and pr > 0:
            payload["sales"]["arpu"] = _kpi(pr / pv, money=True)
    if active_subs:
        payload["subscriptions"]["active_plans"] = active_subs
        payload["subscriptions"]["paid_plans"] = active_subs

    # Trend: scrape serileri
    dl_s = list((total_dl or units or {}).get("series") or [])
    pr_s = list((proceeds or {}).get("series") or [])
    imp_s = list((impressions or {}).get("series") or [])
    pv_s = list((page_views or {}).get("series") or [])
    n = max(len(dl_s), len(pr_s), len(imp_s), len(pv_s), 0)
    if n:
        payload["trend_daily"] = [
            {
                "i": i,
                "downloads": round(dl_s[i] if i < len(dl_s) else 0),
                "proceeds": round(pr_s[i] if i < len(pr_s) else 0, 2),
                "impressions": round(imp_s[i] if i < len(imp_s) else 0),
                "page_views": round(pv_s[i] if i < len(pv_s) else 0),
            }
            for i in range(n)
        ]
        payload["trend_daily_is_demo"] = False

    payload["source"] = "asc_scrape"
    payload["source_note"] = (
        "Canlı veri — /ios sekmesiyle aynı ASC facts"
        + (f" · {overview.get('scrape_fact_count')} fact" if overview.get("scrape_fact_count") else "")
        + "."
    )
    return payload


def _overlay_real_ratings(payload: dict[str, Any], pid: str) -> dict[str, Any]:
    """Gerçek mağaza puanı + histogram — önce app_intel cache, sonra tek ülke iTunes."""
    ios_app_id = str(APP_PRODUCTS.get(pid, {}).get("ios_app_id") or "")
    live: dict[str, Any] | None = None
    hist: dict[str, int] = {}
    try:
        from backend.services.app_intel import get_raw_product_data

        raw = get_raw_product_data(pid, cache_only=True)
        meta = ((raw or {}).get("ios") or {}).get("meta") or {}
        star = meta.get("star_histogram")
        if isinstance(star, dict):
            hist = {str(k): int(v or 0) for k, v in star.items()}
        elif isinstance(star, (list, tuple)) and len(star) >= 5:
            hist = {str(i + 1): int(star[i] or 0) for i in range(5)}
        if meta.get("score") is not None:
            live = {
                "average": round(float(meta["score"]), 2),
                "total": int(meta.get("ratings_count") or sum(hist.values()) or 0),
            }
    except Exception:
        logger.debug("ASC ratings histogram attach failed", exc_info=True)

    if live is None and ios_app_id:
        # Tek ülke — 4 ülke sırayla ağ çağrısı yapma
        live = _itunes_rating(ios_app_id, "tr") or _itunes_rating(ios_app_id, "us")

    if not live:
        return payload
    payload["ratings"] = {
        "average": live["average"],
        "total": live["total"],
        "distribution": hist,
        "delta_avg": None,
        "source": "live",
    }
    return payload


def _overlay_live_sales(payload: dict[str, Any], live: dict[str, Any]) -> dict[str, Any]:
    """Sales raporu — yalnızca scrape'te boş kalan alanları doldurur."""
    first_dl = int(live.get("first_time_downloads") or 0)
    updates = int(live.get("updates") or 0)
    iap_units = int(live.get("iap_units") or 0)
    total_dl = int(live.get("total_downloads") or first_dl)
    proceeds_v = float(live.get("proceeds_usd") or 0)
    dl_series = [float(x) for x in (live.get("dl_series") or [])]
    pr_series = [float(x) for x in (live.get("pr_series") or [])]

    def _missing(block: str, key: str) -> bool:
        cur = ((payload.get(block) or {}).get(key) or {})
        return cur.get("value") is None or cur.get("is_unavailable")

    if first_dl and _missing("acquisition", "first_time_downloads"):
        payload["acquisition"]["first_time_downloads"] = _kpi(float(first_dl), series=dl_series)
    if updates:
        payload["acquisition"]["updates"] = _kpi(float(updates))
    if total_dl and _missing("acquisition", "total_downloads"):
        payload["acquisition"]["total_downloads"] = _kpi(float(total_dl), series=dl_series)
        payload["kpis"]["total_downloads"] = payload["acquisition"]["total_downloads"]
    if iap_units and _missing("sales", "in_app_purchases"):
        payload["sales"]["in_app_purchases"] = _kpi(float(iap_units))
    if proceeds_v and _missing("sales", "proceeds"):
        payload["sales"]["proceeds"] = _kpi(proceeds_v, money=True, series=pr_series)
        payload["kpis"]["proceeds"] = payload["sales"]["proceeds"]

    country_agg = live.get("country_breakdown") or {}
    if country_agg and not payload.get("top_countries"):
        cc_total = sum(c["downloads"] for c in country_agg.values()) or 1
        new_top = []
        for code, vals in country_agg.items():
            new_top.append(
                {
                    "code": code.lower(),
                    "name": code,
                    "downloads": int(vals["downloads"]),
                    "share_pct": round(vals["downloads"] / cc_total * 100, 1),
                    "delta_pct": 0.0,
                    "proceeds": round(float(vals.get("proceeds") or 0), 2),
                }
            )
        new_top.sort(key=lambda r: r["downloads"], reverse=True)
        payload["top_countries"] = new_top[:15]
        payload["top_countries_is_demo"] = False

    version_agg = live.get("version_breakdown") or {}
    if version_agg and not payload.get("top_versions"):
        v_total = sum(v["downloads"] for v in version_agg.values()) or 1
        new_v = []
        for ver, vals in version_agg.items():
            share = vals["downloads"] / v_total
            new_v.append(
                {
                    "version": ver,
                    "downloads": int(vals["downloads"]),
                    "active_devices": 0,
                    "crash_rate_pct": None,
                    "share_pct": round(share * 100, 1),
                    "is_latest": False,
                }
            )
        new_v.sort(key=lambda r: r["downloads"], reverse=True)
        if new_v:
            new_v[0]["is_latest"] = True
        payload["top_versions"] = new_v[:10]

    if not payload.get("trend_daily") and dl_series:
        payload["trend_daily"] = [
            {
                "i": i,
                "downloads": round(dl_series[i]),
                "proceeds": round(pr_series[i] if i < len(pr_series) else 0, 2),
            }
            for i in range(len(dl_series))
        ]
        payload["trend_daily_is_demo"] = False

    if payload.get("source") in ("empty", None):
        payload["source"] = "live"
        payload["source_note"] = (
            "Canlı veri — App Store Connect Sales raporları (24-48 saat gecikme normaldir)."
        )
    elif payload.get("source") == "asc_scrape":
        payload["source_note"] = (
            (payload.get("source_note") or "ASC sync.")
            + " Sales raporu eksik alanları tamamladı."
        )
    return payload


def _overlay_live_analytics(
    payload: dict[str, Any],
    analytics: dict[str, Any],
    *,
    sales_live: dict[str, Any] | None,
) -> dict[str, Any]:
    def _from_analytics(key: str, *, pct: bool = False) -> dict[str, Any] | None:
        block = analytics.get(key) or {}
        if not isinstance(block, dict):
            return None
        val = block.get("value")
        if val is None:
            return None
        series = [float(x) for x in (block.get("series") or [])]
        return _kpi(float(val), series=series, pct=pct)

    mapping = [
        ("impressions", "impressions", False, "acquisition", "impressions"),
        ("page_views", "product_page_views", False, "acquisition", "product_page_views"),
        ("conversion_rate", "conversion_rate", True, "acquisition", "conversion_rate"),
        ("redownloads", "redownloads", False, "acquisition", "redownloads"),
        ("total_downloads", "total_downloads", False, "acquisition", "total_downloads"),
        ("first_time_downloads", "first_time_downloads", False, "acquisition", "first_time_downloads"),
    ]
    for src_key, dst_key, is_pct, block, field in mapping:
        cur = ((payload.get(block) or {}).get(field) or {})
        if cur.get("value") is not None and not cur.get("is_unavailable"):
            continue
        kpi = _from_analytics(src_key, pct=is_pct)
        if not kpi:
            continue
        payload[block][field] = kpi
        if dst_key in payload.get("kpis", {}):
            payload["kpis"][dst_key] = kpi

    if sales_live is None:
        pass
    payload["analytics_live"] = True
    payload["analytics_warnings"] = analytics.get("warnings") or []
    if payload.get("source") == "empty":
        payload["source"] = "live"
    return payload


def _overlay_live_subscriptions(payload: dict[str, Any], subs: dict[str, Any]) -> dict[str, Any]:
    ap = int(subs.get("active_plans") or 0)
    pp = int(subs.get("paid_plans") or 0)
    ft = int(subs.get("free_trials") or 0)
    if ap and (payload["subscriptions"]["active_plans"].get("value") is None):
        payload["subscriptions"]["active_plans"] = {
            "value": ap,
            "value_label": _fmt_compact(float(ap)),
            "delta_pct": 0.0,
        }
    if pp:
        payload["subscriptions"]["paid_plans"] = {
            "value": pp,
            "value_label": _fmt_compact(float(pp)),
            "delta_pct": 0.0,
        }
    if ft:
        payload["subscriptions"]["free_trials"] = {
            "value": ft,
            "value_label": _fmt_compact(float(ft)),
            "delta_pct": 0.0,
        }
    # MRR / trial_conversion sentetik üretme — sadece gerçek alan varsa
    if subs.get("mrr") is not None:
        mrr_v = float(subs["mrr"])
        payload["subscriptions"]["mrr"] = {
            "value": round(mrr_v, 2),
            "value_label": f"${mrr_v:.0f}",
            "delta_pct": None,
        }
    if subs.get("trial_conversion_pct") is not None:
        payload["subscriptions"]["trial_conversion_pct"] = round(
            float(subs["trial_conversion_pct"]), 1
        )
    return payload


def build_asc_connect_preview_payload(
    product_id: str,
    period_days: int,
    country: str = "all",
    source: str = "all",
    device: str = "all",
    progress_cb=None,
    *,
    include_live_api: bool = False,
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
    effective_p = 365 if p == 0 else p

    cc = (country or "all").strip().lower()
    if cc not in {c["code"] for c in _COUNTRIES}:
        cc = "all"
    src = (source or "all").strip().lower()
    if src not in {s["id"] for s in _SOURCES}:
        src = "all"
    dev = (device or "all").strip().lower()
    if dev not in {d["id"] for d in _DEVICES}:
        dev = "all"

    cache_key = f"asc|{pid}|{effective_p}|{cc}|{src}|{dev}|live={int(bool(include_live_api))}"
    if not force_refresh and not include_live_api:
        with _PREVIEW_CACHE_LOCK:
            hit = _PREVIEW_CACHE.get(cache_key)
            if hit and (time.time() - hit[0]) < _PREVIEW_CACHE_TTL_S:
                if progress_cb:
                    try:
                        progress_cb(1, 1)
                    except Exception:
                        pass
                return hit[1]

    label = APP_PRODUCTS[pid]["label"]
    payload = _empty_payload(pid, label, p, cc, src, dev)

    # 1) /ios ile aynı ASC scrape overview (birincil — hızlı)
    try:
        from backend.services.asc_metrics_warehouse import query_asc_overview

        end_d = date.today()
        start_d = end_d - timedelta(days=max(effective_p - 1, 0))
        bundle_id = APP_PRODUCTS[pid].get("ios_bundle_id") or None
        overview = query_asc_overview(
            start=start_d.isoformat(),
            end=end_d.isoformat(),
            bundle_id=bundle_id,
        )
        if overview and overview.get("ok"):
            payload = _overlay_asc_scrape(payload, overview)
            if progress_cb:
                try:
                    progress_cb(1, 1)
                except Exception:
                    pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("ASC scrape overview overlay başarısız: %s", exc)

    # 2) Sales / Analytics API — yavaş; yalnızca açıkça istenirse
    if include_live_api:
        live = None
        try:
            from backend.services import asc_client

            if asc_client.is_configured():
                bundle = APP_PRODUCTS[pid].get("ios_bundle_id") or ""
                live = asc_client.fetch_daily_sales_summary(
                    bundle_id=bundle,
                    days=effective_p,
                    country=cc,
                    device=dev,
                    progress_cb=progress_cb,
                )
                if live:
                    payload = _overlay_live_sales(payload, live)
                from backend.services import asc_analytics

                analytics = asc_analytics.fetch_analytics_summary(
                    bundle_id=bundle,
                    days=effective_p,
                    country=cc,
                    progress_cb=progress_cb,
                )
                if analytics and analytics.get("ok"):
                    payload = _overlay_live_analytics(payload, analytics, sales_live=live)
                subs = asc_client.fetch_subscription_summary(days=effective_p)
                if subs:
                    payload = _overlay_live_subscriptions(payload, subs)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ASC live overlay başarısız: %s", exc)

    payload = _overlay_real_ratings(payload, pid)

    # Crash-free: yalnız bellek cache (senkron BQ yok — /app paneli hızı)
    try:
        from backend.services.stability_free import build_stability_free_payload

        pkg = APP_PRODUCTS[pid].get("android_package") or "com.Doviz"
        sf = build_stability_free_payload(
            product_id=pid, package_name=pkg, vitals={}, force_refresh=False
        )
        ios_cf = (((sf or {}).get("crashlytics") or {}).get("platforms") or {}).get("ios") or {}
        latest = ios_cf.get("latest") or {}
        overall = ios_cf.get("overall") or {}
        cf_pct = latest.get("crash_free_pct")
        if cf_pct is None:
            cf_pct = overall.get("crash_free_pct")
        if cf_pct is not None:
            payload["engagement"]["crash_rate_pct"] = round(max(0.0, 100.0 - float(cf_pct)), 3)
            payload["engagement"]["crash_free_pct"] = round(float(cf_pct), 3)
            payload["engagement"]["crash_free_version"] = ios_cf.get("latest_version")
    except Exception:
        logger.debug("ASC preview crash-free attach failed", exc_info=True)

    if not include_live_api:
        with _PREVIEW_CACHE_LOCK:
            _PREVIEW_CACHE[cache_key] = (time.time(), payload)
            if len(_PREVIEW_CACHE) > 48:
                cutoff = time.time() - _PREVIEW_CACHE_TTL_S
                for k, (ts, _) in list(_PREVIEW_CACHE.items()):
                    if ts < cutoff:
                        del _PREVIEW_CACHE[k]
    return payload
