"""App Store Connect Metrikler — Android play_scrape_warehouse benzeri sorgu katmanı.

Kaynak: ASC API key (üyelik) — Sales & Trends + Analytics Reports + Subscription.
Varsayılan uygulama: Döviz iOS (465599322 / com.nokta.Finans.Takip).
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any

from backend.services import asc_analytics, asc_client

logger = logging.getLogger(__name__)

DEFAULT_BUNDLE = "com.nokta.Finans.Takip"
DEFAULT_APP_ID = "465599322"

# UI metrik anahtarı → (kaynak alanı, toplam modu: sum|avg|last)
_METRIC_META: dict[str, tuple[str, str, str]] = {
    "units": ("first_time_downloads", "sum", "İlk indirme (units)"),
    "redownloads": ("redownloads", "sum", "Yeniden indirme"),
    "total_downloads": ("total_downloads", "sum", "Toplam indirme"),
    "impressions": ("impressions", "sum", "Gösterim"),
    "page_views": ("product_page_views", "sum", "Ürün sayfası görüntüleme"),
    "conversion_rate": ("conversion_rate_pct", "avg", "Dönüşüm oranı (%)"),
    "iap": ("in_app_purchases", "sum", "Uygulama içi satın alma"),
    "paying_users": ("paying_users", "sum", "Ödeyen kullanıcı"),
    "proceeds": ("proceeds_usd", "sum", "Gelir (USD)"),
    "active_subscriptions": ("active_plans", "last", "Aktif abonelik"),
    "free_trials": ("free_trials", "last", "Ücretsiz deneme"),
}

_AVG_METRICS = frozenset({"conversion_rate"})
_STOCK_LAST = frozenset({"active_subscriptions", "free_trials"})


def metric_catalog() -> list[dict[str, str]]:
    return [
        {"value": k, "label": meta[2], "mode": meta[1]}
        for k, meta in _METRIC_META.items()
    ]


def _span_days(start: date, end: date) -> int:
    return max((end - start).days + 1, 1)


def _series_from_parallel(
    dates: list[str],
    values: list[float],
    *,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ds, v in zip(dates, values):
        try:
            d = date.fromisoformat(str(ds)[:10])
        except ValueError:
            continue
        if d < start or d > end:
            continue
        out.append({"key": d.isoformat(), "value": round(float(v or 0), 4)})
    out.sort(key=lambda r: r["key"])
    return out


def _series_total(series: list[dict[str, Any]], metric: str) -> tuple[float, str]:
    if not series:
        return 0.0, "sum"
    vals = [float(r.get("value") or 0) for r in series]
    if metric in _AVG_METRICS:
        return round(sum(vals) / len(vals), 4), "avg"
    if metric in _STOCK_LAST:
        return float(vals[-1]), "last"
    return round(sum(vals), 4), "sum"


def _load_bundle(
    *,
    bundle_id: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    days = _span_days(start, end) + 3  # Apple gecikmesi payı
    days = min(max(days, 7), 365)
    analytics = asc_analytics.fetch_analytics_summary(
        bundle_id=bundle_id, days=days, country="all"
    ) or {}
    sales = None
    if asc_client.is_configured():
        try:
            sales = asc_client.fetch_daily_sales_summary(
                bundle_id=bundle_id, days=days, country="all", device="all"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("ASC sales load failed: %s", exc)
            sales = None
    subs = None
    try:
        subs = asc_client.fetch_subscription_daily_series(days=days)
    except Exception as exc:  # noqa: BLE001
        logger.exception("ASC subscription load failed: %s", exc)
        subs = None
    return {"analytics": analytics, "sales": sales, "subs": subs}


def _pick_series(
    bundle: dict[str, Any],
    metric: str,
    *,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    a = bundle.get("analytics") or {}
    s = bundle.get("sales") or {}
    sub = bundle.get("subs") or {}

    if metric == "units":
        if a.get("ok") and a.get("dates") and a.get("first_downloads_series"):
            return _series_from_parallel(
                a["dates"], a["first_downloads_series"], start=start, end=end
            )
        if s and s.get("dates") and s.get("dl_series"):
            return _series_from_parallel(s["dates"], s["dl_series"], start=start, end=end)
        return []

    if metric == "proceeds":
        if s and s.get("dates") and s.get("pr_series"):
            return _series_from_parallel(s["dates"], s["pr_series"], start=start, end=end)
        return []

    if metric == "active_subscriptions":
        if sub and sub.get("dates"):
            return _series_from_parallel(
                sub["dates"], sub["active_plans_series"], start=start, end=end
            )
        return []

    if metric == "free_trials":
        if sub and sub.get("dates"):
            return _series_from_parallel(
                sub["dates"], sub["free_trials_series"], start=start, end=end
            )
        return []

    if not a.get("ok"):
        return []

    key_map = {
        "redownloads": "redownloads_series",
        "total_downloads": "total_downloads_series",
        "impressions": "impressions_series",
        "page_views": "page_views_series",
        "conversion_rate": "conversion_series",
        "iap": "iap_series",
        "paying_users": "paying_users_series",
    }
    series_key = key_map.get(metric)
    if not series_key or not a.get("dates"):
        return []
    values = a.get(series_key) or []
    return _series_from_parallel(a["dates"], values, start=start, end=end)


def query_asc_metric(
    *,
    start: str | None = None,
    end: str | None = None,
    metric: str = "units",
    bundle_id: str | None = None,
    bundle_cache: dict[str, Any] | None = None,
) -> dict[str, Any]:
    end_d = date.fromisoformat(end) if end else date.today()
    start_d = date.fromisoformat(start) if start else end_d - timedelta(days=29)
    if start_d > end_d:
        start_d = end_d - timedelta(days=29)
    metric_key = (metric or "units").strip()
    if metric_key not in _METRIC_META:
        return {
            "ok": False,
            "message": f"Bilinmeyen metrik: {metric_key}",
            "series": [],
            "total": 0,
            "facets": {"metrics": [m["value"] for m in metric_catalog()]},
        }
    bid = (bundle_id or DEFAULT_BUNDLE).strip()
    if not asc_client.is_configured():
        return {
            "ok": False,
            "configured": False,
            "message": "ASC_KEY_ID / ASC_ISSUER_ID / ASC_PRIVATE_KEY tanımlı değil.",
            "series": [],
            "total": 0,
            "metric": metric_key,
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
        }

    bundle = bundle_cache if bundle_cache is not None else _load_bundle(
        bundle_id=bid, start=start_d, end=end_d
    )
    series = _pick_series(bundle, metric_key, start=start_d, end=end_d)
    total, mode = _series_total(series, metric_key)
    a = bundle.get("analytics") or {}
    warnings = list(a.get("warnings") or [])
    if metric_key == "proceeds" and not series:
        warnings.append("Gelir için ASC_VENDOR_NUMBER ve Sales raporu gerekir.")
    if metric_key in ("active_subscriptions", "free_trials") and not series:
        warnings.append("Abonelik serisi için Sales SUBSCRIPTION raporu gerekir.")

    label = _METRIC_META[metric_key][2]
    return {
        "ok": bool(series),
        "configured": True,
        "source": "asc_api",
        "app_id": DEFAULT_APP_ID,
        "bundle_id": bid,
        "metric": metric_key,
        "label": label,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "series": series,
        "total": total,
        "total_mode": mode,
        "message": (
            f"ASC · {label} · {len(series)} gün"
            + (f" · {'; '.join(warnings[:2])}" if warnings and not series else "")
        ),
        "warnings": warnings,
        "facets": {"metrics": [m["value"] for m in metric_catalog()]},
    }


def query_asc_overview(
    *,
    start: str | None = None,
    end: str | None = None,
    metrics: list[str] | None = None,
    bundle_id: str | None = None,
) -> dict[str, Any]:
    end_d = date.fromisoformat(end) if end else date.today()
    start_d = date.fromisoformat(start) if start else end_d - timedelta(days=29)
    metric_list = metrics or [
        "units",
        "redownloads",
        "impressions",
        "page_views",
        "conversion_rate",
        "iap",
        "paying_users",
        "proceeds",
        "active_subscriptions",
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for m in metric_list:
        m = (m or "").strip()
        if not m or m in seen or m not in _METRIC_META:
            continue
        seen.add(m)
        ordered.append(m)

    if not asc_client.is_configured():
        return {
            "ok": False,
            "configured": False,
            "message": "ASC API anahtarları yok.",
            "bundles": [],
        }

    bid = (bundle_id or DEFAULT_BUNDLE).strip()
    bundle = _load_bundle(bundle_id=bid, start=start_d, end=end_d)
    out_bundles: list[dict[str, Any]] = []
    for m in ordered:
        data = query_asc_metric(
            start=start_d.isoformat(),
            end=end_d.isoformat(),
            metric=m,
            bundle_id=bid,
            bundle_cache=bundle,
        )
        out_bundles.append(
            {
                "metric": m,
                "label": data.get("label") or m,
                "series": data.get("series") or [],
                "total": data.get("total"),
                "total_mode": data.get("total_mode") or "sum",
                "ok": bool(data.get("ok")),
                "message": data.get("message"),
            }
        )
    return {
        "ok": True,
        "configured": True,
        "source": "asc_api",
        "app_id": DEFAULT_APP_ID,
        "bundle_id": bid,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "bundles": out_bundles,
    }


def asc_metrics_status() -> dict[str, Any]:
    configured = asc_client.is_configured()
    vendor = bool((os.getenv("ASC_VENDOR_NUMBER") or "").strip())
    app_id = None
    if configured:
        try:
            app_id = asc_client.find_app_id_by_bundle(DEFAULT_BUNDLE)
        except Exception:  # noqa: BLE001
            app_id = None
    return {
        "ok": configured,
        "configured": configured,
        "vendor_configured": vendor,
        "bundle_id": DEFAULT_BUNDLE,
        "app_id": app_id or DEFAULT_APP_ID,
        "metrics": metric_catalog(),
        "console_urls": {
            "distribution": f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/distribution",
            "ratings": f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/distribution/ratings/ios",
            "analytics": f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics",
            "finance": "https://appstoreconnect.apple.com/itc/payments_and_financial_reports#/",
        },
    }
