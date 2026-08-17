"""App Store Connect Metrikler — Android play_scrape_warehouse benzeri sorgu katmanı.

Kaynak: Mac ASC console tarama (explorer_facts). Sales / Analytics / Subscription API yedek kapalı.
Varsayılan uygulama: Döviz iOS (465599322 / com.nokta.Finans.Takip).
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any

from backend.services.asc_console_store import load_asc_scrape_facts

logger = logging.getLogger(__name__)

DEFAULT_BUNDLE = "com.nokta.Finans.Takip"
DEFAULT_APP_ID = "465599322"

# Kısa TTL: overview → filtre / hızlı preset değişiminde Apple/scrape tekrarını kes
_BUNDLE_TTL_SEC = 300.0
_SCRAPE_TTL_SEC = 300.0
_bundle_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_scrape_cache: tuple[float, list[dict[str, Any]], dict[str, Any]] | None = None


def invalidate_asc_metrics_cache() -> None:
    """ASC ingest sonrası okuma önbelleklerini düşür — /ios sayfası taze veriyi görsün."""
    global _scrape_cache
    _scrape_cache = None
    _bundle_cache.clear()

_SALES_METRICS = frozenset({"units", "proceeds", "total_downloads", "sales"})
_ANALYTICS_METRICS = frozenset(
    {
        "units",
        "redownloads",
        "total_downloads",
        "impressions",
        "page_views",
        "conversion_rate",
        "iap",
        "paying_users",
        "sales",
        "active_devices",
        "sessions",
        "installs",
        "crashes",
        "uninstalls",
    }
)
_SUBS_METRICS = frozenset(
    {
        "active_subscriptions",
        "free_trials",
        "subscription_churned",
        "subscription_renewals",
    }
)

# UI metrik anahtarı → (kaynak alanı, toplam modu: sum|avg|last)
_METRIC_META: dict[str, tuple[str, str, str]] = {
    "units": ("first_time_downloads", "sum", "First-time downloads"),
    "redownloads": ("redownloads", "sum", "Redownloads"),
    "total_downloads": ("total_downloads", "sum", "Total downloads"),
    "impressions": ("impressions", "sum", "Impressions"),
    "page_views": ("product_page_views", "sum", "Product page views"),
    "conversion_rate": ("conversion_rate_pct", "avg", "Conversion rate (%)"),
    "iap": ("in_app_purchases", "sum", "In-app purchases"),
    "paying_users": ("paying_users", "sum", "Paying users"),
    "proceeds": ("proceeds_usd", "sum", "Proceeds (USD)"),
    "sales": ("sales", "sum", "Sales"),
    "active_devices": ("active_devices", "sum", "Active devices"),
    "sessions": ("sessions", "sum", "Sessions"),
    "installs": ("installs", "sum", "Installs"),
    "crashes": ("crashes", "sum", "Crashes"),
    "uninstalls": ("uninstalls", "sum", "Uninstalls"),
    "active_subscriptions": ("active_plans", "last", "Active subscriptions"),
    "subscription_churned": ("subscription_churned", "sum", "Subscription churn"),
    "subscription_renewals": ("subscription_renewals", "sum", "Subscription renewals"),
    "free_trials": ("free_trials", "last", "Free trials"),
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


def _cached_scrape_facts() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    global _scrape_cache
    now = time.monotonic()
    if _scrape_cache is not None and (now - _scrape_cache[0]) < _SCRAPE_TTL_SEC:
        return _scrape_cache[1], _scrape_cache[2]
    facts: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    try:
        facts, meta = load_asc_scrape_facts()
    except Exception as exc:  # noqa: BLE001
        logger.exception("ASC scrape facts load failed: %s", exc)
    _scrape_cache = (now, facts, meta)
    return facts, meta


def _scrape_covers_metric(
    facts: list[dict[str, Any]],
    metric: str,
    *,
    start: date,
    end: date,
) -> bool:
    if metric == "total_downloads":
        # Doğrudan fact veya units/redownloads türetimi
        if _series_from_scrape_facts(facts, "total_downloads", start=start, end=end, _derive=False):
            return True
        return bool(
            _series_from_scrape_facts(facts, "units", start=start, end=end, _derive=False)
            or _series_from_scrape_facts(
                facts, "redownloads", start=start, end=end, _derive=False
            )
        )
    return bool(_series_from_scrape_facts(facts, metric, start=start, end=end, _derive=False))


def _series_from_scrape_facts(
    facts: list[dict[str, Any]],
    metric: str,
    *,
    start: date,
    end: date,
    _derive: bool = True,
) -> list[dict[str, Any]]:
    if metric == "total_downloads" and _derive:
        direct = _series_from_scrape_facts(
            facts, "total_downloads", start=start, end=end, _derive=False
        )
        if direct:
            return direct
        units = {
            r["key"]: float(r["value"])
            for r in _series_from_scrape_facts(
                facts, "units", start=start, end=end, _derive=False
            )
        }
        redos = {
            r["key"]: float(r["value"])
            for r in _series_from_scrape_facts(
                facts, "redownloads", start=start, end=end, _derive=False
            )
        }
        keys = sorted(set(units) | set(redos))
        return [
            {
                "key": k,
                "value": round(units.get(k, 0.0) + redos.get(k, 0.0), 4),
            }
            for k in keys
        ]

    by_date: dict[str, float] = {}
    for f in facts:
        if str(f.get("metric") or "") != metric:
            continue
        if str(f.get("dim") or "overview") not in ("overview", "", "all"):
            continue
        ds = str(f.get("date") or "")[:10]
        if not ds:
            continue
        try:
            d = date.fromisoformat(ds)
        except ValueError:
            continue
        if d < start or d > end:
            continue
        try:
            by_date[ds] = float(f.get("value") or 0)
        except (TypeError, ValueError):
            continue
    return [{"key": k, "value": round(by_date[k], 4)} for k in sorted(by_date.keys())]


def _load_bundle(
    *,
    bundle_id: str,
    start: date,
    end: date,
    needed_metrics: list[str] | None = None,
) -> dict[str, Any]:
    metrics = [
        m
        for m in (needed_metrics or list(_METRIC_META.keys()))
        if m in _METRIC_META
    ]
    if not metrics:
        metrics = list(_METRIC_META.keys())
    cache_key = (
        f"{bundle_id}|{start.isoformat()}|{end.isoformat()}|"
        + ",".join(sorted(metrics))
    )
    now = time.monotonic()
    hit = _bundle_cache.get(cache_key)
    if hit is not None and (now - hit[0]) < _BUNDLE_TTL_SEC:
        return hit[1]

    scrape_facts, scrape_meta = _cached_scrape_facts()
    out = {
        "analytics": {},
        "sales": None,
        "subs": None,
        "scrape_facts": scrape_facts,
        "scrape_meta": scrape_meta,
    }
    _bundle_cache[cache_key] = (now, out)
    if len(_bundle_cache) > 48:
        cutoff = now - _BUNDLE_TTL_SEC
        for k, (ts, _) in list(_bundle_cache.items()):
            if ts < cutoff:
                _bundle_cache.pop(k, None)
    return out


def _pick_series(
    bundle: dict[str, Any],
    metric: str,
    *,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    # 1) Mac scrape facts (Android Play pattern) — Analytics boşsa asıl kaynak
    scrape_facts = bundle.get("scrape_facts") or []
    if scrape_facts:
        scraped = _series_from_scrape_facts(
            scrape_facts, metric, start=start, end=end
        )
        if scraped:
            return scraped

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
    compare: str | None = None,
    breakdown: str | None = "date",
    dim: str | None = "overview",
    segment: str | None = "all",
) -> dict[str, Any]:
    end_d = date.fromisoformat(end) if end else date.today()
    start_d = date.fromisoformat(start) if start else end_d - timedelta(days=29)
    if start_d > end_d:
        start_d = end_d - timedelta(days=29)
    metric_key = (metric or "units").strip()
    dim_key = (dim or "overview").strip().lower() or "overview"
    seg_key = (segment or "all").strip() or "all"
    if metric_key not in _METRIC_META:
        return {
            "ok": False,
            "message": f"Bilinmeyen metrik: {metric_key}",
            "series": [],
            "total": 0,
            "facets": _asc_facets(metric_key),
        }
    bid = (bundle_id or DEFAULT_BUNDLE).strip()
    scrape_facts_probe, _ = _cached_scrape_facts()
    if not scrape_facts_probe:
        return {
            "ok": False,
            "configured": False,
            "message": (
                "ASC tarama verisi yok — "
                "Mac’te ASC console login + sync + ingest."
            ),
            "series": [],
            "total": 0,
            "metric": metric_key,
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "facets": _asc_facets(metric_key),
        }

    # Ülke / cihaz / sürüm kırılımı tarama fact’lerinde yok (eski Sales API yedek kapalı)
    if dim_key in ("country", "device", "app_version"):
        br = (breakdown or "segment").strip().lower()
        if br not in ("segment", "date", "week", "month"):
            br = "segment"
        if seg_key.lower() == "all" and br == "date":
            br = "segment"
        label = _METRIC_META[metric_key][2]
        return {
            "ok": True,
            "configured": True,
            "source": "asc_scrape",
            "app_id": DEFAULT_APP_ID,
            "bundle_id": bid,
            "metric": metric_key,
            "label": label,
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "breakdown": br,
            "dim": dim_key,
            "segment": seg_key,
            "series": [],
            "total": 0.0,
            "total_mode": "sum",
            "compare": None,
            "message": f"{label} · {dim_key} kırılımı tarama verisinde yok.",
            "warnings": [],
            "facets": _asc_facets(metric_key),
            "empty": True,
        }

    # Compare için önceki dönemi de kapsayan aralık yükle
    load_start, load_end = start_d, end_d
    want_compare = (compare or "").strip() == "previous_period"
    if want_compare:
        span = (end_d - start_d).days + 1
        pe = start_d - timedelta(days=1)
        ps = pe - timedelta(days=span - 1)
        load_start = ps

    bundle = bundle_cache if bundle_cache is not None else _load_bundle(
        bundle_id=bid,
        start=load_start,
        end=load_end,
        needed_metrics=[metric_key],
    )
    series = _pick_series(bundle, metric_key, start=start_d, end=end_d)
    br = (breakdown or "date").strip().lower()
    if br in ("week", "month"):
        series = _aggregate_series_client(series, br, metric_key)
    total, mode = _series_total(series, metric_key)
    a = bundle.get("analytics") or {}
    warnings = list(a.get("warnings") or [])
    scrape_facts = bundle.get("scrape_facts") or []
    source = "asc_scrape"

    compare_payload = None
    if want_compare:
        span = (end_d - start_d).days + 1
        pe = start_d - timedelta(days=1)
        ps = pe - timedelta(days=span - 1)
        prev_series = _pick_series(bundle, metric_key, start=ps, end=pe)
        if br in ("week", "month"):
            prev_series = _aggregate_series_client(prev_series, br, metric_key)
        prev_total, _ = _series_total(prev_series, metric_key)
        prev_available = bool(prev_series)
        delta_pct = None
        if prev_available and prev_total:
            delta_pct = round((total - prev_total) / abs(prev_total) * 100.0, 2)
        compare_payload = {
            "mode": "previous_period",
            "start": ps.isoformat(),
            "end": pe.isoformat(),
            "total": prev_total if prev_available else None,
            "delta_pct": delta_pct,
            "series": prev_series if prev_available else [],
            "total_mode": mode,
            "available": prev_available,
            "missing_reason": (
                None
                if prev_available
                else "İlgili dönem için önceki dönem verisi bulunamadı"
            ),
        }

    label = _METRIC_META[metric_key][2]
    return {
        "ok": True,
        "configured": True,
        "source": source,
        "app_id": DEFAULT_APP_ID,
        "bundle_id": bid,
        "metric": metric_key,
        "label": label,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "breakdown": br if br in ("week", "month", "date") else "date",
        "dim": "overview",
        "segment": "all",
        "series": series,
        "total": total,
        "total_mode": mode,
        "compare": compare_payload,
        "message": (
            f"ASC · {label} · {len(series)} nokta · {source}"
            + (f" · {'; '.join(warnings[:2])}" if warnings and not series else "")
        ),
        "warnings": warnings,
        "facets": _asc_facets(metric_key),
        "empty": not bool(series),
    }


def _asc_facets(metric: str, segments: list[str] | None = None) -> dict[str, Any]:
    dims = ["overview"]
    return {
        "metrics": [m["value"] for m in metric_catalog()],
        "dims": dims,
        "breakdowns": ["date", "week", "month", "segment"],
        "segments": list(segments or []),
    }


def _aggregate_series_client(
    series: list[dict[str, Any]], breakdown: str, metric: str
) -> list[dict[str, Any]]:
    """Günlük seriyi hafta/ay kovalarına topla (Play UI ile aynı)."""
    if not series or breakdown not in ("week", "month"):
        return series
    as_avg = metric in ("conversion_rate",)
    buckets: dict[str, list[float]] = {}
    order: list[str] = []
    for row in series:
        key_src = str(row.get("key") or "")[:10]
        if len(key_src) < 10:
            continue
        if breakdown == "month":
            bkey = key_src[:7]
        else:
            try:
                d = date.fromisoformat(key_src)
            except ValueError:
                continue
            # ISO week
            iso = d.isocalendar()
            bkey = f"{iso[0]}-W{iso[1]:02d}"
        if bkey not in buckets:
            buckets[bkey] = []
            order.append(bkey)
        try:
            buckets[bkey].append(float(row.get("value") or 0))
        except (TypeError, ValueError):
            pass
    out: list[dict[str, Any]] = []
    for k in order:
        vals = buckets[k]
        if not vals:
            continue
        v = (sum(vals) / len(vals)) if as_avg else sum(vals)
        out.append({"key": k, "value": round(v, 4)})
    return out


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

    bid = (bundle_id or DEFAULT_BUNDLE).strip()
    scrape_facts_probe, scrape_meta_probe = _cached_scrape_facts()
    if not scrape_facts_probe:
        return {
            "ok": False,
            "configured": False,
            "message": "ASC tarama verisi yok — Mac’te console login + sync + ingest.",
            "bundles": [],
            "scrape_ok": False,
        }

    bundle = _load_bundle(
        bundle_id=bid, start=start_d, end=end_d, needed_metrics=ordered
    )
    out_bundles: list[dict[str, Any]] = []
    for m in ordered:
        data = query_asc_metric(
            start=start_d.isoformat(),
            end=end_d.isoformat(),
            metric=m,
            bundle_id=bid,
            bundle_cache=bundle,
            compare=None,
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
    analytics = bundle.get("analytics") or {}
    scrape_meta = bundle.get("scrape_meta") or scrape_meta_probe or {}
    scrape_facts = bundle.get("scrape_facts") or scrape_facts_probe or []
    scrape_ok = bool(scrape_facts)
    analytics_ok = bool(analytics.get("ok"))
    warnings = list(analytics.get("warnings") or [])
    if scrape_ok:
        warnings.insert(
            0,
            f"Sync · {len(scrape_facts)} fact"
            + (f" · {scrape_meta.get('synced_at')}" if scrape_meta.get("synced_at") else ""),
        )
    elif not analytics_ok:
        warnings.insert(
            0,
            "Sync yok — Mac’te ASC console login sonra bridge "
            "`POST /sync-asc` veya `--sync --ingest`.",
        )
    return {
        "ok": True,
        "configured": True,
        "source": "asc_scrape",
        "app_id": DEFAULT_APP_ID,
        "bundle_id": bid,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "analytics_ok": analytics_ok or scrape_ok,
        "scrape_ok": scrape_ok,
        "scrape_fact_count": len(scrape_facts),
        "warnings": warnings,
        "bundles": out_bundles,
    }


def asc_metrics_status() -> dict[str, Any]:
    scrape_facts, scrape_meta = [], {}
    try:
        scrape_facts, scrape_meta = _cached_scrape_facts()
    except Exception:  # noqa: BLE001
        pass
    scrape_ok = bool(scrape_facts)
    data_date_max = None
    try:
        dates = [
            str(f.get("date") or "")[:10]
            for f in scrape_facts
            if isinstance(f, dict) and str(f.get("date") or "")[:10]
        ]
        if dates:
            data_date_max = max(dates)
    except Exception:
        data_date_max = None
    return {
        "ok": scrape_ok,
        "configured": scrape_ok,
        "vendor_configured": False,
        "scrape_ok": bool(scrape_facts),
        "scrape_fact_count": len(scrape_facts),
        "scrape_synced_at": scrape_meta.get("synced_at"),
        "scrape_message": scrape_meta.get("message"),
        "data_date_max": data_date_max,
        "ratings": scrape_meta.get("ratings"),
        "measure_keys": scrape_meta.get("measure_keys"),
        "bundle_id": DEFAULT_BUNDLE,
        "app_id": DEFAULT_APP_ID,
        "metrics": metric_catalog(),
        "console_urls": {
            "distribution": f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/distribution",
            "ratings": f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/distribution/ratings/ios",
            "analytics": f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics",
            "pageViewCount": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=pageViewCount"
            ),
            "sales": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=sales"
            ),
            "iap": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=iap"
            ),
            "activeDevices": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=activeDevices"
            ),
            "sessions": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=sessions"
            ),
            "installs": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=installs"
            ),
            "crashes": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=crashes"
            ),
            "uninstalls": (
                f"https://appstoreconnect.apple.com/apps/{DEFAULT_APP_ID}/analytics/metrics"
                f"?chartType=singleaxis&dateSpec=d365&frequency=day&measureKey=uninstalls"
            ),
            "finance": "https://appstoreconnect.apple.com/itc/payments_and_financial_reports#/",
        },
    }
