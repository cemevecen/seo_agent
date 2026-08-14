"""Play analytics API — tarih / kırılım / karşılaştırma.

Kaynak: Mac tarama explorer_facts (Play Console statistics).
GCS installs CSV ve Reporting API yedek kapalı.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services.play_scrape_warehouse import (
    enrich_rating_series_review_splits,
    load_scrape_facts,
    query_scrape_analytics,
    scrape_metric_keys,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["play-analytics"])

# Android +grafik → GA4 sekmesi KPI’ları (snapshot daily_trend anahtarları)
_GA4_OVERLAY_METRICS: dict[str, tuple[str, str]] = {
    "sessions": ("sessions", "Sessions"),
    "users": ("activeUsers", "Users"),
    "engaged_sessions": ("engagedSessions", "Engaged sessions"),
    "new_users": ("newUsers", "New users"),
    "avg_session": ("averageSessionDuration", "Avg. session"),
    "page_views": ("screenPageViews", "Page views"),
}

# Android +grafik → Virgül /ad-virgul Android sekmesi KPI’ları (by_date anahtarları)
_VIRGUL_OVERLAY_METRICS: dict[str, tuple[str, str]] = {
    "net_revenue": ("net_revenue", "Net revenue (TL)"),
    "ad_request": ("ad_request", "Ad request"),
    "matched_request": ("matched_request", "Matched request"),
    "impression": ("impression", "Impression"),
    "click": ("click", "Click"),
    "ad_request_ecpm": ("ad_request_ecpm", "Ad request eCPM (TL)"),
    "ad_ecpm": ("ad_ecpm", "Ad impression eCPM (TL)"),
    "viewability_pct": ("viewability_pct", "Viewability (%)"),
    "ctr_pct": ("ctr_pct", "CTR (%)"),
    "coverage_pct": ("coverage_pct", "Coverage (%)"),
}
_VIRGUL_AVG_METRICS = frozenset({
    "ad_request_ecpm",
    "ad_ecpm",
    "viewability_pct",
    "ctr_pct",
    "coverage_pct",
})

@router.get("/play-analytics/viz20/meta")
def get_play_viz20_meta() -> dict[str, Any]:
    from backend.services.play_viz20 import build_viz20_meta

    return build_viz20_meta()


@router.get("/play-analytics/viz20/{viz_id}")
def get_play_viz20_data(
    viz_id: str,
    db: Session = Depends(get_db),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str | None = Query(default=None),
    dim: str | None = Query(default=None),
    metric_left: str | None = Query(default=None),
    metric_right: str | None = Query(default=None),
    metrics: str | None = Query(default=None),
    etype: str = Query(default="CRASH"),
    limit: int = Query(default=15, ge=3, le=50),
) -> dict[str, Any]:
    from backend.services.play_viz20 import build_viz20_data

    return build_viz20_data(
        db,
        viz_id=viz_id,
        start=start,
        end=end,
        metric=metric,
        dim=dim,
        metric_left=metric_left,
        metric_right=metric_right,
        metrics=metrics,
        etype=etype,
        limit=limit,
    )


@router.get("/play-analytics/status")
def get_play_analytics_status() -> dict[str, Any]:
    scrape = query_scrape_analytics(metric="active_devices", breakdown="segment", dim="country")
    return {
        "ok": bool(scrape.get("ok")),
        "gcs": {"ok": False, "message": "GCS yedek kapalı"},
        "scrape": {
            "ok": scrape.get("ok"),
            "message": scrape.get("message"),
            "fact_count": scrape.get("row_count"),
            "stats_views": scrape.get("stats_views") or [],
            "synced_at": scrape.get("synced_at"),
        },
        "metrics": scrape_metric_keys(),
    }


def resolve_play_analytics_query(
    *,
    start: str | None = None,
    end: str | None = None,
    metric: str = "anrs",
    breakdown: str = "date",
    dim: str = "overview",
    segment: str | None = None,
    compare: str | None = "previous_period",
    source: str | None = None,
    facts: list[dict[str, Any]] | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    def _finalize(payload: dict[str, Any]) -> dict[str, Any]:
        if metric == "rating":
            return enrich_rating_series_review_splits(payload)
        return payload

    try:
        scrape_res = query_scrape_analytics(
            start=start,
            end=end,
            metric=metric,
            breakdown=breakdown,
            dim=dim,
            segment=segment,
            compare=compare,
            facts=facts,
            meta=meta,
        )
        return _finalize(scrape_res)
    except Exception as exc:  # noqa: BLE001
        logger.exception("scrape analytics query failed")
        return {"ok": False, "message": str(exc), "series": [], "total": 0}


@router.get("/play-analytics/query")
def get_play_analytics_query(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str = Query(default="anrs"),
    breakdown: str = Query(default="date"),
    dim: str = Query(default="overview"),
    segment: str | None = Query(default=None),
    compare: str | None = Query(default="previous_period"),
    source: str | None = Query(default=None, description="scrape|auto (gcs yedek kapalı)"),
) -> dict[str, Any]:
    return resolve_play_analytics_query(
        start=start,
        end=end,
        metric=metric,
        breakdown=breakdown,
        dim=dim,
        segment=segment,
        compare=compare,
        source=source,
    )


@router.get("/play-analytics/overview")
def get_play_analytics_overview(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metrics: str = Query(
        default=(
            "anrs,crashes,user_lost,device_acquisition,revenue,"
            "ar2_acquisitions,ar2_visitors,user_acquisition,store_listing_conversion,"
            "dau,dau_mau,active_devices,active_users,rating"
        ),
        description="Virgülle ayrılmış metrik listesi (özet ekranı)",
    ),
) -> dict[str, Any]:
    """İlk açılış özeti: explorer_facts bir kez yüklenir, tüm metrikler aynı bellekten kesilir."""
    metric_list = [m.strip() for m in (metrics or "").split(",") if m.strip()]
    # Tekrarları koru ama sırayı bozma
    seen: set[str] = set()
    ordered: list[str] = []
    for m in metric_list:
        if m in seen:
            continue
        seen.add(m)
        ordered.append(m)
    if not ordered:
        return {"ok": False, "message": "metrics boş", "bundles": []}

    from datetime import date, timedelta

    facts, meta = load_scrape_facts()
    requested_start, requested_end = start, end
    eff_start, eff_end = start, end
    auto_shifted = False
    shift_message = None

    # Özet: tüm metrikler için ortak kaydırma — seçili pencere scrape bucket dışında kalmasın
    if facts and start and end:
        try:
            start_d = date.fromisoformat(str(start)[:10])
            end_d = date.fromisoformat(str(end)[:10])
            bound_dates: list[date] = []
            for f in facts:
                if not isinstance(f, dict):
                    continue
                ds = f.get("date")
                if not (isinstance(ds, str) and len(ds) >= 8 and not ds.startswith("i")):
                    continue
                try:
                    bound_dates.append(date.fromisoformat(ds[:10]))
                except ValueError:
                    continue
            if bound_dates:
                data_min, data_max = min(bound_dates), max(bound_dates)
                in_range = any(start_d <= d <= end_d for d in bound_dates)
                if not in_range:
                    span_days = max((end_d - start_d).days + 1, 1)
                    end_d = data_max
                    start_d = end_d - timedelta(days=span_days - 1)
                    if start_d < data_min:
                        start_d = data_min
                    eff_start, eff_end = start_d.isoformat(), end_d.isoformat()
                    auto_shifted = True
                    shift_message = (
                        f"Seçili aralık boştu ({requested_start}…{requested_end}); "
                        f"mevcut sync verisine kaydırıldı ({eff_start}…{eff_end})."
                    )
        except ValueError:
            pass

    bundles: list[dict[str, Any]] = []
    for metric in ordered:
        try:
            data = resolve_play_analytics_query(
                start=eff_start,
                end=eff_end,
                metric=metric,
                breakdown="date",
                dim="overview",
                segment=None,
                compare="",
                source="auto",
                facts=facts,
                meta=meta,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("play-analytics overview metric failed: %s", metric)
            data = {
                "ok": False,
                "series": [],
                "total": 0,
                "total_mode": "sum",
                "message": str(exc),
                "source": "error",
            }
        if data.get("auto_shifted") and data.get("start") and data.get("end"):
            # Tek metrik kaydırdıysa özet tarihlerini hizala
            if not auto_shifted:
                auto_shifted = True
                eff_start = data.get("start") or eff_start
                eff_end = data.get("end") or eff_end
                shift_message = data.get("message") or shift_message
        bundles.append(
            {
                "metric": metric,
                "series": data.get("series") or [],
                "total": data.get("total"),
                "total_mode": data.get("total_mode") or "sum",
                "ok": bool(data.get("ok")),
                "message": data.get("message"),
                "source": data.get("source"),
            }
        )
    fact_count = int(meta.get("explorer_fact_count") or len(facts) or 0)
    ok_any = any(bool(b.get("series")) for b in bundles)
    message = shift_message
    if not facts:
        message = (
            "Play Console explorer_facts boş — Mac’te "
            "play_console sync + ingest çalıştır."
        )
    elif not ok_any and not message:
        message = "Özet metrikleri için seçili aralıkta seri yok."
    return {
        "ok": ok_any or fact_count > 0,
        "start": eff_start,
        "end": eff_end,
        "requested_start": requested_start,
        "requested_end": requested_end,
        "auto_shifted": auto_shifted,
        "message": message,
        "fact_count": fact_count,
        "synced_at": meta.get("synced_at"),
        "bundles": bundles,
    }


def _resolve_doviz_site(db: Session, project: str = "doviz"):
    from sqlalchemy import case

    from backend.models import Site

    pid = (project or "doviz").strip().lower()
    if pid == "sinemalar":
        domain_like = "%sinemalar.com%"
        www_rank = case((Site.domain.ilike("www.sinemalar.com%"), 0), else_=1)
    else:
        domain_like = "%doviz.com%"
        www_rank = case((Site.domain.ilike("www.doviz.com%"), 0), else_=1)
    return (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .filter(Site.domain.ilike(domain_like))
        .order_by(www_rank, Site.id.asc())
        .first()
    )


def _ga4_daily_trend_payload(db: Session, *, site_id: int, profile: str) -> dict[str, Any] | None:
    from backend.config import settings
    from backend.models import Ga4ReportSnapshot

    try:
        pd12 = int(settings.ga4_trend_12m_period_days)
    except Exception:  # noqa: BLE001
        pd12 = 365
    preferred = [pd12, 90, 60, 30, 7]
    # Tek sorgu: tercih edilen period_days’lerden en güncel satırları al, önce uzun aralığı seç
    rows = (
        db.query(Ga4ReportSnapshot)
        .filter(
            Ga4ReportSnapshot.site_id == site_id,
            Ga4ReportSnapshot.profile == str(profile).strip().lower(),
            Ga4ReportSnapshot.period_days.in_(preferred),
        )
        .order_by(Ga4ReportSnapshot.collected_at.desc(), Ga4ReportSnapshot.id.desc())
        .limit(12)
        .all()
    )
    best_by_period: dict[int, Any] = {}
    for row in rows:
        pd = int(row.period_days or 0)
        if pd in best_by_period:
            continue
        best_by_period[pd] = row
    for period_days in preferred:
        row = best_by_period.get(int(period_days))
        if row is None:
            continue
        try:
            import json as _json

            payload = _json.loads(row.payload_json or "{}")
        except Exception:  # noqa: BLE001
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        dt = payload.get("daily_trend") if isinstance(payload.get("daily_trend"), dict) else {}
        dates = dt.get("dates") or []
        if not dates:
            continue
        return {
            "profile": profile,
            "period_days": period_days,
            "collected_at": row.collected_at.isoformat() if row.collected_at else None,
            "last_start": row.last_start,
            "last_end": row.last_end,
            "daily_trend": dt,
        }
    return None


def _slice_ga4_series(
    daily_trend: dict[str, Any],
    *,
    array_key: str,
    start: str | None,
    end: str | None,
) -> list[dict[str, Any]]:
    dates = list(daily_trend.get("dates") or [])
    raw_vals = list(daily_trend.get(array_key) or [])
    # users fallback
    if array_key == "activeUsers" and not any(v is not None for v in raw_vals):
        raw_vals = list(daily_trend.get("totalUsers") or [])
    start_s = (start or "")[:10]
    end_s = (end or "")[:10]
    if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", start_s):
        start_s = ""
    if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", end_s):
        end_s = ""
    out: list[dict[str, Any]] = []
    for i, ds in enumerate(dates):
        key = str(ds or "")[:10]
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", key):
            continue
        if start_s and key < start_s:
            continue
        if end_s and key > end_s:
            continue
        v = raw_vals[i] if i < len(raw_vals) else None
        try:
            num = float(v) if v is not None and v != "" else None
        except (TypeError, ValueError):
            num = None
        if num is not None and not (num == num):  # NaN
            num = None
        out.append({"key": key, "value": num})
    out.sort(key=lambda r: str(r.get("key") or ""))
    return out


@router.get("/play-analytics/ga4-metrics")
def list_play_ga4_overlay_metrics() -> dict[str, Any]:
    """+grafik panelinde listelenecek GA4 metrikleri."""
    return {
        "ok": True,
        "metrics": [
            {"value": f"ga4:{key}", "label": label, "array_key": arr}
            for key, (arr, label) in _GA4_OVERLAY_METRICS.items()
        ],
    }


@router.get("/play-analytics/ga4-series")
def get_play_ga4_overlay_series(
    db: Session = Depends(get_db),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str = Query(default="sessions"),
    project: str = Query(default="doviz"),
    profile: str = Query(default="android"),
) -> dict[str, Any]:
    """GA4 Android (veya seçili profil) günlük serisini Play Metrikler overlay formatında döner."""
    raw = (metric or "sessions").strip().lower()
    if raw.startswith("ga4:"):
        raw = raw[4:]
    meta = _GA4_OVERLAY_METRICS.get(raw)
    if not meta:
        return {
            "ok": False,
            "source": "ga4",
            "message": f"Bilinmeyen GA4 metrik: {metric}",
            "series": [],
            "metric": f"ga4:{raw}",
            "facets": {
                "metrics": [f"ga4:{k}" for k in _GA4_OVERLAY_METRICS],
            },
        }
    array_key, label = meta
    prof = (profile or "android").strip().lower()
    if prof not in ("android", "ios", "web", "mweb"):
        prof = "android"

    site = _resolve_doviz_site(db, project)
    if site is None:
        return {
            "ok": False,
            "source": "ga4",
            "message": "GA4 sitesi bulunamadı (doviz.com)",
            "series": [],
            "metric": f"ga4:{raw}",
            "label": label,
        }

    pack = _ga4_daily_trend_payload(db, site_id=int(site.id), profile=prof)
    if not pack:
        return {
            "ok": False,
            "source": "ga4",
            "message": (
                f"GA4 `{prof}` daily_trend yok — /ga4 sekmesinde {prof} için sync gerekir."
            ),
            "series": [],
            "metric": f"ga4:{raw}",
            "label": label,
            "site_id": site.id,
            "profile": prof,
        }

    series = _slice_ga4_series(
        pack["daily_trend"],
        array_key=array_key,
        start=start,
        end=end,
    )
    vals = [float(r["value"]) for r in series if r.get("value") is not None]
    total = round(sum(vals), 4) if vals else 0.0
    total_mode = "sum"
    if raw in ("avg_session",):
        total = round(sum(vals) / len(vals), 4) if vals else 0.0
        total_mode = "avg"

    start_s = (start or "")[:10] or (series[0]["key"] if series else None)
    end_s = (end or "")[:10] or (series[-1]["key"] if series else None)
    # ok=True: snapshot bulundu ve aralık dilimlendi (boş seri de geçerli — tarih değişiminde overlay kalır)
    return {
        "ok": True,
        "has_data": bool(vals),
        "source": "ga4",
        "configured": True,
        "message": (
            f"GA4 · {label} · profile={prof} · period_days={pack.get('period_days')} · "
            f"{len(series)} gün"
            + ("" if vals else " · seçili aralıkta veri yok")
        ),
        "start": start_s,
        "end": end_s,
        "metric": f"ga4:{raw}",
        "label": f"GA4 · {label}",
        "breakdown": "date",
        "dim": "overview",
        "segment": "all",
        "total": total,
        "total_mode": total_mode,
        "series": series,
        "compare": None,
        "site_id": site.id,
        "domain": site.domain,
        "profile": prof,
        "collected_at": pack.get("collected_at"),
        "facets": {
            "metrics": [f"ga4:{k}" for k in _GA4_OVERLAY_METRICS],
        },
        "row_count": len(series),
    }


def _slice_virgul_series(
    by_date: list[dict[str, Any]],
    *,
    field: str,
    start: str | None,
    end: str | None,
) -> list[dict[str, Any]]:
    start_s = (start or "")[:10]
    end_s = (end or "")[:10]
    if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", start_s):
        start_s = ""
    if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", end_s):
        end_s = ""
    out: list[dict[str, Any]] = []
    for row in by_date or []:
        if not isinstance(row, dict):
            continue
        key = str(row.get("date") or "")[:10]
        if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", key):
            continue
        if start_s and key < start_s:
            continue
        if end_s and key > end_s:
            continue
        v = row.get(field)
        try:
            num = float(v) if v is not None and v != "" else None
        except (TypeError, ValueError):
            num = None
        if num is not None and not (num == num):  # NaN
            num = None
        out.append({"key": key, "value": num})
    out.sort(key=lambda r: str(r.get("key") or ""))
    return out


@router.get("/play-analytics/virgul-metrics")
def list_play_virgul_overlay_metrics() -> dict[str, Any]:
    """+grafik panelinde listelenecek Virgül Android KPI’ları."""
    return {
        "ok": True,
        "metrics": [
            {"value": f"virgul:{key}", "label": label, "field": field}
            for key, (field, label) in _VIRGUL_OVERLAY_METRICS.items()
        ],
    }


@router.get("/play-analytics/virgul-series")
def get_play_virgul_overlay_series(
    db: Session = Depends(get_db),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str = Query(default="net_revenue"),
    project: str = Query(default="doviz"),
    branch: str = Query(default="android"),
) -> dict[str, Any]:
    """Virgül /ad-virgul Android günlük serisini Play Metrikler overlay formatında döner."""
    from backend.services.ad_analytics_store import query_by_date_for_overlay

    raw = (metric or "net_revenue").strip().lower()
    if raw.startswith("virgul:"):
        raw = raw[7:]
    meta = _VIRGUL_OVERLAY_METRICS.get(raw)
    if not meta:
        return {
            "ok": False,
            "source": "virgul",
            "configured": False,
            "message": f"Bilinmeyen Virgül metrik: {metric}",
            "series": [],
            "metric": f"virgul:{raw}",
            "facets": {
                "metrics": [f"virgul:{k}" for k in _VIRGUL_OVERLAY_METRICS],
            },
        }
    field, label = meta
    proj = (project or "doviz").strip().lower() or "doviz"
    br = (branch or "android").strip().lower() or "android"
    if br not in ("android", "ios", "web", "mweb"):
        br = "android"

    try:
        payload = query_by_date_for_overlay(
            db,
            start=start,
            end=end,
            project=proj,
            branch=br,
            warehouse="virgul",
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("virgul overlay summary failed")
        return {
            "ok": False,
            "source": "virgul",
            "configured": False,
            "message": f"Virgül özeti alınamadı: {exc}",
            "series": [],
            "metric": f"virgul:{raw}",
            "label": f"Virgül · {label}",
        }

    by_date = payload.get("by_date") if isinstance(payload, dict) else None
    if not isinstance(by_date, list):
        by_date = []
    series = _slice_virgul_series(by_date, field=field, start=start, end=end)
    vals = [float(r["value"]) for r in series if r.get("value") is not None]
    as_avg = raw in _VIRGUL_AVG_METRICS
    total = (
        round(sum(vals) / len(vals), 4)
        if as_avg and vals
        else (round(sum(vals), 4) if vals else 0.0)
    )
    start_s = (start or "")[:10] or (series[0]["key"] if series else None)
    end_s = (end or "")[:10] or (series[-1]["key"] if series else None)
    return {
        "ok": True,
        "has_data": bool(vals),
        "source": "virgul",
        "configured": True,
        "message": (
            f"Virgül · {label} · {proj}:{br} · {len(series)} gün"
            + ("" if vals else " · seçili aralıkta veri yok")
        ),
        "start": start_s,
        "end": end_s,
        "metric": f"virgul:{raw}",
        "label": f"Virgül · {label}",
        "breakdown": "date",
        "dim": "overview",
        "segment": "all",
        "total": total,
        "total_mode": "avg" if as_avg else "sum",
        "series": series,
        "compare": None,
        "project": proj,
        "branch": br,
        "facets": {
            "metrics": [f"virgul:{k}" for k in _VIRGUL_OVERLAY_METRICS],
        },
        "row_count": len(series),
    }
