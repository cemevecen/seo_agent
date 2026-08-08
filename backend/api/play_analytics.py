"""Play analytics API — tarih / kırılım / karşılaştırma.

Önce Mac scrape explorer_facts (Play Console statistics URL kataloğu),
yoksa GCS installs CSV warehouse.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Query

from backend.services.play_analytics_warehouse import play_analytics_status, query_play_analytics
from backend.services.play_scrape_warehouse import query_scrape_analytics, scrape_metric_keys

logger = logging.getLogger(__name__)

router = APIRouter(tags=["play-analytics"])

# Bu metrikler scrape kataloğundan gelir (GCS installs değil)
_SCRAPE_FIRST = {
    "device_acquisition",
    "user_lost",
    "active_devices",
    "dau",
    "ar2_acquisitions",
    "rating",
    "active_users",
    "crashes",
    "anrs",
    "revenue",
    "ar2_visitors",
    "active",
}

# GCS installs CSV’de olmayan metrikler — installs “kolon yok” mesajına düşmesin
# rating artık stats/ratings/*.csv’den gelir → GCS fallback açık
_NO_GCS_FALLBACK = {
    "dau",
    "revenue",
    "ar2_visitors",
    "ar2_acquisitions",
    "active_users",
    "active_devices",
    "device_acquisition",
    "user_lost",
}
# ANR/çökme: scrape+Reporting önce; boşsa GCS crashes_* CSV denenebilir
_ANR_CRASH = {"anrs", "crashes"}


@router.get("/play-analytics/status")
def get_play_analytics_status() -> dict[str, Any]:
    gcs = play_analytics_status()
    scrape = query_scrape_analytics(metric="active_devices", breakdown="segment", dim="country")
    return {
        "ok": bool(gcs.get("ok")) or bool(scrape.get("ok")),
        "gcs": gcs,
        "scrape": {
            "ok": scrape.get("ok"),
            "message": scrape.get("message"),
            "fact_count": scrape.get("row_count"),
            "stats_views": scrape.get("stats_views") or [],
            "synced_at": scrape.get("synced_at"),
        },
        "metrics": scrape_metric_keys(),
    }


@router.get("/play-analytics/query")
def get_play_analytics_query(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    metric: str = Query(default="anrs"),
    breakdown: str = Query(default="date"),
    dim: str = Query(default="overview"),
    segment: str | None = Query(default=None),
    compare: str | None = Query(default="previous_period"),
    source: str | None = Query(default=None, description="scrape|gcs|auto"),
) -> dict[str, Any]:
    prefer = (source or "auto").strip().lower()
    try_scrape = prefer in ("scrape", "auto") and (
        prefer == "scrape" or metric in _SCRAPE_FIRST or prefer == "auto"
    )
    try_gcs = prefer in ("gcs", "auto") and not (
        prefer == "auto" and metric in _NO_GCS_FALLBACK
    )

    # Puan: önce GCS stats/ratings CSV (günlük ort.), sonra scrape
    if prefer == "auto" and metric == "rating":
        try:
            gcs = query_play_analytics(
                start=start,
                end=end,
                metric="rating",
                breakdown=breakdown,
                dim=dim if dim in ("overview", "country", "os_version", "app_version", "device", "language", "carrier") else "overview",
                segment=segment,
                compare=compare,
            )
            gcs["source"] = "gcs"
            if gcs.get("ok") and gcs.get("series"):
                return gcs
        except Exception as exc:  # noqa: BLE001
            logger.exception("rating gcs-first failed")

    scrape_res: dict[str, Any] | None = None
    if try_scrape:
        try:
            scrape_res = query_scrape_analytics(
                start=start,
                end=end,
                metric=metric,
                breakdown=breakdown,
                dim=dim,
                segment=segment,
                compare=compare,
            )
            if scrape_res.get("ok") and scrape_res.get("series"):
                # Puan: tarihsiz OVERALL kartı (tek nokta "5") GCS’i engellemesin
                if metric == "rating" and breakdown in ("date", "week", "month"):
                    keys = [str(r.get("key") or "") for r in (scrape_res.get("series") or [])]
                    dated_ok = any(
                        len(k) >= 8 and k[0:4].isdigit() and k[4] in "-/"
                        for k in keys
                    )
                    if dated_ok:
                        return scrape_res
                else:
                    return scrape_res
            # Scrape metrikleri için installs CSV’ye düşme
            if (
                scrape_res is not None
                and prefer != "gcs"
                and metric in _NO_GCS_FALLBACK
            ):
                return scrape_res
        except Exception as exc:  # noqa: BLE001
            logger.exception("scrape analytics query failed")
            scrape_res = {"ok": False, "message": str(exc), "series": [], "total": 0}
            if prefer != "gcs" and metric in _NO_GCS_FALLBACK:
                return scrape_res

    if try_gcs:
        try:
            gcs_metric = metric
            if metric not in ("installs", "uninstalls", "active", "net", "crashes", "anrs", "rating"):
                gcs_metric = "installs"
            gcs = query_play_analytics(
                start=start,
                end=end,
                metric=gcs_metric,
                breakdown=breakdown,
                dim=dim if dim in ("overview", "country", "os_version", "app_version", "device", "language", "carrier") else "overview",
                segment=segment,
                compare=compare,
            )
            gcs["source"] = "gcs"
            if scrape_res and not scrape_res.get("ok"):
                gcs["scrape_message"] = scrape_res.get("message")
            if gcs.get("ok") and (gcs.get("series") or gcs.get("total")):
                return gcs
            # ANR/çökme: installs CSV “kolon yok” mesajını gösterme — scrape uyarısı kalsın
            if metric in _ANR_CRASH and scrape_res is not None:
                scrape_res["gcs_message"] = gcs.get("message")
                return scrape_res
            if scrape_res and not scrape_res.get("ok"):
                scrape_res["gcs_message"] = gcs.get("message")
                return scrape_res
            return gcs
        except Exception as exc:  # noqa: BLE001
            logger.exception("play-analytics gcs query failed")
            if scrape_res:
                scrape_res["gcs_message"] = str(exc)
                return scrape_res
            return {"ok": False, "message": str(exc), "series": [], "total": 0}

    return scrape_res or {"ok": False, "message": "veri yok", "series": [], "total": 0}
