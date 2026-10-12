"""ASC Sales + Analytics API → explorer_facts ingest (tarayıcı gerekmez).

Mac scrape oturumu düşünce /ios boş günlerini kapatır. Play CSV fill ile aynı rol.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

LOGGER = logging.getLogger(__name__)

_DEFAULT_BUNDLE = "com.nokta.Finans.Takip"
_DEFAULT_APP_ID = "465599322"


def _fact(metric: str, day: str, value: float, *, source: str) -> dict[str, Any]:
    return {
        "metric": metric,
        "view_id": f"{metric}_api",
        "dim": "overview",
        "segment": "OVERALL",
        "date": day[:10],
        "value": float(value),
        "label": metric,
        "source": source,
        "value_kind": "daily",
    }


def build_asc_api_facts(*, days: int = 20) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from backend.services import asc_analytics, asc_client

    bundle = (
        os.environ.get("ASC_CONSOLE_BUNDLE_ID")
        or os.environ.get("ASC_BUNDLE_ID")
        or _DEFAULT_BUNDLE
    ).strip()
    meta: dict[str, Any] = {
        "bundle_id": bundle,
        "days": days,
        "sales_ok": False,
        "analytics_ok": False,
        "errors": [],
    }
    facts: list[dict[str, Any]] = []
    if not asc_client.is_configured():
        meta["errors"].append("ASC API key yok")
        return [], meta

    try:
        sales = asc_client.fetch_daily_sales_summary(bundle_id=bundle, days=days)
    except Exception as exc:  # noqa: BLE001
        meta["errors"].append(f"sales: {exc}"[:160])
        sales = None
    if sales and sales.get("dates"):
        meta["sales_ok"] = True
        meta["sales_range"] = [sales["dates"][0], sales["dates"][-1]]
        for ds, dl, pr in zip(sales["dates"], sales["dl_series"], sales["pr_series"]):
            facts.append(_fact("units", ds, dl, source="asc_sales_api"))
            facts.append(_fact("total_downloads", ds, dl, source="asc_sales_api"))
            facts.append(_fact("proceeds", ds, pr, source="asc_sales_api"))
            facts.append(_fact("sales", ds, pr, source="asc_sales_api"))

    try:
        analytics = asc_analytics.fetch_analytics_summary(bundle_id=bundle, days=days)
    except Exception as exc:  # noqa: BLE001
        meta["errors"].append(f"analytics: {exc}"[:160])
        analytics = None
    if analytics and analytics.get("ok"):
        meta["analytics_ok"] = True
        dates = analytics.get("dates") or []
        meta["analytics_range"] = [dates[0], dates[-1]] if dates else []
        series_map = {
            "units": "first_downloads_series",
            "redownloads": "redownloads_series",
            "total_downloads": "total_downloads_series",
            "impressions": "impressions_series",
            "page_views": "page_views_series",
            "conversion_rate": "conversion_series",
            "iap": "iap_series",
            "paying_users": "paying_users_series",
        }
        for metric, key in series_map.items():
            vals = analytics.get(key) or []
            for ds, val in zip(dates, vals):
                if val is None:
                    continue
                facts.append(_fact(metric, ds, float(val), source="asc_analytics_api"))

    by: dict[tuple[str, str], dict[str, Any]] = {}
    for f in facts:
        key = (str(f["metric"]), str(f["date"])[:10])
        prev = by.get(key)
        if (
            prev
            and prev.get("source") == "asc_analytics_api"
            and f.get("source") != "asc_analytics_api"
        ):
            continue
        by[key] = f
    out = list(by.values())
    meta["fact_count"] = len(out)
    return out, meta


def ingest_asc_api_facts(
    facts: list[dict[str, Any]],
    *,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not facts:
        return {"ok": False, "message": "ASC API fact yok", "meta": meta or {}}
    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        return {"ok": False, "message": "NOTIFICATION_INGEST_TOKEN yok"}
    url = (
        os.environ.get("ASC_CONSOLE_INGEST_URL")
        or "https://projectcontrol.up.railway.app/api/asc-console/ingest"
    )
    bundle = (meta or {}).get("bundle_id") or _DEFAULT_BUNDLE
    app_id = (
        os.environ.get("ASC_CONSOLE_APP_ID") or os.environ.get("ASC_APP_ID") or _DEFAULT_APP_ID
    )
    body = {
        "ok": True,
        "source": "asc_api_fill",
        "bundle_id": bundle,
        "app_id": str(app_id),
        "sync_ok": True,
        "sync_message": f"ASC API fill · {len(facts)} facts",
        "sync_mode": "api_fill",
        "panels": {
            "version": 1,
            "explorer_facts": facts,
            "scrape_meta": {"source": "asc_api_fill", **(meta or {})},
        },
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Notification-Ingest-Token": token,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            status = getattr(resp, "status", 200)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")[:300]
        return {
            "ok": False,
            "http_status": exc.code,
            "message": raw or str(exc),
            "meta": meta,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "message": str(exc)[:200], "meta": meta}
    out = dict(payload) if isinstance(payload, dict) else {"ok": True, "body": payload}
    out["http_status"] = status
    out["meta"] = meta
    out["kind"] = "asc_api"
    return out


def run_asc_api_fill(*, days: int = 20) -> dict[str, Any]:
    facts, meta = build_asc_api_facts(days=days)
    LOGGER.info(
        "ASC API fill · facts=%s sales=%s analytics=%s",
        len(facts),
        meta.get("sales_ok"),
        meta.get("analytics_ok"),
    )
    return ingest_asc_api_facts(facts, meta=meta)
