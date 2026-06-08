"""
App Store Connect — Analytics Reports API (impression, page view, conversion, redownloads).

Sales & Trends'te olmayan metrikler buradan gelir. Aynı ASC JWT key'leri yeterlidir;
ONGOING report request yoksa Admin rolüyle bir kez oluşturulur (1–2 gün ilk veri gecikmesi olabilir).
"""
from __future__ import annotations

import csv
import gzip
import io
import logging
import re
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Callable

import httpx

from backend.services import asc_client

logger = logging.getLogger(__name__)

_ASC_BASE = "https://api.appstoreconnect.apple.com"
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 6 * 3600
_MAX_INSTANCES_PER_REPORT = 1


def _cache_get(key: str) -> dict[str, Any] | None:
    hit = _CACHE.get(key)
    if not hit:
        return None
    ts, data = hit
    if time.time() - ts > _CACHE_TTL:
        return None
    return data


def _cache_set(key: str, data: dict[str, Any]) -> None:
    _CACHE[key] = (time.time(), data)


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (h or "").strip().lower())


def _pick_column(headers: list[str], *needles: str) -> str | None:
    norm = {_norm_header(h): h for h in headers}
    for n in needles:
        key = _norm_header(n)
        if key in norm:
            return norm[key]
    for nh, orig in norm.items():
        for n in needles:
            if _norm_header(n) in nh:
                return orig
    return None


def _parse_float(val: Any) -> float:
    if val is None:
        return 0.0
    s = str(val).strip().replace(",", "")
    if not s or s in ("-", "—"):
        return 0.0
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except ValueError:
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_int(val: Any) -> int:
    return int(round(_parse_float(val)))


def _api_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    headers = asc_client._auth_headers()  # noqa: SLF001
    if not headers:
        return None
    url = path if path.startswith("http") else f"{_ASC_BASE}{path}"
    try:
        with httpx.Client(timeout=45) as cli:
            resp = cli.get(url, headers=headers, params=params or {})
        if resp.status_code != 200:
            logger.warning("ASC analytics GET %s → %d %s", path, resp.status_code, resp.text[:180])
            return None
        return resp.json()
    except Exception as exc:
        logger.error("ASC analytics GET %s: %s", path, exc)
        return None


def _api_post(path: str, body: dict[str, Any]) -> dict[str, Any] | None:
    headers = asc_client._auth_headers()  # noqa: SLF001
    if not headers:
        return None
    headers = {**headers, "Content-Type": "application/json"}
    url = f"{_ASC_BASE}{path}"
    try:
        with httpx.Client(timeout=45) as cli:
            resp = cli.post(url, headers=headers, json=body)
        if resp.status_code not in (200, 201):
            logger.warning("ASC analytics POST %s → %d %s", path, resp.status_code, resp.text[:220])
            return None
        return resp.json()
    except Exception as exc:
        logger.error("ASC analytics POST %s: %s", path, exc)
        return None


def _paginate_first_path(path: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Tek path için sayfalama (links.next tam URL)."""
    out: list[dict] = []
    rel = path if path.startswith("/") else f"/{path}"
    params = dict(params or {})
    params.setdefault("limit", 200)
    chunk = _api_get(rel, params)
    while chunk:
        out.extend(chunk.get("data") or [])
        nxt = (chunk.get("links") or {}).get("next")
        if not nxt:
            break
        chunk = _api_get(nxt, None)
    return out


def _ensure_ongoing_request(app_id: str) -> str | None:
    rows = _paginate_first_path(f"/v1/apps/{app_id}/analyticsReportRequests")
    for r in rows:
        attr = r.get("attributes") or {}
        if (attr.get("accessType") or "").upper() == "ONGOING" and not attr.get("stoppedDueToInactivity"):
            return r.get("id")
    body = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONGOING"},
            "relationships": {"app": {"data": {"type": "apps", "id": app_id}}},
        }
    }
    created = _api_post("/v1/analyticsReportRequests", body)
    if created and created.get("data"):
        rid = created["data"].get("id")
        logger.info("ASC analytics ONGOING request oluşturuldu app=%s request=%s", app_id, rid)
        return rid
    return None


def _reports_for_request(request_id: str) -> list[dict]:
    return _paginate_first_path(f"/v1/analyticsReportRequests/{request_id}/reports")


def _select_report_id(reports: list[dict], *keywords: str) -> str | None:
    matches: list[dict] = []
    for r in reports:
        name = ((r.get("attributes") or {}).get("name") or "").lower()
        if all(kw.lower() in name for kw in keywords):
            matches.append(r)
    if not matches:
        return None
    detailed = [m for m in matches if "detailed" in ((m.get("attributes") or {}).get("name") or "").lower()]
    pick = (detailed or matches)[0]
    return pick.get("id")


def _latest_instance_ids(report_id: str, *, max_instances: int) -> list[str]:
    inst = _paginate_first_path(
        f"/v1/analyticsReports/{report_id}/instances",
        {"filter[granularity]": "DAILY"},
    )
    if not inst:
        return []
    inst.sort(
        key=lambda x: (x.get("attributes") or {}).get("processingDate") or "",
        reverse=True,
    )
    return [i["id"] for i in inst[:max_instances] if i.get("id")]


def _download_segment_rows(instance_id: str) -> list[dict[str, str]]:
    segs = _paginate_first_path(f"/v1/analyticsReportInstances/{instance_id}/segments")
    if not segs:
        return []
    url = (segs[0].get("attributes") or {}).get("url")
    if not url:
        return []
    try:
        with httpx.Client(timeout=120, follow_redirects=True) as cli:
            resp = cli.get(url)
        if resp.status_code != 200:
            logger.warning("ASC segment download %s → %d", instance_id, resp.status_code)
            return []
        raw = resp.content
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        text = raw.decode("utf-8", errors="replace")
        if not text.strip():
            return []
        delim = "\t" if "\t" in text.splitlines()[0] else ","
        return list(csv.DictReader(io.StringIO(text), delimiter=delim))
    except Exception as exc:
        logger.error("ASC segment parse %s: %s", instance_id, exc)
        return []


def _row_date(row: dict[str, str], headers: list[str]) -> str | None:
    col = _pick_column(headers, "Date", "Report Date", "Day")
    if not col:
        return None
    v = (row.get(col) or "").strip()
    return v[:10] if v else None


def _country_ok(row: dict[str, str], headers: list[str], country: str) -> bool:
    cc = (country or "all").strip().upper()
    if cc in ("", "ALL"):
        return True
    col = _pick_column(
        headers,
        "Country or Region",
        "Country Or Region",
        "Country",
        "Territory",
        "Storefront",
    )
    if not col:
        return True
    val = (row.get(col) or "").strip().upper()
    if not val:
        return True
    return val == cc or val.startswith(cc)


def _aggregate_report_rows(
    rows: list[dict[str, str]],
    *,
    start: date,
    end: date,
    country: str,
    metrics: dict[str, list[str]],
) -> tuple[dict[str, dict[str, float]], dict[str, float]]:
    """Günlük toplamlar + dönem toplamları."""
    if not rows:
        return {}, {k: 0.0 for k in metrics}
    headers = list(rows[0].keys())
    daily: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    totals = {k: 0.0 for k in metrics}
    for row in rows:
        if not _country_ok(row, headers, country):
            continue
        ds = _row_date(row, headers)
        if not ds:
            continue
        try:
            d = date.fromisoformat(ds[:10])
        except ValueError:
            continue
        if d < start or d > end:
            continue
        for key, needles in metrics.items():
            col = _pick_column(headers, *needles)
            if not col:
                continue
            if key == "conversion_rate_pct":
                val = _parse_float(row.get(col))
                n = daily[ds].get("_conv_n", 0) + 1
                prev = daily[ds].get(key, 0.0)
                daily[ds][key] = prev + (val - prev) / n
                daily[ds]["_conv_n"] = n
                totals[key] = val if val else totals[key]
            else:
                val = float(_parse_int(row.get(col)))
                daily[ds][key] += val
                totals[key] += val
    return {k: dict(v) for k, v in daily.items()}, totals


def fetch_analytics_summary(
    *,
    bundle_id: str,
    days: int,
    country: str = "all",
    progress_cb: Callable[[int, int], None] | None = None,
) -> dict[str, Any] | None:
    """Analytics Reports'tan impression, page view, conversion, redownload vb."""
    if not asc_client.is_configured():
        return None

    effective_days = 365 if days == 0 else max(1, min(int(days), 365))
    cache_key = f"asc_analytics:{bundle_id}:{effective_days}:{country}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    app_id = asc_client.find_app_id_by_bundle(bundle_id)
    if not app_id:
        return {"ok": False, "message": "App Store Connect'te bundle bulunamadı."}

    request_id = _ensure_ongoing_request(app_id)
    if not request_id:
        return {
            "ok": False,
            "message": "Analytics report request yok; API key Admin veya Sales and Reports rolü gerekir.",
        }

    reports = _reports_for_request(request_id)
    if not reports:
        return {
            "ok": False,
            "message": "Analytics raporları henüz üretilmedi (ONGOING istek sonrası 1–2 gün bekleyin).",
        }

    rid_discovery = _select_report_id(reports, "discovery", "engagement")
    rid_downloads = _select_report_id(reports, "download")
    rid_commerce = _select_report_id(reports, "commerce") or _select_report_id(reports, "purchase")

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=effective_days - 1)

    engagement_metrics = {
        "impressions": ("Impressions", "App Impressions", "Impressions Unique Device"),
        "product_page_views": ("Product Page Views", "Page Views"),
        "conversion_rate_pct": ("Conversion Rate", "Download Conversion Rate"),
    }
    download_metrics = {
        "first_time_downloads": ("First-Time Downloads", "First Time Downloads"),
        "redownloads": ("Redownloads", "Re-Downloads"),
        "total_downloads": ("Total Downloads", "Downloads"),
    }
    commerce_metrics = {
        "paying_users": ("Paying Users", "Unique Paying Users"),
        "in_app_purchases": ("In-App Purchases", "Purchases"),
    }

    daily_merged: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    totals: dict[str, float] = defaultdict(float)
    warnings: list[str] = []

    jobs: list[tuple[str, dict[str, list[str]]]] = []
    if rid_discovery:
        jobs.append((rid_discovery, engagement_metrics))
    else:
        warnings.append("Discovery & Engagement raporu listede yok.")
    if rid_downloads:
        jobs.append((rid_downloads, download_metrics))
    else:
        warnings.append("Downloads raporu listede yok.")
    if rid_commerce:
        jobs.append((rid_commerce, commerce_metrics))

    total_steps = len(jobs) * _MAX_INSTANCES_PER_REPORT
    done = 0
    for report_id, metric_map in jobs:
        for inst_id in _latest_instance_ids(report_id, max_instances=_MAX_INSTANCES_PER_REPORT):
            rows = _download_segment_rows(inst_id)
            done += 1
            if progress_cb:
                try:
                    progress_cb(done, max(total_steps, 1))
                except Exception:
                    pass
            daily, part_totals = _aggregate_report_rows(
                rows, start=start, end=end, country=country, metrics=metric_map
            )
            for ds, vals in daily.items():
                for k, v in vals.items():
                    if k.startswith("_"):
                        continue
                    if k == "conversion_rate_pct":
                        daily_merged[ds][k] = v
                    else:
                        daily_merged[ds][k] += v
            for k, v in part_totals.items():
                if k == "conversion_rate_pct" and v:
                    totals[k] = v
                else:
                    totals[k] += v

    if not daily_merged and not any(totals.values()):
        out = {
            "ok": False,
            "message": "Analytics segment indirilemedi veya seçili aralıkta satır yok.",
            "warnings": warnings,
        }
        _cache_set(cache_key, out)
        return out

    dates_sorted = sorted(daily_merged.keys())
    imp_series = [daily_merged[d].get("impressions", 0) for d in dates_sorted]
    pv_series = [daily_merged[d].get("product_page_views", 0) for d in dates_sorted]
    ft_series = [daily_merged[d].get("first_time_downloads", 0) for d in dates_sorted]
    rd_series = [daily_merged[d].get("redownloads", 0) for d in dates_sorted]
    td_series = [
        daily_merged[d].get("total_downloads", 0)
        or daily_merged[d].get("first_time_downloads", 0) + daily_merged[d].get("redownloads", 0)
        for d in dates_sorted
    ]

    conv = totals.get("conversion_rate_pct") or 0.0
    if not conv and totals.get("impressions") and totals.get("total_downloads"):
        conv = totals["total_downloads"] / totals["impressions"] * 100.0
    elif not conv and dates_sorted:
        conv_vals = [daily_merged[d].get("conversion_rate_pct", 0) for d in dates_sorted if daily_merged[d].get("conversion_rate_pct")]
        if conv_vals:
            conv = sum(conv_vals) / len(conv_vals)

    total_dl = int(totals.get("total_downloads") or 0)
    if not total_dl:
        total_dl = int(totals.get("first_time_downloads", 0) + totals.get("redownloads", 0))

    out = {
        "ok": True,
        "impressions": int(totals.get("impressions", 0)),
        "product_page_views": int(totals.get("product_page_views", 0)),
        "conversion_rate_pct": round(conv, 3),
        "first_time_downloads": int(totals.get("first_time_downloads", 0)),
        "redownloads": int(totals.get("redownloads", 0)),
        "total_downloads": total_dl,
        "paying_users": int(totals.get("paying_users", 0)),
        "iap_units_analytics": int(totals.get("in_app_purchases", 0)),
        "dates": dates_sorted,
        "impressions_series": imp_series,
        "page_views_series": pv_series,
        "first_downloads_series": ft_series,
        "redownloads_series": rd_series,
        "total_downloads_series": td_series,
        "warnings": warnings,
    }
    _cache_set(cache_key, out)
    logger.info(
        "ASC analytics özet bundle=%s days=%d imp=%s pv=%s total_dl=%s",
        bundle_id, effective_days, out["impressions"], out["product_page_views"], total_dl,
    )
    return out
