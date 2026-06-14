"""Search Console ek raporlar — Discover, News, Appearance, Page×Query, Country, Sitemap."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.collectors.search_console import build_search_console_service_and_targets
from backend.collectors.url_inspection import _extract_summary, _normalize_url
from backend.services.timezone_utils import report_calendar_yesterday

SC_VIEW_SPECS: dict[str, dict[str, Any]] = {
    "performance": {
        "title": "Arama sonuçları",
        "slug": "performance",
        "kind": "performance",
        "group": "Performans",
        "order": 1,
    },
    "discover": {
        "title": "Discover",
        "slug": "discover",
        "kind": "analytics",
        "group": "Performans",
        "order": 2,
        "report_key": "discover",
        "search_type": "discover",
        "dimensions": ["page"],
        "primary_col": "page",
        "primary_label": "Sayfa",
    },
    "news": {
        "title": "Google News",
        "slug": "news",
        "kind": "analytics",
        "group": "Performans",
        "order": 3,
        "report_key": "news",
        "search_type": "googleNews",
        "dimensions": ["page"],
        "primary_col": "page",
        "primary_label": "Sayfa",
    },
    "appearance": {
        "title": "Görünüm",
        "slug": "appearance",
        "kind": "analytics",
        "group": "Analiz",
        "order": 4,
        "report_key": "appearance",
        "dimensions": ["searchAppearance"],
        "primary_col": "searchAppearance",
        "primary_label": "Görünüm tipi",
    },
    "page-query": {
        "title": "Sayfa × Sorgu",
        "slug": "page-query",
        "kind": "analytics",
        "group": "Analiz",
        "order": 5,
        "report_key": "page_query",
        "dimensions": ["page", "query"],
        "primary_col": "page",
        "secondary_col": "query",
        "primary_label": "Sayfa",
        "secondary_label": "Sorgu",
    },
    "url-inspection": {
        "title": "URL Inspection",
        "slug": "url-inspection",
        "kind": "inspection",
        "group": "İndeks",
        "order": 6,
    },
    "sitemaps": {
        "title": "Sitemapler",
        "slug": "sitemaps",
        "kind": "sitemaps",
        "group": "İndeks",
        "order": 7,
    },
}


def sc_view_groups() -> list[str]:
    seen: list[str] = []
    for spec in sorted(SC_VIEW_SPECS.values(), key=lambda s: int(s.get("order") or 0)):
        g = str(spec.get("group") or "")
        if g and g not in seen:
            seen.append(g)
    return seen


def sc_views_for_nav() -> list[dict[str, Any]]:
    return sorted(SC_VIEW_SPECS.values(), key=lambda s: int(s.get("order") or 0))


def sc_extra_views_for_nav() -> list[dict[str, Any]]:
    return [v for v in sc_views_for_nav() if v.get("kind") != "performance"]


def _normalize_dimension_rows(
    rows: list[dict],
    dimensions: list[str],
    *,
    property_url: str = "",
    forced_device: str | None = None,
) -> list[dict]:
    out: list[dict] = []
    for row in rows or []:
        keys = row.get("keys") or []
        clicks = float(row.get("clicks") or 0.0)
        impressions = float(row.get("impressions") or 0.0)
        ctr = float(row.get("ctr") or 0.0)
        if impressions > 0 and ctr <= 0 and clicks > 0:
            ctr = clicks / impressions
        item: dict[str, Any] = {
            "clicks": clicks,
            "impressions": impressions,
            "ctr": ctr,
            "position": float(row.get("position") or 0.0),
            "property_url": property_url,
        }
        for idx, dim in enumerate(dimensions):
            item[dim] = str(keys[idx] if idx < len(keys) else "")
        if forced_device:
            item["device"] = forced_device
        out.append(item)
    return out


def _merge_rows_by_key(rows: list[dict], key_fields: list[str]) -> list[dict]:
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(str(row.get(f) or "") for f in key_fields)
        prev = merged.get(key)
        if prev is None:
            merged[key] = dict(row)
            continue
        clicks = float(prev.get("clicks") or 0) + float(row.get("clicks") or 0)
        impressions = float(prev.get("impressions") or 0) + float(row.get("impressions") or 0)
        pos_a = float(prev.get("position") or 0) * float(prev.get("impressions") or 0)
        pos_b = float(row.get("position") or 0) * float(row.get("impressions") or 0)
        position = (pos_a + pos_b) / impressions if impressions > 0 else 0.0
        prev["clicks"] = clicks
        prev["impressions"] = impressions
        prev["ctr"] = clicks / impressions if impressions > 0 else 0.0
        prev["position"] = position
    result = list(merged.values())
    result.sort(key=lambda r: (-float(r.get("clicks") or 0), -float(r.get("impressions") or 0)))
    return result


def _fetch_analytics_raw(
    service,
    property_url: str,
    start_date: date,
    end_date: date,
    *,
    dimensions: list[str],
    search_type: str | None = None,
    row_limit: int = 250,
    device: str | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(row_limit), 5000))
    body: dict[str, Any] = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "dimensions": dimensions,
        "rowLimit": safe_limit,
        "startRow": 0,
    }
    if search_type:
        body["type"] = search_type
    if device:
        body["dimensionFilterGroups"] = [
            {
                "filters": [
                    {
                        "dimension": "device",
                        "operator": "equals",
                        "expression": str(device).upper(),
                    }
                ]
            }
        ]
    response = (
        service.searchanalytics()
        .query(siteUrl=property_url, body=body)
        .execute()
    )
    return response.get("rows") or []


def fetch_sc_analytics_report(
    db: Session,
    site_id: int,
    report_key: str,
    *,
    days: int = 28,
    row_limit: int = 250,
) -> dict[str, Any]:
    spec = next(
        (v for v in SC_VIEW_SPECS.values() if v.get("report_key") == report_key),
        None,
    )
    if not spec:
        raise ValueError(f"Bilinmeyen rapor: {report_key}")

    dimensions = list(spec.get("dimensions") or [])
    search_type = spec.get("search_type")
    site, service, targets = build_search_console_service_and_targets(db, site_id)
    if not targets:
        raise ValueError("Search Console property bulunamadi.")
    end_date = report_calendar_yesterday()
    span = max(1, min(int(days), 90))
    start_date = end_date - timedelta(days=span - 1)

    collected: list[dict] = []
    errors: list[str] = []

    # Discover / Google News: cihaz filtresi ve çift sorgu genelde hata verir; tek property sorgusu.
    if search_type:
        seen_props: set[str] = set()
        for target in targets:
            property_url = str(target.get("property_url") or "")
            if not property_url or property_url in seen_props:
                continue
            seen_props.add(property_url)
            try:
                raw = _fetch_analytics_raw(
                    service,
                    property_url,
                    start_date,
                    end_date,
                    dimensions=dimensions,
                    search_type=search_type,
                    row_limit=row_limit,
                    device=None,
                )
                collected.extend(
                    _normalize_dimension_rows(
                        raw,
                        dimensions,
                        property_url=property_url,
                    )
                )
            except Exception as exc:
                errors.append(f"{property_url}: {exc}")
        if not collected and errors:
            raise ValueError(errors[0][:400])
    else:
        for target in targets:
            property_url = str(target.get("property_url") or "")
            device = str(target.get("device") or "").upper() or None
            try:
                raw = _fetch_analytics_raw(
                    service,
                    property_url,
                    start_date,
                    end_date,
                    dimensions=dimensions,
                    search_type=search_type,
                    row_limit=row_limit,
                    device=device,
                )
                collected.extend(
                    _normalize_dimension_rows(
                        raw,
                        dimensions,
                        property_url=property_url,
                        forced_device=device,
                    )
                )
            except Exception as exc:
                errors.append(f"{device or 'ALL'}: {exc}")

        if not collected and errors:
            raise ValueError(errors[0][:400])

    merged = _merge_rows_by_key(collected, dimensions)
    return {
        "site_id": site.id,
        "domain": site.domain,
        "display_name": site.display_name,
        "report_key": report_key,
        "search_type": search_type,
        "dimensions": dimensions,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows": merged[: max(1, min(int(row_limit), 500))],
        "row_count": len(merged),
    }


def fetch_sc_sitemaps(db: Session, site_id: int) -> dict[str, Any]:
    site, service, targets = build_search_console_service_and_targets(db, site_id)
    entries: list[dict[str, Any]] = []
    for target in targets:
        property_url = str(target.get("property_url") or "")
        try:
            raw = service.sitemaps().list(siteUrl=property_url).execute()
        except Exception as exc:
            entries.append(
                {
                    "property_url": property_url,
                    "device": target.get("device"),
                    "error": str(exc)[:300],
                    "sitemap": [],
                }
            )
            continue
        for sm in raw.get("sitemap") or []:
            entries.append(
                {
                    "property_url": property_url,
                    "device": target.get("device"),
                    "path": sm.get("path") or "",
                    "lastSubmitted": sm.get("lastSubmitted") or "",
                    "lastDownloaded": sm.get("lastDownloaded") or "",
                    "isPending": bool(sm.get("isPending")),
                    "isSitemapsIndex": bool(sm.get("isSitemapsIndex")),
                    "type": sm.get("type") or "",
                    "warnings": int(sm.get("warnings") or 0),
                    "errors": int(sm.get("errors") or 0),
                    "contents": sm.get("contents") or [],
                }
            )
    entries.sort(key=lambda x: (str(x.get("path") or ""), str(x.get("property_url") or "")))
    return {
        "site_id": site.id,
        "domain": site.domain,
        "display_name": site.display_name,
        "sitemaps": entries,
        "count": len(entries),
    }


def inspect_sc_url(db: Session, site_id: int, inspection_url: str) -> dict[str, Any]:
    site, service, targets = build_search_console_service_and_targets(db, site_id)
    if not targets:
        raise ValueError("Search Console property bulunamadi.")
    property_url = str((targets[0] or {}).get("property_url") or "")
    url = (inspection_url or "").strip() or _normalize_url(site.domain)
    payload = (
        service.urlInspection()
        .index()
        .inspect(
            body={
                "inspectionUrl": url,
                "siteUrl": property_url,
                "languageCode": "tr-TR",
            }
        )
        .execute()
    )
    summary = _extract_summary(payload, url, property_url)
    return {
        "site_id": site.id,
        "domain": site.domain,
        "summary": summary,
        "raw": payload,
    }
