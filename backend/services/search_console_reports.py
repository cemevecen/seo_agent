"""Search Console ek raporlar — Discover, News, Appearance, Page×Query, Country, Sitemap."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any
from urllib.parse import unquote, urlparse

from sqlalchemy import or_
from sqlalchemy.orm import Session

from backend.collectors.search_console import build_search_console_service_and_targets
from backend.collectors.url_inspection import _extract_summary, _normalize_url
from backend.models import NewsIntelligenceItem
from backend.services.timezone_utils import report_calendar_yesterday

_PAGE_DATE_ID_RE = re.compile(r"/(\d+)(?:/)?(?:\?|#|$)")

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
        "position_supported": False,
        "page_date_column": True,
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
        "position_supported": False,
        "page_date_column": True,
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
        "property_aggregate": True,
        "row_limit_default": 100,
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


def sc_extra_card_should_render(
    spec: dict[str, Any],
    *,
    connection: dict[str, Any] | None,
    report: dict[str, Any] | None,
    error: str | None,
) -> bool:
    """Bağlantı yok / veri yok kartları sayfada gösterme (HTMX boş yanıt)."""
    if error:
        return True
    kind = str(spec.get("kind") or "")
    if kind == "inspection":
        return bool((connection or {}).get("connected"))
    if not (connection or {}).get("connected"):
        return False
    if kind == "analytics":
        return len((report or {}).get("rows") or []) > 0
    if kind == "sitemaps":
        items = (report or {}).get("sitemaps") or []
        return any(str(s.get("path") or "").strip() for s in items)
    return False


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


def _page_lookup_key(url: str) -> str:
    u = unquote(str(url or "").strip())
    if not u:
        return ""
    try:
        p = urlparse(u)
        path = p.path or "/"
        host = (p.netloc or "").lower()
        if host:
            return f"{host}{path}".rstrip("/") or host
        return path.rstrip("/") or "/"
    except Exception:
        return u.rstrip("/")


def _news_id_from_page_url(url: str) -> int | None:
    try:
        path = urlparse(str(url or "")).path or ""
    except Exception:
        return None
    m = _PAGE_DATE_ID_RE.search(path)
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _fetch_first_traffic_by_page(
    service,
    targets: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    *,
    search_type: str | None,
    property_aggregate: bool,
) -> dict[str, str]:
    """Sayfa başına dönem içindeki ilk gösterim/tıklama günü (YYYY-MM-DD)."""
    min_by_page: dict[str, date] = {}

    def _ingest(property_url: str, device: str | None) -> None:
        try:
            raw = _fetch_analytics_raw(
                service,
                property_url,
                start_date,
                end_date,
                dimensions=["date", "page"],
                search_type=search_type,
                row_limit=25000,
                max_row_limit=25000,
                device=device,
            )
        except Exception:
            return
        for row in raw or []:
            keys = row.get("keys") or []
            if len(keys) < 2:
                continue
            d_str, page = str(keys[0]), str(keys[1])
            clicks = float(row.get("clicks") or 0.0)
            impressions = float(row.get("impressions") or 0.0)
            if clicks <= 0 and impressions <= 0:
                continue
            try:
                d = date.fromisoformat(d_str[:10])
            except ValueError:
                continue
            key = _page_lookup_key(page)
            prev = min_by_page.get(key)
            if prev is None or d < prev:
                min_by_page[key] = d

    if property_aggregate:
        seen: set[str] = set()
        for target in targets:
            property_url = str(target.get("property_url") or "")
            if not property_url or property_url in seen:
                continue
            seen.add(property_url)
            _ingest(property_url, None)
    else:
        for target in targets:
            property_url = str(target.get("property_url") or "")
            device = str(target.get("device") or "").upper() or None
            if property_url:
                _ingest(property_url, device)

    return {k: v.isoformat() for k, v in min_by_page.items()}


def _lookup_publish_dates(db: Session, page_urls: list[str]) -> dict[str, str]:
    """Haber URL'lerinde yayın tarihi — news_intelligence_items eşleşmesi."""
    id_by_key: dict[str, int] = {}
    ids: set[int] = set()
    for url in page_urls:
        key = _page_lookup_key(url)
        if not key:
            continue
        nid = _news_id_from_page_url(url)
        if nid is not None:
            id_by_key[key] = nid
            ids.add(nid)

    publish_by_id: dict[int, date] = {}
    if ids:
        id_list = sorted(ids)
        chunk = 80
        for i in range(0, len(id_list), chunk):
            part = id_list[i : i + chunk]
            conds = []
            for nid in part:
                sid = str(nid)
                conds.append(NewsIntelligenceItem.url.like(f"%/{sid}"))
                conds.append(NewsIntelligenceItem.url.like(f"%/{sid}/%"))
            rows = (
                db.query(NewsIntelligenceItem.url, NewsIntelligenceItem.published_at)
                .filter(or_(*conds))
                .all()
            )
            for url, published_at in rows:
                if not published_at:
                    continue
                nid = _news_id_from_page_url(str(url or ""))
                if nid is None:
                    continue
                d = published_at.date() if hasattr(published_at, "date") else None
                if d is None:
                    continue
                prev = publish_by_id.get(nid)
                if prev is None or d < prev:
                    publish_by_id[nid] = d

    out: dict[str, str] = {}
    for url in page_urls:
        key = _page_lookup_key(url)
        if not key:
            continue
        nid = id_by_key.get(key)
        if nid is not None and nid in publish_by_id:
            out[key] = publish_by_id[nid].isoformat()
    return out


def _enrich_rows_with_page_dates(
    db: Session,
    rows: list[dict[str, Any]],
    *,
    service,
    targets: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    search_type: str | None,
    property_aggregate: bool,
) -> None:
    pages = [str(r.get("page") or "") for r in rows if r.get("page")]
    if not pages:
        return
    traffic_dates = _fetch_first_traffic_by_page(
        service,
        targets,
        start_date,
        end_date,
        search_type=search_type,
        property_aggregate=property_aggregate,
    )
    publish_dates = _lookup_publish_dates(db, pages)
    for row in rows:
        page = str(row.get("page") or "")
        key = _page_lookup_key(page)
        pub = publish_dates.get(key)
        traffic = traffic_dates.get(key)
        if pub:
            row["page_date"] = pub
            row["page_date_kind"] = "publish"
        elif traffic:
            row["page_date"] = traffic
            row["page_date_kind"] = "traffic"
        else:
            row["page_date"] = None
            row["page_date_kind"] = None


def _fetch_analytics_raw(
    service,
    property_url: str,
    start_date: date,
    end_date: date,
    *,
    dimensions: list[str],
    search_type: str | None = None,
    row_limit: int = 250,
    max_row_limit: int = 5000,
    device: str | None = None,
) -> list[dict]:
    safe_limit = max(1, min(int(row_limit), int(max_row_limit)))
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
    property_aggregate = bool(search_type or spec.get("property_aggregate"))
    cap = int(spec.get("row_limit_default") or row_limit or 250)
    safe_limit = max(1, min(int(row_limit), cap))
    site, service, targets = build_search_console_service_and_targets(db, site_id)
    if not targets:
        raise ValueError("Search Console property bulunamadi.")
    end_date = report_calendar_yesterday()
    span = max(1, min(int(days), 90))
    start_date = end_date - timedelta(days=span - 1)

    collected: list[dict] = []
    errors: list[str] = []

    if property_aggregate:
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
                    row_limit=safe_limit,
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
                    row_limit=safe_limit,
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
    trimmed = merged[: max(1, min(int(safe_limit), 500))]
    if spec.get("page_date_column") and trimmed:
        _enrich_rows_with_page_dates(
            db,
            trimmed,
            service=service,
            targets=targets,
            start_date=start_date,
            end_date=end_date,
            search_type=search_type,
            property_aggregate=property_aggregate,
        )
    return {
        "site_id": site.id,
        "domain": site.domain,
        "display_name": site.display_name,
        "report_key": report_key,
        "search_type": search_type,
        "dimensions": dimensions,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows": trimmed,
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
