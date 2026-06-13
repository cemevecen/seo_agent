"""Notification içerik ID → GA4 pagePath / GSC page URL trafik eşlemesi."""

from __future__ import annotations

import logging
import re
from datetime import timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.ga4_auth import get_ga4_connection_status
from backend.services.timezone_utils import report_calendar_yesterday
from backend.services.warehouse import get_latest_search_console_rows

LOGGER = logging.getLogger(__name__)

_ARTICLE_ID_IN_URL = re.compile(r"/(\d{5,})(?:/amp)?(?:[/?#]|$)", re.I)
_DEFAULT_SITE_ID = 1
_GSC_PAGE_SCOPES = ("current_7d_pages", "current_30d_pages", "previous_7d_pages", "previous_30d_pages")


def normalize_article_id(raw: str | None) -> str:
    s = re.sub(r"[\s\u00a0.,·']", "", str(raw or "").strip())
    if s.isdigit():
        return s
    m = _ARTICLE_ID_IN_URL.search(s) or re.search(r"(\d{5,})", s)
    return m.group(1) if m else ""


def page_url_matches_article_id(page_url: str, article_id: str) -> bool:
    aid = normalize_article_id(article_id)
    if not aid or not page_url:
        return False
    return bool(re.search(rf"/{re.escape(aid)}(?:/amp)?(?:[/?#]|$)", page_url, re.I))


def extract_article_id_from_path(path: str) -> str:
    m = _ARTICLE_ID_IN_URL.search(str(path or ""))
    return m.group(1) if m else ""


def _aggregate_gsc_rows(rows: list[dict]) -> dict[str, Any]:
    clicks = impressions = 0.0
    weighted_pos = 0.0
    for row in rows:
        c = float(row.get("clicks") or 0.0)
        i = float(row.get("impressions") or 0.0)
        clicks += c
        impressions += i
        if i > 0:
            weighted_pos += float(row.get("position") or 0.0) * i
    ctr = (clicks / impressions) if impressions > 0 else 0.0
    position = (weighted_pos / impressions) if impressions > 0 else 0.0
    return {
        "clicks": round(clicks, 2),
        "impressions": round(impressions, 2),
        "ctr": round(ctr * 100.0, 4),
        "position": round(position, 2),
    }


def _lookup_gsc_from_db(db: Session, site_id: int, article_id: str) -> dict[str, Any]:
    out: dict[str, Any] = {"scopes": {}, "pages": [], "source": "db"}
    seen_urls: set[str] = set()
    for scope in _GSC_PAGE_SCOPES:
        try:
            rows = get_latest_search_console_rows(db, site_id=site_id, data_scope=scope)
        except Exception:
            rows = []
        matched = [r for r in rows if page_url_matches_article_id(str(r.get("query") or ""), article_id)]
        if not matched:
            continue
        totals = _aggregate_gsc_rows(matched)
        period = {}
        if matched:
            period = {
                "start_date": matched[0].get("start_date") or "",
                "end_date": matched[0].get("end_date") or "",
            }
        out["scopes"][scope] = {**totals, **period, "page_count": len(matched)}
        for row in matched:
            url = str(row.get("query") or "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                out["pages"].append(
                    {
                        "url": url,
                        "device": row.get("device") or "ALL",
                        "clicks": float(row.get("clicks") or 0.0),
                        "impressions": float(row.get("impressions") or 0.0),
                        "ctr": float(row.get("ctr") or 0.0),
                        "position": float(row.get("position") or 0.0),
                        "scope": scope,
                    }
                )
    return out


def _fetch_gsc_live(db: Session, site_id: int, article_id: str, days: int) -> dict[str, Any]:
    from backend.collectors.search_console import (
        build_search_console_service_and_targets,
        fetch_search_console_pages_for_article,
    )

    end = report_calendar_yesterday()
    start = end - timedelta(days=max(1, int(days)) - 1)
    pages: list[dict] = []
    try:
        _site, service, targets = build_search_console_service_and_targets(db, site_id)
        for target in targets:
            property_url = str(target.get("property_url") or "")
            device = str(target.get("device") or "").upper() or None
            rows = fetch_search_console_pages_for_article(
                service,
                property_url,
                start,
                end,
                article_id,
                device=device,
            )
            for row in rows:
                url = str(row.get("query") or "")
                if not page_url_matches_article_id(url, article_id):
                    continue
                pages.append(
                    {
                        "url": url,
                        "device": row.get("device") or device or "ALL",
                        "clicks": float(row.get("clicks") or 0.0),
                        "impressions": float(row.get("impressions") or 0.0),
                        "ctr": float(row.get("ctr") or 0.0),
                        "position": float(row.get("position") or 0.0),
                        "scope": "live",
                    }
                )
    except Exception as exc:
        LOGGER.warning("GSC live article fetch başarısız site=%s id=%s: %s", site_id, article_id, exc)
        return {"scopes": {}, "pages": [], "source": "live_error", "error": str(exc)}

    totals = _aggregate_gsc_rows(pages)
    return {
        "scopes": {
            "live": {
                **totals,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "page_count": len({p["url"] for p in pages}),
            }
        },
        "pages": pages,
        "source": "live",
    }


def _fetch_ga4_live(db: Session, site_id: int, article_id: str, days: int) -> dict[str, Any]:
    from backend.collectors.ga4 import fetch_ga4_article_paths_metrics
    from backend.services.ga4_page_urls import enrich_ga4_page_rows

    ga4_status = get_ga4_connection_status(db, site_id)
    properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}
    profiles: dict[str, list[dict]] = {}
    totals = {"views": 0.0, "sessions": 0.0}
    urls: list[str] = []

    for pf in ("web", "mweb", "android", "ios"):
        prop = str(properties.get(pf) or "").strip()
        if not prop:
            continue
        try:
            raw = fetch_ga4_article_paths_metrics(property_id=prop, article_id=article_id, days=days)
            enriched = enrich_ga4_page_rows(raw, keep_news_articles=True)
        except Exception as exc:
            LOGGER.warning("GA4 article fetch başarısız site=%s profile=%s: %s", site_id, pf, exc)
            enriched = []
        profiles[pf] = enriched
        for row in enriched:
            totals["views"] += float(row.get("views") or 0.0)
            totals["sessions"] += float(row.get("sessions") or 0.0)
            u = str(row.get("page_url") or row.get("page") or "")
            if u and u not in urls:
                urls.append(u)

    return {
        "profiles": profiles,
        "totals": {k: round(v, 2) for k, v in totals.items()},
        "urls": urls,
        "source": "live",
        "connected": bool(ga4_status.get("connected")),
    }


def resolve_content_traffic(
    db: Session,
    *,
    content_id: str,
    site_id: int | None = None,
    days: int = 7,
    live: bool = True,
) -> dict[str, Any]:
    """Bildirim içerik ID'si için GA4 + GSC trafik özeti."""
    aid = normalize_article_id(content_id)
    sid = int(site_id or _DEFAULT_SITE_ID)
    site = db.query(Site).filter(Site.id == sid).first()
    safe_days = max(1, min(int(days or 7), 90))

    if not aid:
        return {
            "content_id": content_id,
            "article_id": "",
            "site_id": sid,
            "site_domain": site.domain if site else "",
            "error": "Geçerli içerik ID bulunamadı.",
            "ga4": None,
            "gsc": None,
        }

    gsc_db = _lookup_gsc_from_db(db, sid, aid)
    gsc: dict[str, Any]
    if gsc_db.get("pages"):
        gsc = gsc_db
    elif live:
        gsc = _fetch_gsc_live(db, sid, aid, safe_days)
    else:
        gsc = gsc_db

    ga4: dict[str, Any] | None = None
    if live:
        ga4 = _fetch_ga4_live(db, sid, aid, safe_days)

    primary_gsc = gsc.get("scopes") or {}
    gsc_7 = primary_gsc.get("current_7d_pages") or primary_gsc.get("live") or {}
    gsc_30 = primary_gsc.get("current_30d_pages") or {}

    return {
        "content_id": content_id,
        "article_id": aid,
        "site_id": sid,
        "site_domain": site.domain if site else "",
        "days": safe_days,
        "ga4": ga4,
        "gsc": gsc,
        "summary": {
            "ga4_views": (ga4 or {}).get("totals", {}).get("views", 0),
            "ga4_sessions": (ga4 or {}).get("totals", {}).get("sessions", 0),
            "gsc_clicks_7d": gsc_7.get("clicks", 0),
            "gsc_impressions_7d": gsc_7.get("impressions", 0),
            "gsc_clicks_30d": gsc_30.get("clicks", 0),
            "gsc_impressions_30d": gsc_30.get("impressions", 0),
            "matched_urls": list(
                dict.fromkeys(
                    ((ga4 or {}).get("urls") or [])
                    + [p.get("url") for p in (gsc.get("pages") or []) if p.get("url")]
                )
            ),
        },
    }
