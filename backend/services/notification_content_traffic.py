"""Notification içerik ID → GA4 pagePath / GSC page URL trafik eşlemesi."""

from __future__ import annotations

import logging
import re
import unicodedata
from datetime import date, timedelta
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
_HEADLINE_MATCH_MIN = 0.55


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


def _parse_day(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def resolve_traffic_date_range(
    *,
    send_date: str | None = None,
    days: int = 14,
) -> tuple[str, str, dict[str, str]]:
    """Gönderim tarihi etrafında GA4/GSC penceresi (varsayılan: gönderim günü + sonraki N-1 gün)."""
    safe_days = max(1, min(int(days or 14), 90))
    yesterday = report_calendar_yesterday()
    send = _parse_day(send_date)
    if send is None:
        end = yesterday
        start = end - timedelta(days=safe_days - 1)
        return start.isoformat(), end.isoformat(), {
            "mode": "rolling",
            "send_date": "",
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
    start = send
    end = min(send + timedelta(days=safe_days - 1), yesterday)
    if end < start:
        end = start
    return start.isoformat(), end.isoformat(), {
        "mode": "send_date",
        "send_date": send.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


def _normalize_headline(text: str) -> str:
    s = unicodedata.normalize("NFKD", str(text or ""))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _headline_match_score(headline: str, candidate: str) -> float:
    a = _normalize_headline(headline)
    b = _normalize_headline(candidate)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.92
    ta = [w for w in a.split() if len(w) >= 3]
    tb = set(w for w in b.split() if len(w) >= 3)
    if len(ta) < 2 or not tb:
        return 0.0
    overlap = sum(1 for w in ta if w in tb)
    return overlap / len(ta)


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


def _merge_ga4_rows(rows: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for row in rows or []:
        key = str(row.get("page_url") or row.get("page") or "")
        if not key:
            continue
        bucket = merged.setdefault(
            key,
            {
                "page": row.get("page", ""),
                "page_host": row.get("page_host", ""),
                "page_url": row.get("page_url", ""),
                "page_title": row.get("page_title", ""),
                "views": 0.0,
                "sessions": 0.0,
            },
        )
        bucket["views"] += float(row.get("views") or 0.0)
        bucket["sessions"] += float(row.get("sessions") or 0.0)
        if row.get("page_title") and not bucket.get("page_title"):
            bucket["page_title"] = row.get("page_title")
    out = list(merged.values())
    out.sort(key=lambda item: float(item.get("views") or 0.0), reverse=True)
    return out


def _match_ga4_by_headline(
    pages: list[dict],
    headline: str,
) -> list[dict]:
    if not headline or not pages:
        return []
    scored: list[tuple[float, dict]] = []
    for page in pages:
        title = str(page.get("page_title") or "")
        score = max(
            _headline_match_score(headline, title),
            _headline_match_score(headline, str(page.get("page") or "")),
        )
        if score >= _HEADLINE_MATCH_MIN:
            scored.append((score, page))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [p for _, p in scored[:8]]


def _lookup_gsc_from_db(db: Session, site_id: int, article_id: str, urls: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {"scopes": {}, "pages": [], "source": "db"}
    seen_urls: set[str] = set()
    url_set = {u for u in urls if u}
    for scope in _GSC_PAGE_SCOPES:
        try:
            rows = get_latest_search_console_rows(db, site_id=site_id, data_scope=scope)
        except Exception:
            rows = []
        matched = []
        for r in rows:
            q = str(r.get("query") or "")
            if q in url_set or page_url_matches_article_id(q, article_id):
                matched.append(r)
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


def _fetch_gsc_live(
    db: Session,
    site_id: int,
    article_id: str,
    start: date,
    end: date,
    urls: list[str],
) -> dict[str, Any]:
    from backend.collectors.search_console import (
        build_search_console_service_and_targets,
        fetch_search_console_for_page_urls,
        fetch_search_console_pages_for_article,
    )

    pages: list[dict] = []
    try:
        _site, service, targets = build_search_console_service_and_targets(db, site_id)
        for target in targets:
            property_url = str(target.get("property_url") or "")
            device = str(target.get("device") or "").upper() or None
            if urls:
                rows = fetch_search_console_for_page_urls(
                    service,
                    property_url,
                    start,
                    end,
                    urls,
                    device=device,
                )
                pages.extend(rows)
            else:
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
                    if page_url_matches_article_id(url, article_id):
                        pages.append(row)
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
                "page_count": len({p.get("query") or p.get("url") for p in pages}),
            }
        },
        "pages": [
            {
                "url": str(p.get("query") or ""),
                "device": p.get("device") or "ALL",
                "clicks": float(p.get("clicks") or 0.0),
                "impressions": float(p.get("impressions") or 0.0),
                "ctr": float(p.get("ctr") or 0.0),
                "position": float(p.get("position") or 0.0),
                "scope": "live",
            }
            for p in pages
            if p.get("query")
        ],
        "source": "live",
    }


def _fetch_ga4_live(
    db: Session,
    site_id: int,
    article_id: str,
    headline: str,
    start: str,
    end: str,
    days: int,
) -> dict[str, Any]:
    from backend.collectors.ga4 import (
        fetch_ga4_article_paths_metrics,
        fetch_ga4_news_detail_pages_metrics,
    )
    from backend.services.ga4_page_urls import enrich_ga4_page_rows

    ga4_status = get_ga4_connection_status(db, site_id)
    properties = (ga4_status.get("properties") or {}) if isinstance(ga4_status, dict) else {}
    profiles: dict[str, list[dict]] = {}
    totals = {"views": 0.0, "sessions": 0.0}
    urls: list[str] = []
    match_method = "none"
    resolved_article_id = article_id
    headline_pool: list[dict] = []

    for pf in ("web", "mweb"):
        prop = str(properties.get(pf) or "").strip()
        if not prop:
            continue
        try:
            by_id = fetch_ga4_article_paths_metrics(
                property_id=prop,
                article_id=article_id,
                start=start,
                end=end,
            )
            if by_id:
                match_method = "path_id"
            if not headline_pool:
                headline_pool = fetch_ga4_news_detail_pages_metrics(
                    property_id=prop,
                    start=start,
                    end=end,
                )
            if not by_id and headline and headline_pool:
                by_headline = _match_ga4_by_headline(headline_pool, headline)
                if by_headline:
                    by_id = by_headline
                    match_method = "headline"
                    path_id = extract_article_id_from_path(str(by_headline[0].get("page") or ""))
                    if path_id:
                        resolved_article_id = path_id
            enriched = enrich_ga4_page_rows(by_id, keep_news_articles=True)
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
        "match_method": match_method,
        "resolved_article_id": resolved_article_id,
        "date_range": {"start": start, "end": end},
    }


def resolve_content_traffic(
    db: Session,
    *,
    content_id: str,
    headline: str | None = None,
    send_date: str | None = None,
    site_id: int | None = None,
    days: int = 14,
    live: bool = True,
) -> dict[str, Any]:
    """Bildirim içerik ID'si (+ isteğe bağlı başlık/gönderim tarihi) için GA4 + GSC trafik."""
    aid = normalize_article_id(content_id)
    sid = int(site_id or _DEFAULT_SITE_ID)
    site = db.query(Site).filter(Site.id == sid).first()
    safe_days = max(1, min(int(days or 14), 90))
    start, end, range_meta = resolve_traffic_date_range(send_date=send_date, days=safe_days)
    start_d = _parse_day(start) or report_calendar_yesterday()
    end_d = _parse_day(end) or start_d

    if not aid:
        return {
            "content_id": content_id,
            "article_id": "",
            "site_id": sid,
            "site_domain": site.domain if site else "",
            "error": "Geçerli içerik ID bulunamadı.",
            "ga4": None,
            "gsc": None,
            "date_range": range_meta,
        }

    ga4: dict[str, Any] | None = None
    if live:
        ga4 = _fetch_ga4_live(
            db,
            sid,
            aid,
            str(headline or "").strip(),
            start,
            end,
            safe_days,
        )

    resolved_urls = list((ga4 or {}).get("urls") or [])
    resolved_aid = str((ga4 or {}).get("resolved_article_id") or aid)

    gsc_db = _lookup_gsc_from_db(db, sid, resolved_aid, resolved_urls)
    gsc: dict[str, Any]
    if gsc_db.get("pages"):
        gsc = gsc_db
    elif live:
        gsc = _fetch_gsc_live(db, sid, resolved_aid, start_d, end_d, resolved_urls)
    else:
        gsc = gsc_db

    primary_gsc = gsc.get("scopes") or {}
    gsc_live = primary_gsc.get("live") or {}
    gsc_7 = primary_gsc.get("current_7d_pages") or gsc_live or {}
    gsc_30 = primary_gsc.get("current_30d_pages") or {}

    return {
        "content_id": content_id,
        "article_id": aid,
        "resolved_article_id": resolved_aid,
        "headline": str(headline or "").strip(),
        "site_id": sid,
        "site_domain": site.domain if site else "",
        "days": safe_days,
        "date_range": range_meta,
        "ga4": ga4,
        "gsc": gsc,
        "summary": {
            "ga4_views": (ga4 or {}).get("totals", {}).get("views", 0),
            "ga4_sessions": (ga4 or {}).get("totals", {}).get("sessions", 0),
            "gsc_clicks_7d": gsc_7.get("clicks", 0),
            "gsc_impressions_7d": gsc_7.get("impressions", 0),
            "gsc_clicks_30d": gsc_30.get("clicks", 0),
            "gsc_impressions_30d": gsc_30.get("impressions", 0),
            "match_method": (ga4 or {}).get("match_method") or "none",
            "matched_urls": list(
                dict.fromkeys(
                    resolved_urls + [p.get("url") for p in (gsc.get("pages") or []) if p.get("url")]
                )
            ),
        },
    }
