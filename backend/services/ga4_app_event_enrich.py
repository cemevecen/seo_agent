"""GA4 mobil event parametre satırlarını web/mweb haber URL'leri ile zenginleştirir."""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterable

from backend.services.ga4_page_urls import (
    enrich_ga4_page_rows,
    ga4_row_news_display_text,
    ga4_row_page_href,
    ga4_row_page_label,
)
from backend.services.notification_content_traffic import (
    extract_article_id_from_path,
    normalize_article_id,
)

_NEWS_ID_PARAMS = frozenset({"news_id", "newsid"})
_NEWS_TITLE_PARAMS = frozenset({"news_title", "newstitle"})
_COMBINED_SEP = " · "
_EVENT_DETAIL_CACHE: dict[tuple, tuple[float, list[dict[str, Any]]]] = {}
_LOOKUP_CACHE: dict[tuple, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 180.0
_CACHE_VER = 5


def _param_key(name: str | None) -> str:
    return re.sub(r"[\s_]", "", str(name or "").strip().lower())


def section_enriches_news(param: str | None, param2: str | None = None) -> bool:
    keys = {_param_key(param), _param_key(param2)}
    keys.discard("")
    return bool(keys & (_NEWS_ID_PARAMS | _NEWS_TITLE_PARAMS))


def _normalize_title_key(title: str) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip().lower())


def build_news_article_lookup(
    property_ids: Iterable[str],
    *,
    days: int,
) -> dict[str, Any]:
    """Web/mweb GA4 haber detay listesinden article_id → sayfa satırı indeksi."""
    from backend.collectors.ga4 import _calendar_windows, fetch_ga4_news_detail_pages_metrics

    (last_start, last_end), _ = _calendar_windows(max(1, int(days)))
    by_id: dict[str, dict] = {}
    by_title: dict[str, str] = {}
    pids = [str(raw_pid or "").strip() for raw_pid in property_ids]
    pids = list(dict.fromkeys(p for p in pids if p))
    if not pids:
        return {"by_id": by_id, "by_title": by_title}

    def _fetch_pid(pid: str) -> list[dict]:
        try:
            return fetch_ga4_news_detail_pages_metrics(
                property_id=pid,
                start=last_start,
                end=last_end,
                limit=500,
            )
        except Exception:
            return []

    raw_by_pid: list[list[dict]] = []
    workers = min(2, len(pids))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ga4-news-lookup") as pool:
        for rows in pool.map(_fetch_pid, pids):
            raw_by_pid.append(rows)

    for raw_rows in raw_by_pid:
        rows = enrich_ga4_page_rows(raw_rows, keep_news_articles=True)
        for row in rows:
            if not isinstance(row, dict):
                continue
            aid = extract_article_id_from_path(str(row.get("page") or ""))
            if not aid:
                continue
            views = float(row.get("views") or 0.0)
            prev = by_id.get(aid)
            if prev is None:
                by_id[aid] = {**row, "article_id": aid, "views": views}
            else:
                prev_views = float(prev.get("views") or 0.0)
                prev_amp = "/amp" in str(prev.get("page") or "").lower()
                row_amp = "/amp" in str(row.get("page") or "").lower()
                if views > prev_views or (views == prev_views and prev_amp and not row_amp):
                    by_id[aid] = {**row, "article_id": aid, "views": views}
            title = str(row.get("page_title") or "").strip()
            if title:
                by_title[_normalize_title_key(title)] = aid

    return {"by_id": by_id, "by_title": by_title}


def _parse_combined_value(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    if _COMBINED_SEP in raw:
        left, right = raw.split(_COMBINED_SEP, 1)
        return left.strip(), right.strip()
    return raw, ""


def _resolve_article_id(
    *,
    value: str,
    param: str,
    param2: str | None,
    by_id: dict[str, dict],
    by_title: dict[str, str],
) -> tuple[str, str]:
    """article_id ve başlık ipucu döner."""
    param_k = _param_key(param)
    title_hint = ""

    if param2:
        id_part, title_hint = _parse_combined_value(value)
        aid = normalize_article_id(id_part)
        if aid:
            return aid, title_hint
        title_hint = title_hint or id_part
    elif param_k in _NEWS_ID_PARAMS:
        return normalize_article_id(value), ""
    elif param_k in _NEWS_TITLE_PARAMS:
        title_hint = str(value or "").strip()
        nk = _normalize_title_key(title_hint)
        if nk in by_title:
            return by_title[nk], title_hint
        for tk, aid in by_title.items():
            if len(nk) >= 12 and (tk.startswith(nk) or nk.startswith(tk[: min(len(tk), 48)])):
                return aid, title_hint
        return "", title_hint

    if title_hint:
        nk = _normalize_title_key(title_hint)
        if nk in by_title:
            return by_title[nk], title_hint
    return "", title_hint


def enrich_event_param_row(
    row: dict,
    *,
    param: str,
    param2: str | None,
    lookup: dict[str, Any],
    site_domain: str | None,
) -> dict:
    out = dict(row)
    value = str(row.get("value") or "").strip()
    out["raw_value"] = value

    if not value or value.lower() in ("(not set)", "not set"):
        out["display_text"] = value
        return out

    by_id = lookup.get("by_id") or {}
    by_title = lookup.get("by_title") or {}
    article_id, title_hint = _resolve_article_id(
        value=value,
        param=param,
        param2=param2,
        by_id=by_id,
        by_title=by_title,
    )

    page_row = by_id.get(article_id) if article_id else None
    if page_row:
        page_url = ga4_row_page_href(page_row, site_domain)
        page_path = ga4_row_page_label(page_row, site_domain)
        slug = ga4_row_news_display_text(page_row, site_domain)
        page_title = str(page_row.get("page_title") or "").strip()
        out["page_url"] = page_url
        out["page_path"] = page_path
        out["article_id"] = article_id
        out["display_text"] = slug or page_title or title_hint or value
        if _param_key(param) in _NEWS_ID_PARAMS and not param2:
            out["display_sub"] = page_title or page_path
        elif param2 or _param_key(param) in _NEWS_TITLE_PARAMS:
            out["display_sub"] = page_path or (f"ID {article_id}" if article_id else "")
        else:
            out["display_sub"] = page_path
        return out

    if _param_key(param) in _NEWS_ID_PARAMS or param2:
        out["display_text"] = title_hint or value
        if article_id:
            out["display_sub"] = f"ID {article_id} — URL eşleşmedi"
            out["article_id"] = article_id
        else:
            out["display_sub"] = ""
    else:
        out["display_text"] = title_hint or value
        out["display_sub"] = ""

    return out


def _row_merge_key(row: dict, *, param: str, param2: str | None) -> str:
    """Aynı haber/sayfa için tek satır — article_id, URL veya normalize başlık."""
    aid = str(row.get("article_id") or "").strip()
    if aid:
        return f"id:{aid}"
    page_url = str(row.get("page_url") or "").strip().lower()
    if page_url:
        return f"url:{page_url}"
    raw = str(row.get("raw_value") or row.get("value") or "").strip()
    raw_l = raw.lower()
    if raw_l in ("(not set)", "not set"):
        return "raw:(not set)"
    if section_enriches_news(param, param2):
        display = str(row.get("display_text") or "").strip().lower()
        if display and display not in ("(not set)", "not set"):
            return f"display:{display}"
        nk = _normalize_title_key(raw)
        if nk:
            return f"title:{nk}"
    return f"raw:{raw_l}"


def merge_enriched_event_rows(
    rows: list[dict],
    *,
    param: str,
    param2: str | None,
) -> list[dict]:
    """Enrichment sonrası aynı habere ait satırları toplar."""
    if not rows:
        return []
    buckets: dict[str, dict] = {}
    order: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _row_merge_key(row, param=param, param2=param2)
        if key not in buckets:
            buckets[key] = dict(row)
            order.append(key)
            continue
        bucket = buckets[key]
        bucket["count"] = float(bucket.get("count") or 0) + float(row.get("count") or 0)
        bucket["count_prev"] = float(bucket.get("count_prev") or 0) + float(row.get("count_prev") or 0)
        if row.get("page_url") and not bucket.get("page_url"):
            for field in ("page_url", "page_path", "display_text", "display_sub", "article_id"):
                if row.get(field):
                    bucket[field] = row[field]
    merged = [buckets[k] for k in order]
    merged.sort(
        key=lambda r: (
            str(r.get("display_text") or r.get("value") or "").lower() in ("(not set)", "not set"),
            -float(r.get("count") or 0),
        )
    )
    return merged


def enrich_app_event_detail_sections(
    sections: list[dict],
    *,
    property_ids: Iterable[str],
    days: int,
    site_domain: str | None,
    lookup: dict[str, Any] | None = None,
) -> list[dict]:
    if not sections:
        return []
    needs_lookup = any(section_enriches_news(s.get("param"), s.get("param2")) for s in sections)
    resolved_lookup: dict[str, Any] = lookup or {"by_id": {}, "by_title": {}}
    if needs_lookup and lookup is None:
        resolved_lookup = build_news_article_lookup(property_ids, days=days)

    out_sections: list[dict] = []
    for section in sections:
        sec = dict(section)
        param = str(sec.get("param") or "")
        param2 = sec.get("param2")
        if section_enriches_news(param, param2) and sec.get("rows"):
            enriched = [
                enrich_event_param_row(
                    r,
                    param=param,
                    param2=str(param2) if param2 else None,
                    lookup=resolved_lookup,
                    site_domain=site_domain,
                )
                for r in sec["rows"]
                if isinstance(r, dict)
            ]
            sec["rows"] = merge_enriched_event_rows(enriched, param=param, param2=param2)
        elif sec.get("rows"):
            sec["rows"] = merge_enriched_event_rows(
                [dict(r) for r in sec["rows"] if isinstance(r, dict)],
                param=param,
                param2=param2,
            )
        out_sections.append(sec)
    return out_sections


def _lookup_cache_key(property_ids: Iterable[str], days: int) -> tuple:
    pids = tuple(sorted(str(p or "").strip() for p in property_ids if str(p or "").strip()))
    return pids + (max(1, int(days)),)


def fetch_enriched_app_event_detail_sections(
    *,
    property_id: str,
    profile: str,
    days: int,
    limit: int,
    lookup_property_ids: Iterable[str],
    site_domain: str | None,
) -> list[dict]:
    """Event detay bölümlerini çeker, URL ile zenginleştirir; kısa süreli bellek önbelleği."""
    from backend.collectors.ga4 import fetch_ga4_app_event_detail_sections

    cache_key = (str(property_id), str(profile).lower(), max(1, int(days)), max(10, int(limit)), _CACHE_VER)
    cached = _EVENT_DETAIL_CACHE.get(cache_key)
    if cached and (time.monotonic() - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    lookup_key = _lookup_cache_key(lookup_property_ids, days)
    lookup_cached = _LOOKUP_CACHE.get(lookup_key)
    lookup: dict[str, Any] | None = None
    if lookup_cached and (time.monotonic() - lookup_cached[0]) < _CACHE_TTL_SEC:
        lookup = lookup_cached[1]

    if lookup is not None:
        sections = fetch_ga4_app_event_detail_sections(
            property_id=property_id,
            profile=profile,
            days=days,
            limit=limit,
        )
        out = enrich_app_event_detail_sections(
            sections,
            property_ids=lookup_property_ids,
            days=days,
            site_domain=site_domain,
            lookup=lookup,
        )
    else:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="ga4-evdetail") as pool:
            fut_sections = pool.submit(
                fetch_ga4_app_event_detail_sections,
                property_id=property_id,
                profile=profile,
                days=days,
                limit=limit,
            )
            fut_lookup = pool.submit(build_news_article_lookup, lookup_property_ids, days=days)
            sections = fut_sections.result()
            lookup = fut_lookup.result()
        _LOOKUP_CACHE[lookup_key] = (time.monotonic(), lookup)
        out = enrich_app_event_detail_sections(
            sections,
            property_ids=lookup_property_ids,
            days=days,
            site_domain=site_domain,
            lookup=lookup,
        )

    _EVENT_DETAIL_CACHE[cache_key] = (time.monotonic(), out)
    return out
