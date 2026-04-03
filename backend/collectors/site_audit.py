"""Gercek sitemap discovery ve URL bazli SEO denetimi."""

from __future__ import annotations

import gzip
import json
import re
import xml.etree.ElementTree as ET
from collections import deque
from datetime import date, datetime, timedelta
from html import unescape
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from sqlalchemy.orm import Session

from backend.collectors.crawler import _build_search_console_service, _normalize_url, _same_site_family
from backend.collectors.search_console import _resolve_latest_available_day, _resolve_search_console_targets
from backend.collectors.url_inspection import _extract_summary, _load_service_and_property
from backend.config import settings
from backend.models import Site
from backend.services.metric_store import save_metrics
from backend.services.search_console_auth import get_search_console_credentials_record
from backend.services.warehouse import finish_collector_run, save_url_audit_records, start_collector_run

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
H1_RE = re.compile(r"<h1\b[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
SPACE_RE = re.compile(r"\s+")
SCHEMA_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\']', re.IGNORECASE)
SITEMAP_LINE_RE = re.compile(r"^\s*Sitemap:\s*(.+?)\s*$", re.IGNORECASE)
COMMON_SITEMAP_PATHS = (
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/sitemap",
    "/sitemap/",
)


def _clean_text(value: str) -> str:
    stripped = TAG_RE.sub(" ", value or "")
    stripped = unescape(stripped)
    return SPACE_RE.sub(" ", stripped).strip()


def _normalize_http_url(url: str) -> str:
    parsed = urlparse(url or "")
    path = parsed.path or "/"
    return urlunparse(
        (
            parsed.scheme or "https",
            (parsed.netloc or "").lower(),
            path,
            "",
            parsed.query or "",
            "",
        )
    )


def _alternate_site_roots(base_url: str) -> list[str]:
    parsed = urlparse(base_url)
    host = (parsed.netloc or "").lower()
    roots = [_normalize_http_url(base_url)]
    if host.startswith("www."):
        roots.append(f"{parsed.scheme or 'https'}://{host[4:]}")
    else:
        roots.append(f"{parsed.scheme or 'https'}://www.{host}")
    deduped: list[str] = []
    for item in roots:
        normalized = item.rstrip("/")
        if normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _parse_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        pass
    try:
        return datetime.combine(date.fromisoformat(raw[:10]), datetime.min.time())
    except ValueError:
        return None


def _fetch_text(url: str, *, timeout_seconds: int) -> tuple[int, str]:
    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": settings.outbound_user_agent,
                "Accept": "application/xml,text/xml,text/plain,text/html;q=0.9,*/*;q=0.8",
            },
            allow_redirects=True,
            timeout=max(1, int(timeout_seconds)),
        )
    except requests.RequestException:
        return 0, ""
    try:
        body = ""
        content_type = str(response.headers.get("content-type") or "").lower()
        content = response.content or b""
        if content.startswith(b"\x1f\x8b") or url.lower().endswith(".gz") or "gzip" in content_type:
            try:
                body = gzip.decompress(content).decode("utf-8", "replace")
            except Exception:
                body = response.text
        else:
            body = response.text
        return int(response.status_code or 0), body or ""
    finally:
        response.close()


def _extract_meta_content(html: str, key: str, value: str) -> str:
    pattern = re.compile(
        rf"<meta\b[^>]*\b{re.escape(key)}\s*=\s*['\"]{re.escape(value)}['\"][^>]*\bcontent\s*=\s*['\"](.*?)['\"][^>]*>",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(html or "")
    if match:
        return _clean_text(match.group(1))
    reverse_pattern = re.compile(
        rf"<meta\b[^>]*\bcontent\s*=\s*['\"](.*?)['\"][^>]*\b{re.escape(key)}\s*=\s*['\"]{re.escape(value)}['\"][^>]*>",
        re.IGNORECASE | re.DOTALL,
    )
    reverse_match = reverse_pattern.search(html or "")
    return _clean_text(reverse_match.group(1)) if reverse_match else ""


def _extract_link_href(html: str, rel: str) -> str:
    pattern = re.compile(
        rf"<link\b[^>]*\brel\s*=\s*['\"]{re.escape(rel)}['\"][^>]*\bhref\s*=\s*['\"](.*?)['\"][^>]*>",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(html or "")
    if match:
        return match.group(1).strip()
    reverse_pattern = re.compile(
        rf"<link\b[^>]*\bhref\s*=\s*['\"](.*?)['\"][^>]*\brel\s*=\s*['\"]{re.escape(rel)}['\"][^>]*>",
        re.IGNORECASE | re.DOTALL,
    )
    reverse_match = reverse_pattern.search(html or "")
    return reverse_match.group(1).strip() if reverse_match else ""


def _discover_sitemap_seeds(base_url: str, *, timeout_seconds: int) -> dict:
    seeds: list[str] = []
    robots_urls: list[str] = []
    tried_robots: list[str] = []
    for root in _alternate_site_roots(base_url):
        robots_url = root.rstrip("/") + "/robots.txt"
        tried_robots.append(robots_url)
        status, body = _fetch_text(robots_url, timeout_seconds=timeout_seconds)
        if status != 200 or not body:
            continue
        for line in body.splitlines():
            match = SITEMAP_LINE_RE.match(line)
            if not match:
                continue
            sitemap_url = _normalize_http_url(urljoin(robots_url, match.group(1).strip()))
            if sitemap_url not in robots_urls:
                robots_urls.append(sitemap_url)
    candidates = robots_urls[:]
    for root in _alternate_site_roots(base_url):
        for path in COMMON_SITEMAP_PATHS:
            candidate = _normalize_http_url(root.rstrip("/") + path)
            if candidate not in candidates:
                candidates.append(candidate)
    for candidate in candidates:
        if candidate not in seeds:
            seeds.append(candidate)
    return {
        "seed_urls": seeds,
        "robots_sitemaps": robots_urls,
        "robots_checked": tried_robots,
    }


def _parse_sitemap_xml(body: str) -> tuple[str, list[dict]]:
    root = ET.fromstring(body)
    root_name = str(root.tag).split("}")[-1].lower()
    entries: list[dict] = []
    for node in root:
        node_name = str(node.tag).split("}")[-1].lower()
        if node_name not in {"url", "sitemap"}:
            continue
        loc = ""
        lastmod = ""
        for child in node:
            child_name = str(child.tag).split("}")[-1].lower()
            child_text = str(child.text or "").strip()
            if child_name == "loc":
                loc = child_text
            elif child_name == "lastmod":
                lastmod = child_text
        if loc:
            entries.append({"loc": loc, "lastmod": lastmod})
    return root_name, entries


def _discover_sitemap_entries(base_url: str, *, max_urls: int, recent_days: int, timeout_seconds: int) -> dict:
    seeds = _discover_sitemap_seeds(base_url, timeout_seconds=timeout_seconds)
    queue: deque[str] = deque(seeds["seed_urls"])
    visited: set[str] = set()
    seen_urls: set[str] = set()
    url_entries: list[dict] = []
    fetched_sitemaps: list[str] = []
    failed_sitemaps: list[str] = []
    filtered_old = 0
    cutoff = datetime.utcnow() - timedelta(days=max(1, int(recent_days)))

    while queue and len(url_entries) < max_urls:
        sitemap_url = queue.popleft()
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)
        status, body = _fetch_text(sitemap_url, timeout_seconds=timeout_seconds)
        if status != 200 or not body.strip():
            failed_sitemaps.append(sitemap_url)
            continue
        try:
            root_name, entries = _parse_sitemap_xml(body)
        except ET.ParseError:
            failed_sitemaps.append(sitemap_url)
            continue
        fetched_sitemaps.append(sitemap_url)
        if root_name == "sitemapindex":
            for entry in entries:
                child = _normalize_http_url(entry["loc"])
                if child not in visited:
                    queue.append(child)
            continue
        for entry in entries:
            normalized = _normalize_http_url(entry["loc"])
            if not normalized or normalized in seen_urls:
                continue
            if not _same_site_family(normalized, base_url=base_url):
                continue
            lastmod = str(entry.get("lastmod") or "").strip()
            lastmod_dt = _parse_datetime(lastmod)
            if lastmod_dt and lastmod_dt < cutoff:
                filtered_old += 1
                continue
            seen_urls.add(normalized)
            url_entries.append(
                {
                    "url": normalized,
                    "sitemap_source": sitemap_url,
                    "sitemap_lastmod": lastmod,
                }
            )
            if len(url_entries) >= max_urls:
                break

    return {
        "entries": url_entries,
        "seed_urls": seeds["seed_urls"],
        "robots_sitemaps": seeds["robots_sitemaps"],
        "robots_checked": seeds["robots_checked"],
        "fetched_sitemaps": fetched_sitemaps,
        "failed_sitemaps": failed_sitemaps,
        "filtered_old_count": filtered_old,
    }


def _fetch_search_console_page_weights(db: Session, site: Site, *, recent_days: int, limit: int) -> dict[str, dict]:
    credential = get_search_console_credentials_record(db, site.id)
    if credential is None:
        return {}
    try:
        service = _build_search_console_service(credential)
        if service is None:
            return {}
        targets = _resolve_search_console_targets(service, site)
        latest_supported_end_date = _resolve_latest_available_day(
            service,
            targets,
            fallback_end_date=date.today() - timedelta(days=1),
        )
        start_date = latest_supported_end_date - timedelta(days=max(1, int(recent_days)) - 1)
        page_size = min(max(250, limit), settings.search_console_row_batch_size)
        aggregated: dict[str, dict] = {}
        for target in targets:
            property_url = str(target.get("property_url") or "")
            device = str(target.get("device") or "").upper() or None
            start_row = 0
            while start_row < limit:
                body = {
                    "startDate": start_date.isoformat(),
                    "endDate": latest_supported_end_date.isoformat(),
                    "dimensions": ["page"],
                    "rowLimit": page_size,
                    "startRow": start_row,
                }
                if device:
                    body["dimensionFilterGroups"] = [
                        {"filters": [{"dimension": "device", "expression": device}]}
                    ]
                response = service.searchanalytics().query(siteUrl=property_url, body=body).execute()
                rows = response.get("rows", []) or []
                for row in rows:
                    keys = row.get("keys") or []
                    page_url = _normalize_http_url(str(keys[0] if keys else ""))
                    if not page_url or not _same_site_family(page_url, base_url=_normalize_url(site.domain)):
                        continue
                    item = aggregated.setdefault(
                        page_url,
                        {"clicks": 0.0, "impressions": 0.0},
                    )
                    item["clicks"] += float(row.get("clicks") or 0.0)
                    item["impressions"] += float(row.get("impressions") or 0.0)
                if len(rows) < page_size:
                    break
                start_row += page_size
        for url, item in aggregated.items():
            impressions = float(item.get("impressions") or 0.0)
            clicks = float(item.get("clicks") or 0.0)
            item["ctr"] = round((clicks / impressions) if impressions > 0 else 0.0, 6)
        return aggregated
    except Exception:
        return {}


def _inspection_is_indexed(summary: dict) -> bool:
    verdict = str(summary.get("verdict") or "").upper()
    indexing_state = str(summary.get("indexing_state") or "").lower()
    coverage_state = str(summary.get("coverage_state") or "").lower()
    if verdict == "PASS":
        return True
    indexed_tokens = ("indexed", "submitted and indexed")
    blocked_tokens = ("not indexed", "blocked", "excluded", "noindex", "error")
    if any(token in indexing_state for token in indexed_tokens) and not any(token in indexing_state for token in blocked_tokens):
        return True
    if any(token in coverage_state for token in indexed_tokens) and not any(token in coverage_state for token in blocked_tokens):
        return True
    return False


def _inspect_urls_exact(db: Session, site: Site, urls: list[str], *, limit: int) -> dict[str, dict]:
    if not urls:
        return {}
    service, property_url = _load_service_and_property(db, site)
    results: dict[str, dict] = {}
    for url in urls[:limit]:
        try:
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
            results[url] = {
                "indexed": _inspection_is_indexed(summary),
                "verdict": str(summary.get("verdict") or ""),
                "coverage_state": str(summary.get("coverage_state") or ""),
                "indexing_state": str(summary.get("indexing_state") or ""),
            }
        except Exception:
            results[url] = {
                "indexed": False,
                "verdict": "ERROR",
                "coverage_state": "",
                "indexing_state": "",
            }
    return results


def _fetch_url_audit(url: str, *, timeout_seconds: int) -> dict:
    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": settings.outbound_user_agent,
                "Accept": "text/html,application/xhtml+xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
            },
            allow_redirects=True,
            timeout=max(1, int(timeout_seconds)),
        )
    except requests.RequestException:
        return {
            "url": _normalize_http_url(url),
            "final_url": _normalize_http_url(url),
            "status_code": 0,
            "content_type": "",
            "checks": {
                "status_ok": False,
                "html_content": False,
                "title": False,
                "title_length_ok": False,
                "desc": False,
                "desc_length_ok": False,
                "h1": False,
                "single_h1": False,
                "canonical": False,
                "canonical_matches_final": False,
                "schema": False,
                "indexable": False,
                "og_title": False,
                "og_description": False,
                "indexed_quick": False,
                "indexed_exact": False,
            },
            "issue_count": 8,
            "seo_score": "poor",
        }

    try:
        final_url = _normalize_http_url(response.url or url)
        status_code = int(response.status_code or 0)
        content_type = str(response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
        html = response.text if "html" in content_type or content_type == "" else ""
    finally:
        response.close()

    title_match = TITLE_RE.search(html or "")
    title = _clean_text(title_match.group(1)) if title_match else ""
    h1_matches = H1_RE.findall(html or "")
    h1_values = [_clean_text(match) for match in h1_matches if _clean_text(match)]
    h1 = h1_values[0] if h1_values else ""
    meta_description = _extract_meta_content(html, "name", "description")
    meta_robots = _extract_meta_content(html, "name", "robots")
    canonical_raw = _extract_link_href(html, "canonical")
    canonical_url = _normalize_http_url(urljoin(final_url, canonical_raw)) if canonical_raw else ""
    has_schema = bool(SCHEMA_RE.search(html or ""))
    has_og_title = bool(_extract_meta_content(html, "property", "og:title"))
    has_og_description = bool(_extract_meta_content(html, "property", "og:description"))
    is_noindex = "noindex" in (meta_robots or "").lower()

    checks = {
        "status_ok": status_code == 200,
        "html_content": bool(html),
        "title": bool(title),
        "title_length_ok": 20 <= len(title) <= 65 if title else False,
        "desc": bool(meta_description),
        "desc_length_ok": 70 <= len(meta_description) <= 170 if meta_description else False,
        "h1": bool(h1),
        "single_h1": len(h1_values) == 1 if h1_values else False,
        "canonical": bool(canonical_url),
        "canonical_matches_final": bool(canonical_url) and canonical_url == final_url,
        "schema": has_schema,
        "indexable": not is_noindex and status_code == 200,
        "og_title": has_og_title,
        "og_description": has_og_description,
        "indexed_quick": False,
        "indexed_exact": False,
    }

    issue_count = sum(1 for key, ok in checks.items() if key not in {"indexed_quick", "indexed_exact"} and not ok)
    if not checks["status_ok"] or not checks["html_content"] or not checks["title"] or not checks["desc"] or not checks["h1"]:
        seo_score = "poor"
    elif (
        not checks["title_length_ok"]
        or not checks["desc_length_ok"]
        or not checks["canonical"]
        or not checks["canonical_matches_final"]
        or not checks["schema"]
        or not checks["og_title"]
        or not checks["og_description"]
        or not checks["single_h1"]
        or not checks["indexable"]
    ):
        seo_score = "needs_improvement"
    else:
        seo_score = "good"

    return {
        "url": _normalize_http_url(url),
        "final_url": final_url,
        "status_code": status_code,
        "content_type": content_type,
        "has_title": bool(title),
        "title": title,
        "title_length": len(title),
        "has_meta_description": bool(meta_description),
        "meta_description": meta_description,
        "meta_description_length": len(meta_description),
        "has_h1": bool(h1),
        "h1": h1,
        "h1_count": len(h1_values),
        "has_canonical": bool(canonical_url),
        "canonical_url": canonical_url,
        "canonical_matches_final": checks["canonical_matches_final"],
        "has_schema": has_schema,
        "is_noindex": is_noindex,
        "meta_robots": meta_robots,
        "has_og_title": has_og_title,
        "has_og_description": has_og_description,
        "checks": checks,
        "issue_count": issue_count,
        "seo_score": seo_score,
    }


def _build_empty_summary(base_url: str, *, recent_days: int, index_mode: str) -> dict:
    return {
        "base_url": base_url,
        "recent_days": recent_days,
        "requested_index_mode": index_mode,
        "index_mode": index_mode,
        "seed_sitemaps": [],
        "robots_sitemaps": [],
        "fetched_sitemaps": [],
        "failed_sitemaps": [],
        "sitemap_url_count": 0,
        "candidate_url_count": 0,
        "audited_url_count": 0,
        "filtered_old_count": 0,
        "score_distribution": {"good": 0, "needs_improvement": 0, "poor": 0},
        "check_summary": {},
        "issue_examples": [],
        "top_weighted_examples": [],
        "fallback_used": False,
        "search_console_weighted_urls": 0,
        "exact_inspection_count": 0,
        "exact_inspection_skipped": 0,
    }


def collect_site_audit(
    db: Session,
    site: Site,
    *,
    sitemap_url_limit: int | None = None,
    request_timeout_seconds: int | None = None,
    recent_days: int | None = None,
    index_mode: str | None = None,
) -> dict:
    """Gercek sitemap discovery + URL bazli audit + index modu filtreleri."""
    collected_at = datetime.utcnow()
    base_url = _normalize_url(site.domain)
    url_limit = max(1, int(sitemap_url_limit or settings.site_audit_sitemap_url_limit))
    timeout_seconds = max(1, int(request_timeout_seconds or settings.site_audit_request_timeout_seconds))
    recent_days = max(1, int(recent_days or settings.site_audit_recent_days))
    requested_index_mode = str(index_mode or settings.site_audit_index_mode_default or "quick").strip().lower()
    if requested_index_mode not in {"all", "quick", "exact"}:
        requested_index_mode = "quick"
    index_mode = requested_index_mode

    run = start_collector_run(
        db,
        site_id=site.id,
        provider="site_audit",
        strategy=f"sitemap_{index_mode}_{recent_days}d",
        target_url=base_url,
        requested_at=collected_at,
    )

    discovery = _discover_sitemap_entries(
        base_url,
        max_urls=url_limit,
        recent_days=recent_days,
        timeout_seconds=timeout_seconds,
    )
    discovered_entries = discovery["entries"]
    page_weights = _fetch_search_console_page_weights(
        db,
        site,
        recent_days=recent_days,
        limit=settings.site_audit_sc_page_limit,
    )

    rows: list[dict] = []
    for entry in discovered_entries:
        row = _fetch_url_audit(entry["url"], timeout_seconds=timeout_seconds)
        row["sitemap_source"] = str(entry.get("sitemap_source") or "")
        row["sitemap_lastmod"] = str(entry.get("sitemap_lastmod") or "")
        weight = page_weights.get(row["url"]) or page_weights.get(row["final_url"]) or {}
        row["search_clicks"] = float(weight.get("clicks") or 0.0)
        row["search_impressions"] = float(weight.get("impressions") or 0.0)
        row["search_ctr"] = float(weight.get("ctr") or 0.0)
        row["search_console_seen"] = bool(row["search_clicks"] or row["search_impressions"])
        row["indexed_via"] = "none"
        row["inspection_verdict"] = ""

        quick_indexed = bool(
            row["checks"]["indexable"]
            and (
                row["search_console_seen"]
                or row["canonical_matches_final"]
                or not row["has_canonical"]
            )
        )
        row["checks"]["indexed_quick"] = quick_indexed
        rows.append(row)

    exact_results: dict[str, dict] = {}
    exact_inspection_count = 0
    exact_inspection_skipped = 0
    if index_mode == "exact" and rows:
        ordered_for_exact = sorted(
            rows,
            key=lambda item: (
                -(item.get("search_clicks") or 0.0),
                -(item.get("search_impressions") or 0.0),
                item.get("url") or "",
            ),
        )
        exact_limit = max(1, int(settings.site_audit_exact_inspection_limit))
        exact_targets = [item["url"] for item in ordered_for_exact[:exact_limit]]
        exact_inspection_skipped = max(0, len(rows) - len(exact_targets))
        if exact_targets:
            try:
                exact_results = _inspect_urls_exact(db, site, exact_targets, limit=exact_limit)
                exact_inspection_count = len(exact_results)
            except Exception:
                exact_results = {}
                exact_inspection_count = 0
                exact_inspection_skipped = len(rows)
                index_mode = "quick"

    filtered_rows: list[dict] = []
    for row in rows:
        if index_mode == "quick":
            if not row["checks"]["indexed_quick"]:
                continue
            row["indexed_via"] = "quick"
        elif index_mode == "exact":
            result = exact_results.get(row["url"]) or exact_results.get(row["final_url"])
            row["checks"]["indexed_exact"] = bool(result and result.get("indexed"))
            row["inspection_verdict"] = str((result or {}).get("verdict") or "")
            if not row["checks"]["indexed_exact"]:
                continue
            row["indexed_via"] = "exact"
        filtered_rows.append(row)

    if not filtered_rows:
        summary = _build_empty_summary(base_url, recent_days=recent_days, index_mode=index_mode)
        summary.update(
            {
                "requested_index_mode": requested_index_mode,
                "seed_sitemaps": discovery["seed_urls"],
                "robots_sitemaps": discovery["robots_sitemaps"],
                "fetched_sitemaps": discovery["fetched_sitemaps"],
                "failed_sitemaps": discovery["failed_sitemaps"],
                "candidate_url_count": len(discovered_entries),
                "filtered_old_count": discovery["filtered_old_count"],
                "search_console_weighted_urls": sum(1 for item in rows if item.get("search_console_seen")),
                "exact_inspection_count": exact_inspection_count,
                "exact_inspection_skipped": exact_inspection_skipped,
            }
        )
        finish_collector_run(
            db,
            run,
            status="success",
            finished_at=collected_at,
            summary=summary,
            row_count=0,
        )
        db.commit()
        return {"site_id": site.id, "collector_run_id": run.id, "summary": summary, "rows": []}

    save_url_audit_records(
        db,
        site_id=site.id,
        rows=filtered_rows,
        collected_at=collected_at,
        collector_run_id=run.id,
    )

    score_distribution = {
        "good": sum(1 for row in filtered_rows if row.get("seo_score") == "good"),
        "needs_improvement": sum(1 for row in filtered_rows if row.get("seo_score") == "needs_improvement"),
        "poor": sum(1 for row in filtered_rows if row.get("seo_score") == "poor"),
    }

    check_keys = [
        "title",
        "title_length_ok",
        "desc",
        "desc_length_ok",
        "h1",
        "single_h1",
        "canonical",
        "canonical_matches_final",
        "schema",
        "indexable",
        "og_title",
        "og_description",
    ]
    check_summary: dict[str, dict] = {}
    for key in check_keys:
        ok_count = sum(1 for row in filtered_rows if bool((row.get("checks") or {}).get(key)))
        check_summary[key] = {
            "ok": ok_count,
            "fail": len(filtered_rows) - ok_count,
            "share": round((ok_count / len(filtered_rows)) * 100, 1) if filtered_rows else 0.0,
        }

    weighted_rows = sorted(
        filtered_rows,
        key=lambda row: (
            -(row.get("search_clicks") or 0.0),
            -(row.get("search_impressions") or 0.0),
            row.get("url") or "",
        ),
    )
    issue_examples = [
        {
            "url": row.get("url"),
            "score": row.get("seo_score"),
            "status_code": row.get("status_code"),
            "checks": row.get("checks") or {},
            "search_clicks": float(row.get("search_clicks") or 0.0),
            "search_impressions": float(row.get("search_impressions") or 0.0),
        }
        for row in weighted_rows
        if row.get("seo_score") != "good"
    ][:20]
    top_weighted_examples = [
        {
            "url": row.get("url"),
            "search_clicks": float(row.get("search_clicks") or 0.0),
            "search_impressions": float(row.get("search_impressions") or 0.0),
            "search_ctr": float(row.get("search_ctr") or 0.0),
            "seo_score": row.get("seo_score"),
        }
        for row in weighted_rows
        if row.get("search_console_seen")
    ][:15]

    summary = {
        "base_url": base_url,
        "recent_days": recent_days,
        "requested_index_mode": requested_index_mode,
        "index_mode": index_mode,
        "seed_sitemaps": discovery["seed_urls"],
        "robots_sitemaps": discovery["robots_sitemaps"],
        "fetched_sitemaps": discovery["fetched_sitemaps"],
        "failed_sitemaps": discovery["failed_sitemaps"],
        "sitemap_url_count": len(discovered_entries),
        "candidate_url_count": len(discovered_entries),
        "audited_url_count": len(filtered_rows),
        "filtered_old_count": discovery["filtered_old_count"],
        "score_distribution": score_distribution,
        "check_summary": check_summary,
        "issue_examples": issue_examples,
        "top_weighted_examples": top_weighted_examples,
        "fallback_used": False,
        "search_console_weighted_urls": sum(1 for item in filtered_rows if item.get("search_console_seen")),
        "exact_inspection_count": exact_inspection_count,
        "exact_inspection_skipped": exact_inspection_skipped,
    }

    metrics = {
        "site_audit_total_urls": float(len(filtered_rows)),
        "site_audit_good_urls": float(score_distribution["good"]),
        "site_audit_needs_improvement_urls": float(score_distribution["needs_improvement"]),
        "site_audit_poor_urls": float(score_distribution["poor"]),
        "site_audit_title_ok_ratio": check_summary["title"]["share"],
        "site_audit_desc_ok_ratio": check_summary["desc"]["share"],
        "site_audit_h1_ok_ratio": check_summary["h1"]["share"],
        "site_audit_canonical_ok_ratio": check_summary["canonical"]["share"],
        "site_audit_weighted_urls": float(summary["search_console_weighted_urls"]),
        "site_audit_candidate_urls": float(summary["candidate_url_count"]),
    }
    save_metrics(db, site.id, metrics, collected_at)

    finish_collector_run(
        db,
        run,
        status="success",
        finished_at=collected_at,
        summary=summary,
        row_count=len(filtered_rows),
    )
    db.commit()
    return {"site_id": site.id, "collector_run_id": run.id, "summary": summary, "rows": filtered_rows}
