"""Teknik SEO kontrollerini yapan crawler collector'ı."""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

from backend.config import settings
from backend.models import Site
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.metric_store import save_metrics
from backend.services.polite_fetch import fetch_text
from backend.services.search_console_auth import SEARCH_CONSOLE_SCOPES, get_search_console_credentials_record, load_google_credentials
from backend.services.warehouse import finish_collector_run, start_collector_run


def _normalize_url(domain: str) -> str:
    # Çıplak domain değerini HTTPS URL'ye çevirir.
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain.rstrip("/")
    return f"https://{domain}"


def _fetch_text(url: str, *, timeout_seconds: int | None = None) -> tuple[int, str]:
    # Hedef kaynağı getirir; erişilemezse durum kodunu sıfır döndürür.
    effective_timeout = max(1, int(timeout_seconds or settings.crawler_request_timeout_seconds))
    return fetch_text(
        url,
        timeout_seconds=effective_timeout,
        cache_ttl_seconds=settings.outbound_cache_ttl_seconds,
        min_interval_seconds=settings.outbound_min_interval_seconds,
    )


def _has_json_ld(html: str) -> bool:
    # JSON-LD script etiketini regex ile tespit eder.
    return bool(re.search(r'<script[^>]+type=["\']application/ld\+json["\']', html, re.IGNORECASE))


def _has_canonical(html: str) -> bool:
    # Canonical link etiketinin varlığını tespit eder.
    return bool(re.search(r'<link[^>]+rel=["\']canonical["\']', html, re.IGNORECASE))


def _host_key(host: str) -> str:
    normalized = (host or "").strip().lower()
    return normalized[4:] if normalized.startswith("www.") else normalized


def _site_family_key(host: str) -> str:
    normalized = _host_key(host)
    return normalized[2:] if normalized.startswith("m.") else normalized


def _normalized_http_url(url: str) -> str:
    parsed = urlparse(url)
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


def _extract_internal_links(html: str, base_url: str, *, max_links: int = 12) -> list[str]:
    # Sayfa icindeki dahili linkleri sirali ve tekil bicimde ornekler.
    hrefs = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html or "", re.IGNORECASE)
    base_host = _site_family_key(urlparse(base_url).netloc)
    links: list[str] = []
    seen: set[str] = set()
    ignored_prefixes = ("#", "mailto:", "tel:", "javascript:", "data:")

    for href in hrefs:
        candidate = (href or "").strip()
        if not candidate or candidate.lower().startswith(ignored_prefixes):
            continue
        absolute = _normalized_http_url(urljoin(base_url + "/", candidate))
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if _site_family_key(parsed.netloc) != base_host:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
        if len(links) >= max_links:
            break
    return links


def _probe_internal_link(url: str, *, timeout_seconds: int | None = None) -> dict:
    # Linkin son durumunu, redirect gecmisiyle birlikte hafifce olcer.
    timeout = max(1, int(timeout_seconds or settings.crawler_request_timeout_seconds))
    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": settings.outbound_user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            allow_redirects=True,
            timeout=timeout,
            stream=True,
        )
        try:
            history = [
                {
                    "status": int(item.status_code),
                    "url": _normalized_http_url(item.url),
                }
                for item in response.history
            ]
            final_url = _normalized_http_url(response.url or url)
            final_status = int(response.status_code or 0)
        finally:
            response.close()
    except requests.RequestException as exc:
        LOGGER.debug("Link probe başarısız (%s): %s", url, exc)
        history = []
        final_url = _normalized_http_url(url)
        final_status = 0

    hops = len(history)
    return {
        "url": _normalized_http_url(url),
        "final_url": final_url,
        "final_status": final_status,
        "history": history,
        "hops": hops,
        "redirect": hops >= 1,
        "redirect_chain": hops >= 2,
        "broken": final_status == 0 or final_status >= 400,
    }


def _same_site_family(url: str, *, base_url: str) -> bool:
    parsed = urlparse(url)
    return _site_family_key(parsed.netloc) == _site_family_key(urlparse(base_url).netloc)


def _build_search_console_service(credential):
    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        return None

    credential_data = load_google_credentials(credential)
    if credential.credential_type == "search_console_oauth":
        credentials = credential_data
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(GoogleAuthRequest())
    else:
        credentials = service_account.Credentials.from_service_account_info(
            credential_data,
            scopes=SEARCH_CONSOLE_SCOPES,
        )
    return build("searchconsole", "v1", credentials=credentials, cache_discovery=False)


def _fetch_top_search_console_pages(db: Session, site: Site, *, limit: int) -> list[str]:
    credential = get_search_console_credentials_record(db, site.id)
    if credential is None:
        return []

    try:
        from datetime import timedelta

        from backend.collectors.search_console import _resolve_latest_available_day, _resolve_search_console_targets
        from backend.services.timezone_utils import report_calendar_yesterday
    except Exception:  # noqa: BLE001
        return []

    try:
        service = _build_search_console_service(credential)
        if service is None:
            return []
        targets = _resolve_search_console_targets(service, site)
        latest_supported_end_date = _resolve_latest_available_day(
            service,
            targets,
            fallback_end_date=report_calendar_yesterday(),
        )
        start_date = latest_supported_end_date - timedelta(days=27)
        aggregated: dict[str, float] = {}
        page_size = max(100, min(int(limit), 250))

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
                        {
                            "filters": [
                                {
                                    "dimension": "device",
                                    "expression": device,
                                }
                            ]
                        }
                    ]
                response = service.searchanalytics().query(siteUrl=property_url, body=body).execute()
                rows = response.get("rows", []) or []
                for row in rows:
                    keys = row.get("keys") or []
                    page_url = _normalized_http_url(str(keys[0] if keys else ""))
                    if not page_url or not _same_site_family(page_url, base_url=_normalize_url(site.domain)):
                        continue
                    aggregated[page_url] = aggregated.get(page_url, 0.0) + float(row.get("clicks") or 0.0)
                if len(rows) < page_size:
                    break
                start_row += page_size

        ranked = sorted(aggregated.items(), key=lambda item: item[1], reverse=True)
        return [url for url, _clicks in ranked[:limit]]
    except Exception:
        return []
def _fetch_sitemap_urls(base_url: str, *, max_urls: int, timeout_seconds: int | None = None) -> list[str]:
    sitemap_queue = [f"{base_url}/sitemap.xml"]
    visited_sitemaps: set[str] = set()
    discovered_urls: list[str] = []
    seen_urls: set[str] = set()

    while sitemap_queue and len(discovered_urls) < max_urls:
        sitemap_url = sitemap_queue.pop(0)
        if sitemap_url in visited_sitemaps:
            continue
        visited_sitemaps.add(sitemap_url)
        status, body = _fetch_text(sitemap_url, timeout_seconds=timeout_seconds)
        if status != 200 or not body.strip():
            continue
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            continue

        root_name = str(root.tag).split("}")[-1].lower()
        loc_values = [
            str(node.text).strip()
            for node in root.iter()
            if str(node.tag).split("}")[-1].lower() == "loc" and node.text
        ]
        if root_name == "sitemapindex":
            for child_url in loc_values:
                normalized = _normalized_http_url(child_url)
                if normalized not in visited_sitemaps:
                    sitemap_queue.append(normalized)
            continue

        for loc in loc_values:
            normalized = _normalized_http_url(loc)
            if not normalized or normalized in seen_urls:
                continue
            if not _same_site_family(normalized, base_url=base_url):
                continue
            seen_urls.add(normalized)
            discovered_urls.append(normalized)
            if len(discovered_urls) >= max_urls:
                break

    return discovered_urls


def _seed_source_pages(
    db: Session,
    site: Site,
    base_url: str,
    *,
    source_page_limit: int | None = None,
    sitemap_url_limit: int | None = None,
    timeout_seconds: int | None = None,
) -> tuple[list[str], str]:
    seed_limit = max(2, int(source_page_limit or settings.crawler_source_page_limit))
    ordered: list[str] = []
    seen: set[str] = set()

    def add_many(urls: list[str]) -> None:
        for item in urls:
            normalized = _normalized_http_url(item)
            if not normalized or normalized in seen:
                continue
            if not _same_site_family(normalized, base_url=base_url):
                continue
            seen.add(normalized)
            ordered.append(normalized)
            if len(ordered) >= seed_limit:
                break

    top_pages = _fetch_top_search_console_pages(db, site, limit=seed_limit)
    add_many(top_pages)
    source_strategy = "Search Console öncelikli URL listesi" if ordered else "Sitemap URL listesi"

    if len(ordered) < seed_limit:
        sitemap_urls = _fetch_sitemap_urls(
            base_url,
            max_urls=max(seed_limit * 3, int(sitemap_url_limit or settings.crawler_sitemap_url_limit)),
            timeout_seconds=timeout_seconds,
        )
        add_many(sitemap_urls)

    if len(ordered) < seed_limit:
        status, homepage_html = _fetch_text(base_url, timeout_seconds=timeout_seconds)
        if status == 200 and homepage_html:
            add_many(_extract_internal_links(homepage_html, base_url, max_links=seed_limit * 2))

    if base_url not in seen:
        ordered.insert(0, base_url)
    return ordered[:seed_limit], source_strategy


def _link_issue_label(item: dict) -> str:
    labels: list[str] = []
    if item.get("broken"):
        labels.append("Kırık")
    if item.get("redirect_chain"):
        labels.append("Redirect zinciri")
    if item.get("has_302"):
        labels.append("302")
    if item.get("has_301"):
        labels.append("301")
    return ", ".join(labels) if labels else "Normal"


def _audit_internal_links(
    db: Session,
    site: Site,
    base_url: str,
    homepage_html: str,
    *,
    source_page_limit: int | None = None,
    target_url_limit: int | None = None,
    links_per_page_limit: int | None = None,
    issue_sample_limit: int | None = None,
    sitemap_url_limit: int | None = None,
    request_timeout_seconds: int | None = None,
) -> dict:
    source_pages, source_strategy = _seed_source_pages(
        db,
        site,
        base_url,
        source_page_limit=source_page_limit,
        sitemap_url_limit=sitemap_url_limit,
        timeout_seconds=request_timeout_seconds,
    )
    source_page_limit = max(2, int(source_page_limit or settings.crawler_source_page_limit))
    target_limit = max(4, int(target_url_limit or settings.crawler_target_url_limit))
    per_page_limit = max(2, int(links_per_page_limit or settings.crawler_links_per_page_limit))
    issue_sample_limit = max(1, int(issue_sample_limit or settings.crawler_issue_sample_limit))

    target_sources: dict[str, set[str]] = defaultdict(set)
    ordered_targets: list[str] = []
    seen_targets: set[str] = set()

    def register_target(target_url: str, source_url: str) -> None:
        normalized_target = _normalized_http_url(target_url)
        normalized_source = _normalized_http_url(source_url)
        if not normalized_target or normalized_target in seen_targets and normalized_source in target_sources.get(normalized_target, set()):
            return
        if not _same_site_family(normalized_target, base_url=base_url):
            return
        if normalized_target not in seen_targets:
            seen_targets.add(normalized_target)
            ordered_targets.append(normalized_target)
        target_sources[normalized_target].add(normalized_source)

    for source_url in source_pages[:source_page_limit]:
        register_target(source_url, source_url)
        status, source_html = (
            (200, homepage_html)
            if source_url == base_url and homepage_html
            else _fetch_text(source_url, timeout_seconds=request_timeout_seconds)
        )
        if status != 200 or not source_html:
            if len(ordered_targets) >= target_limit:
                break
            continue
        for target_url in _extract_internal_links(source_html, source_url, max_links=per_page_limit):
            register_target(target_url, source_url)
            if len(ordered_targets) >= target_limit:
                break
        if len(ordered_targets) >= target_limit:
            break

    results = []
    for target_url in ordered_targets[:target_limit]:
        probe = _probe_internal_link(target_url, timeout_seconds=request_timeout_seconds)
        history = list(probe.get("history") or [])
        status_chain = [int(step.get("status") or 0) for step in history]
        final_status = int(probe.get("final_status") or 0)
        if final_status:
            status_chain.append(final_status)
        source_urls = sorted(target_sources.get(target_url) or {target_url})
        has_301 = any(int(step.get("status") or 0) == 301 for step in history)
        has_302 = any(int(step.get("status") or 0) == 302 for step in history)
        results.append(
            {
                **probe,
                "source_urls": source_urls,
                "source_count": len(source_urls),
                "status_chain": status_chain,
                "chain_label": " -> ".join(str(status) for status in status_chain if status) or ("erişilemedi" if final_status == 0 else str(final_status)),
                "has_301": has_301,
                "has_302": has_302,
                "issue_label": _link_issue_label(
                    {
                        "broken": probe.get("broken"),
                        "redirect_chain": probe.get("redirect_chain"),
                        "has_301": has_301,
                        "has_302": has_302,
                    }
                ),
            }
        )

    redirect_count = sum(1 for item in results if item["redirect"])
    redirect_chain_count = sum(1 for item in results if item["redirect_chain"])
    redirect_301_count = sum(1 for item in results if item["has_301"])
    redirect_302_count = sum(1 for item in results if item["has_302"])
    broken_count = sum(1 for item in results if item["broken"])
    max_hops = max((int(item["hops"]) for item in results), default=0)

    redirect_samples = [
        {
            "url": item["url"],
            "final_url": item["final_url"],
            "final_status": item["final_status"],
            "hops": item["hops"],
            "chain": item["chain_label"],
            "has_301": item["has_301"],
            "has_302": item["has_302"],
            "source_urls": item["source_urls"][:3],
            "source_count": item["source_count"],
            "issue_label": item["issue_label"],
        }
        for item in results
        if item["redirect"] or item["redirect_chain"]
    ][:issue_sample_limit]
    broken_samples = [
        {
            "url": item["url"],
            "final_url": item["final_url"],
            "final_status": item["final_status"],
            "source_urls": item["source_urls"][:3],
            "source_count": item["source_count"],
        }
        for item in results
        if item["broken"]
    ][:issue_sample_limit]

    return {
        "source_pages": len(source_pages),
        "audited_urls": len(results),
        "redirect_links": redirect_count,
        "redirect_301_links": redirect_301_count,
        "redirect_302_links": redirect_302_count,
        "redirect_chains": redirect_chain_count,
        "broken_links": broken_count,
        "max_hops": max_hops,
        "source_strategy": source_strategy,
        "source_pages_sample": source_pages[: min(5, len(source_pages))],
        "redirect_samples": redirect_samples,
        "broken_samples": broken_samples,
    }


def collect_crawler_metrics(
    db: Session,
    site: Site,
    *,
    source_page_limit: int | None = None,
    target_url_limit: int | None = None,
    links_per_page_limit: int | None = None,
    issue_sample_limit: int | None = None,
    sitemap_url_limit: int | None = None,
    request_timeout_seconds: int | None = None,
) -> dict:
    """robots, sitemap, schema, canonical ve site ici link denetimlerini yapip kaydeder."""
    collected_at = datetime.utcnow()
    base_url = _normalize_url(site.domain)
    run = start_collector_run(
        db,
        site_id=site.id,
        provider="crawler",
        strategy="sitewide",
        target_url=base_url,
        requested_at=collected_at,
    )
    robots_status, robots_body = _fetch_text(f"{base_url}/robots.txt", timeout_seconds=request_timeout_seconds)
    sitemap_status, sitemap_body = _fetch_text(f"{base_url}/sitemap.xml", timeout_seconds=request_timeout_seconds)
    homepage_status, homepage_body = _fetch_text(base_url, timeout_seconds=request_timeout_seconds)

    robots_accessible = robots_status == 200
    robots_rules_ok = robots_accessible and "user-agent" in robots_body.lower()
    sitemap_exists = sitemap_status == 200
    try:
        ET.fromstring(sitemap_body) if sitemap_exists and sitemap_body else None
        sitemap_valid = sitemap_exists and bool(sitemap_body.strip())
    except ET.ParseError:
        sitemap_valid = False
    schema_found = homepage_status == 200 and _has_json_ld(homepage_body)
    canonical_found = homepage_status == 200 and _has_canonical(homepage_body)
    link_audit = _audit_internal_links(
        db,
        site,
        base_url,
        homepage_body,
        source_page_limit=source_page_limit,
        target_url_limit=target_url_limit,
        links_per_page_limit=links_per_page_limit,
        issue_sample_limit=issue_sample_limit,
        sitemap_url_limit=sitemap_url_limit,
        request_timeout_seconds=request_timeout_seconds,
    ) if homepage_status == 200 and homepage_body else {
        "source_pages": 0,
        "audited_urls": 0,
        "redirect_links": 0,
        "redirect_301_links": 0,
        "redirect_302_links": 0,
        "redirect_chains": 0,
        "broken_links": 0,
        "max_hops": 0,
        "source_strategy": "Kaynak sayfa listesi oluşmadı",
        "source_pages_sample": [],
        "redirect_samples": [],
        "broken_samples": [],
    }

    metrics = {
        "crawler_robots_accessible": 1.0 if robots_accessible else 0.0,
        "crawler_robots_rules_ok": 1.0 if robots_rules_ok else 0.0,
        "crawler_sitemap_exists": 1.0 if sitemap_exists else 0.0,
        "crawler_sitemap_valid": 1.0 if sitemap_valid else 0.0,
        "crawler_schema_found": 1.0 if schema_found else 0.0,
        "crawler_canonical_found": 1.0 if canonical_found else 0.0,
        "crawler_source_pages_count": float(link_audit["source_pages"]),
        "crawler_audited_urls_count": float(link_audit["audited_urls"]),
        "crawler_redirect_links_count": float(link_audit["redirect_links"]),
        "crawler_redirect_301_count": float(link_audit["redirect_301_links"]),
        "crawler_redirect_302_count": float(link_audit["redirect_302_links"]),
        "crawler_redirect_chain_count": float(link_audit["redirect_chains"]),
        "crawler_broken_links_count": float(link_audit["broken_links"]),
        "crawler_redirect_max_hops": float(link_audit["max_hops"]),
    }
    save_metrics(db, site.id, metrics, collected_at)
    evaluate_site_alerts(db, site)
    finish_collector_run(
        db,
        run,
        status="success",
        finished_at=collected_at,
        summary={
            "robots_status": robots_status,
            "sitemap_status": sitemap_status,
            "homepage_status": homepage_status,
            "cache_ttl_seconds": settings.outbound_cache_ttl_seconds,
            "link_audit": link_audit,
        },
        row_count=3 + int(link_audit["audited_urls"]),
    )
    db.commit()
    return {
        "site_id": site.id,
        "collector_run_id": run.id,
        "robots_status": robots_status,
        "sitemap_status": sitemap_status,
        "homepage_status": homepage_status,
        "summary": {"link_audit": link_audit},
        "metrics": metrics,
    }
