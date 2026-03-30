"""Teknik SEO kontrollerini yapan crawler collector'ı."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse

import requests

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.metric_store import save_metrics
from backend.services.polite_fetch import fetch_text
from backend.services.warehouse import finish_collector_run, start_collector_run


def _normalize_url(domain: str) -> str:
    # Çıplak domain değerini HTTPS URL'ye çevirir.
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain.rstrip("/")
    return f"https://{domain}"


def _fetch_text(url: str) -> tuple[int, str]:
    # Hedef kaynağı getirir; erişilemezse durum kodunu sıfır döndürür.
    return fetch_text(
        url,
        timeout_seconds=settings.crawler_request_timeout_seconds,
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
    # Ana sayfadaki dahili linkleri sirali ve tekil bicimde ornekler.
    hrefs = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html or "", re.IGNORECASE)
    base_host = _host_key(urlparse(base_url).netloc)
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
        if _host_key(parsed.netloc) != base_host:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
        if len(links) >= max_links:
            break
    return links


def _probe_internal_link(url: str) -> dict:
    # Linkin son durumunu, redirect gecmisiyle birlikte hafifce olcer.
    timeout = max(1, int(settings.crawler_request_timeout_seconds))
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
    except requests.RequestException:
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


def _audit_internal_links(base_url: str, homepage_html: str) -> dict:
    sampled_links = _extract_internal_links(homepage_html, base_url)
    results = [_probe_internal_link(link) for link in sampled_links]
    redirect_count = sum(1 for item in results if item["redirect"])
    redirect_chain_count = sum(1 for item in results if item["redirect_chain"])
    broken_count = sum(1 for item in results if item["broken"])
    max_hops = max((int(item["hops"]) for item in results), default=0)

    redirect_samples = [
        {
            "url": item["url"],
            "final_url": item["final_url"],
            "final_status": item["final_status"],
            "hops": item["hops"],
            "chain": " -> ".join(str(step["status"]) for step in [*item["history"], {"status": item["final_status"]}]),
        }
        for item in results
        if item["redirect"]
    ][:5]
    broken_samples = [
        {
            "url": item["url"],
            "final_status": item["final_status"],
        }
        for item in results
        if item["broken"]
    ][:5]

    return {
        "sampled_links": len(sampled_links),
        "redirect_links": redirect_count,
        "redirect_chains": redirect_chain_count,
        "broken_links": broken_count,
        "max_hops": max_hops,
        "redirect_samples": redirect_samples,
        "broken_samples": broken_samples,
    }


def collect_crawler_metrics(db: Session, site: Site) -> dict:
    """robots, sitemap, schema ve canonical kontrollerini yapıp kaydeder."""
    collected_at = datetime.utcnow()
    base_url = _normalize_url(site.domain)
    run = start_collector_run(
        db,
        site_id=site.id,
        provider="crawler",
        strategy="homepage",
        target_url=base_url,
        requested_at=collected_at,
    )
    robots_status, robots_body = _fetch_text(f"{base_url}/robots.txt")
    sitemap_status, sitemap_body = _fetch_text(f"{base_url}/sitemap.xml")
    homepage_status, homepage_body = _fetch_text(base_url)

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
    link_audit = _audit_internal_links(base_url, homepage_body) if homepage_status == 200 and homepage_body else {
        "sampled_links": 0,
        "redirect_links": 0,
        "redirect_chains": 0,
        "broken_links": 0,
        "max_hops": 0,
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
        "crawler_sampled_links_count": float(link_audit["sampled_links"]),
        "crawler_redirect_links_count": float(link_audit["redirect_links"]),
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
        row_count=3 + int(link_audit["sampled_links"]),
    )
    db.commit()
    return {
        "site_id": site.id,
        "robots_status": robots_status,
        "sitemap_status": sitemap_status,
        "homepage_status": homepage_status,
        "summary": {"link_audit": link_audit},
        "metrics": metrics,
    }
