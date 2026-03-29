"""Teknik SEO kontrollerini yapan crawler collector'ı."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime

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

    metrics = {
        "crawler_robots_accessible": 1.0 if robots_accessible else 0.0,
        "crawler_robots_rules_ok": 1.0 if robots_rules_ok else 0.0,
        "crawler_sitemap_exists": 1.0 if sitemap_exists else 0.0,
        "crawler_sitemap_valid": 1.0 if sitemap_valid else 0.0,
        "crawler_schema_found": 1.0 if schema_found else 0.0,
        "crawler_canonical_found": 1.0 if canonical_found else 0.0,
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
        },
        row_count=3,
    )
    db.commit()
    return {
        "site_id": site.id,
        "robots_status": robots_status,
        "sitemap_status": sitemap_status,
        "homepage_status": homepage_status,
        "metrics": metrics,
    }
