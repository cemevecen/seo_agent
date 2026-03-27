"""Teknik SEO kontrollerini yapan crawler collector'ı."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from sqlalchemy.orm import Session

from backend.models import Site
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.metric_store import save_metrics


def _normalize_url(domain: str) -> str:
    # Çıplak domain değerini HTTPS URL'ye çevirir.
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain.rstrip("/")
    return f"https://{domain}"


def _fetch_text(url: str) -> tuple[int, str]:
    # Hedef kaynağı getirir; erişilemezse durum kodunu sıfır döndürür.
    try:
        with urlopen(url, timeout=20) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.status, response.read().decode(charset, errors="ignore")
    except HTTPError as exc:
        charset = exc.headers.get_content_charset() or "utf-8"
        return exc.code, exc.read().decode(charset, errors="ignore")
    except URLError:
        return 0, ""


def _has_json_ld(html: str) -> bool:
    # JSON-LD script etiketini regex ile tespit eder.
    return bool(re.search(r'<script[^>]+type=["\']application/ld\+json["\']', html, re.IGNORECASE))


def _has_canonical(html: str) -> bool:
    # Canonical link etiketinin varlığını tespit eder.
    return bool(re.search(r'<link[^>]+rel=["\']canonical["\']', html, re.IGNORECASE))


def collect_crawler_metrics(db: Session, site: Site) -> dict:
    """robots, sitemap, schema ve canonical kontrollerini yapıp kaydeder."""
    base_url = _normalize_url(site.domain)
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
    save_metrics(db, site.id, metrics, datetime.utcnow())
    evaluate_site_alerts(db, site)
    return {
        "site_id": site.id,
        "robots_status": robots_status,
        "sitemap_status": sitemap_status,
        "homepage_status": homepage_status,
        "metrics": metrics,
    }