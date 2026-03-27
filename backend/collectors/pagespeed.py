"""PageSpeed Insights collector'ı."""

from __future__ import annotations

import json
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import Site
from backend.services.alert_engine import evaluate_site_alerts
from backend.services.metric_store import save_metrics
from backend.services.quota_guard import consume_api_quota

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"


def _normalize_url(domain: str) -> str:
    # API çağrıları için çıplak domain değerini HTTPS URL'ye çevirir.
    if domain.startswith("http://") or domain.startswith("https://"):
        return domain
    return f"https://{domain}"


def _extract_lighthouse_metrics(payload: dict) -> dict[str, float]:
    # Lighthouse sonucundan gerekli temel performans alanlarını ayıklar.
    lighthouse = payload.get("lighthouseResult", {})
    categories = lighthouse.get("categories", {})
    audits = lighthouse.get("audits", {})
    performance_score = categories.get("performance", {}).get("score") or 0
    lcp = (audits.get("largest-contentful-paint") or {}).get("numericValue") or 0
    cls = (audits.get("cumulative-layout-shift") or {}).get("numericValue") or 0
    inp = (
        (audits.get("interaction-to-next-paint") or {}).get("numericValue")
        or (audits.get("experimental-interaction-to-next-paint") or {}).get("numericValue")
        or 0
    )
    return {
        "performance_score": float(performance_score) * 100,
        "lcp": float(lcp),
        "cls": float(cls),
        "inp": float(inp),
    }


def _fetch_pagespeed(url: str, strategy: str) -> dict[str, float]:
    # API key yoksa deterministic mock veri döndürür, varsa gerçek API çağrısı yapar.
    api_key = settings.google_api_key.strip()
    if not api_key or api_key.startswith("local-"):
        if strategy == "mobile":
            return {"performance_score": 72.0, "lcp": 2850.0, "cls": 0.08, "inp": 180.0}
        return {"performance_score": 89.0, "lcp": 1650.0, "cls": 0.03, "inp": 110.0}

    query = urlencode({"url": url, "strategy": strategy, "key": api_key, "category": "performance"})
    with urlopen(f"{PAGESPEED_ENDPOINT}?{query}", timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _extract_lighthouse_metrics(payload)


def collect_pagespeed_metrics(db: Session, site: Site) -> dict:
    """Mobile ve desktop performans verilerini toplayıp Metric tablosuna kaydeder."""
    decision = consume_api_quota(db, site, provider="pagespeed", units=2)
    if not decision.allowed:
        return {
            "site_id": site.id,
            "blocked": True,
            "reason": decision.reason,
        }

    target_url = _normalize_url(site.domain)
    mobile = _fetch_pagespeed(target_url, "mobile")
    desktop = _fetch_pagespeed(target_url, "desktop")
    collected_at = datetime.utcnow()

    metrics = {
        "pagespeed_mobile_score": mobile["performance_score"],
        "pagespeed_mobile_lcp": mobile["lcp"],
        "pagespeed_mobile_cls": mobile["cls"],
        "pagespeed_mobile_inp": mobile["inp"],
        "pagespeed_desktop_score": desktop["performance_score"],
        "pagespeed_desktop_lcp": desktop["lcp"],
        "pagespeed_desktop_cls": desktop["cls"],
        "pagespeed_desktop_inp": desktop["inp"],
    }
    save_metrics(db, site.id, metrics, collected_at)
    evaluate_site_alerts(db, site)
    return {"site_id": site.id, "url": target_url, "mobile": mobile, "desktop": desktop}