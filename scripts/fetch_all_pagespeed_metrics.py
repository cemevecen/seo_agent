#!/usr/bin/env python3
"""
Complete script to fetch all PageSpeed metrics including INP data.
Fetches from Google's PageSpeed API and logs all available metrics.
"""

import json
import sys
import os
from datetime import datetime
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import logging

sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')
os.chdir('/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.config import settings
from backend.database import SessionLocal
from backend.models import Site, Metric
from sqlalchemy import desc

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

def fetch_and_analyze_pagespeed(domain: str, strategy: str) -> dict:
    """
    Fetch PageSpeed data and extract all metrics.
    Returns dict with all available metrics.
    """
    api_key = settings.google_api_key.strip()
    url = f"https://{domain}" if not domain.startswith('http') else domain
    
    query = urlencode({
        "url": url,
        "strategy": strategy,
        "key": api_key,
        "category": "performance"
    })
    
    logger.info(f"🔄 Fetching {strategy.upper()} metrics for {domain}...")
    
    try:
        with urlopen(f"{PAGESPEED_ENDPOINT}?{query}", timeout=75) as response:
            payload = json.loads(response.read().decode("utf-8"))
        
        # Extract metrics
        lighthouse = payload.get("lighthouseResult", {})
        categories = lighthouse.get("categories", {})
        audits = lighthouse.get("audits", {})
        
        # Performance score
        perf_score = (categories.get("performance", {}).get("score") or 0) * 100
        
        # Web Vitals
        lcp = (audits.get("largest-contentful-paint", {}).get("numericValue") or 0)
        cls = (audits.get("cumulative-layout-shift", {}).get("numericValue") or 0)
        
        # INP - check all possible names
        inp = 0
        for audit_name in ["interaction-to-next-paint", "experimental-interaction-to-next-paint",
                           "experimental-interaction-to-next-paint-v2", "first-input-delay"]:
            if audit_name in audits:
                numeric_value = (audits.get(audit_name) or {}).get("numericValue")
                if numeric_value is not None and numeric_value > 0:
                    inp = numeric_value
                    logger.info(f"   ✓ Found INP from {audit_name}: {inp} ms")
                    break
        
        if inp == 0:
            logger.warning(f"   ⚠️  INP = 0 (check if audit names changed)")
        
        logger.info(f"   ✓ Performance: {perf_score:.0f}")
        logger.info(f"   ✓ LCP: {lcp:.1f} ms")
        logger.info(f"   ✓ CLS: {cls:.3f}")
        logger.info(f"   ✓ INP: {inp:.1f} ms")
        
        return {
            "performance_score": float(perf_score),
            "lcp": float(lcp),
            "cls": float(cls),
            "inp": float(inp),
            "success": True
        }
        
    except Exception as e:
        logger.error(f"   ✗ Error: {e}")
        return {"success": False, "error": str(e)}

def main():
    db = SessionLocal()
    
    try:
        # Get doviz.com site
        site = db.query(Site).filter(Site.domain == "doviz.com").first()
        if not site:
            logger.error("❌ Site 'doviz.com' not found")
            return 1
        
        logger.info("=" * 80)
        logger.info("PAGESPEED METRICS FETCHER - WITH INP DATA")
        logger.info("=" * 80)
        logger.info(f"Domain: {site.domain}\n")
        
        collected_at = datetime.utcnow()
        all_metrics = {}
        
        # Fetch mobile metrics
        logger.info("📱 MOBILE:")
        mobile_result = fetch_and_analyze_pagespeed(site.domain, "mobile")
        if mobile_result["success"]:
            all_metrics[f"pagespeed_mobile_score"] = mobile_result["performance_score"]
            all_metrics[f"pagespeed_mobile_lcp"] = mobile_result["lcp"]
            all_metrics[f"pagespeed_mobile_cls"] = mobile_result["cls"]
            all_metrics[f"pagespeed_mobile_inp"] = mobile_result["inp"]
        
        # Fetch desktop metrics
        logger.info("\n🖥️  DESKTOP:")
        desktop_result = fetch_and_analyze_pagespeed(site.domain, "desktop")
        if desktop_result["success"]:
            all_metrics[f"pagespeed_desktop_score"] = desktop_result["performance_score"]
            all_metrics[f"pagespeed_desktop_lcp"] = desktop_result["lcp"]
            all_metrics[f"pagespeed_desktop_cls"] = desktop_result["cls"]
            all_metrics[f"pagespeed_desktop_inp"] = desktop_result["inp"]
        
        # Save to database
        logger.info("\n" + "=" * 80)
        logger.info("💾 SAVING METRICS TO DATABASE:")
        logger.info("=" * 80)
        
        if all_metrics:
            for metric_type, value in all_metrics.items():
                metric = Metric(
                    site_id=site.id,
                    metric_type=metric_type,
                    value=float(value),
                    collected_at=collected_at
                )
                db.add(metric)
            db.commit()
            logger.info(f"✅ Saved {len(all_metrics)} metrics")
        
        # Show latest from DB
        logger.info("\n" + "=" * 80)
        logger.info("📊 LATEST METRICS IN DATABASE:")
        logger.info("=" * 80)
        
        latest_metrics = db.query(Metric).filter(
            Metric.site_id == site.id
        ).order_by(desc(Metric.collected_at)).limit(8).all()
        
        current_metrics = {}
        for metric in latest_metrics:
            if metric.metric_type not in current_metrics:
                current_metrics[metric.metric_type] = metric.value
        
        for metric_type in sorted(current_metrics.keys()):
            value = current_metrics[metric_type]
            logger.info(f"  {metric_type:40} = {value:10.1f}")
        
        logger.info("\n✅ Complete! INP data is now available.")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db.close()

if __name__ == "__main__":
    sys.exit(main())
