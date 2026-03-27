#!/usr/bin/env python3
"""
Fetch real INP data from PageSpeed API and update database.
This is a utility to refresh metrics with actual API data.
"""

import json
import sys
import os
from datetime import datetime
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')
os.chdir('/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.config import settings
from backend.database import SessionLocal
from backend.models import Site, Metric
from sqlalchemy import desc

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

def fetch_pagespeed_data(url: str, strategy: str) -> dict:
    """Fetch raw PageSpeed API response."""
    api_key = settings.google_api_key.strip()
    query = urlencode({
        "url": url,
        "strategy": strategy,
        "key": api_key,
        "category": "performance"
    })
    
    print(f"\n📡 Fetching {strategy} data from PageSpeed API...")
    print(f"   URL: {url}")
    
    try:
        with urlopen(f"{PAGESPEED_ENDPOINT}?{query}", timeout=75) as response:
            payload = json.loads(response.read().decode("utf-8"))
        
        print(f"   ✓ API call successful")
        return payload
        
    except HTTPError as exc:
        print(f"   ✗ HTTP Error {exc.code}: {exc.reason}")
        return None
    except Exception as exc:
        print(f"   ✗ Error: {exc}")
        return None

def extract_inp_from_response(payload: dict, strategy: str) -> float:
    """Extract INP value from API response."""
    if not payload:
        return 0
    
    lighthouse = payload.get("lighthouseResult", {})
    audits = lighthouse.get("audits", {})
    
    # Try different audit names
    inbox_audit_names = [
        "interaction-to-next-paint",
        "experimental-interaction-to-next-paint",
        "experimental-interaction-to-next-paint-v2",
        "first-input-delay"
    ]
    
    print(f"\n🔍 Looking for INP audit ({strategy}):")
    print(f"   Available audit count: {len(audits)}")
    
    # Check interaction-related audits
    interaction_audits = [name for name in audits.keys() if 'interaction' in name.lower()]
    if interaction_audits:
        print(f"   Found {len(interaction_audits)} interaction audit(s): {interaction_audits}")
    
    for audit_name in inbox_audit_names:
        if audit_name in audits:
            audit_data = audits[audit_name]
            numeric_value = audit_data.get("numericValue")
            print(f"   ✓ Found '{audit_name}': {numeric_value} ms")
            return float(numeric_value) if numeric_value else 0
    
    print(f"   ✗ No INP audit found in: {inbox_audit_names}")
    print(f"   All audits: {list(audits.keys())[:10]}...")
    return 0

def save_metric(db, site_id: int, metric_type: str, value: float):
    """Save metric to database."""
    metric = Metric(
        site_id=site_id,
        metric_type=metric_type,
        value=value,
        collected_at=datetime.utcnow()
    )
    db.add(metric)
    db.commit()
    print(f"   💾 Saved: {metric_type} = {value}")

def main():
    db = SessionLocal()
    
    try:
        # Get doviz.com site
        site = db.query(Site).filter(Site.domain == "doviz.com").first()
        if not site:
            print("❌ Site 'doviz.com' not found")
            return 1
        
        print(f"🌐 Fetching data for: {site.domain}")
        
        # Collect all metrics
        all_metrics = {}
        
        for strategy in ["mobile", "desktop"]:
            print(f"\n{'='*60}")
            print(f"Processing {strategy.upper()}")
            print(f"{'='*60}")
            
            # Fetch data
            payload = fetch_pagespeed_data(f"https://{site.domain}", strategy)
            if not payload:
                print(f"❌ Failed to fetch {strategy} data")
                continue
            
            # Extract all metrics
            lighthouse = payload.get("lighthouseResult", {})
            categories = lighthouse.get("categories", {})
            audits = lighthouse.get("audits", {})
            
            # Performance score
            perf_score = (categories.get("performance", {}).get("score") or 0) * 100
            all_metrics[f"pagespeed_{strategy}_score"] = perf_score
            print(f"\n📊 Performance Score: {perf_score:.0f}")
            
            # LCP
            lcp = (audits.get("largest-contentful-paint", {}).get("numericValue") or 0)
            all_metrics[f"pagespeed_{strategy}_lcp"] = lcp
            print(f"   LCP: {lcp:.1f} ms")
            
            # CLS
            cls = (audits.get("cumulative-layout-shift", {}).get("numericValue") or 0)
            all_metrics[f"pagespeed_{strategy}_cls"] = cls
            print(f"   CLS: {cls:.3f}")
            
            # INP
            inp = extract_inp_from_response(payload, strategy)
            all_metrics[f"pagespeed_{strategy}_inp"] = inp
            print(f"   INP: {inp:.1f} ms")
        
        # Save all metrics
        print(f"\n{'='*60}")
        print(f"Saving metrics to database")
        print(f"{'='*60}")
        
        for metric_type, value in all_metrics.items():
            save_metric(db, site.id, metric_type, value)
        
        print(f"\n✅ Done! Saved {len(all_metrics)} metrics")
        
        # Show latest metrics from database
        print(f"\n{'='*60}")
        print(f"Latest metrics in database:")
        print(f"{'='*60}")
        
        latest_metrics = db.query(Metric).filter(
            Metric.site_id == site.id
        ).order_by(desc(Metric.collected_at)).limit(10).all()
        
        for metric in latest_metrics:
            print(f"  {metric.metric_type}: {metric.value} ({metric.collected_at})")
        
        return 0
        
    finally:
        db.close()

if __name__ == "__main__":
    sys.exit(main())
