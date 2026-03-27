#!/usr/bin/env python3
"""Check what metrics are in the database."""

import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.models import Site, Metric, Base
from backend.database import SessionLocal
from sqlalchemy import create_engine

# Get database session
db = SessionLocal()

try:
    # Get doviz.com site
    site = db.query(Site).filter(Site.domain == "doviz.com").first()
    
    if not site:
        print("Site 'doviz.com' not found in database")
        sys.exit(1)
    
    print(f"Site: {site.domain} (ID: {site.id})")
    print("=" * 80)
    
    # Get all metrics for this site
    metrics = db.query(Metric).filter(Metric.site_id == site.id).order_by(Metric.collected_at.desc()).all()
    
    print(f"Total metrics: {len(metrics)}")
    print("\nLatest metrics (grouped by type):")
    print("-" * 80)
    
    seen_types = set()
    for metric in metrics:
        if metric.metric_type not in seen_types:
            print(f"{metric.metric_type:40} = {metric.value:12.2f}  ({metric.collected_at})")
            seen_types.add(metric.metric_type)
    
    print("\n" + "=" * 80)
    print("INP-related metrics:")
    print("-" * 80)
    
    inp_metrics = [m for m in metrics if 'inp' in m.metric_type.lower()]
    if inp_metrics:
        for metric in inp_metrics[:10]:  # Show last 10
            print(f"  {metric.metric_type:40} = {metric.value:12.2f}  ({metric.collected_at})")
    else:
        print("  No INP metrics found in database")
    
    print("\nOther WebVitals metrics:")
    print("-" * 80)
    
    for keyword in ['lcp', 'cls', 'fcp']:
        metrics_of_type = [m for m in metrics if keyword in m.metric_type.lower()]
        if metrics_of_type:
            print(f"\n  {keyword.upper()}:")
            for metric in metrics_of_type[:5]:
                print(f"    {metric.metric_type:40} = {metric.value:12.2f}")
        else:
            print(f"\n  {keyword.upper()}: No metrics found")
            
finally:
    db.close()
