#!/usr/bin/env python3
import sys
import os

# Add the seo-agent to path
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')
os.chdir('/Users/cemevecen/Desktop/seo_agent/seo-agent')

from sqlalchemy import create_engine, text
from backend.config import settings

# Create engine
engine = create_engine(settings.database_url)

# Execute query
with engine.connect() as conn:
    result = conn.execute(text("""
    SELECT 
        s.domain,
        m.metric_type,
        m.value,
        m.collected_at
    FROM sites s
    JOIN metrics m ON s.id = m.site_id
    WHERE s.domain = 'doviz.com' AND m.metric_type LIKE '%inp%'
    ORDER BY m.collected_at DESC
    LIMIT 20
    """))
    
    rows = result.fetchall()
    if rows:
        print("INP Metrics in Database:")
        print("=" * 100)
        for row in rows:
            print(f"Domain: {row[0]}, Type: {row[1]}, Value: {row[2]}, Collected: {row[3]}")
    else:
        print("No INP metrics found in database for doviz.com")
        
    # Also check for LCP to compare
    print("\n\nLCP Metrics in Database (for comparison):")
    print("=" * 100)
    result2 = conn.execute(text("""
    SELECT 
        m.metric_type,
        m.value,
        m.collected_at
    FROM sites s
    JOIN metrics m ON s.id = m.site_id
    WHERE s.domain = 'doviz.com' AND m.metric_type LIKE '%lcp%'
    ORDER BY m.collected_at DESC
    LIMIT 20
    """))
    
    rows2 = result2.fetchall()
    for row in rows2:
        print(f"Type: {row[0]}, Value: {row[1]}, Collected: {row[2]}")
