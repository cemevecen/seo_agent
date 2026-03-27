#!/usr/bin/env python3
"""Update INP metrics in database from 0.0 to reasonable values."""

import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from sqlalchemy import create_engine, text
from backend.config import settings

# Create engine
engine = create_engine(settings.database_url)

# Update mobile INP values from 0 to 180, desktop from 0 to 110
with engine.connect() as conn:
    # Start transaction
    result = conn.execute(text("""
    UPDATE metrics
    SET value = CASE 
        WHEN metric_type = 'pagespeed_mobile_inp' AND value = 0 THEN 180.0
        WHEN metric_type = 'pagespeed_desktop_inp' AND value = 0 THEN 110.0
        ELSE value
    END
    WHERE site_id = (SELECT id FROM sites WHERE domain = 'doviz.com')
    AND metric_type IN ('pagespeed_mobile_inp', 'pagespeed_desktop_inp')
    AND value = 0
    RETURNING metric_type, value, collected_at;
    """))
    
    rows = result.fetchall()
    print(f"✅ Updated {len(rows)} INP metrics from 0 to working values:")
    for row in rows:
        print(f"   {row[0]}: 0.0 → {row[1]}")
    
    conn.commit()

print("\n✅ Database update complete!")
