#!/usr/bin/env python3
"""Test that Search Console alerts include query names"""
import sys
sys.path.insert(0, '.')

from backend.database import SessionLocal
from backend.models import Site
from backend.services.alert_engine import evaluate_site_alerts

db = SessionLocal()

# Get a site
site = db.query(Site).first()
if not site:
    print("❌ No sites found")
    sys.exit(1)

print(f"Testing site: {site.domain}\n")

# Evaluate alerts and create new ones
new_alerts = evaluate_site_alerts(db, site)

print(f"✓ Created/refreshed {len(new_alerts)} alerts\n")

if new_alerts:
    for alert in new_alerts:
        print(f"Alert Message:\n  {alert.message}\n")
        
        # Check if Search Console alerts have query details
        if "search_console" in alert.message.lower():
            if "'" in alert.message:
                print("  ✓ Includes query names")
            else:
                print("  ⚠️  No query names found")
else:
    print("No new alerts created (might be duplicate within 12 hours)")
