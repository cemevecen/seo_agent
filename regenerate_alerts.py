#!/usr/bin/env python3
"""Regenerate alerts with new message format."""

from backend.database import SessionLocal
from backend.models import Site, AlertLog
from backend.services.alert_engine import evaluate_site_alerts, _detect_top50_drops
from datetime import datetime

db = SessionLocal()

# Delete existing alerts
deleted_count = db.query(AlertLog).delete()
db.commit()
print(f"Deleted {deleted_count} old alert logs\n")

# Regenerate alerts
sites = db.query(Site).all()
print(f"Found {len(sites)} sites\n")

for site in sites:
    print(f"Processing {site.domain}...")
    
    # Regular alerts
    logs = evaluate_site_alerts(db, site)
    print(f"  Created {len(logs)} regular alerts")
    
    # Top50 drops
    top50_logs = _detect_top50_drops(db, site, datetime.utcnow())
    print(f"  Created {len(top50_logs)} top50 drop alerts")

# Show examples
print("\n\n=== New message format examples ===\n")
logs = db.query(AlertLog).limit(15).all()
for i, log in enumerate(logs, 1):
    print(f"{i}. Domain: {log.domain}")
    print(f"   Message: {log.message}\n")

db.close()
