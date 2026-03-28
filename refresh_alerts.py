#!/usr/bin/env python3
from backend.database import SessionLocal
from backend.models import AlertLog, Alert, Site
from backend.services.alert_engine import evaluate_site_alerts
from datetime import datetime, timedelta

db = SessionLocal()

# Get all Search Console alerts
search_console_alerts = (
    db.query(Alert)
    .filter(Alert.alert_type.in_(["search_console_dropped_queries", "search_console_biggest_drop"]))
    .all()
)

print(f"Found {len(search_console_alerts)} Search Console alert rules")

# Delete all old logs to start fresh
total_deleted = 0
for alert in search_console_alerts:
    old_logs = (
        db.query(AlertLog)
        .filter(AlertLog.alert_id == alert.id)
        .delete()
    )
    total_deleted += old_logs
    print(f"  Alert {alert.id}: deleted {old_logs} logs")

db.commit()
print(f"\n✓ Total deleted: {total_deleted} old alert logs\n")

# Regenerate fresh alerts with current data
sites = db.query(Site).all()
for site in sites:
    print(f"Regenerating alerts for {site.domain}...")
    new_logs = evaluate_site_alerts(db, site)
    print(f"  ✓ Generated {len(new_logs)} fresh alerts\n")

print("Done! UI now shows current query-specific data.")
