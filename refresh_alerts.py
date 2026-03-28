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
print(f"Found {len(sites)} active sites to process\n")

for site in sites:
    print(f"Processing: {site.domain} (Site ID: {site.id})...")
    
    # Check if site has Search Console alerts
    site_alerts = db.query(Alert).filter(
        Alert.site_id == site.id,
        Alert.alert_type.in_(["search_console_dropped_queries", "search_console_biggest_drop"])
    ).all()
    print(f"  - Has {len(site_alerts)} Search Console alerts")
    
    # Try to get metrics for this site
    from backend.services.metric_store import get_latest_metrics
    metrics = get_latest_metrics(db, site.id)
    print(f"  - Has {len(metrics)} latest metrics")
    for m in metrics:
        print(f"    - {m.metric_type}: {m.value}")
    
    try:
        new_logs = evaluate_site_alerts(db, site)
        print(f"  ✓ Generated {len(new_logs)} fresh alerts")
    except Exception as e:
        print(f"  ✗ ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
    print()

print("Done! UI now shows current query-specific data.")
