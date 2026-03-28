#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.database import SessionLocal
from backend.models import Site, Alert, AlertLog

with SessionLocal() as db:
    print("=" * 80)
    print("SITES IN DATABASE")
    print("=" * 80)
    sites = db.query(Site).all()
    for s in sites:
        print(f"  ID: {s.id}, Domain: {s.domain}, Display: {s.display_name}, Active: {s.is_active}")
    
    print("\n" + "=" * 80)
    print("ALERT RULES (Linked to Sites)")
    print("=" * 80)
    alerts = db.query(Alert).all()
    for a in alerts:
        site = db.query(Site).filter(Site.id == a.site_id).first()
        site_domain = site.domain if site else "UNKNOWN"
        print(f"  Alert ID: {a.id:2d}, Type: {a.alert_type:30s}, Site ID: {a.site_id} ({site_domain}), Active: {a.is_active}")
    
    print("\n" + "=" * 80)
    print("ALERT LOGS (Last 15 - Shown on Page)")
    print("=" * 80)
    logs = db.query(AlertLog).order_by(AlertLog.triggered_at.desc()).limit(15).all()
    for log in logs:
        alert = db.query(Alert).filter(Alert.id == log.alert_id).first()
        site = db.query(Site).filter(Site.id == alert.site_id).first() if alert else None
        site_domain = site.domain if site else "?"
        alert_type = alert.alert_type if alert else "?"
        msg_preview = log.message[:70] if len(log.message) > 70 else log.message
        print(f"  Log ID: {log.id:3d}, Alert: {log.alert_id:2d} ({alert_type:30s})")
        print(f"           Site: {site_domain:20s}, Time: {log.triggered_at}")
        print(f"           Message: {msg_preview}...")
        print()
