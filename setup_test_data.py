#!/usr/bin/env python3
"""Create test sites and alerts for development."""
from backend.database import SessionLocal, init_db
from backend.models import Site, Alert, AlertLog
from datetime import datetime, timedelta
import random

init_db()

with SessionLocal() as db:
    # Create test sites
    sites_data = [
        {"domain": "doviz.com", "display_name": "Döviz"},
        {"domain": "www.sinemalar.com", "display_name": "Sinemalar"},
    ]
    
    for site_data in sites_data:
        existing = db.query(Site).filter(Site.domain == site_data["domain"]).first()
        if not existing:
            site = Site(
                domain=site_data["domain"],
                display_name=site_data["display_name"],
                is_active=True,
                created_at=datetime.utcnow()
            )
            db.add(site)
            db.commit()
            print(f"✓ Created site: {site_data['domain']}")
        else:
            print(f"- Site exists: {site_data['domain']}")
    
    # Create alert rules for each site
    sites = db.query(Site).all()
    for site in sites:
        alert_types = [
            {"alert_type": "traffic_drop", "threshold": 20},
            {"alert_type": "rankings_dropped", "threshold": 50},
            {"alert_type": "organic_clicks_low", "threshold": 100},
        ]
        
        for alert_def in alert_types:
            existing = db.query(Alert).filter(
                Alert.site_id == site.id,
                Alert.alert_type == alert_def["alert_type"]
            ).first()
            
            if not existing:
                alert = Alert(
                    site_id=site.id,
                    alert_type=alert_def["alert_type"],
                    threshold=alert_def["threshold"],
                    is_active=True
                )
                db.add(alert)
        
        db.commit()
        print(f"✓ Created alerts for: {site.domain}")
    
    # Create sample alert logs
    sites = db.query(Site).all()
    for site in sites:
        alerts = db.query(Alert).filter(Alert.site_id == site.id).all()
        for alert in alerts[:2]:  # Create 2 sample logs per site
            log = AlertLog(
                alert_id=alert.id,
                domain=site.domain,
                triggered_at=datetime.utcnow() - timedelta(hours=random.randint(1, 24)),
                message=f"[NEGATIVE] {alert.alert_type}: Değer {alert.threshold}% düştü",
                sent_mail=False
            )
            db.add(log)
        db.commit()
        print(f"✓ Created sample alert logs for: {site.domain}")

print("\n✓ All test data created!")
