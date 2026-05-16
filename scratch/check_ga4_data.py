import os
import sys
import json
from datetime import datetime

# Add the current directory to sys.path to import backend
sys.path.append(os.getcwd())

from backend.database import SessionLocal
from backend.models import Site, Ga4ReportSnapshot

def check_ga4():
    with SessionLocal() as db:
        sites = db.query(Site).all()
        print(f"Total sites: {len(sites)}")
        for s in sites:
            print(f"Site ID: {s.id}, Domain: {s.domain}, Name: {s.display_name}")

        print("\nChecking Ga4ReportSnapshot for period_days=7...")
        snapshots = db.query(Ga4ReportSnapshot).filter(Ga4ReportSnapshot.period_days == 7).order_by(Ga4ReportSnapshot.collected_at.desc()).limit(20).all()
        print(f"Found {len(snapshots)} snapshots for 7 days.")
        for snap in snapshots:
            print(f"ID: {snap.id}, SiteID: {snap.site_id}, Profile: {snap.profile}, CollectedAt: {snap.collected_at}")
            try:
                payload = json.loads(snap.payload_json or '{}')
                keys = list(payload.keys())
                print(f"  Keys: {keys[:5]}... (Total keys: {len(keys)})")
                
                prof_key = snap.profile
                last_key = f"ga4_{prof_key}_sessions_last7d_total"
                prev_key = f"ga4_{prof_key}_sessions_prev7d_total"
                
                print(f"  Looking for: {last_key} and {prev_key}")
                print(f"  Values: {payload.get(last_key)} / {payload.get(prev_key)}")
            except Exception as e:
                print(f"  Error parsing payload: {e}")

if __name__ == "__main__":
    check_ga4()
