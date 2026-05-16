import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
sys.path.append(os.getcwd())

from backend.database import SessionLocal
from backend.models import Site

def run_refresh():
    with SessionLocal() as db:
        sites = db.query(Site).filter(Site.is_active.is_(True)).all()
        
        # 1. GA4 Sessions
        from backend.collectors.ga4 import collect_ga4_channel_sessions
        print("--- Refreshing GA4 Sessions ---")
        for site in sites:
            try:
                print(f"Refreshing {site.domain}...")
                collect_ga4_channel_sessions(db, site)
            except Exception as e:
                print(f"Error GA4 {site.domain}: {e}")

        # 2. Realtime Snapshots
        from backend.services.ga4_realtime import run_all_sites_realtime_check
        print("--- Refreshing Realtime ---")
        try:
            run_all_sites_realtime_check(db)
        except Exception as e:
            print(f"Error Realtime: {e}")

        # 3. App Intel
        from backend.services.app_intel import build_intel_payload
        print("--- Refreshing App Intel ---")
        try:
            build_intel_payload("doviz", 30, force_refresh=True)
            print("App Intel refreshed.")
        except Exception as e:
            print(f"Error App Intel: {e}")

if __name__ == "__main__":
    run_refresh()
