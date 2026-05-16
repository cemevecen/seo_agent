import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# 1. Patch environment BEFORE importing anything from backend
os.environ["DATABASE_URL"] = "sqlite:///backend/seo_agent.db"
os.environ["GA4_REALTIME_ENABLED"] = "true"

# Add current dir to path
sys.path.append(os.getcwd())

# 2. Now import
from backend.database import SessionLocal, engine
from backend.models import Site, RealtimeSnapshot
from backend.services.ga4_realtime import run_all_sites_realtime_check

def manual_run():
    print(f"Using database: {engine.url}")
    with SessionLocal() as db:
        print("Starting manual Realtime check...")
        try:
            results = run_all_sites_realtime_check(db, force_run=True)
            print(f"Check completed. Found {len(results)} results.")
            for res in results:
                print(f"Site {res.get('domain')}: {len(res.get('alarms', []))} alarms.")
        except Exception as e:
            print(f"Error during check: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    manual_run()
