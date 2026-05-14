import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.database import SessionLocal
from backend.models import Site, SiteCredential
from backend.services.ga4_realtime import run_all_sites_realtime_check

def test():
    db = SessionLocal()
    try:
        sites = db.query(Site).filter(Site.is_active == True).limit(2).all()
        print(f"Found {len(sites)} active sites.")
        for site in sites:
            print(f"Testing site: {site.domain} (ID: {site.id})")
        
        # Force a small window for speed
        results = run_all_sites_realtime_check(db, window_minutes=10)
        print("Results summary:")
        for r in results:
            if "error" in r:
                print(f"  [{r.get('domain')} / {r.get('profile')}] ERROR: {r['message']}")
            else:
                print(f"  [{r.get('domain')} / {r.get('profile')}] SUCCESS: {r.get('alarm_count', 0)} alarms")
                
    finally:
        db.close()

if __name__ == "__main__":
    test()
