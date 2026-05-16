import os
import sys
import logging

# Configure logging to see output
logging.basicConfig(level=logging.INFO)

sys.path.append(os.getcwd())

from backend.database import SessionLocal
from backend.main import _run_ga4_realtime_check_job

def manual_run():
    print("Starting manual Realtime check...")
    res = _run_ga4_realtime_check_job(force_run=True)
    print(f"Result: {res}")

if __name__ == "__main__":
    # Override DATABASE_URL to be sure
    os.environ["DATABASE_URL"] = "sqlite:///backend/seo_agent.db"
    manual_run()
