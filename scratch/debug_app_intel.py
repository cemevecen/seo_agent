import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
sys.path.append(os.getcwd())

os.environ["DATABASE_URL"] = "sqlite:///backend/seo_agent.db"

from backend.database import SessionLocal
from backend.services.app_intel import build_intel_payload

def run():
    print("Starting collection for 'doviz'...")
    res = build_intel_payload("doviz", 7, force_refresh=True)
    if res.get("error"):
        print(f"FAILED: {res.get('error')} - {res.get('message')}")
    else:
        print("SUCCESS! Data collected.")
        print(f"Android version: {res.get('android', {}).get('meta', {}).get('play_version')}")
        print(f"iOS version: {res.get('ios', {}).get('meta', {}).get('version')}")

if __name__ == "__main__":
    run()
