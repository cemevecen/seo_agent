import os
import sys
import json
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
        print(f"FAILED: {res.get('error')}")
    else:
        print("SUCCESS!")
        # Print keys to see structure
        print(f"Android keys: {res.get('android', {}).keys()}")
        print(f"Android meta keys: {res.get('android', {}).get('meta', {}).keys()}")
        print(f"iOS keys: {res.get('ios', {}).keys()}")
        print(f"iOS meta keys: {res.get('ios', {}).get('meta', {}).keys()}")
        
        # Specific fields
        and_meta = res.get('android', {}).get('meta', {})
        print(f"Android version: {and_meta.get('play_version')}")
        print(f"Android version (version): {and_meta.get('version')}")
        
        ios_meta = res.get('ios', {}).get('meta', {})
        print(f"iOS version: {ios_meta.get('version')}")

if __name__ == "__main__":
    run()
