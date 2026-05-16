import os
import sys
from datetime import datetime

sys.path.append(os.getcwd())

from backend.database import SessionLocal
from backend.models import SearchConsoleQuerySnapshot, Site

def check_sc():
    with SessionLocal() as db:
        print("Checking SearchConsoleQuerySnapshot scopes...")
        scopes = db.query(SearchConsoleQuerySnapshot.data_scope).distinct().all()
        print(f"Scopes found: {[s[0] for s in scopes]}")
        
        for sid in [1, 2]:
            print(f"\nSite ID: {sid}")
            for scope in ["current_7d", "previous_7d"]:
                count = db.query(SearchConsoleQuerySnapshot).filter(
                    SearchConsoleQuerySnapshot.site_id == sid,
                    SearchConsoleQuerySnapshot.data_scope == scope
                ).count()
                print(f"  Scope {scope}: {count} rows")

if __name__ == "__main__":
    # Override DB URL to use local sqlite for this check if postgres fails
    # But wait, if they use postgres, I need to know how to connect.
    # I'll just try to use the environment and hope for the best, 
    # or if it fails, I'll try to find a way.
    try:
        check_sc()
    except Exception as e:
        print(f"Error: {e}")
