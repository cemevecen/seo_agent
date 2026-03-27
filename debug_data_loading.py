#!/usr/bin/env python
"""Debug script to trace what data is actually being loaded"""
from backend.database import SessionLocal
from backend.models import Site
from backend.collectors.search_console import _load_search_console_data, get_search_console_credentials_record

db = SessionLocal()
site = db.query(Site).filter(Site.domain == "doviz.com").first()

print(f"Site: {site.domain}")
print(f"Site ID: {site.id}")

# Check credentials
cred = get_search_console_credentials_record(db, site.id)
print(f"Credential: {cred}")

# Load data
payload = _load_search_console_data(site, cred)
rows = payload.get("rows", [])
print(f"\nLoaded {len(rows)} rows from _load_search_console_data:")
for i, row in enumerate(rows[:6]):
    print(f"  {i}: {row.get('keys', [''])[0]:20} Device: {row.get('device', 'UNKNOWN'):8} Clicks: {row.get('clicks', 0):>5}")

# Group by query like get_top_queries does
queries_dict = {}
for row in rows:
    query_name = row.get("keys", [""])[0]
    row_device = (row.get("device", "DESKTOP") or "DESKTOP").upper().strip()
    
    print(f"Row: query='{query_name}', device='{row_device}'")
    
    if query_name not in queries_dict:
        queries_dict[query_name] = {}
    queries_dict[query_name][row_device] = row

print(f"\nGrouped into {len(queries_dict)} unique queries:")
for query_name in list(queries_dict.keys())[:3]:
    devices = list(queries_dict[query_name].keys())
    print(f"  {query_name}: {devices}")

db.close()
