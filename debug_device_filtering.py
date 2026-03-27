#!/usr/bin/env python
"""Debug script to trace get_top_queries for MOBILE device"""
from backend.database import SessionLocal
from backend.models import Site
from backend.collectors.search_console import get_top_queries, _mock_search_console_response

db = SessionLocal()
site = db.query(Site).filter(Site.domain == "doviz.com").first()

print(f"Site: {site.domain if site else 'NOT FOUND'}")

if site:
    # Check mock data directly
    print("\n=== Mock Data ===")
    mock_data = _mock_search_console_response("doviz.com")
    rows = mock_data.get("rows", [])
    print(f"Total rows in mock: {len(rows)}")
    
    # Count devices in mock data
    desktop_count = sum(1 for r in rows if r.get("device") == "DESKTOP")
    mobile_count = sum(1 for r in rows if r.get("device") == "MOBILE")
    print(f"Desktop rows: {desktop_count}, Mobile rows: {mobile_count}")
    
    # Show first 3 rows
    print("First 6 rows:")
    for i, row in enumerate(rows[:6]):
        print(f"  {i}: {row.get('keys', [''])[0]:20} Device: {row.get('device'):8} CTR: {row.get('ctr', 0):.4f}")
    
    # Now test get_top_queries
    print("\n=== get_top_queries Tests ===")
    
    # Test ALL
    print("\n1. device=ALL, limit=3:")
    queries = get_top_queries(db, site, limit=3, device="ALL")
    print(f"   Returned {len(queries)} rows")
    for q in queries:
        print(f"     - {q['query']:20} Device: {q['device']:8}")
    
    # Test DESKTOP
    print("\n2. device=DESKTOP, limit=3:")
    queries = get_top_queries(db, site, limit=3, device="DESKTOP")
    print(f"   Returned {len(queries)} rows")
    for q in queries:
        print(f"     - {q['query']:20} Device: {q['device']:8}")
    
    # Test MOBILE
    print("\n3. device=MOBILE, limit=3:")
    queries = get_top_queries(db, site, limit=3, device="MOBILE")
    print(f"   Returned {len(queries)} rows")
    for q in queries:
        print(f"     - {q['query']:20} Device: {q['device']:8}")

db.close()
