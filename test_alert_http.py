#!/usr/bin/env python3
"""Simple HTTP test for alert details API"""
import sys
import json

try:
    import urllib.request
    import urllib.parse
    
    # Test basic alerts endpoint
    print("Testing /api/alerts endpoint...")
    try:
        res = urllib.request.urlopen('http://127.0.0.1:8012/api/alerts', timeout=5)
        data = json.loads(res.read().decode())
        print(f"  ✓ Got {len(data.get('items', []))} alert rules")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
    
    # Get recent alerts from backend and test details endpoint
    print("\nTesting /api/alert-details/{id} endpoint...")
    from backend.database import SessionLocal
    from backend.services.alert_engine import get_recent_alerts
    
    db = SessionLocal()
    alerts = get_recent_alerts(db, limit=1)
    
    if not alerts:
        print("  ✗ No alerts found")
        sys.exit(1)
    
    alert_id = alerts[0]['id']
    domain = alerts[0]['domain']
    
    print(f"  Testing with alert ID {alert_id} ({domain})")
    
    url = f'http://127.0.0.1:8012/api/alert-details/{alert_id}'
    print(f"  URL: {url}")
    
    try:
        res = urllib.request.urlopen(url, timeout=5)
        data = json.loads(res.read().decode())
        print(f"  ✓ Response successful (status {res.status})")
        print(f"    Domain: {data['site']['domain']}")
        print(f"    Rule: {data['rule']['title']}")
        print(f"    Description (TR) present: {bool(data['rule']['description_short'])}")
        print(f"    Recommendations present: {bool(data['rule']['recommendations'])}")
        print(f"    Trend items: {len(data['trend'])}")
    except urllib.error.HTTPError as e:
        print(f"  ✗ HTTP {e.code}: {e.reason}")
        try:
            error_text = e.read().decode()
            print(f"    Response: {error_text[:200]}")
        except:
            pass
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        
except Exception as e:
    print(f"Fatal error: {e}")
    import traceback
    traceback.print_exc()
