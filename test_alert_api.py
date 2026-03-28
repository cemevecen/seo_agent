#!/usr/bin/env python3
"""Test alert API endpoints"""
import sys
sys.path.insert(0, '.')

from backend.database import SessionLocal
from backend.services.alert_engine import get_recent_alerts

db = SessionLocal()

# Get an alert ID
alerts = get_recent_alerts(db, limit=1)
if not alerts:
    print("❌ No alertLogs found!")
    sys.exit(1)

alert_id = alerts[0]['id']
print(f"Testing with alert ID: {alert_id}")
print(f"Alert: {alerts[0]['domain']} - {alerts[0]['message'][:50]}...")

# Now test the endpoint directly
from backend.api.alerts import get_alert_details
from unittest.mock import MagicMock

mock_request = MagicMock()
mock_request.headers = {}

try:
    result = get_alert_details(mock_request, alert_id, db)
    
    print(f"\n✅ API Response successful!")
    print(f"\nRule Info:")
    print(f"  - Title: {result['rule']['title']}")
    print(f"  - Description (TR): {result['rule']['description_short'][:60] if result['rule']['description_short'] else 'EMPTY'}...")
    print(f"  - Description (EN): {result['rule']['description_short_en'][:60] if result['rule']['description_short_en'] else 'EMPTY'}...")
    print(f"  - Recommendations: {result['rule']['recommendations'][:60] if result['rule']['recommendations'] else 'EMPTY'}...")
    
    print(f"\nMetrics:")
    print(f"  - Current: {result['metrics']['current_value']}")
    print(f"  - Threshold: {result['metrics']['threshold_value']}")
    print(f"  - Min: {result['metrics']['min_value']}")
    print(f"  - Max: {result['metrics']['max_value']}")
    
    print(f"\nTrend history count: {len(result['trend'])}")
    
except Exception as e:
    print(f"\n❌ ERROR: {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc()
