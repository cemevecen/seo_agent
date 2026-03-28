#!/usr/bin/env python3
"""Test API endpoint to verify alert details rendering."""

import sys
import json
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.database import SessionLocal
from backend.models import AlertLog

db = SessionLocal()

# Get a doviz alert with position data
alert = db.query(AlertLog).filter(
    AlertLog.domain == 'doviz.com',
    AlertLog.message.like('%search_console_position_change%')
).first()

if alert:
    print(f"\n📊 Alert ID: {alert.id}")
    print(f"Domain: {alert.domain}")
    print(f"Message: {alert.message}")
    
    # Test parsing
    from backend.api.alerts import _extract_query_details_from_message
    details = _extract_query_details_from_message(alert.message)
    
    print(f"\n✓ Parsed Details:")
    print(json.dumps(details if isinstance(details, list) else [details], indent=2, ensure_ascii=False))
    
    # Simulate what API returns
    print(f"\n✓ What API would return:")
    api_response = {
        "alert_log": {
            "id": alert.id,
            "domain": alert.domain,
            "message": alert.message,
            "triggered_at": str(alert.triggered_at)
        },
        "comparison": {
            "message": alert.message,
            "query_details": details if isinstance(details, list) else [details]
        }
    }
    print(json.dumps(api_response, indent=2, ensure_ascii=False))
else:
    print("❌ No alerts found with position data")

db.close()
