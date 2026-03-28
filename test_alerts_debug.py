#!/usr/bin/env python3
"""Debug script to test the alerts endpoint."""
import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from starlette.testclient import TestClient
from backend.main import app

print("="*50)
print("Testing alerts endpoint...")
print("="*50)

client = TestClient(app)
response = client.get("/alerts")

print(f"\nStatus Code: {response.status_code}")
print(f"Response Headers: {dict(response.headers)}")

if response.status_code == 200:
    print("\n✅ SUCCESS - Alerts endpoint is working!")
    print(f"Response length: {len(response.text)} characters")
    if "<!DOCTYPE" in response.text or "<html" in response.text.lower():
        print("✅ HTML content received")
    else:
        print("⚠️  Content doesn't look like HTML")
elif response.status_code == 403:
    print(f"\n❌ Access Denied (403)")
    try:
        data = response.json()
        print(f"Client IP seen by server: {data.get('client_ip')}")
        print(f"Error message: {data.get('detail')}")
    except:
        print(f"Response: {response.text}")
else:
    print(f"\n❌ Unexpected status code: {response.status_code}")
    print(f"Response: {response.text[:500]}")

print("\n" + "="*50)
