#!/usr/bin/env python3
"""Test API endpoint directly."""

import sys
import json
import time
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

time.sleep(3)  # Wait for server to start

import requests

# Test API endpoint
try:
    response = requests.get('http://localhost:8000/api/alert-details/5?comparison=daily')
    if response.status_code == 200:
        data = response.json()
        print("✓ API Response successful\n")
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(f"✗ API returned status {response.status_code}")
        print(response.text)
except Exception as e:
    print(f"✗ Error: {e}")
