#!/usr/bin/env python
import requests
import json
import time

time.sleep(2)

try:
    # Test DESKTOP only
    resp = requests.get('http://localhost:8000/api/site/doviz.com/top-queries?device=DESKTOP&limit=3')
    print('=== DESKTOP ===')
    if resp.status_code == 200:
        data = resp.json()
        print(f'Queries found: {len(data.get("queries", []))}')
        for q in data['queries']:
            print(f'  - {q["query"]}: {q["ctr"]}% (device: {q.get("device", "N/A")})')
    
    # Test MOBILE only
    resp = requests.get('http://localhost:8000/api/site/doviz.com/top-queries?device=MOBILE&limit=3')
    print('\n=== MOBILE ===')
    if resp.status_code == 200:
        data = resp.json()
        print(f'Queries found: {len(data.get("queries", []))}')
        for q in data['queries']:
            print(f'  - {q["query"]}: {q["ctr"]}% (device: {q.get("device", "N/A")})')
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
