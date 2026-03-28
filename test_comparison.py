#!/usr/bin/env python3
import urllib.request
import json
import re

# Get first alert ID from alerts page
resp = urllib.request.urlopen('http://127.0.0.1:8012/alerts', timeout=5)
html = resp.read().decode('utf-8')

match = re.search(r'data-alert-id="(\d+)"', html)
if not match:
    print("No alert found")
    exit(1)

alert_id = match.group(1)
print(f"Testing with alert ID: {alert_id}\n")

# Test daily comparison
print("=== Daily Comparison ===")
api_url = f'http://127.0.0.1:8012/api/alert-details/{alert_id}?comparison=daily'
resp = urllib.request.urlopen(api_url, timeout=5)
data = json.loads(resp.read().decode('utf-8'))
if data.get('comparison'):
    msg = data['comparison']['message']
    print(msg[:200] if len(msg) > 200 else msg)

# Test weekly comparison
print("\n=== Weekly Comparison ===")
api_url = f'http://127.0.0.1:8012/api/alert-details/{alert_id}?comparison=weekly'
resp = urllib.request.urlopen(api_url, timeout=5)
data = json.loads(resp.read().decode('utf-8'))
if data.get('comparison'):
    msg = data['comparison']['message']
    print(msg[:200] if len(msg) > 200 else msg)
