#!/usr/bin/env python3
import urllib.request
import re
from collections import Counter

resp = urllib.request.urlopen('http://127.0.0.1:8012/alerts', timeout=5)
html = resp.read().decode('utf-8')

# Extract all alert card message divs (main alert messages)
main_pattern = r'<div class="alert-card.*?data-alert-id="(\d+)".*?<p class="mt-1 text-sm text-slate-600">([^<]*)</p>'
main_matches = re.findall(main_pattern, html, re.DOTALL)

print(f"Main Alert Cards: {len(main_matches)}\n")
print("Alert Messages:")
message_counter = Counter()
for alert_id, msg in main_matches:
    message_counter[msg] += 1
    print(f"  ID {alert_id}: {msg[:80]}")

print("\n\nDuplicate Check:")
duplicates = {msg: count for msg, count in message_counter.items() if count > 1}
if duplicates:
    for msg, count in duplicates.items():
        print(f"  [{count}x] {msg[:80]}")
else:
    print("  No exact duplicates found!")

# Extract trend messages per alert
print("\n\nTrend History Items per Alert Card:")
card_pattern = r'data-alert-id="(\d+)".*?Tetikleme Geçmişi.*?<p class="text-slate-700 mt-1">([^<]*)</p>'
trend_matches = re.findall(card_pattern, html, re.DOTALL)
if trend_matches:
    for alert_id, trend_msg in trend_matches[:5]:
        print(f"  ID {alert_id}: {trend_msg[:70]}")
