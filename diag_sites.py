#!/usr/bin/env python3
import urllib.request, json

# Get full alerts list  
response = urllib.request.urlopen('http://127.0.0.1:8012/api/alerts', timeout=10)
alerts_api = json.loads(response.read().decode())

# Group by site_id
by_site_id = {}
for alert in alerts_api['items']:
    site_id = alert['site_id']
    if site_id not in by_site_id:
        by_site_id[site_id] = []
    by_site_id[site_id].append(alert['alert_type'])

print("ALERT RULES BY SITE_ID:")
print("="*60)
for site_id in sorted(by_site_id.keys()):
    types = by_site_id[site_id]
    has_sc = any('search_console' in t for t in types)
    print(f"\nSite ID: {site_id}")
    print(f"  Has Search Console: {has_sc}")
    print(f"  Types: {len([t for t in types if 'search_console' in t])} SC alerts, {len([t for t in types if 'search_console' not in t])} others")

print("\nDiagnosing the mismatch:")
print("get_recent_alerts uses: AlertLog -> Alert (join) -> Site")
print("This means: AlertLog.alert_id -> Alert.id -> Alert.site_id -> Site.id")
print("\nProblem: If Site(id=1) = doviz.com but get_top_queries gets sinemalar data")
print("Then AlertLog.message will have sinemalar queries but mapped to doviz.com alert")
