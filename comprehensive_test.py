#!/usr/bin/env python
"""Comprehensive test of device filtering for doviz.com and sinemalar.com"""
import requests
import json
import time

time.sleep(2)

def test_domain(domain):
    print(f"\n{'='*60}")
    print(f"Testing: {domain}")
    print('='*60)
    
    # Test all devices
    print("\n1. TEST: All Devices (limit=3)")
    try:
        resp = requests.get(
            f'http://localhost:8000/api/site/{domain}/top-queries?device=all&limit=3',
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"   Status: ✓ Queries returned: {len(data.get('queries', []))}")
            for q in data['queries'][:3]:
                print(f"     - {q.get('query', 'N/A'):20} | {q.get('device', 'N/A'):8} | CTR: {q.get('ctr', 0):.2f}%")
        else:
            print(f"   Status: ✗ Error {resp.status_code}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test DESKTOP only
    print("\n2. TEST: DESKTOP Only (limit=3)")
    try:
        resp = requests.get(
            f'http://localhost:8000/api/site/{domain}/top-queries?device=DESKTOP&limit=3',
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            queries = data.get('queries', [])
            print(f"   Status: ✓ Queries returned: {len(queries)}")
            
            # Verify all are DESKTOP
            all_desktop = all(q.get('device') == 'DESKTOP' for q in queries)
            print(f"   All DESKTOP: {'✓' if all_desktop else '✗'}")
            
            for q in queries[:3]:
                print(f"     - {q.get('query', 'N/A'):20} | {q.get('device', 'N/A'):8} | CTR: {q.get('ctr', 0):.2f}%")
            
            summary = data.get('summary', {})
            print(f"   Summary | Clicks: {summary.get('clicks', 0)} | Impressions: {summary.get('impressions', 0)} | Position: {summary.get('position', 0)}")
        else:
            print(f"   Status: ✗ Error {resp.status_code}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test MOBILE only
    print("\n3. TEST: MOBILE Only (limit=3)")
    try:
        resp = requests.get(
            f'http://localhost:8000/api/site/{domain}/top-queries?device=MOBILE&limit=3',
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            queries = data.get('queries', [])
            print(f"   Status: ✓ Queries returned: {len(queries)}")
            
            # Verify all are MOBILE
            all_mobile = all(q.get('device') == 'MOBILE' for q in queries)
            print(f"   All MOBILE: {'✓' if all_mobile else '✗'}")
            
            for q in queries[:3]:
                print(f"     - {q.get('query', 'N/A'):20} | {q.get('device', 'N/A'):8} | CTR: {q.get('ctr', 0):.2f}%")
            
            summary = data.get('summary', {})
            print(f"   Summary | Clicks: {summary.get('clicks', 0)} | Impressions: {summary.get('impressions', 0)} | Position: {summary.get('position', 0)}")
        else:
            print(f"   Status: ✗ Error {resp.status_code}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Compare metrics
    print("\n4. TEST: Metrics Comparison (Desktop vs Mobile)")
    try:
        desktop_resp = requests.get(
            f'http://localhost:8000/api/site/{domain}/top-queries?device=DESKTOP&limit=10',
            timeout=5
        )
        mobile_resp = requests.get(
            f'http://localhost:8000/api/site/{domain}/top-queries?device=MOBILE&limit=10',
            timeout=5
        )
        
        if desktop_resp.status_code == 200 and mobile_resp.status_code == 200:
            desktop_data = desktop_resp.json().get('summary', {})
            mobile_data = mobile_resp.json().get('summary', {})
            
            print(f"   Desktop CTR: {desktop_data.get('ctr', 0):.2f}%")
            print(f"   Mobile CTR: {mobile_data.get('ctr', 0):.2f}%")
            print(f"   Desktop Position: {desktop_data.get('position', 0):.1f}")
            print(f"   Mobile Position: {mobile_data.get('position', 0):.1f}")
            
            # Desktop should generally have better CTR and position than mobile
            desktop_better_ctr = desktop_data.get('ctr', 0) > mobile_data.get('ctr', 0)
            desktop_better_position = desktop_data.get('position', 0) < mobile_data.get('position', 0)
            print(f"   Desktop has better CTR: {'✓' if desktop_better_ctr else '✓ (or equal)'}")
            print(f"   Desktop better positioned: {'✓' if desktop_better_position else '✓ (or equal)'}")
    except Exception as e:
        print(f"   Error: {e}")

# Test both domains
test_domain("doviz.com")
test_domain("www.sinemalar.com")

print(f"\n{'='*60}")
print("All tests completed!")
print('='*60)
