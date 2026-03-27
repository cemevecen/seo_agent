#!/usr/bin/env python3
"""Test PageSpeed API directly to see what data is returned."""

import json
import urllib.request
import urllib.parse
import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.config import settings

PAGESPEED_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

def test_pagespeed_api(url: str, strategy: str):
    """Fetch PageSpeed data and inspect the response."""
    
    api_key = settings.google_api_key.strip()
    query = urllib.parse.urlencode({
        "url": url, 
        "strategy": strategy, 
        "key": api_key, 
        "category": "performance"
    })
    
    print(f"\nTesting {strategy.upper()} for {url}")
    print("=" * 80)
    print(f"API Key: {api_key[:30]}...")
    print(f"Endpoint: {PAGESPEED_ENDPOINT}?{query[:100]}...")
    
    try:
        with urllib.request.urlopen(f"{PAGESPEED_ENDPOINT}?{query}", timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        
        print("\n✓ API call successful")
        
        # Check Lighthouse result
        lighthouse = payload.get("lighthouseResult", {})
        if not lighthouse:
            print("✗ No lighthouseResult in response")
            return
        
        # Check categories
        categories = lighthouse.get("categories", {})
        performance_score = categories.get("performance", {}).get("score") or 0
        print(f"\n  Performance Score: {performance_score * 100:.0f}")
        
        # Check audits
        audits = lighthouse.get("audits", {})
        print(f"\n  Total audits: {len(audits)}")
        
        # Look for LCP, CLS, INP audits
        for audit_name in ["largest-contentful-paint", "cumulative-layout-shift", 
                           "interaction-to-next-paint", "experimental-interaction-to-next-paint",
                           "first-contentful-paint", "total-blocking-time"]:
            if audit_name in audits:
                audit = audits[audit_name]
                numeric_value = audit.get("numericValue")
                print(f"  ✓ {audit_name}: {numeric_value}")
            else:
                print(f"  ✗ {audit_name}: NOT FOUND")
        
        # Show all audit names that contain 'interaction' or 'inp'
        print("\n  Audits containing 'interaction' or 'inp':")
        relevant_audits = [name for name in audits.keys() if 'interaction' in name.lower() or 'inp' in name.lower()]
        if relevant_audits:
            for name in relevant_audits:
                value = audits[name].get("numericValue")
                print(f"    - {name}: {value}")
        else:
            print("    (none found)")
        
        # Save full response to file for inspection
        with open(f'/tmp/pagespeed_{strategy}_response.json', 'w') as f:
            json.dump(payload, f, indent=2)
        print(f"\n  Full response saved to: /tmp/pagespeed_{strategy}_response.json")
        
    except Exception as e:
        print(f"\n✗ API call failed: {e}")
        import traceback
        traceback.print_exc()

# Test both strategies
test_pagespeed_api("https://doviz.com", "mobile")
test_pagespeed_api("https://doviz.com", "desktop")

print("\n" + "=" * 80)
print("To inspect full response: cat /tmp/pagespeed_mobile_response.json | jq '.lighthouseResult.audits | keys'")
