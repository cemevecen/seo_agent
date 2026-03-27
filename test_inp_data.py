#!/usr/bin/env python3
"""Test script to check INP data in the site detail page."""

import urllib.request
import re
import sys

try:
    # Get the site detail page
    print("Fetching site detail page...")
    response = urllib.request.urlopen('http://127.0.0.1:8012/site/doviz.com', timeout=10)
    html = response.read().decode('utf-8')
    print(f"Page fetched successfully. Length: {len(html)} chars\n")
    
    # Search for INP values
    print("=" * 60)
    print("SEARCHING FOR INP DATA IN PAGE")
    print("=" * 60)
    
    # Look for Interaction to Next Paint mentions
    inp_pattern = r'Interaction to Next Paint.*?(\d+)\s*ms'
    inp_matches = re.findall(inp_pattern, html, re.IGNORECASE | re.DOTALL)
    
    if inp_matches:
        print(f"✓ Found {len(inp_matches)} INP value(s):")
        for i, value in enumerate(inp_matches, 1):
            print(f"  {i}. {value} ms")
    else:
        print("✗ No INP values found in rendered HTML")
    
    # Check for template variables
    print("\nTemplate Variables:")
    if re.search(r'{{.*?mobile_inp', html):
        print("  ✓ mobile_inp variable found")
    else:
        print("  ✗ mobile_inp not found")
        
    if re.search(r'{{.*?desktop_inp', html):
        print("  ✓ desktop_inp variable found")
    else:
        print("  ✗ desktop_inp not found")
    
    # Check Core Web Vitals section
    print("\nCore Web Vitals Section Check:")
    if 'Core Web Vitals Assessment' in html:
        print("  ✓ 'Core Web Vitals Assessment' found")
        
        # Find all instances
        vitals_start_indices = [m.start() for m in re.finditer('Core Web Vitals Assessment', html)]
        print(f"  Found in {len(vitals_start_indices)} location(s)")
        
        # Extract context around each
        for idx, start in enumerate(vitals_start_indices, 1):
            context = html[start:start+500]
            # Look for INP in this context
            inp_in_context = re.search(r'Interaction to Next Paint.*?(\d+)', context)
            if inp_in_context:
                print(f"    Section {idx}: INP = {inp_in_context.group(1)} ms")
            else:
                print(f"    Section {idx}: No INP data found")
                # Show a snippet of what's there
                snippet = re.search(r'Interaction to Next Paint.*?(?=<|$)', context)
                if snippet:
                    print(f"      Content: {snippet.group(0)[:80]}")
    else:
        print("  ✗ 'Core Web Vitals Assessment' not found")
    
    # List all Core Web Vitals metrics we can find
    print("\nAll Core Web Vitals Metrics Found:")
    metrics_pattern = r'>(LCP|INP|CLS|Cumulative|Interaction|Largest).*?(\d+(?:\.\d+)?)\s*(s|ms)'
    metrics = re.findall(metrics_pattern, html, re.IGNORECASE)
    if metrics:
        for metric_type, value, unit in metrics[:10]:
            print(f"  {metric_type}: {value} {unit}")
    else:
        print("  None found")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
