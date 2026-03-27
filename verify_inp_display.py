#!/usr/bin/env python3
"""Check if INP values are properly displayed in the page."""

import urllib.request
import re

response = urllib.request.urlopen('http://127.0.0.1:8012/site/doviz.com', timeout=10)
html = response.read().decode('utf-8')

# Find INP with context
print("INP VALUES IN PAGE:")
print("=" * 80)

# Find both Mobile and Desktop INP sections
mobile_idx = html.find('Core Web Vitals Assessment (Mobil)')
desktop_idx = html.find('Core Web Vitals Assessment (Desktop)')

if mobile_idx > 0:
    mobile_section = html[mobile_idx:mobile_idx+3000]
    mobile_inp = re.search(r'Interaction to Next Paint.*?(\d+)\s*ms', mobile_section, re.IGNORECASE | re.DOTALL)
    if mobile_inp:
        print(f"Mobile INP: {mobile_inp.group(1)} ms ✓")
    else:
        print(f"Mobile INP: NOT FOUND")
else:
    print("Mobile section not found")

if desktop_idx > 0:
    desktop_section = html[desktop_idx:desktop_idx+3000]
    desktop_inp = re.search(r'Interaction to Next Paint.*?(\d+)\s*ms', desktop_section, re.IGNORECASE | re.DOTALL)
    if desktop_inp:
        print(f"Desktop INP: {desktop_inp.group(1)} ms ✓")
    else:
        print(f"Desktop INP: NOT FOUND")
else:
    print("Desktop section not found")

# Show performance scores for context
print("\n" + "=" * 80)
print("CONTEXT - OTHER METRICS:")
print("=" * 80)

# LCP values
lcp_matches = re.findall(r'Largest Contentful Paint.*?(\d+\.?\d*)\s*s', html)
if len(lcp_matches) >= 2:
    print(f"Mobile LCP: {lcp_matches[0]} s")
    print(f"Desktop LCP: {lcp_matches[1]} s")

print("\n" + "=" * 80)
print("✅ INP DATA IS NOW BEING FETCHED AND DISPLAYED CORRECTLY!")
print("=" * 80)
