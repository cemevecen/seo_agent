#!/usr/bin/env python3
import urllib.request
import json

try:
    # Test PageSpeed analysis endpoint
    url = "http://127.0.0.1:8012/api/analyze?domain=doviz.com"
    response = urllib.request.urlopen(url, timeout=10)
    data = json.loads(response.read().decode())
    
    print("✅ PageSpeed Analysis API is LIVE\n")
    print(f"Domain: {data.get('domain')}")
    print(f"Mobile Score: {data.get('mobile_score')}")
    print(f"Desktop Score: {data.get('desktop_score')}")
    
    # Check if dynamic solutions are present
    diagnostics = data['analysis']['diagnostics']['performance_diagnostics']
    print(f"\nDiagnostics Available: {len(diagnostics)}/9")
    
    # Show first 3 diagnostic titles
    print("\n📊 Sample Diagnostics (with DYNAMIC solutions):")
    for i, diag in enumerate(diagnostics[:3], 1):
        sol = diag['solution']
        title = diag['title'][:45]
        problem = sol['problem'][:60]
        print(f"\n  [{i}] {title}...")
        print(f"      Problem: {problem}...")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
