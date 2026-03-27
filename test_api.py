import subprocess
import time
import json

# Wait for server to be ready
time.sleep(2)

# Test the API endpoint
try:
    result = subprocess.run([
        '/Users/cemevecen/Desktop/seo_agent/seo-agent/.venv/bin/python',
        '-c',
        """
import requests
import json

# Test with device=all
resp = requests.get('http://localhost:8000/api/site/doviz.com/top-queries?device=all&limit=5')
print('Status:', resp.status_code)
print('Response:', json.dumps(resp.json(), indent=2)[:500])
"""
    ], capture_output=True, text=True, timeout=10)
    
    print(result.stdout)
    if result.stderr:
        print("Error:", result.stderr)
except Exception as e:
    print(f"Exception: {e}")
