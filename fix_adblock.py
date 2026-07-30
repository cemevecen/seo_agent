import re
import os

paths = [
    'templates/ad.html',
    'templates/base.html',
    'backend/api/ad_analytics.py',
    'backend/main.py'
]

def process_file(path):
    if not os.path.exists(path):
        print(f"File {path} not found")
        return
        
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Replace word-boundary "ad-" with "mz-"
    # This will match id="ad-page", class="ad-page", id="ad-body-grid", /ad-analytics, etc.
    new_content = re.sub(r'\bad-', 'mz-', content)

    if new_content != content:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {path}")
    else:
        print(f"No changes for {path}")

for p in paths:
    process_file(p)
