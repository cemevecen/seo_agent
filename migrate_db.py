#!/usr/bin/env python3
"""Migrate database schema - back up old DB and create new one with AlertLog.domain field"""
import os
import shutil
from datetime import datetime

db_path = "/Users/cemevecen/Desktop/seo_agent/seo_agent.db"
backup_path = f"/Users/cemevecen/Desktop/seo_agent/seo_agent.db.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"

if os.path.exists(db_path):
    shutil.copy2(db_path, backup_path)
    print(f"✓ Backup created: {backup_path}")
    os.remove(db_path)
    print(f"✓ Old database removed")
else:
    print(f"Database not found at {db_path}")

print("\nNow start backend - it will create new schema with AlertLog.domain field")
print("Then run: python3 refresh_alerts.py")
