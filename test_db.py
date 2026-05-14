import os
import sys
from backend.database import SessionLocal
from backend.models import SiteCredential, Site
from backend.services.crypto import decrypt_text
import json

db = SessionLocal()
creds = db.query(SiteCredential).filter_by(credential_type="ga4").all()
for c in creds:
    site = db.query(Site).get(c.site_id)
    raw = decrypt_text(c.encrypted_data)
    print(f"Site: {site.domain}, ID: {c.site_id}, GA4: {raw}")
