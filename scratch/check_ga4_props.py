import os
import sys
import json

sys.path.append(os.getcwd())
os.environ["DATABASE_URL"] = "sqlite:///backend/seo_agent.db"

from backend.database import SessionLocal
from backend.models import Site, SiteCredential
from backend.services.ga4_auth import decrypt_text

def run():
    with SessionLocal() as db:
        sites = db.query(Site).all()
        for s in sites:
            print(f"Site: {s.id} - {s.domain}")
            cred = db.query(SiteCredential).filter_by(site_id=s.id, credential_type="ga4").first()
            if cred:
                try:
                    data = json.loads(decrypt_text(cred.encrypted_data))
                    print(f"  GA4 Properties: {data}")
                except Exception as e:
                    print(f"  Error decrypting: {e}")
            else:
                print("  No GA4 credentials")

if __name__ == "__main__":
    run()
