import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.database import SessionLocal
from backend.models import Site, SiteCredential
from backend.collectors.search_console import get_top_queries

with SessionLocal() as db:
    # Get first site
    site = db.query(Site).first()
    if not site:
        print("❌ No sites in database")
    else:
        print(f"📍 Testing with site: {site.domain}")
        
        # Check if has Search Console token
        sc_cred = db.query(SiteCredential).filter(
            (SiteCredential.site_id == site.id) &
            (SiteCredential.credential_type == 'google_search_console')
        ).first()
        
        if sc_cred:
            print(f"✅ Search Console credential found")
        else:
            print(f"❌ NO Search Console OAuth token - NEED TO AUTHORIZE IN SETTINGS")
            
        # Try to fetch data
        try:
            queries = get_top_queries(site.domain, 1000, days=90)
            print(f"✅ Got {len(queries)} queries from Search Console")
        except Exception as e:
            print(f"❌ Error fetching data: {str(e)[:200]}")
