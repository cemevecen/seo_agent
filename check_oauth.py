import sys
sys.path.insert(0, '/Users/cemevecen/Desktop/seo_agent/seo-agent')

from backend.database import SessionLocal
from backend.models import Site, SiteCredential, Metric, Alert
from datetime import datetime

with SessionLocal() as db:
    sites = db.query(Site).all()
    
    print("\n" + "=" * 70)
    print("🔍 OAUTH VE VERİ DURUMU")
    print("=" * 70)
    
    for site in sites:
        print(f"\n📍 {site.domain.upper()}")
        print(f"   Status: {'✅ Aktif' if site.is_active else '❌ Pasif'}")
        
        # OAuth token kontrol
        creds = db.query(SiteCredential).filter(
            SiteCredential.site_id == site.id
        ).all()
        
        if creds:
            print(f"   ✅ {len(creds)} tane credential var")
            for cred in creds:
                print(f"      - {cred.credential_type}")
        else:
            print(f"   ❌ Credential yok")
        
        # Metric kontrol
        metrics = db.query(Metric).filter(Metric.site_id == site.id).count()
        print(f"   📊 Metrics: {metrics} kayıt")
        
        # Alert kontrol
        alerts = db.query(Alert).filter(Alert.site_id == site.id).count()
        print(f"   ⚠️  Alerts: {alerts} kayıt")
    
    print("\n" + "=" * 70)
