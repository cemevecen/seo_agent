import sys
import os
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend.database import SessionLocal
from backend.models import SmtpDailySendLedger
from backend.config import settings
from backend.services.mailer import _smtp_configured, is_realtime_mail_ready

def dump():
    print("--- Settings ---")
    print(f"SMTP_HOST: {settings.smtp_host}")
    print(f"SMTP_USER: {settings.smtp_user}")
    print(f"MAIL_FROM: {settings.mail_from}")
    print(f"MAIL_TO: {settings.mail_to}")
    print(f"GA4_REALTIME_EMAIL_ENABLED: {settings.ga4_realtime_email_enabled}")
    print(f"GA4_REALTIME_PAGE_ALERT_EMAIL: {settings.ga4_realtime_page_alert_email}")
    print(f"SMTP_DAILY_QUOTA_ENABLED: {settings.smtp_daily_quota_enabled}")
    print(f"SMTP_DAILY_SEND_LIMIT: {settings.smtp_daily_send_limit}")
    
    print("\n--- Readiness ---")
    print(f"SMTP Configured: {_smtp_configured()}")
    print(f"Realtime Mail Ready: {is_realtime_mail_ready()}")
    
    print("\n--- Quota Ledger ---")
    with SessionLocal() as db:
        rows = db.query(SmtpDailySendLedger).order_by(SmtpDailySendLedger.day_key.desc()).limit(5).all()
        for r in rows:
            print(f"Day: {r.day_key}, Count: {r.send_count}")

if __name__ == "__main__":
    dump()
