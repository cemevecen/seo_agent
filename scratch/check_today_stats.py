import sys
import os
from pathlib import Path
from datetime import datetime, date

# Add backend to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend.database import SessionLocal
from backend.models import RealtimeAlarmLog, SmtpDailySendLedger
from backend.services.smtp_quota import _calendar_day_key

def check_counts():
    day_key = _calendar_day_key()
    today = date.fromisoformat(day_key)
    print(f"Checking counts for {day_key}...")
    
    with SessionLocal() as db:
        # Count alarms logged today
        alarm_count = db.query(RealtimeAlarmLog).filter(RealtimeAlarmLog.triggered_at >= datetime.combine(today, datetime.min.time())).count()
        print(f"Total Alarms Logged Today: {alarm_count}")
        
        # Check SMTP Ledger
        ledger = db.query(SmtpDailySendLedger).filter(SmtpDailySendLedger.day_key == day_key).first()
        if ledger:
            print(f"SMTP Ledger Count: {ledger.send_count}")
        else:
            print("No SMTP ledger record for today.")
            
        # Last 5 alarms
        last_alarms = db.query(RealtimeAlarmLog).order_by(RealtimeAlarmLog.triggered_at.desc()).limit(5).all()
        print("\nLast 5 Alarms:")
        for a in last_alarms:
            print(f"[{a.triggered_at}] {a.domain if hasattr(a, 'domain') else a.site_id} - {a.metric}: {a.message}")

if __name__ == "__main__":
    check_counts()
