import sys
import os
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend.database import SessionLocal
from backend.models import SmtpDailySendLedger
from backend.services.smtp_quota import _calendar_day_key

def reset_and_dump():
    day_key = _calendar_day_key()
    print(f"Checking quota for {day_key}...")
    
    with SessionLocal() as db:
        ledger = db.query(SmtpDailySendLedger).filter(SmtpDailySendLedger.day_key == day_key).first()
        if ledger:
            print(f"Current Quota: {ledger.send_count}")
            print("Resetting to 0...")
            ledger.send_count = 0
            db.commit()
            print("Reset complete.")
        else:
            print("No record for today.")

if __name__ == "__main__":
    reset_and_dump()
