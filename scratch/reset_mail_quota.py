import sys
import os
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from backend.database import SessionLocal
from backend.models import SmtpDailySendLedger
from backend.services.smtp_quota import _calendar_day_key

def reset_quota():
    day_key = _calendar_day_key()
    print(f"Resetting quota for {day_key}...")
    with SessionLocal() as db:
        with db.begin():
            row = db.query(SmtpDailySendLedger).filter(SmtpDailySendLedger.day_key == day_key).first()
            if row:
                print(f"Found record: {row.send_count} emails. Resetting to 0.")
                row.send_count = 0
            else:
                print("No record found for today.")

if __name__ == "__main__":
    reset_quota()
