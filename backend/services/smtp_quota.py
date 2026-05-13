"""Günlük SMTP gönderim kotası (Postgres satır kilidi; tek süreç SQLite’da yeterli)."""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from backend.config import settings
from backend.database import SessionLocal
from backend.models import SmtpDailySendLedger

LOGGER = logging.getLogger(__name__)


def _quota_active() -> bool:
    return bool(settings.smtp_daily_quota_enabled) and int(settings.smtp_daily_send_limit) > 0


def _calendar_day_key() -> str:
    tz_name = (settings.smtp_quota_calendar_timezone or "Europe/Istanbul").strip() or "Europe/Istanbul"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Europe/Istanbul")
    return datetime.now(tz).date().isoformat()


def smtp_quota_try_reserve_one_send() -> bool:
    """Günlük tavan dolmadıysa sayacı 1 artırır ve True döner; aksi halde False."""
    if not _quota_active():
        return True
    limit = int(settings.smtp_daily_send_limit)
    day_key = _calendar_day_key()
    try:
        with SessionLocal() as session:
            with session.begin():
                row = (
                    session.query(SmtpDailySendLedger)
                    .filter(SmtpDailySendLedger.day_key == day_key)
                    .with_for_update()
                    .one_or_none()
                )
                if row is None:
                    row = SmtpDailySendLedger(day_key=day_key, send_count=0)
                    session.add(row)
                    session.flush()
                if row.send_count >= limit:
                    LOGGER.warning(
                        "SMTP günlük gönderim tavanı: %s için %d/%d — bu gönderim atlandı.",
                        day_key,
                        row.send_count,
                        limit,
                    )
                    return False
                row.send_count = int(row.send_count) + 1
        return True
    except Exception:
        LOGGER.exception("SMTP kota rezervasyonu başarısız (day_key=%s)", day_key)
        return False


def smtp_quota_release_one_send() -> None:
    """SMTP gönderimi tüm denemeler sonunda başarısız kaldıysa rezervi geri al."""
    if not _quota_active():
        return
    day_key = _calendar_day_key()
    try:
        with SessionLocal() as session:
            with session.begin():
                row = (
                    session.query(SmtpDailySendLedger)
                    .filter(SmtpDailySendLedger.day_key == day_key)
                    .with_for_update()
                    .one_or_none()
                )
                if row is None or row.send_count <= 0:
                    return
                row.send_count = int(row.send_count) - 1
    except Exception:
        LOGGER.exception("SMTP kota geri alma hatası (day_key=%s)", day_key)


def smtp_quota_current_count() -> int | None:
    """Aynı takvim günü için kayıtlı gönderim sayısı (tanılama); kota kapalıysa None."""
    if not _quota_active():
        return None
    day_key = _calendar_day_key()
    try:
        with SessionLocal() as session:
            row = session.query(SmtpDailySendLedger).filter(SmtpDailySendLedger.day_key == day_key).one_or_none()
            return int(row.send_count) if row else 0
    except Exception:
        LOGGER.debug("SMTP kota okunamadı (day_key=%s)", day_key, exc_info=True)
        return None


def smtp_recipients_allowed(recipient_count: int) -> bool:
    mx = int(settings.smtp_max_recipients_per_message)
    if recipient_count > mx:
        LOGGER.error(
            "SMTP: alıcı sayısı (%d), izin verilen üst sınırı (%d) aşıyor — gönderim iptal. "
            "Workspace SMTP için Google dokümantasyonunda mesaj başına RCPT sınırı 100’dür; "
            "tüketici Gmail için .env ile SMTP_MAX_RECIPIENTS_PER_MESSAGE yükseltilebilir.",
            recipient_count,
            mx,
        )
        return False
    return True


