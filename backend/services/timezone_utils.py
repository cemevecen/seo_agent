"""Uygulama genelinde tarih/saatleri yerel saat dilimine ceviren yardimcilar."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from backend.config import settings


def app_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(settings.scheduled_refresh_timezone)
    except Exception:
        return ZoneInfo("Europe/Istanbul")


def report_calendar_tz() -> ZoneInfo:
    """GA4/GSC ve dönem filtreleri için takvim günü (varsayılan TSİ)."""
    raw = getattr(settings, "report_calendar_timezone", None)
    if raw and str(raw).strip():
        try:
            return ZoneInfo(str(raw).strip())
        except Exception:
            pass
    return app_timezone()


def report_calendar_today() -> date:
    return datetime.now(report_calendar_tz()).date()


def report_calendar_yesterday() -> date:
    return report_calendar_today() - timedelta(days=1)


def local_calendar_start_utc(d: date) -> datetime:
    """Verilen yerel takvim gününün 00:00 anı (UTC aware)."""
    tz = report_calendar_tz()
    utc = ZoneInfo("UTC")
    return datetime.combine(d, time.min, tzinfo=tz).astimezone(utc)


def inclusive_local_period_start_utc(n_calendar_days: int) -> datetime | None:
    """Bugün (yerel) dahil `n_calendar_days` günlük pencerenin ilk anı (UTC). n<=0 ise None."""
    if n_calendar_days <= 0:
        return None
    today = report_calendar_today()
    oldest = today - timedelta(days=n_calendar_days - 1)
    return local_calendar_start_utc(oldest)


def now_local() -> datetime:
    return datetime.now(app_timezone())


def local_schedule_datetime(target_date: date, hour: int, minute: int) -> datetime:
    return datetime.combine(target_date, time(hour=hour, minute=minute), tzinfo=app_timezone())


def local_schedule_to_utc_naive(target_date: date, hour: int, minute: int) -> datetime:
    localized = local_schedule_datetime(target_date, hour, minute)
    return localized.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def utc_naive_bounds_for_local_calendar_day(d: date) -> tuple[datetime, datetime]:
    """Yerel takvim günü `d` için [start, end) aralığı (`Metric.collected_at` ile uyumlu UTC naive)."""
    tz = app_timezone()
    utc = ZoneInfo("UTC")
    start_local = datetime.combine(d, time.min, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return (
        start_local.astimezone(utc).replace(tzinfo=None),
        end_local.astimezone(utc).replace(tzinfo=None),
    )


def to_local_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    timezone = app_timezone()
    if value.tzinfo is None:
        return value.replace(tzinfo=ZoneInfo("UTC")).astimezone(timezone)
    return value.astimezone(timezone)


def parse_datetime_like(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    normalized = str(value).strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except Exception:
        return None


def format_local_datetime(
    value: datetime | None,
    fmt: str = "%d.%m.%Y %H:%M",
    fallback: str = "N/A",
    include_suffix: bool = True,
) -> str:
    localized = to_local_datetime(value)
    if localized is None:
        return fallback
    text = localized.strftime(fmt)
    return f"{text} TSİ" if include_suffix else text


def format_datetime_like(
    value: datetime | str | None,
    fmt: str = "%d.%m.%Y %H:%M",
    fallback: str = "N/A",
    include_suffix: bool = True,
) -> str:
    parsed = parse_datetime_like(value)
    return format_local_datetime(parsed, fmt=fmt, fallback=fallback, include_suffix=include_suffix)
