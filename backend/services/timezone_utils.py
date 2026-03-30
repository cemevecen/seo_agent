"""Uygulama genelinde tarih/saatleri yerel saat dilimine ceviren yardimcilar."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from backend.config import settings


def app_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(settings.scheduled_refresh_timezone)
    except Exception:
        return ZoneInfo("Europe/Istanbul")


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
