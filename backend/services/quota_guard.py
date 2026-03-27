"""Harici API kota ve fatura kontrol katmani."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import ApiUsage, Site
from backend.services.alert_engine import emit_custom_alert


@dataclass(frozen=True)
class QuotaDecision:
    allowed: bool
    reason: str


def _period_start(now: datetime, period_type: str) -> datetime:
    if period_type == "day":
        return datetime(now.year, now.month, now.day)
    return datetime(now.year, now.month, 1)


def _limit_for(provider: str, period_type: str) -> int:
    if provider == "pagespeed":
        return settings.pagespeed_daily_limit if period_type == "day" else settings.pagespeed_monthly_limit
    if provider == "search_console":
        return (
            settings.search_console_daily_limit
            if period_type == "day"
            else settings.search_console_monthly_limit
        )
    return 0


def _get_or_create_usage(
    db: Session,
    site_id: int,
    provider: str,
    period_type: str,
    period_start: datetime,
) -> ApiUsage:
    row = (
        db.query(ApiUsage)
        .filter(
            ApiUsage.site_id == site_id,
            ApiUsage.provider == provider,
            ApiUsage.period_type == period_type,
            ApiUsage.period_start == period_start,
        )
        .first()
    )
    if row:
        return row

    row = ApiUsage(
        site_id=site_id,
        provider=provider,
        period_type=period_type,
        period_start=period_start,
        call_count=0,
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()
    return row


def consume_api_quota(db: Session, site: Site, provider: str, units: int = 1) -> QuotaDecision:
    # Guard kapaliysa cagrilari oldugu gibi gecirir.
    if not settings.quota_guard_enabled:
        return QuotaDecision(allowed=True, reason="quota guard disabled")

    now = datetime.utcnow()
    warning_ratio = max(0.0, min(settings.quota_warning_ratio, 0.99))

    day_limit = _limit_for(provider, "day")
    month_limit = _limit_for(provider, "month")
    if day_limit <= 0 or month_limit <= 0:
        return QuotaDecision(allowed=True, reason="limit not configured")

    day_row = _get_or_create_usage(db, site.id, provider, "day", _period_start(now, "day"))
    month_row = _get_or_create_usage(db, site.id, provider, "month", _period_start(now, "month"))

    next_day = day_row.call_count + units
    next_month = month_row.call_count + units

    if next_day > day_limit or next_month > month_limit:
        message = (
            f"{site.domain} icin {provider} kota limiti asildi. "
            f"Gunluk: {day_row.call_count}/{day_limit}, Aylik: {month_row.call_count}/{month_limit}. "
            f"Yeni API cagrisı bloke edildi."
        )
        emit_custom_alert(db, site, f"quota_{provider}_hard_limit", message, dedupe_hours=2)
        db.commit()
        return QuotaDecision(allowed=False, reason=message)

    # Kullanim warning bandina giriyorsa bir kez uyari logu olustur.
    crossed_daily = day_row.call_count < int(day_limit * warning_ratio) <= next_day
    crossed_monthly = month_row.call_count < int(month_limit * warning_ratio) <= next_month
    if crossed_daily or crossed_monthly:
        message = (
            f"{site.domain} icin {provider} kota kullanimı warning seviyesine geldi. "
            f"Gunluk: {next_day}/{day_limit}, Aylik: {next_month}/{month_limit}."
        )
        emit_custom_alert(db, site, f"quota_{provider}_warning", message, dedupe_hours=12)

    day_row.call_count = next_day
    day_row.updated_at = now
    month_row.call_count = next_month
    month_row.updated_at = now
    db.commit()
    return QuotaDecision(
        allowed=True,
        reason=(
            f"{provider} quota ok: daily {day_row.call_count}/{day_limit}, "
            f"monthly {month_row.call_count}/{month_limit}"
        ),
    )


def get_quota_status(db: Session) -> list[dict]:
    # Settings ekrani icin guncel period kullanimlarini dondurur.
    now = datetime.utcnow()
    day_start = _period_start(now, "day")
    month_start = _period_start(now, "month")

    rows = (
        db.query(ApiUsage, Site)
        .join(Site, ApiUsage.site_id == Site.id)
        .filter(ApiUsage.period_start.in_([day_start, month_start]))
        .all()
    )

    status_map: dict[tuple[int, str], dict] = {}
    for usage, site in rows:
        key = (site.id, usage.provider)
        if key not in status_map:
            status_map[key] = {
                "site_id": site.id,
                "domain": site.domain,
                "provider": usage.provider,
                "daily_used": 0,
                "daily_limit": _limit_for(usage.provider, "day"),
                "monthly_used": 0,
                "monthly_limit": _limit_for(usage.provider, "month"),
            }
        if usage.period_type == "day":
            status_map[key]["daily_used"] = usage.call_count
        if usage.period_type == "month":
            status_map[key]["monthly_used"] = usage.call_count

    return sorted(status_map.values(), key=lambda x: (x["domain"], x["provider"]))
