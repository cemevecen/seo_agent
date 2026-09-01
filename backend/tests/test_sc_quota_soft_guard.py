"""Search Console soft kota — ana sayfa spam sync / mail koruması."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

from backend.database import SessionLocal, init_db
from backend.models import ApiUsage, Site
from backend.services.quota_guard import (
    consume_api_quota,
    is_provider_daily_quota_exhausted,
)


def _fresh_db():
    init_db()
    return SessionLocal()


def _add_site(db, label: str) -> Site:
    site = Site(
        domain=f"{label}-{uuid4().hex[:8]}.example.test",
        display_name=label,
        is_active=True,
    )
    db.add(site)
    db.commit()
    db.refresh(site)
    return site


def test_is_provider_daily_quota_exhausted_true_at_limit():
    db = _fresh_db()
    try:
        site = _add_site(db, "doviz-quota-full")
        now = datetime.utcnow()
        day_start = datetime(now.year, now.month, now.day)
        db.add(
            ApiUsage(
                site_id=site.id,
                provider="search_console",
                period_type="day",
                period_start=day_start,
                call_count=80,
                updated_at=now,
            )
        )
        db.commit()
        assert is_provider_daily_quota_exhausted(db, site.id, "search_console") is True
    finally:
        db.close()


def test_is_provider_daily_quota_exhausted_false_when_under():
    db = _fresh_db()
    try:
        site = _add_site(db, "sinemalar-quota-ok")
        assert is_provider_daily_quota_exhausted(db, site.id, "search_console") is False
    finally:
        db.close()


def test_hard_limit_alert_dedupe_hours_is_24():
    db = _fresh_db()
    try:
        site = _add_site(db, "doviz-quota-alert")
        now = datetime.utcnow()
        day_start = datetime(now.year, now.month, now.day)
        month_start = datetime(now.year, now.month, 1)
        for period_type, period_start, count in (
            ("day", day_start, 71),
            ("month", month_start, 71),
        ):
            db.add(
                ApiUsage(
                    site_id=site.id,
                    provider="search_console",
                    period_type=period_type,
                    period_start=period_start,
                    call_count=count,
                    updated_at=now,
                )
            )
        db.commit()

        with patch("backend.services.quota_guard.emit_custom_alert") as emit:
            decision = consume_api_quota(
                db,
                site,
                provider="search_console",
                units=10,
                send_alert_emails=True,
            )
            assert decision.allowed is False
            assert emit.called
            kwargs = emit.call_args.kwargs
            assert kwargs.get("dedupe_hours") == 24
            assert kwargs.get("send_mail") is True
    finally:
        db.close()


def test_hard_limit_repeat_blocked_does_not_emit_again():
    db = _fresh_db()
    try:
        site = _add_site(db, "doviz-quota-repeat")
        now = datetime.utcnow()
        day_start = datetime(now.year, now.month, now.day)
        month_start = datetime(now.year, now.month, 1)
        for period_type, period_start, count in (
            ("day", day_start, 80),
            ("month", month_start, 334),
        ):
            db.add(
                ApiUsage(
                    site_id=site.id,
                    provider="search_console",
                    period_type=period_type,
                    period_start=period_start,
                    call_count=count,
                    updated_at=now,
                )
            )
        db.commit()

        with patch("backend.services.quota_guard.emit_custom_alert") as emit:
            for _ in range(5):
                decision = consume_api_quota(
                    db,
                    site,
                    provider="search_console",
                    units=10,
                    send_alert_emails=True,
                )
                assert decision.allowed is False
            assert emit.call_count == 0
    finally:
        db.close()
