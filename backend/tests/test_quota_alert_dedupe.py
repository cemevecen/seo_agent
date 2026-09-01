"""Quota alert dedupe — farkli mesaj olsa bile ayni pencerede tek mail."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

from backend.database import SessionLocal, init_db
from backend.models import Alert, AlertLog, Site
from backend.services.alert_engine import emit_custom_alert


def _fresh_db():
    init_db()
    return SessionLocal()


def _site(db) -> Site:
    site = Site(
        domain=f"quota-dedupe-{uuid4().hex[:8]}.example.test",
        display_name="quota-dedupe",
        is_active=True,
    )
    db.add(site)
    db.commit()
    db.refresh(site)
    return site


def test_quota_alert_never_emails_when_disabled():
    db = _fresh_db()
    try:
        site = _site(db)
        with patch("backend.services.alert_engine.send_email") as send_mail:
            out = emit_custom_alert(
                db,
                site,
                "quota_search_console_hard_limit",
                "test mesaj",
                dedupe_hours=24,
                send_mail=True,
            )
            assert out is not None
            send_mail.assert_not_called()
            assert out.sent_mail is False
    finally:
        db.close()


def test_quota_alert_dedupes_within_window_even_if_message_differs():
    db = _fresh_db()
    try:
        site = _site(db)
        now = datetime.utcnow()
        alert = Alert(site_id=site.id, alert_type="quota_search_console_hard_limit", threshold=0.0, is_active=True)
        db.add(alert)
        db.commit()
        db.refresh(alert)
        db.add(
            AlertLog(
                alert_id=alert.id,
                domain=site.domain,
                triggered_at=now - timedelta(hours=1),
                message="ilk mesaj 80/80",
                sent_mail=True,
            )
        )
        db.commit()

        with patch("backend.services.alert_engine.send_email") as send_mail:
            out = emit_custom_alert(
                db,
                site,
                "quota_search_console_hard_limit",
                "farkli mesaj 80/80",
                dedupe_hours=24,
                send_mail=True,
            )
            assert out is None
            send_mail.assert_not_called()
    finally:
        db.close()
