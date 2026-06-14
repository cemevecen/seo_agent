"""OAuth bağlantı kopması e-posta uyarıları."""

from unittest.mock import patch

from backend.database import Base, SessionLocal, engine
from backend.models import NotificationDeliveryLog, Site
from backend.services.connection_alerts import (
    clear_oauth_connection_alert,
    collect_broken_connections,
    notify_oauth_connection_event,
)


def test_notify_oauth_connection_event_dedupes():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        with patch("backend.services.connection_alerts.send_admin_security_email", return_value=True) as mock_send:
            first = notify_oauth_connection_event(
                db,
                notification_key="inbox:gmail",
                integration="Gmail Inbox",
                title="Test",
                detail="Token revoked",
            )
            second = notify_oauth_connection_event(
                db,
                notification_key="inbox:gmail",
                integration="Gmail Inbox",
                title="Test",
                detail="Token revoked",
            )
        assert first is True
        assert second is False
        assert mock_send.call_count == 1
        assert db.query(NotificationDeliveryLog).filter_by(notification_key="inbox:gmail").count() == 1
    finally:
        db.query(NotificationDeliveryLog).delete()
        db.commit()
        db.close()


def test_clear_oauth_connection_alert_allows_resend():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        db.add(
            NotificationDeliveryLog(
                notification_type="oauth_connection",
                notification_key="search_console:site:1",
                subject="x",
                recipient="a@b.com",
            )
        )
        db.commit()
        clear_oauth_connection_alert(db, "search_console:site:1")
        assert db.query(NotificationDeliveryLog).filter_by(notification_key="search_console:site:1").count() == 0
    finally:
        db.query(NotificationDeliveryLog).delete()
        db.commit()
        db.close()


def test_collect_broken_connections_sc_reauth():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        site = Site(domain="doviz.com", display_name="doviz.com", is_active=True)
        db.add(site)
        db.commit()
        db.refresh(site)
        with patch(
            "backend.services.connection_alerts.get_search_console_connection_status",
            return_value={"requires_reauth": True, "label": "OAuth yeniden bağlanmalı"},
        ), patch(
            "backend.services.connection_alerts.get_ga4_connection_status",
            return_value={"connected": True, "properties": {}},
        ), patch(
            "backend.services.connection_alerts.inbox_oauth_is_configured",
            return_value=False,
        ):
            items = collect_broken_connections(db)
        assert len(items) == 1
        assert items[0]["notification_key"] == f"search_console:site:{site.id}"
    finally:
        db.query(Site).delete()
        db.commit()
        db.close()
