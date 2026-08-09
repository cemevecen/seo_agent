from datetime import datetime
from unittest.mock import patch

from backend.services import panel_visitor_alerts as pva
from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS


def test_maybe_alert_requires_owner_online():
    pva._last_visitor_alert_at.clear()
    sessions = {
        "v1": {
            "email": "onur@nokta.com",
            "label": "Onur",
            "ip": "1.2.3.4",
            "device": "Mac",
            "user_agent": "Test",
            "first_seen": datetime.utcnow(),
            "last_seen": datetime.utcnow(),
        }
    }
    with patch.object(pva, "send_admin_security_email", create=True):
        with patch("backend.services.mailer.send_admin_security_email") as send:
            send.return_value = True
            ok = pva.maybe_alert_visitor_joined(
                sessions,
                email="onur@nokta.com",
                session=sessions["v1"],
                owner_emails=ADMIN_MEMBER_EMAILS,
            )
            assert ok is False
            send.assert_not_called()


def test_maybe_alert_when_owner_online():
    pva._last_visitor_alert_at.clear()
    now = datetime.utcnow()
    sessions = {
        "o1": {
            "email": "cemevecen@nokta.com",
            "last_seen": now,
            "first_seen": now,
        },
        "v1": {
            "email": "onur@nokta.com",
            "label": "Onur",
            "ip": "1.2.3.4",
            "device": "Mac",
            "user_agent": "TestUA",
            "first_seen": now,
            "last_seen": now,
        },
    }
    with patch("backend.services.mailer.send_admin_security_email", return_value=True) as send:
        ok = pva.maybe_alert_visitor_joined(
            sessions,
            email="onur@nokta.com",
            session=sessions["v1"],
            owner_emails=ADMIN_MEMBER_EMAILS,
        )
        assert ok is True
        send.assert_called_once()
        assert send.call_args[0][0] == "PC panel bildirimi"
        assert "cemevecen@nokta.com" in send.call_args[0][2]


def test_usage_summary_subject_fixed():
    with patch("backend.services.mailer.send_admin_security_email", return_value=True) as send:
        ok = pva.send_usage_summary_email(
            email="onur@nokta.com",
            display_name="Onur",
            session={
                "ip": "1.1.1.1",
                "device": "iPhone",
                "user_agent": "Safari",
                "first_seen": datetime(2026, 6, 24, 10, 0, 0),
                "last_seen": datetime(2026, 6, 24, 11, 0, 0),
                "paths": [{"path": "/realtime", "label": "Realtime", "at_tr": "10:05:00"}],
            },
        )
        assert ok is True
        assert send.call_args[0][0] == "PC kullanım özeti"
        body = send.call_args[0][1]
        assert "onur@nokta.com" in body
        assert "/realtime" in body
