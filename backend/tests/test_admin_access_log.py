"""Admin giriş geçmişi ve tanıdık cihaz testleri."""

from unittest.mock import MagicMock, patch

from backend.services import admin_access_log as aal


def test_device_fingerprint_stable():
    a = aal.device_fingerprint("1.2.3.4", "Mozilla/5.0 Firefox")
    b = aal.device_fingerprint("1.2.3.4", "Mozilla/5.0 Firefox")
    c = aal.device_fingerprint("9.9.9.9", "Mozilla/5.0 Firefox")
    assert a == b
    assert a != c


def test_first_login_auto_trust_no_alert():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    db.query.return_value.first.return_value = None  # no trusted devices yet

    with patch.object(aal, "trust_fingerprint") as mock_trust:
        with patch.object(aal, "_send_unknown_login_alert") as mock_alert:
            with patch.object(aal, "_trim_old_events"):
                aal.record_access_event(
                    db,
                    event_type="login_ok",
                    ip="78.187.20.15",
                    user_agent="Firefox",
                )
    mock_trust.assert_called_once()
    mock_alert.assert_not_called()


def test_unknown_device_sends_alert():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    db.query.return_value.first.return_value = MagicMock()  # has trusted devices

    with patch.object(aal, "is_trusted", return_value=False):
        with patch.object(aal, "_send_unknown_login_alert", return_value=True) as mock_alert:
            with patch.object(aal, "_trim_old_events"):
                row = aal.record_access_event(
                    db,
                    event_type="login_ok",
                    ip="203.0.113.9",
                    user_agent="Chrome",
                )
    mock_alert.assert_called_once()
    assert row.alert_sent is True


def test_enrich_active_session_uses_tr_timezone():
    from datetime import datetime

    utc_dt = datetime(2026, 5, 23, 20, 47, 0)
    session = {
        "ip": "78.187.20.15",
        "device": "Masaüstü / Firefox",
        "user_agent": "Firefox",
        "first_seen": utc_dt,
        "last_seen": utc_dt,
    }
    out = aal.enrich_active_session(
        session,
        trusted_fps=set(),
        current_key="abc",
        session_key="xyz",
    )
    assert out["first_seen_tr"] == "23.05.2026 23:47"
    assert out["last_seen_tr"] == "23.05.2026 23:47"
