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
        with patch.object(aal, "schedule_unknown_login_alert", return_value=True) as mock_alert:
            with patch.object(aal, "_trim_old_events"):
                row = aal.record_access_event(
                    db,
                    event_type="login_ok",
                    ip="203.0.113.9",
                    user_agent="Chrome",
                )
    mock_alert.assert_called_once()
    assert row.alert_sent is True


def test_member_login_alert_includes_email():
    mock_db = MagicMock()
    mock_db.query.return_value.count.return_value = 0
    with patch.object(aal, "_lookup_ip_geo", return_value={}):
        with patch("backend.database.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.return_value = mock_db
            with patch("backend.services.mailer.send_admin_security_email", return_value=True) as mock_send:
                with patch.object(aal.settings, "admin_login_alert_enabled", True):
                    with patch.object(aal.settings, "admin_login_alert_email", "cemevecen@nokta.com"):
                        ok = aal._deliver_unknown_login_alert(
                            ip="78.187.20.15",
                            device_label="Masaüstü / Chrome",
                            user_agent="Mozilla/5.0 Chrome",
                            fingerprint="abc123",
                            event_type="member_login_ok",
                            actor_email="user@nokta.com",
                        )
    assert ok is True
    subject = mock_send.call_args[0][0]
    body = mock_send.call_args[0][1]
    assert subject == "panel girişi - 'user@nokta.com' - '78.187.20.15'"
    assert "user@nokta.com" in body
    assert "78.187.20.15" in body


def test_member_login_fail_record_triggers_alert():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None

    with patch.object(aal, "schedule_unknown_login_alert", return_value=True) as mock_alert:
        with patch.object(aal, "_trim_old_events"):
            row = aal.record_access_event(
                db,
                event_type="member_login_fail",
                ip="203.0.113.9",
                user_agent="Chrome",
                actor_email="outsider@gmail.com",
            )
    mock_alert.assert_called_once()
    assert mock_alert.call_args.kwargs.get("actor_email") == "outsider@gmail.com"
    assert row.alert_sent is True


def test_member_login_fail_alert_subject():
    mock_db = MagicMock()
    mock_db.query.return_value.count.return_value = 0
    with patch.object(aal, "_lookup_ip_geo", return_value={}):
        with patch("backend.database.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.return_value = mock_db
            with patch("backend.services.mailer.send_admin_security_email", return_value=True) as mock_send:
                with patch.object(aal.settings, "admin_login_alert_enabled", True):
                    with patch.object(aal.settings, "admin_login_alert_email", "cemevecen@nokta.com"):
                        ok = aal._deliver_unknown_login_alert(
                            ip="78.187.20.15",
                            device_label="Masaüstü / Chrome",
                            user_agent="Mozilla/5.0 Chrome",
                            fingerprint="abc123",
                            event_type="member_login_fail",
                            actor_email="outsider@gmail.com",
                        )
    assert ok is True
    assert mock_send.call_args[0][0] == "panel girişi başarısız - 'outsider@gmail.com' - '78.187.20.15'"


def test_member_login_record_triggers_alert():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None

    with patch.object(aal, "schedule_unknown_login_alert", return_value=True) as mock_alert:
        with patch.object(aal, "_trim_old_events"):
            row = aal.record_access_event(
                db,
                event_type="member_login_ok",
                ip="203.0.113.9",
                user_agent="Chrome",
                actor_email="colleague@nokta.com",
            )
    mock_alert.assert_called_once()
    assert mock_alert.call_args.kwargs.get("actor_email") == "colleague@nokta.com"
    assert row.alert_sent is True


def test_unknown_login_alert_subject_format():
    mock_db = MagicMock()
    mock_db.query.return_value.count.return_value = 0
    with patch.object(aal, "_lookup_ip_geo", return_value={}):
        with patch("backend.database.SessionLocal") as mock_sl:
            mock_sl.return_value.__enter__.return_value = mock_db
            with patch("backend.services.mailer.send_admin_security_email", return_value=True) as mock_send:
                with patch.object(aal.settings, "admin_login_alert_enabled", True):
                    with patch.object(aal.settings, "admin_login_alert_email", "admin@example.com"):
                        ok = aal._deliver_unknown_login_alert(
                            ip="78.187.20.15",
                            device_label="Masaüstü / Firefox",
                            user_agent="Mozilla/5.0 Firefox",
                            fingerprint="abc123",
                            event_type="login_ok",
                            nav_paths=[{"at_tr": "12:00:01", "label": "Home", "path": "/"}],
                        )
    assert ok is True
    mock_send.assert_called_once()
    subject = mock_send.call_args[0][0]
    body = mock_send.call_args[0][1]
    assert subject == "admin girişi - 'Firefox' - '78.187.20.15'"
    assert "Menü / sayfa gezintisi" in body
    assert "Home" in body


def test_admin_path_label_and_nav():
    assert aal.admin_path_label("/realtime") == "Realtime"
    assert aal.admin_path_label("/ad/app-banner") == "Ad · GA4 banner"
    assert aal.should_track_admin_path("/api/home/realtime") is False
    assert aal.should_track_admin_path("/ga4") is True
    fp = "testfp"
    aal.begin_nav_watch(fp, meta={"ip": "1.1.1.1"})
    aal.record_admin_nav(fp, "/realtime")
    aal.record_admin_nav(fp, "/realtime")
    bucket = aal._pop_nav_watch(fp)
    assert bucket and len(bucket["paths"]) == 1


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
