"""Panel ziyaret günlüğü — gerçek auth girişi, sayfa, özellik, çıkış."""

from datetime import datetime, timedelta

from backend.database import SessionLocal, init_db
from backend.models import PanelVisitLog
from backend.services import panel_visit_log as pvl


def _wipe() -> None:
    init_db()
    with SessionLocal() as db:
        db.query(PanelVisitLog).delete()
        db.commit()


def test_middleware_touch_does_not_open_visit():
    _wipe()
    pvl.touch_visit(
        session_key="m:ghost",
        email="ghost@nokta.com",
        path="/android",
        allow_open=False,
    )
    assert pvl.recent_visits(auth_only=False) == []


def test_open_auth_records_login_and_pages():
    _wipe()
    pvl.open_auth_visit(
        session_key="m:abc",
        email="uye@nokta.com",
        display_name="Üye",
        session_kind="member",
        ip="1.1.1.1",
        device="Chrome",
        path="/android",
    )
    pvl.touch_visit(session_key="m:abc", email="uye@nokta.com", path="/android")
    pvl.touch_visit(session_key="m:abc", email="uye@nokta.com", path="/notification")
    rows = pvl.recent_visits()
    assert len(rows) == 1
    row = rows[0]
    assert row["email"] == "uye@nokta.com"
    assert row["is_open"] is True
    assert row["logged_out_tr"] == "Open"
    assert row["start_reason"] == "auth"
    assert [p["path"] for p in row["pages"]] == ["/android", "/notification"]


def test_feature_activity_appends_to_open_auth_visit():
    _wipe()
    pvl.open_auth_visit(session_key="m:feat", email="f@nokta.com", path="/home")
    assert pvl.record_feature_activity(
        session_key="m:feat",
        feature="nav:android",
        label="android",
        path="/android",
    )
    rows = pvl.recent_visits()
    kinds = [p.get("kind") for p in rows[0]["pages"]]
    assert "feature" in kinds


def test_close_visit_sets_logout_time():
    _wipe()
    pvl.open_auth_visit(session_key="m:out", email="a@nokta.com", path="/")
    pvl.close_visit("m:out", reason="logout")
    rows = pvl.recent_visits()
    assert rows[0]["is_open"] is False
    assert rows[0]["end_reason"] == "logout"
    assert rows[0]["end_label"] == "Logout"
    assert rows[0]["logged_out_tr"] != "Open"


def test_idle_expire_uses_last_seen_as_exit():
    _wipe()
    pvl.open_auth_visit(session_key="m:idle", email="b@nokta.com", path="/ios")
    with SessionLocal() as db:
        row = db.query(PanelVisitLog).filter(PanelVisitLog.session_key == "m:idle").one()
        row.last_seen_at = datetime.utcnow() - timedelta(minutes=45)
        db.commit()
    pvl.expire_idle(idle_minutes=30)
    rows = pvl.recent_visits()
    assert rows[0]["is_open"] is False
    assert rows[0]["end_reason"] == "idle"
    assert rows[0]["end_label"] == "Idle"


def test_api_paths_are_not_logged_as_pages():
    _wipe()
    pvl.open_auth_visit(session_key="m:api", email="c@nokta.com", path="/api/page-tarama/quota")
    pvl.touch_visit(session_key="m:api", email="c@nokta.com", path="/api/page-tarama/quota")
    pvl.touch_visit(session_key="m:api", email="c@nokta.com", path="/home")
    rows = pvl.recent_visits()
    assert [p["path"] for p in rows[0]["pages"]] == ["/home"]


def test_legacy_open_hidden_when_auth_only():
    _wipe()
    pvl.touch_visit(
        session_key="m:legacy",
        email="old@nokta.com",
        path="/",
        allow_open=True,
    )
    assert pvl.recent_visits(auth_only=True) == []
    assert len(pvl.recent_visits(auth_only=False)) == 1


def test_ensure_auth_visit_open_on_first_request():
    _wipe()
    assert pvl.ensure_auth_visit_open(
        session_key="m:req1",
        email="req@nokta.com",
        display_name="Req",
        session_kind="member",
        ip="2.2.2.2",
        device="Firefox",
        path="/settings",
    )
    assert not pvl.ensure_auth_visit_open(session_key="m:req1", email="req@nokta.com", path="/home")
    pvl.touch_visit(session_key="m:req1", email="req@nokta.com", path="/android")
    rows = pvl.recent_visits()
    assert len(rows) == 1
    assert rows[0]["email"] == "req@nokta.com"
    assert rows[0]["is_open"] is True
    assert "/settings" in [p["path"] for p in rows[0]["pages"]]
    assert "/android" in [p["path"] for p in rows[0]["pages"]]


def test_backfill_visit_from_login_event():
    from backend.models import AdminLoginEvent

    _wipe()
    with SessionLocal() as db:
        ev = AdminLoginEvent(
            event_type="member_login_ok",
            ip="78.187.20.15",
            device_label="Masaüstü / Firefox",
            user_agent="Mozilla/5.0 Firefox",
            fingerprint="test-fp",
            actor_email="uye@nokta.com",
        )
        db.add(ev)
        db.commit()
        event_id = int(ev.id)
    rows = pvl.recent_visits()
    assert len(rows) == 1
    assert rows[0]["email"] == "uye@nokta.com"
    assert rows[0]["ip"] == "78.187.20.15"
    assert "Firefox" in rows[0]["device"]
    assert rows[0]["end_reason"] == "sync"
    with SessionLocal() as db:
        row = db.query(PanelVisitLog).filter(PanelVisitLog.session_key == f"login:{event_id}").one()
        assert row.start_reason == "auth"


def test_settings_template_has_visit_log_not_user_logins():
    from pathlib import Path

    text = (Path(__file__).resolve().parents[2] / "templates/settings.html").read_text(encoding="utf-8")
    assert "User logins" not in text
    assert "login_history" not in text
    assert "settings-login-history" not in text
    assert "Visit &amp; activity log" in text or "Visit & activity log" in text
    assert "visit_logs" in text
    assert "mac-bridge-live" in text
    assert "logged_in_tr" in text
    assert "logged_out_tr" in text
    assert "Son Girişler" not in text
    assert "Aktif Oturumlar" not in text
    assert "active_sessions" not in text
