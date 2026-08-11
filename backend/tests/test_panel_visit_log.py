"""Panel ziyaret günlüğü — giriş, sayfa, çıkış."""

from datetime import datetime, timedelta

from backend.database import SessionLocal, init_db
from backend.models import PanelVisitLog
from backend.services import panel_visit_log as pvl


def _wipe() -> None:
    init_db()
    with SessionLocal() as db:
        db.query(PanelVisitLog).delete()
        db.commit()


def test_touch_records_login_and_pages():
    _wipe()
    pvl.touch_visit(
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
    assert [p["path"] for p in row["pages"]] == ["/android", "/notification"]


def test_close_visit_sets_logout_time():
    _wipe()
    pvl.touch_visit(session_key="m:out", email="a@nokta.com", path="/")
    pvl.close_visit("m:out", reason="logout")
    rows = pvl.recent_visits()
    assert rows[0]["is_open"] is False
    assert rows[0]["end_reason"] == "logout"
    assert rows[0]["end_label"] == "Logout"
    assert rows[0]["logged_out_tr"] != "Open"


def test_idle_expire_uses_last_seen_as_exit():
    _wipe()
    pvl.touch_visit(session_key="m:idle", email="b@nokta.com", path="/ios")
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
    pvl.touch_visit(session_key="m:api", email="c@nokta.com", path="/api/page-tarama/quota")
    pvl.touch_visit(session_key="m:api", email="c@nokta.com", path="/home")
    rows = pvl.recent_visits()
    assert [p["path"] for p in rows[0]["pages"]] == ["/home"]


def test_settings_template_has_visit_log():
    from pathlib import Path

    text = (Path(__file__).resolve().parents[2] / "templates/settings.html").read_text(encoding="utf-8")
    assert "Visit log" in text
    assert "visit_logs" in text
    assert "logged_in_tr" in text
    assert "logged_out_tr" in text
    assert "Son Girişler" not in text
    assert "Aktif Oturumlar" not in text
    assert "login_history" not in text
    assert "active_sessions" not in text
