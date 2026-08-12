from datetime import datetime

from backend.database import SessionLocal, init_db
from backend.models import AppMember, PanelVisitLog
from backend.services import app_member_auth as ama
from backend.services import panel_visit_log as pvl


def test_nokta_email_allowed():
    assert ama.is_email_eligible_for_membership("user@nokta.com") is True


def test_gmail_exception_allowed():
    assert ama.is_email_eligible_for_membership("cemevecen@gmail.com") is True


def test_other_gmail_rejected():
    assert ama.is_email_eligible_for_membership("other@gmail.com") is False


def test_tmdb_only_gmail_allowed():
    assert ama.is_email_eligible_for_membership("berendemirci@gmail.com") is True
    assert ama.is_tmdb_only_member_email("berendemirci@gmail.com") is True
    assert ama.prelogin_access_note("berendemirci@gmail.com") == "tmdb-only"


def test_redirect_mismatch_message():
    msg = ama.format_member_oauth_login_error("redirect_uri_mismatch", request=None)
    assert "redirect_uri_mismatch" in msg
    assert "/auth/google/callback" in msg


def test_oauth_prompt_first_visit():
    from unittest.mock import MagicMock

    req = MagicMock()
    req.cookies = {}
    assert ama.member_oauth_authorization_extra_params(req) == {"prompt": "select_account"}


def test_oauth_prompt_returning_browser():
    from unittest.mock import MagicMock

    req = MagicMock()
    req.cookies = {ama.PANEL_MEMBER_SEEN_COOKIE: "1"}
    assert ama.member_oauth_authorization_extra_params(req) == {}


def test_online_presence_visible_only_for_cem_accounts():
    from unittest.mock import MagicMock

    from backend.models import AppMember

    req = MagicMock()
    req.cookies = {}
    assert ama.can_view_online_presence(req) is False

    with __import__("unittest").mock.patch.object(
        ama, "member_from_request", return_value=AppMember(email="cemevecen@nokta.com")
    ):
        assert ama.can_view_online_presence(req) is True

    with __import__("unittest").mock.patch.object(
        ama, "member_from_request", return_value=AppMember(email="onurtorun@nokta.com")
    ):
        assert ama.can_view_online_presence(req) is False


def test_member_list_shows_pending_tmdb_only_before_first_login():
    from unittest.mock import MagicMock

    db = MagicMock()
    db.query.return_value.order_by.return_value.all.return_value = []
    out = ama.member_list_payload(db)
    gozde = next((r for r in out if r["email"] == "gozdeunaldi@nokta.com"), None)
    assert gozde is not None
    assert gozde["pending_first_login"] is True
    assert gozde["access_note"] == "tmdb-only"
    melih = next((r for r in out if r["email"] == "melihengin@nokta.com"), None)
    assert melih is not None
    assert melih["pending_first_login"] is True
    assert melih["access_note"] == "invited"


def test_member_last_login_uses_later_panel_visit():
    init_db()
    email = "mertkaradeniz@nokta.com"
    stale = datetime(2026, 8, 3, 8, 0, 0)
    fresh = datetime(2026, 8, 10, 12, 0, 0)
    with SessionLocal() as db:
        db.query(PanelVisitLog).filter(PanelVisitLog.email == email).delete()
        db.query(AppMember).filter(AppMember.email == email).delete()
        db.commit()
        db.add(
            AppMember(
                email=email,
                google_sub="sub-mert",
                display_name="Mert",
                role="member",
                is_active=True,
                created_at=stale,
                last_login_at=stale,
            )
        )
        db.add(
            PanelVisitLog(
                session_key="m:mert-test",
                email=email,
                display_name="Mert",
                session_kind="member",
                logged_in_at=fresh,
                last_seen_at=fresh,
                start_reason="auth",
            )
        )
        db.commit()
        payload = ama.member_list_payload(db)
        row = next(r for r in payload if r["email"] == email)
        assert row["pending_first_login"] is False
        assert "10.08.2026" in row["last_login_at_tr"]
        stored = db.query(AppMember).filter(AppMember.email == email).one()
        assert stored.last_login_at == fresh


def test_new_panel_visit_bumps_member_last_login():
    init_db()
    email = "visit-bump@nokta.com"
    stale = datetime(2026, 8, 3, 8, 0, 0)
    with SessionLocal() as db:
        db.query(PanelVisitLog).filter(PanelVisitLog.email == email).delete()
        db.query(AppMember).filter(AppMember.email == email).delete()
        db.commit()
        db.add(
            AppMember(
                email=email,
                google_sub="sub-visit",
                role="member",
                is_active=True,
                created_at=stale,
                last_login_at=stale,
            )
        )
        db.commit()
    pvl.open_auth_visit(session_key="m:visit-bump", email=email, path="/settings")
    with SessionLocal() as db:
        stored = db.query(AppMember).filter(AppMember.email == email).one()
        assert stored.last_login_at > stale
