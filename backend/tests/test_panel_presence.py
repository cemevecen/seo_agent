from datetime import datetime

from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS
from backend.services.panel_presence import build_online_presence_api_payload, dedupe_online_users


def test_dedupe_online_users_merges_tabs():
    t1 = datetime(2026, 6, 24, 10, 0, 0)
    t2 = datetime(2026, 6, 24, 10, 5, 0)
    sessions = [
        {
            "email": "onurtorun@nokta.com",
            "label": "Onur",
            "last_seen": t1,
            "last_seen_tr": "10:00",
            "is_current": False,
        },
        {
            "email": "onurtorun@nokta.com",
            "label": "Onur Torun",
            "last_seen": t2,
            "last_seen_tr": "10:05",
            "is_current": True,
        },
        {
            "email": "other@nokta.com",
            "label": "Other",
            "last_seen": t1,
            "last_seen_tr": "10:00",
            "is_current": False,
        },
    ]
    out = dedupe_online_users(sessions)
    assert len(out) == 2
    onur = next(r for r in out if r["email"] == "onurtorun@nokta.com")
    assert onur["is_current"] is True
    assert onur["last_seen_tr"] == "10:05"


def test_dot_green_only_for_non_owner_visitors():
    sessions = [
        {
            "email": "cemevecen@nokta.com",
            "label": "Cem",
            "last_seen": datetime(2026, 6, 24, 10, 0, 0),
            "last_seen_tr": "10:00",
            "is_current": True,
        },
        {
            "email": "onurtorun@nokta.com",
            "label": "Onur",
            "last_seen": datetime(2026, 6, 24, 10, 1, 0),
            "last_seen_tr": "10:01",
        },
    ]
    out = build_online_presence_api_payload(sessions, owner_emails=ADMIN_MEMBER_EMAILS)
    assert out["show"] is True
    assert out["dot_green"] is True
    assert out["visitor_count"] == 1
    assert [u["email"] for u in out["visitors"]] == ["onurtorun@nokta.com"]


def test_two_owners_alone_dot_not_green():
    sessions = [
        {
            "email": "cemevecen@nokta.com",
            "label": "Cem N",
            "last_seen": datetime(2026, 6, 24, 10, 0, 0),
            "is_current": True,
        },
        {
            "email": "cemevecen@gmail.com",
            "label": "Cem G",
            "last_seen": datetime(2026, 6, 24, 10, 2, 0),
            "is_current": False,
        },
    ]
    out = build_online_presence_api_payload(sessions, owner_emails=ADMIN_MEMBER_EMAILS)
    assert out["dot_green"] is False
    assert out["visitor_count"] == 0
    assert out["owners_online_count"] == 2


def test_build_online_presence_includes_any_visitor_email():
    sessions = [
        {
            "email": "cemevecen@nokta.com",
            "label": "Cem",
            "last_seen": datetime(2026, 6, 24, 10, 0, 0),
            "is_current": True,
        },
        {
            "email": "gozdeunaldi@nokta.com",
            "label": "Gözde",
            "last_seen": datetime(2026, 6, 24, 10, 2, 0),
            "is_current": False,
        },
    ]
    out = build_online_presence_api_payload(sessions, owner_emails=ADMIN_MEMBER_EMAILS)
    emails = {u["email"] for u in out["users"]}
    assert emails == {"cemevecen@nokta.com", "gozdeunaldi@nokta.com"}
    assert out["dot_green"] is True


def test_dedupe_skips_sessions_without_email():
    out = dedupe_online_users([{"label": "Admin şifre", "last_seen": datetime.utcnow()}])
    assert out == []
