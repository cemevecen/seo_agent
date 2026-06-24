from datetime import datetime

from backend.services.panel_presence import dedupe_online_users


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


def test_dedupe_skips_sessions_without_email():
    out = dedupe_online_users([{"label": "Admin şifre", "last_seen": datetime.utcnow()}])
    assert out == []
