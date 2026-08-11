"""Owner PM lab — erişim ve payload birleştirme."""

from pathlib import Path

from backend.database import Base, SessionLocal, engine
from backend.services.pm_lab_access import (
    is_pm_lab_allowed_email,
    is_pm_lab_path,
    resolve_pm_lab_visible,
)
from backend.services.pm_lab_store import SECTION_DEFS, ingest_pm_lab_payload, page_context


def test_owner_emails_only():
    assert is_pm_lab_allowed_email("cemevecen@nokta.com")
    assert is_pm_lab_allowed_email("CEMEVECEN@Gmail.com")
    assert is_pm_lab_allowed_email("cemevecen@example.com")
    assert not is_pm_lab_allowed_email("onur@nokta.com")
    assert not is_pm_lab_allowed_email("")
    assert resolve_pm_lab_visible(member_email="ops@nokta.com") is False
    assert resolve_pm_lab_visible(member_email="cemevecen@gmail.com") is True


def test_paths():
    assert is_pm_lab_path("/pm-lab")
    assert is_pm_lab_path("/pm-lab/image/serp/dolar_p1")
    assert is_pm_lab_path("/api/pm-lab/state")
    assert not is_pm_lab_path("/api/pm-lab/ingest")
    assert not is_pm_lab_path("/settings")


def test_ten_closed_sections():
    assert len(SECTION_DEFS) == 10
    assert [s["no"] for s in SECTION_DEFS] == [2, 3, 7, 9, 10, 11, 12, 14, 15, 17]


def test_ingest_merges_sections():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        ingest_pm_lab_payload(
            db,
            {
                "sections": {
                    "serp": {"ok": True, "summary": "a", "shots": {"x": "YQ=="}},
                },
                "source": "test",
            },
        )
        ingest_pm_lab_payload(
            db,
            {
                "sections": {
                    "serp": {"summary": "b", "shots": {"y": "Yg=="}},
                    "competitors": {"ok": False, "message": "yok"},
                }
            },
        )
        ctx = page_context(db)
        by_id = {c["id"]: c for c in ctx["cards"]}
        assert by_id["serp"]["summary"] == "b"
        assert by_id["serp"]["ok"] is True
        assert set(by_id["serp"]["shot_names"]) == {"x", "y"}
        assert by_id["competitors"]["ok"] is False
        assert by_id["google_news"]["ok"] is None
    finally:
        db.close()


def test_template_dropdowns_start_closed():
    html = Path("templates/pm_lab.html").read_text(encoding="utf-8")
    assert html.count("<details") >= 1
    assert "<details open" not in html
    assert "pm_lab_visible" not in html  # nav is in base; page itself is gated
