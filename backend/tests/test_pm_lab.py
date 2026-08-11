"""Owner PM lab — erişim, geçmiş birleştirme, şablon."""

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
    assert not is_pm_lab_allowed_email("onur@nokta.com")
    assert resolve_pm_lab_visible(member_email="cemevecen@gmail.com") is True


def test_paths():
    assert is_pm_lab_path("/pm-lab")
    assert is_pm_lab_path("/api/pm-lab/state")
    assert not is_pm_lab_path("/api/pm-lab/ingest")


def test_five_live_sections():
    assert [s["id"] for s in SECTION_DEFS] == [
        "serp",
        "competitors",
        "sikayet",
        "store_charts",
        "google_news",
    ]
    assert [s["no"] for s in SECTION_DEFS] == [2, 3, 9, 12, 17]


def test_ingest_serp_history_and_no_shots():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        ingest_pm_lab_payload(
            db,
            {
                "sections": {
                    "serp": {
                        "ok": True,
                        "summary": "a",
                        "keywords": [
                            {
                                "keyword": "gram altın",
                                "rows": [
                                    {
                                        "rank": 3,
                                        "domain": "www.doviz.com",
                                        "title": "eski",
                                        "url": "https://www.doviz.com/",
                                    }
                                ],
                            }
                        ],
                        "shots": {"x": "YQ=="},
                    }
                }
            },
        )
        ingest_pm_lab_payload(
            db,
            {
                "sections": {
                    "serp": {
                        "ok": True,
                        "summary": "b",
                        "keywords": [
                            {
                                "keyword": "gram altın",
                                "rows": [
                                    {
                                        "rank": 1,
                                        "domain": "www.doviz.com",
                                        "title": "yeni",
                                        "url": "https://www.doviz.com/",
                                    },
                                    {
                                        "rank": 2,
                                        "domain": "bigpara.hurriyet.com.tr",
                                        "title": "bp",
                                        "url": "https://bigpara.hurriyet.com.tr/",
                                    },
                                ],
                            }
                        ],
                    }
                }
            },
        )
        ctx = page_context(db)
        by_id = {c["id"]: c for c in ctx["cards"]}
        assert len(ctx["cards"]) == 5
        serp = by_id["serp"]["data"]
        assert "shots" not in serp
        rows = serp["keywords"][0]["rows"]
        ours = next(r for r in rows if "doviz.com" in r["domain"])
        assert ours["delta"] == "up"
        assert ours["delta_n"] == 2
        newbie = next(r for r in rows if "bigpara" in r["domain"])
        assert newbie["delta"] == "new"
        assert len(serp["runs"]) == 2
        assert "boot_json" in ctx
        assert "gram altın" in ctx["boot_json"]
    finally:
        db.close()


def test_template_has_no_photos_and_js_shell():
    html = Path("templates/pm_lab.html").read_text(encoding="utf-8")
    assert "<img" not in html
    assert "shot_grid" not in html
    assert "pm_lab.js" in html
    assert "data-pml" in html
    js = Path("static/js/pm_lab.js").read_text(encoding="utf-8")
    assert "renderSerp" in js
    assert "renderCompetitors" in js
