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
    assert 'concat(["Toplam"])' in js
    assert "missRank" in js
    assert 'label: "X"' in js
    assert "function tabs(labels, onPick, active)" in js
    assert "rankDeltaHtml" in js
    assert any(s["title"] == "x - ekşi - şikayetvar" for s in SECTION_DEFS)
    assert "doviz.com · sinemalar.com · her kaynaktan son 10" in next(
        s["hint"] for s in SECTION_DEFS if s["id"] == "sikayet"
    )


def test_mention_query_match_requires_brand_string():
    import importlib.util

    spec = importlib.util.spec_from_file_location("pm_lab_scrape", Path("scripts/pm_lab_scrape.py"))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    assert mod._matches_query("doviz.com uygulaması donuyor", "doviz.com")
    assert mod._matches_query("https://x.com/i/status/1 doviz com", "doviz.com")
    assert not mod._matches_query("ziraat döviz hesabım bloke", "doviz.com")
    assert not mod._matches_query("sinema bileti kampanyası", "sinemalar.com")
    assert mod._matches_query("sinemalar.com giriş yapamıyorum", "sinemalar.com")


def test_store_chart_rank_deltas():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        ingest_pm_lab_payload(
            db,
            {
                "sections": {
                    "store_charts": {
                        "ok": True,
                        "charts": [
                            {
                                "id": "ios",
                                "apps": [
                                    {"rank": 4, "id": "465599322", "name": "Döviz"},
                                    {"rank": 10, "id": "111", "name": "Eski"},
                                ],
                            }
                        ],
                    }
                }
            },
        )
        ingest_pm_lab_payload(
            db,
            {
                "sections": {
                    "store_charts": {
                        "ok": True,
                        "charts": [
                            {
                                "id": "ios",
                                "apps": [
                                    {"rank": 2, "id": "465599322", "name": "Döviz"},
                                    {"rank": 12, "id": "222", "name": "Yeni"},
                                ],
                            }
                        ],
                    }
                }
            },
        )
        ctx = page_context(db)
        chart = next(c["data"]["charts"][0] for c in ctx["cards"] if c["id"] == "store_charts")
        by_id = {a["id"]: a for a in chart["apps"]}
        assert by_id["465599322"]["delta"] == "up"
        assert by_id["465599322"]["delta_n"] == 2
        assert by_id["222"]["delta"] == "new"
        assert chart["dropped"][0]["id"] == "111"
        assert chart["moves"]["up"] == 1
    finally:
        db.close()


def test_competitor_parser_keeps_distinct_asset_prices():
    import importlib.util

    spec = importlib.util.spec_from_file_location("pm_lab_scrape", Path("scripts/pm_lab_scrape.py"))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    text = """
GRAM ALTIN 6.734,49  %0,04  (2,69)
DOLAR 47,7396  %0,08  (0,0382)
EURO 55,1249  %0,03  (0,0165)
BIST 100 13.704,52  %-0,78  (-107,74)
GRAM GÜMÜŞ 99,60  %-1,25  (-1,26)
BRENT $88,28  %0,64  ($0,56)
ÇEYREK ALTIN 11.104,62 %0,24
ONS ALTIN 4.397,56 %0,18
HAREM GRAM ALTIN 6.736,38
"""
    found = mod._parse_assets_from_text(text)
    assert found["usd"]["value"] == "47,7396"
    assert found["eur"]["value"] == "55,1249"
    assert found["gram_altin"]["value"] == "6.734,49"
    assert found["bist100"]["value"] == "13.704,52"
    assert found["gram_gumus"]["value"] == "99,60"
    assert found["brent"]["value"] == "88,28"
    assert found["ceyrek_altin"]["value"] == "11.104,62"
    assert found["ons_altin"]["value"] == "4.397,56"
    assert found["harem_gram_altin"]["value"] == "6.736,38"
    assert len({v["value"] for v in found.values()}) == len(found)
    mixed = mod._parse_assets_from_text("GRAM GÜMÜŞ\nDOLAR 47,7396  %0,08\nGRAM GÜMÜŞ 99,60")
    assert mixed["usd"]["value"] == "47,7396"
    assert mixed["gram_gumus"]["value"] == "99,60"
