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
    assert "{{ card.no }}" not in html
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
    assert "@media (max-width: 639px)" in html
    assert "html.dark .pml-heat-up" in html
    assert "html.dark .pml-card" in html
    assert "pml-card" in js
    assert "pml-table-fit" in js
    assert "pml-link" in js
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
    assert mod._matches_query("Döviz.com 3 gündür manipülatif", "doviz.com")
    assert mod._matches_query("https://x.com/i/status/1 doviz com", "doviz.com")
    assert not mod._matches_query("ziraat döviz hesabım bloke", "doviz.com")
    assert not mod._matches_query("sinema bileti kampanyası", "sinemalar.com")
    assert mod._matches_query("sinemalar.com giriş yapamıyorum", "sinemalar.com")
    assert mod._url_has_brand("https://x.com/dovizcom/status/1", "doviz.com")
    assert not mod._url_has_brand("https://x.com/Merkez_Bankasi/status/1", "doviz.com")
    assert mod._sikayet_complaint_url(
        "https://www.sikayetvar.com/dovizcom/dovizcom-3-gundur-manipulatif-oldugunu-dusundugum-icerikle-karsi",
        brand_slug="dovizcom",
    )
    assert not mod._sikayet_complaint_url(
        "https://www.sikayetvar.com/nadir-doviz/nadirgoldda-altin-bozma-isleminde-eksik-gram",
        brand_slug="dovizcom",
    )


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


def test_store_chart_skips_delta_when_top_slice_shifted():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        old_apps = [{"rank": i, "id": f"old-{i}", "name": f"Eski {i}"} for i in range(1, 16)]
        new_apps = [{"rank": i, "id": f"new-{i}", "name": f"Yeni {i}"} for i in range(1, 16)]
        ingest_pm_lab_payload(db, {"sections": {"store_charts": {"ok": True, "charts": [{"id": "ios", "apps": old_apps}]}}})
        ingest_pm_lab_payload(db, {"sections": {"store_charts": {"ok": True, "charts": [{"id": "ios", "apps": new_apps}]}}})
        ctx = page_context(db)
        chart = next(c["data"]["charts"][0] for c in ctx["cards"] if c["id"] == "store_charts")
        assert chart["moves"].get("reset") is True
        assert chart["apps"][0]["delta"] is None
        assert chart["dropped"] == []
    finally:
        db.close()


def test_ios_lockup_id_uses_adam_id():
    import importlib.util

    spec = importlib.util.spec_from_file_location("pm_lab_scrape", Path("scripts/pm_lab_scrape.py"))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    assert mod._ios_lockup_id({"adamId": "521117624", "id": "x"}) == "521117624"
    assert mod._ios_lockup_id({"id": "99"}) == "99"
    assert mod._ios_lockup_id({}) == ""


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
    cnbce = """
BIST 100 13.704,52 -107,08 -0,78%
DOLAR 47,73 0,03 0,08%
EURO 55,11 0,00 0,02%
ALTIN/ONS 4.366,21 -22,95 -0,52%
"""
    cnb = mod._parse_assets_from_text(cnbce)
    assert cnb["bist100"]["value"] == "13.704,52"
    assert cnb["usd"]["value"] == "47,73"
    assert cnb["eur"]["value"] == "55,11"
    assert cnb["ons_altin"]["value"] == "4.366,21"
    bigpara = """
USDTRY Dolar Amerikan Doları Türk Lirası %+0,08 Alış 47,7307 Satış 47,7434
EURTRY Euro Euro Türk Lirası %+0,02 Alış 55,1137 Satış 55,1242
GLDGR Gram Altın Spot ALTIN (TL/GR) %-0,19 Alış 6.717,98 Satış 6.718,91
SGLDC Çeyrek Altın %-0,32 Alış 10.851,00 Satış 10.961,00
XAUUSD Altın ($/ONS) %-0,40 Alış 4.371,11 Satış 4.371,75
SXAGGR Gümüş (TL/GR) 99,4365 %-1,41
"""
    bp = mod._parse_assets_from_text(bigpara)
    assert bp["usd"]["value"] == "47,7307"
    assert bp["eur"]["value"] == "55,1137"
    assert bp["gram_altin"]["value"] == "6.717,98"
    assert bp["ceyrek_altin"]["value"] == "10.851,00"
    assert bp["ons_altin"]["value"] == "4.371,11"
    assert bp["gram_gumus"]["value"] == "99,4365"
    tv = "USDTRY U.S. DOLLAR / TURKISH LIRA 47.732710 +0.13%"
    assert mod._parse_assets_from_text(tv)["usd"]["value"] == "47.732710"
    canli = (
        "USD 47.7431 %0.05 EUR Euro 55.1026 55.1219 %-0.11 GA Gram Altın 6707.93 6708.83 %-0.38 "
        "GBP İngiliz Sterlini 64.2839 64.6062 %0.04 BIST100 13704.52"
    )
    cl = mod._parse_assets_from_text(canli)
    assert cl["usd"]["value"] == "47.7431"
    assert cl["eur"]["value"] == "55.1026"
    assert cl["gram_altin"]["value"] == "6707.93"
    assert cl["bist100"]["value"] == "13704.52"
    cnn = "BIST100 % -0,78 13.705 DOLAR % 0,09 47,7427 EURO % 0,04 55,1261 ALTIN % -0,18 6.719,10 PETROL % 1,19 88,76"
    cn = mod._parse_assets_from_text(cnn)
    assert cn["usd"]["value"] == "47,7427"
    assert cn["eur"]["value"] == "55,1261"
    assert cn["gram_altin"]["value"] == "6.719,10"
    assert cn["brent"]["value"] == "88,76"
    assert cn["bist100"]["value"] == "13.705"
    pairs = mod._parse_assets_from_text("USD/EGP 50,2144 %0,63\nUSD/RUB 82,6201\nUSD 47,7374 %0,08")
    assert pairs["usd"]["value"] == "47,7374"
    lists = mod.SITE_LIST_URLS
    assert "https://www.enuygunfinans.com/doviz-fiyatlari/" in lists["enuygun"]
    assert "https://www.enuygunfinans.com/altin-fiyatlari/" in lists["enuygun"]
    assert "https://www.enuygunfinans.com/borsa/bist-100-hisseleri/" in lists["enuygun"]
    assert "https://www.investing.com/currencies/" in lists["investing"]
    assert "https://tr.investing.com/" in lists["investing"]
    assert "https://bigpara.hurriyet.com.tr/doviz/" in lists["bigpara"]
    assert "https://bigpara.hurriyet.com.tr/altin/" in lists["bigpara"]
    assert "https://bigpara.hurriyet.com.tr/borsa/" in lists["bigpara"]
    assert "https://www.tradingview.com/markets/turkey/" in lists["tradingview"]
    assert "https://www.tradingview.com/markets/currencies/rates-middle-east/" in lists["tradingview"]
    assert "https://www.cnbce.com/doviz" in lists["cnbce"]
    assert "https://finans.cnnturk.com/canli-borsa" in lists["cnnturk"]
    assert mod.TV_SCANNER_SYMBOLS["usd"] == "FX_IDC:USDTRY"
    assert mod.TV_SCANNER_SYMBOLS["bist100"] == "BIST:XU100"


def test_pm_lab_doviz_rank_chip_labels():
    js = Path("static/js/pm_lab.js").read_text(encoding="utf-8")
    html = Path("templates/pm_lab.html").read_text(encoding="utf-8")
    assert "bizim sıra" not in js
    assert "doviz.com: " in js
    assert "doviz.com sıra:" in js
    assert "pm_lab.js?v=13" in html
