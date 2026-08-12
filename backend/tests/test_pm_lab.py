"""Owner PM lab — erişim, geçmiş birleştirme, şablon."""

from pathlib import Path

from backend.database import Base, SessionLocal, engine
from backend.services.pm_lab_access import (
    is_pm_lab_allowed_email,
    is_pm_lab_path,
    resolve_pm_lab_visible,
)
from backend.services.pm_lab_store import (
    COMPETITORS_INTERVAL_MIN,
    SECTION_DEFS,
    _pm_lab_page_specs,
    _prune_refresh_state,
    claim_pm_lab_refresh,
    enqueue_pm_lab_refresh,
    format_pm_lab_when,
    ingest_pm_lab_payload,
    page_context,
)


def test_owner_emails_only():
    assert is_pm_lab_allowed_email("cemevecen@nokta.com")
    assert is_pm_lab_allowed_email("CEMEVECEN@Gmail.com")
    assert not is_pm_lab_allowed_email("onur@nokta.com")
    assert resolve_pm_lab_visible(member_email="cemevecen@gmail.com") is True


def test_paths():
    assert is_pm_lab_path("/pm-lab")
    assert is_pm_lab_path("/api/pm-lab/state")
    assert is_pm_lab_path("/api/pm-lab/refresh")
    assert not is_pm_lab_path("/api/pm-lab/ingest")
    assert not is_pm_lab_path("/api/pm-lab/claim-refresh")


def test_live_sections():
    assert [s["id"] for s in SECTION_DEFS] == [
        "serp",
        "competitors",
        "store_charts",
        "google_news",
    ]
    assert [s["no"] for s in SECTION_DEFS] == [2, 3, 12, 17]
    assert [s["title"] for s in SECTION_DEFS] == [
        "SERP — first 4 pages",
        "Competitor FX price comparison",
        "Play / App Store category charts",
        "Google News showcase",
    ]
    assert not any(s["id"] == "sikayet" for s in SECTION_DEFS)
    assert not any("şikayetvar" in (s.get("title") or "").lower() for s in SECTION_DEFS)


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
        assert len(ctx["cards"]) == 3
        assert "store_charts" not in by_id
        assert "sikayet" not in by_id
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
    assert 'concat(["Total"])' in js
    assert "missRank" in js
    assert "function tabs(labels, onPick, active)" in js
    assert "rankDeltaHtml" in js
    assert "@media (max-width: 639px)" in html
    assert "html.dark .pml-heat-up" in html
    assert "pml-card-kicker" not in js
    assert "renderSikayet" not in js
    assert "pml-table-fit" in js
    assert "pml-link" in js
    assert not any(s["title"] == "x - ekşi - şikayetvar" for s in SECTION_DEFS)
    assert "doviz.com · sinemalar.com · her kaynaktan son 10" not in html
    assert "Fotoğraf yok" not in html
    assert "card.hint" not in html
    assert "pml-updated" in html
    assert "pml-refresh" in html
    assert "data-pml-refresh" in html
    assert "pml-card-head" in html
    assert "pml-refresh absolute" not in html
    assert "/api/pm-lab/refresh" in js
    assert format_pm_lab_when("2026-08-11T15:26:00+00:00") == "11.08.2026 18:26"


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
        store = __import__("json").loads(ctx["boot_json"])["sections"]["store_charts"]
        chart = store["charts"][0]
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
        store = __import__("json").loads(ctx["boot_json"])["sections"]["store_charts"]
        chart = store["charts"][0]
        assert chart["moves"].get("reset") is True
        assert chart["apps"][0]["delta"] is None
        assert chart["dropped"] == []
    finally:
        db.close()


def test_store_icons_remembered_across_ingest():
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
                                "id": "android",
                                "apps": [
                                    {
                                        "rank": 1,
                                        "id": "com.doviz.android",
                                        "name": "Döviz",
                                        "icon": "https://example.com/doviz.png",
                                    }
                                ],
                            },
                            {
                                "id": "ios",
                                "apps": [
                                    {
                                        "rank": 1,
                                        "id": "465599322",
                                        "name": "Döviz",
                                        "icon": "https://example.com/doviz-ios.png",
                                    }
                                ],
                            },
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
                                "id": "android",
                                "apps": [{"rank": 2, "id": "com.doviz.android", "name": "Döviz"}],
                            },
                            {
                                "id": "ios",
                                "apps": [{"rank": 3, "id": "465599322", "name": "Döviz"}],
                            },
                        ],
                    }
                }
            },
        )
        ctx = page_context(db)
        store = __import__("json").loads(ctx["boot_json"])["sections"]["store_charts"]
        by_plat = {c["id"]: c for c in store["charts"]}
        assert by_plat["android"]["apps"][0]["icon"] == "https://example.com/doviz.png"
        assert by_plat["ios"]["apps"][0]["icon"] == "https://example.com/doviz-ios.png"
        assert store["icon_map"]["android:com.doviz.android"] == "https://example.com/doviz.png"
    finally:
        db.close()


def test_store_split_layout_in_js():
    js = Path("static/js/pm_lab.js").read_text(encoding="utf-8")
    html = Path("templates/pm_lab.html").read_text(encoding="utf-8")
    assert "pml-store-split" in js
    assert "pml-store-split" in html
    assert "chartById(charts, \"android\")" in js
    assert "appNameHtml" in js
    assert "pml-app-icon" in js
    assert "referrerpolicy" in js
    assert "platIconHtml" in js
    assert "pml-plat-icon-android" in js
    assert "pml-plat-icon-ios" in js
    assert "pml-col-title" in html


def test_hydrate_store_icons_fills_missing(monkeypatch):
    from backend.services import pm_lab_store

    store = {
        "charts": [
            {"id": "android", "apps": [{"id": "com.doviz.android", "name": "Döviz"}]},
            {"id": "ios", "apps": [{"id": "465599322", "name": "Döviz"}]},
        ]
    }
    monkeypatch.setattr(
        pm_lab_store,
        "_itunes_artwork",
        lambda ids: {"465599322": "https://is1.mzstatic.com/doviz.png"},
    )
    monkeypatch.setattr(
        pm_lab_store,
        "_play_artwork",
        lambda pkgs, budget_s=10.0: {"com.doviz.android": "https://play-lh.googleusercontent.com/doviz.png"},
    )
    n = pm_lab_store._hydrate_store_icons(store, fetch=True)
    assert n == 2
    by_plat = {c["id"]: c for c in store["charts"]}
    assert by_plat["ios"]["apps"][0]["icon"].endswith("doviz.png")
    assert "play-lh.googleusercontent.com" in by_plat["android"]["apps"][0]["icon"]
    assert store["icon_map"]["ios:465599322"].endswith("doviz.png")


def test_enqueue_and_claim_pm_lab_refresh():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        while claim_pm_lab_refresh(db):
            pass
        out = enqueue_pm_lab_refresh(db, "google_news")
        assert out["queued"] == ["google_news"]
        enqueue_pm_lab_refresh(db, "google_news")
        enqueue_pm_lab_refresh(db, "competitors")
        assert claim_pm_lab_refresh(db) == "google_news"
        assert claim_pm_lab_refresh(db) == "competitors"
        assert claim_pm_lab_refresh(db) is None
    finally:
        db.close()


def test_stale_refresh_queue_is_pruned():
    data = {
        "refresh_queue": [
            {"job": "competitors", "requested_at": "2020-01-01T00:00:00+00:00"},
        ],
        "refresh_running": "competitors",
        "refresh_running_at": "2020-01-01T00:00:00+00:00",
    }
    assert _prune_refresh_state(data) is True
    assert data["refresh_queue"] == []
    assert data["refresh_running"] == ""


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
    assert "harem_gram_altin" not in found
    assert "kapalicarsi_gram_altin" not in {a["id"] for a in mod.ASSETS}
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
    assert "https://bigpara.hurriyet.com.tr/borsa/endeksler/" in lists["bigpara"]
    assert "https://bigpara.hurriyet.com.tr/emtia/" in lists["bigpara"]
    assert "https://www.bloomberght.com/emtia" in lists["bloomberght"]
    assert "https://www.cnbce.com/emtia" in lists["cnbce"]
    ons = mod._parse_assets_from_text("XAUUSD Altın (ONS) Altın / Dolar 4.369,28 4.369,87 %-0,45")
    assert ons["ons_altin"]["value"] == "4.369,28"
    assert mod._to_float("4,431.09") == 4431.09
    assert mod._to_float("4.369,28") == 4369.28
    assert "https://www.tradingview.com/markets/turkey/" in lists["tradingview"]
    assert "https://www.tradingview.com/markets/currencies/rates-middle-east/" in lists["tradingview"]
    assert "https://www.cnbce.com/doviz" in lists["cnbce"]
    assert "https://finans.cnnturk.com/canli-borsa" in lists["cnnturk"]
    assert mod.TV_SCANNER_SYMBOLS["usd"] == "FX_IDC:USDTRY"
    assert mod.TV_SCANNER_SYMBOLS["bist100"] == "BIST:XU100"
    assert any(s["id"] == "foreks" for s in mod.SITES)
    assert "https://www.foreks.com/doviz/" in lists["foreks"]
    assert "https://www.foreks.com/altin/" in lists["foreks"]
    assert "https://www.foreks.com/emtia/" in lists["foreks"]
    assert mod.FOREKS_FIELDS["usd"] == "o10_l"
    assert mod.FOREKS_FIELDS["gram_gumus"] == "o16_l"
    assert mod.FOREKS_FIELDS["brent"] == "o2627_l"
    assert [a["id"] for a in mod.ASSETS] == [
        "usd",
        "eur",
        "gram_altin",
        "ceyrek_altin",
        "ons_altin",
        "gram_gumus",
        "bitcoin",
        "brent",
        "bist100",
    ]
    assert mod.FOREKS_FIELDS["bitcoin"] == "o1836_l"
    assert mod.TV_SCANNER_SYMBOLS["bitcoin"] == "BITSTAMP:BTCUSD"
    assert any(s["id"] == "paratic" for s in mod.SITES)
    labels = {s["id"]: s["label"] for s in mod.SITES}
    assert labels["enuygun"] == "Enuygun"
    assert labels["bloomberght"] == "Bloomberg"
    assert labels["tradingview"] == "Trading"
    assert labels["cnnturk"] == "CNN"
    assets = mod.ASSET_URLS
    assert assets["paratic"]["usd"] == "https://piyasa.paratic.com/doviz/dolar/"
    assert assets["paratic"]["gram_gumus"] == "https://piyasa.paratic.com/forex/emtia/gumus-gram/"
    assert assets["paratic"]["bitcoin"] == "https://piyasa.paratic.com/kripto-coin/bitcoin/"
    assert assets["paratic"]["ceyrek_altin"] == "https://piyasa.paratic.com/altin/ceyrek/"
    assert assets["paratic"]["brent"] == "https://piyasa.paratic.com/forex/emtia/brent-petrol/"
    assert assets["paratic"]["bist100"] == "https://piyasa.paratic.com/borsa/"
    src = Path("scripts/pm_lab_scrape.py").read_text(encoding="utf-8")
    assert "https://piyasa.paratic.com/doviz/" in src
    assert "https://piyasa.paratic.com/kripto-coin/" in src
    assert "retry_403" in src
    assert "_paratic_merge_html" in src
    assert assets["investing"]["gram_gumus"] == "https://tr.investing.com/currencies/xagg-try"
    assert assets["investing"]["gram_altin"] == "https://tr.investing.com/currencies/gau-try"
    assert assets["tradingview"]["gram_gumus"] == "https://tr.tradingview.com/symbols/XAGTRYG/"
    assert assets["uzmanpara"]["bitcoin"] == "https://uzmanpara.milliyet.com.tr/kripto-paralar/bitcoin/"
    assert "https://uzmanpara.milliyet.com.tr/kripto-paralar/bitcoin/" in lists["uzmanpara"]
    sample = """
    <div class="ins_alsat sat"><div class="label">SAT</div>
    <div class="price" data-code="USDTRY" data-type="ask">47.7403</div></div>
    <div data-type="change">0.08</div>
    """
    pq = mod._paratic_quote_from_html(sample, "usd")
    assert pq["value"] == "47.7403"
    gumus = '<div class="price" data-code="XSLV" data-type="ask">99.1292</div> AL 99.0371 SAT 99.1292'
    assert mod._paratic_quote_from_html(gumus, "gram_gumus")["value"] == "99.1292"
    nested = '<div class="price" data-code="XGLD" data-type="ask"><span>5432.10</span></div>'
    assert mod._paratic_quote_from_html(nested, "gram_altin")["value"] == "5432.10"
    mixed = (
        '<div class="price" data-code="USD/TRL" data-type="ask">47.74</div>'
        '<div class="price" data-code="EUR/TRL" data-type="ask">55.15</div>'
        '<div class="price" data-code="XGLD" data-type="ask">6707.54</div>'
    )
    assert mod._paratic_quote_from_html(mixed, "usd", strict=True)["value"] == "47.74"
    assert mod._paratic_quote_from_html(mixed, "eur", strict=True)["value"] == "55.15"
    assert mod._paratic_quote_from_html(mixed, "gram_altin", strict=True)["value"] == "6707.54"
    assert mod._paratic_quote_from_html(mixed, "brent", strict=True) is None
    bist = "BIST 100 XU100 13704.52 %-0,78"
    assert mod._paratic_quote_from_html(bist, "bist100")["value"] == "13704.52"
    btc = mod._parse_assets_from_text("BITCOIN $63.590 %-0,62 ( -$399 )")
    assert btc["bitcoin"]["value"] == "63.590"
    try_only = mod._parse_assets_from_text(
        "Bitcoin (BTC) 3022380,3129 3022379,8358 % -0,83 Ethereum (ETH) 88931,9171"
    )
    assert "bitcoin" not in try_only


def test_pm_lab_doviz_rank_chip_labels():
    js = Path("static/js/pm_lab.js").read_text(encoding="utf-8")
    html = Path("templates/pm_lab.html").read_text(encoding="utf-8")
    assert "bizim sıra" not in js
    assert "doviz.com: " in js
    assert "doviz.com rank:" in js
    assert "pm_lab.js?v=30" in html
    assert "pingBridge" in js
    assert "127.0.0.1:18765/sync-pm-lab" in js
    assert "position:static" in html
    assert COMPETITORS_INTERVAL_MIN == 10
    assert "fiyat " not in js
    assert "Fotoğraf yok" not in html
    assert "ort. sapma = Döviz" not in js
    bridge = Path("scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    assert 'PM_LAB_COMPETITORS_INTERVAL_SEC") or "600"' in bridge
    assert "run_pm_lab_competitors_once" in bridge
    assert "run_pm_lab_jobs_once" in bridge
    assert "PM_LAB_JOB_IDS" in bridge
    assert "_pm_lab_claim_loop" in bridge
    assert 'enuygun: "Enuygun"' in js
    assert 'bloomberght: "Bloomberg"' in js
    assert 'tradingview: "Trading"' in js
    assert 'cnnturk: "CNN"' in js
    assert '"ceyrek_altin"' in js and "ASSET_ROW_ORDER" in js


def test_news_keywords_include_fuel_and_bitcoin():
    import importlib.util

    spec = importlib.util.spec_from_file_location("pm_lab_scrape", Path("scripts/pm_lab_scrape.py"))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    for kw in ("bitcoin", "benzin", "motorin", "akaryakıt"):
        assert kw in mod.NEWS_KEYWORDS


def test_competitor_sapma_vs_peer_average():
    import importlib.util

    spec = importlib.util.spec_from_file_location("pm_lab_scrape", Path("scripts/pm_lab_scrape.py"))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    assert mod.SAPMA_THRESHOLDS["usd"][0] < mod.SAPMA_THRESHOLDS["ceyrek_altin"][0]
    assert mod.SAPMA_THRESHOLDS["bitcoin"][0] > mod.SAPMA_THRESHOLDS["gram_altin"][0]
    peers = {
        "doviz": {"value": "47,80"},
        "tradingview": {"value": "47.73"},
        "canlidoviz": {"value": "47.74"},
        "foreks": {"value": "47,7350"},
        "investing": {"value": "47.73"},
        "bigpara": {"value": "47,7307"},
        "uzmanpara": {"value": "47,73"},
        "bloomberght": {"value": "47,74"},
        "cnbce": {"value": "47,73"},
        "cnnturk": {"value": "47,7427"},
        "enuygun": {"value": "47,73"},
    }
    hit = mod.compute_price_sapma("usd", peers)
    assert hit["n"] == 10
    assert hit["pct"] is not None and hit["pct"] > 0
    assert 0.10 < hit["pct"] < 0.20
    assert hit["band"] == "warn"
    tight = dict(peers)
    tight["doviz"] = {"value": "47,74"}
    ok = mod.compute_price_sapma("usd", tight)
    assert ok["band"] == "ok"
    hot_cells = dict(peers)
    hot_cells["doviz"] = {"value": "48,20"}
    hot = mod.compute_price_sapma("usd", hot_cells)
    assert hot["band"] == "hot"
    thin = {"doviz": {"value": "47,74"}, "tradingview": {"value": "47.73"}}
    assert mod.compute_price_sapma("usd", thin)["pct"] is None
    fk = mod.compute_pair_sapma("usd", peers)
    assert fk["pct"] is not None and fk["pct"] > 0
    assert fk["band"] == "warn"
    fk_ok = mod.compute_pair_sapma("usd", tight)
    assert fk_ok["band"] == "ok"
    missing_fk = dict(peers)
    missing_fk["foreks"] = {"value": ""}
    assert mod.compute_pair_sapma("usd", missing_fk)["pct"] is None
    quiet = mod.collect_sapma_alerts([{"id": "usd", "label": "Dolar", "cells": peers}])
    assert quiet == []
    mail_cells = dict(peers)
    mail_cells["doviz"] = {"value": "48,80"}
    alerts = mod.collect_sapma_alerts([{"id": "usd", "label": "Dolar", "cells": mail_cells}])
    subjects = [a["subject"] for a in alerts]
    assert any(s.startswith("Doviz - Sapma - Dolar - ") for s in subjects)
    assert any(s.startswith("Doviz - Foreks sapma - Dolar - ") for s in subjects)
    assert all(abs(a["pct"]) >= mod.SAPMA_MAIL_THRESHOLD_PCT for a in alerts)
    js = Path("static/js/pm_lab.js").read_text(encoding="utf-8")
    html = Path("templates/pm_lab.html").read_text(encoding="utf-8")
    assert 'label: "avg. deviation"' in js
    assert "ensureSiteColumns" in js
    assert "ids.paratic" in js
    assert 'label: "Foreks deviation"' in js
    assert "c.id === \"doviz\"" in js
    assert "pml-sapma-hot" in html
    assert "computeSapma" in js
    assert "computeForeksSapma" in js


def test_sapma_alert_mail_subject(monkeypatch):
    from backend.services import pm_lab_sapma_alerts as alerts

    sent: list[str] = []
    monkeypatch.setattr(alerts.settings, "outbound_email_enabled", True)
    monkeypatch.setattr(alerts, "operations_recipients", lambda: ["ops@nokta.com"])
    monkeypatch.setattr(alerts, "send_email", lambda subject, html, recipients=None: sent.append(subject) or True)
    monkeypatch.setattr(alerts, "_delivery_exists", lambda db, **kwargs: False)
    monkeypatch.setattr(alerts, "_record_delivery", lambda db, **kwargs: None)
    section = {
        "matrix": [
            {
                "id": "usd",
                "label": "Dolar",
                "cells": {
                    "doviz": {"value": "48,80"},
                    "foreks": {"value": "47,7350"},
                    "tradingview": {"value": "47.73"},
                    "canlidoviz": {"value": "47.74"},
                },
            }
        ]
    }
    out = alerts.notify_competitor_sapma(None, section)
    assert out
    assert all(s.startswith("Doviz - ") and "Dolar" in s for s in out)
    assert not any("@" in s for s in out)
