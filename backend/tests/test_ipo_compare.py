"""halkarz.com × doviz.com halka arz eşleştirme."""

from pathlib import Path

from backend.services import ipo_compare as ipo

HALKARZ_HOME = """
<div class="tab_item"><ul class="halka-arz-list">
<li><article class="index-list"><div class="il-badge">
<i class="fa-solid fa-check-double snc-badge" title="Halka Arz Sonuçları Açıklandı"></i><div class="il-new">Yeni!</div></div>
<a href="https://halkarz.com/turker-vangolu-enerji-yatirim-a-s/" title="x">
<img src="https://halkarz.com/x.png" class="slogo"></a>
<div class="il-content">
<span class="il-bist-kod">VEYAS</span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/turker-vangolu-enerji-yatirim-a-s/" title="Türker Vangölü Enerji Yatırım A.Ş.">Türker Vangölü Enerji Yatırım A.Ş.</a></h3>
<span class="il-halka-arz-tarihi"><time datetime="12-13-14 Ağustos 2026">12-13-14 Ağustos 2026</time></span>
</div></article></li>
<li><article class="index-list"><div class="il-badge"><div class="il-gonk">Gong!</div></div>
<div class="il-content">
<span class="il-bist-kod">CITAS</span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/citlekci-magazacilik-gida-a-s/">Çitlekçi Mağazacılık Gıda A.Ş.</a></h3>
<span class="il-halka-arz-tarihi"><time>10-11-12 Ağustos 2026</time></span>
</div></article></li>
<li><article class="index-list"><div class="il-badge"><div class="il-ert">
<a href="https://halkarz.com/k/halka-arz/ertelenen/">Ertelendi</a></div></div>
<div class="il-content">
<span class="il-bist-kod">BEWEN</span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/bewen-enerji-a-s/">Bewen Enerji A.Ş.</a></h3>
<span class="il-halka-arz-tarihi"><time>Ertelendi</time></span>
</div></article></li>
</ul></div>
<div class="tab_item"><ul class="halka-arz-list taslak">
<li><article class="index-list"><div class="il-badge"></div>
<div class="il-content">
<span class="il-bist-kod"></span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/asos-proses-makina-san-ve-tic-a-s/">Asos Proses Makina San. ve Tic. A.Ş.</a></h3>
</div></article></li>
<li><article class="index-list"><div class="il-badge"></div>
<div class="il-content">
<span class="il-bist-kod">MRBAS</span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/marbas-menkul-degerler-a-s/">Marbaş Menkul Değerler A.Ş.</a></h3>
</div></article></li>
<li><article class="index-list"><div class="il-badge"></div>
<div class="il-content">
<span class="il-bist-kod"></span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/hedef-grup-satis-dagitim-san-ve-tic-a-s/">Hedef Grup Satış Dağıtım San. ve Tic. A.Ş.</a></h3>
</div></article></li>
<li><article class="index-list"><div class="il-badge"><div class="il-ert">
<a href="https://halkarz.com/k/halka-arz/basvuru-surecinde/">Başvuru Sürecinde</a></div></div>
<div class="il-content">
<span class="il-bist-kod"></span>
<h3 class="il-halka-arz-sirket"><a href="https://halkarz.com/mpg-makine/">Mpg Makine Prodüksiyon Grubu Makine İmalat San. ve Tic. A.Ş.</a></h3>
</div></article></li>
</ul></div>
"""

DOVIZ_HOME = """
<h2>Aktif Halka Arzlar</h2>
<div class="active-ipos mt-8">
  <div class="ipo ipo-0">
    <a href="https://borsa.doviz.com/halka-arz/turker-vangolu-enerji-yatirim-a-s/322" class="ticker">Türker Vangölü Enerji Yatırım A.Ş.</a>
    <div class="detail"><span class="label">Satış Fiyatı: </span><span class="value">₺136,00</span></div>
    <div class="detail"><span class="label">Talep Toplama: </span><span class="value">12.08.2026 - 14.08.2026</span></div>
    <div class="detail"><span class="label">Satılacak Lot: </span><span class="value">65.000.000 Lot</span></div>
    <div class="detail"><span class="label">Halka Arz Büyüklüğü: </span><span class="value">₺6.035.000.000</span></div>
    <div class="detail"><span class="label">Dağıtım Yöntemi: </span><span class="value">Bireysele Eşit</span></div>
  </div>
</div>
<span class="icon icon-ipo-slider-previous ipo-slider-arrow hide"></span>
<h2>Taslak Halka Arzlar</h2>
"""

DOVIZ_TASLAK = """
<table><tbody>
<tr>
  <td>
    <a href="https://borsa.doviz.com/halka-arz/asos-proses-makina-san-ve-tic-a-s/344">
      <img alt="Asos Proses Makina San. ve Tic. A.Ş." class="stock-icon">
      <div class="currency-details"><div>Asos Proses Makina San. ve Tic. A.Ş.</div></div>
    </a>
  </td>
  <td class="h-padding-8">Taslak</td>
</tr>
<tr>
  <td>
    <a href="https://borsa.doviz.com/halka-arz/marbas-menkul-degerler-a-s/114">
      <div class="currency-details">
        <div>Marbaş Menkul Değerler A.Ş.</div>
        <div class="cname">MRBAS</div>
      </div>
    </a>
  </td>
  <td class="h-padding-8">Taslak</td>
</tr>
<tr>
  <td>
    <a href="https://borsa.doviz.com/halka-arz/bewen-enerji-a-s/196">
      <div class="currency-details">
        <div>Bewen Enerji A.Ş.</div>
        <div class="cname">BEWEN</div>
      </div>
    </a>
  </td>
  <td class="h-padding-8">Ertelendi</td>
</tr>
<tr>
  <td>
    <a href="https://borsa.doviz.com/halka-arz/sadece-doviz-a-s/999">
      <div class="currency-details"><div>Sadece Doviz A.Ş.</div></div>
    </a>
  </td>
  <td class="h-padding-8">Taslak</td>
</tr>
</tbody></table>
"""

DOVIZ_GECMIS = """
<table><thead><tr><th>Şirket</th></tr></thead><tbody>
<tr>
  <td>
    <a href="https://borsa.doviz.com/hisseler/citas-citlekci-magazacilik-gida">
      <img alt="Çitlekçi Mağazacılık Gıda A.Ş." class="stock-icon">
      <div class="currency-details">
        <div>CITAS</div>
        <div class="cname">Çitlekçi Mağazacılık Gıda A.Ş.</div>
      </div>
    </a>
  </td>
  <td class="h-padding-8" data-type="date">18.08.2026</td>
  <td class="h-padding-8">556.728</td>
  <td class="h-padding-8">₺2.690.050.000</td>
  <td class="h-padding-8">₺73,70</td>
  <td class="h-padding-8">81,05</td>
  <td class="h-padding-8 text-bold color-up">%9,97</td>
</tr>
</tbody></table>
"""

HALKARZ_DETAIL = """
<table class="sp-table">
<tr><td><em>Halka Arz Tarihi : </em></td><td><time>12-13-14 Ağustos 2026</time></td></tr>
<tr><td><em>Halka Arz Fiyatı/Aralığı : </em></td><td><strong class="f700">136,00 TL</strong></td></tr>
<tr><td><em>Dağıtım Yöntemi : </em></td><td><strong>Eşit Dağıtım **</strong></td></tr>
<tr><td><em>Pay : </em></td><td><strong>65.000.000 Lot</strong></td></tr>
<tr><td><em>Bist Kodu : </em></td><td><strong>VEYAS</strong></td></tr>
<tr><td><em>Pazar : </em></td><td><strong>Yıldız Pazar</strong></td></tr>
<tr><td><em>Bist İlk İşlem Tarihi : </em></td><td><strong>20 Ağustos 2026</strong></td></tr>
</table>
"""


def test_parse_halkarz_splits_ilk_and_taslak():
    lists = ipo.parse_halkarz_home(HALKARZ_HOME)
    assert [x["ticker"] for x in lists["ilk"]] == ["VEYAS", "CITAS", "BEWEN"]
    assert lists["ilk"][0]["status"] == "Sonuçlandı"
    assert lists["ilk"][1]["status"] == "Gong"
    assert lists["ilk"][2]["status"] == "Ertelendi"
    names = [x["name"] for x in lists["taslak"]]
    assert names[0].startswith("Asos")
    assert lists["taslak"][1]["ticker"] == "MRBAS"
    assert any("Hedef Grup" in x["name"] for x in lists["taslak"])
    mpg = next(x for x in lists["taslak"] if x["name"].startswith("Mpg"))
    assert mpg["status"] == "Başvuru Sürecinde"


def test_parse_doviz_lists():
    aktif = ipo.parse_doviz_aktif(DOVIZ_HOME)
    assert len(aktif) == 1
    assert aktif[0]["fields"]["fiyat"] == "₺136,00"
    assert "65.000.000" in aktif[0]["fields"]["pay"]

    taslak = ipo.parse_doviz_taslak(DOVIZ_TASLAK)
    names = [x["name"] for x in taslak]
    assert any(n.startswith("Asos Proses") for n in names)
    assert any(n.startswith("Marbaş") or n.startswith("Marbas") for n in names)
    assert any("Bewen" in n for n in names)
    assert any("Sadece Doviz" in n for n in names)
    marbas = next(x for x in taslak if x["ticker"] == "MRBAS")
    assert marbas["status"] == "Taslak"

    gecmis = ipo.parse_doviz_gecmis(DOVIZ_GECMIS)
    assert len(gecmis) == 1
    assert gecmis[0]["ticker"] == "CITAS"
    assert gecmis[0]["fields"]["ilk_islem"] == "18.08.2026"


def test_parse_halkarz_detail_fields():
    fields = ipo.parse_halkarz_detail(HALKARZ_DETAIL)
    assert fields["fiyat"] == "136,00 TL"
    assert fields["bist_kodu"] == "VEYAS"
    assert fields["pazar"] == "Yıldız Pazar"
    assert "65.000.000" in fields["pay"]


def test_gyo_names_do_not_cross_match():
    ha = {"name": "Mar Gayrimenkul Yatırım Ortaklığı A.Ş.", "ticker": ""}
    dv = {"name": "Ic Gayrimenkul Yatırım Ortaklığı A.Ş.", "ticker": ""}
    assert ipo._score_pair(ha, dv) < 0.84
    same = {"name": "Mar Gayrimenkul Yatırım Ortaklığı A.Ş.", "ticker": ""}
    assert ipo._score_pair(ha, same) >= 0.97
    ha = {"name": "(Intercity) Ekim Turizm Tic. ve San. A.Ş.", "ticker": "EKIM"}
    dv = {"name": "Ekim Turizm Tic. ve San. A.Ş.", "ticker": "EKIM"}
    assert ipo._score_pair(ha, dv) >= 0.97
    ha2 = {"name": "Hedef Grup Satış Dağıtım San. ve Tic. A.Ş.", "ticker": ""}
    dv2 = {"name": "Sadece Doviz A.Ş.", "ticker": ""}
    assert ipo._score_pair(ha2, dv2) < 0.84


def test_build_payload_eksik_fazla_and_buckets():
    payload = ipo.build_payload(
        halkarz_home_html=HALKARZ_HOME,
        doviz_home_html=DOVIZ_HOME,
        doviz_taslak_html=DOVIZ_TASLAK,
        doviz_gecmis_html=DOVIZ_GECMIS,
        halkarz_details={
            "https://halkarz.com/turker-vangolu-enerji-yatirim-a-s/": ipo.parse_halkarz_detail(HALKARZ_DETAIL),
        },
    )
    c = payload["counts"]
    assert c["halkarz_ilk"] == 3
    assert c["halkarz_taslak"] == 4
    assert c["matched"] >= 4
    missing_names = {r["name"] for r in payload["missing"]}
    extra_names = {r["name"] for r in payload["extra"]}
    assert any("Hedef Grup" in n for n in missing_names)
    assert any("Mpg" in n for n in missing_names)
    assert any("Sadece Doviz" in n for n in extra_names)

    olan = payload["buckets"]["olan"]
    assert any(r["ticker"] == "VEYAS" and r["match"] == "both" for r in olan)
    veyas = next(r for r in olan if r["ticker"] == "VEYAS")
    kinds = {d["field"]: d["kind"] for d in veyas["diffs"]}
    assert kinds.get("fiyat") == "ok"
    assert kinds.get("pay") == "ok"
    assert kinds.get("pazar") == "missing"
    assert kinds.get("buyukluk") == "extra"
    assert "durum" not in kinds
    flagged = payload.get("halkarz_flagged_new") or []
    veyas_flag = next(x for x in flagged if x.get("ticker") == "VEYAS")
    assert veyas_flag.get("on_doviz") is True

    olmus = payload["buckets"]["olmus"]
    assert any(r["ticker"] == "CITAS" and r["match"] == "both" for r in olmus)

    arz = payload["buckets"]["arz_olacak"]
    assert any(r["ticker"] == "MRBAS" for r in arz)
    assert any(r["ticker"] == "BEWEN" for r in arz)

    with_detail = ipo.build_payload(
        halkarz_home_html=HALKARZ_HOME,
        doviz_home_html=DOVIZ_HOME,
        doviz_taslak_html=DOVIZ_TASLAK,
        doviz_gecmis_html=DOVIZ_GECMIS,
        halkarz_details={
            "https://halkarz.com/hedef-grup-satis-dagitim-san-ve-tic-a-s/": {
                "fiyat": "10 TL",
                "araci_kurum": "X Yatırım",
            }
        },
    )
    hedef = next(x for x in with_detail["halkarz_snapshot"] if "Hedef Grup" in x["name"])
    assert hedef["fields"].get("fiyat") == "10 TL"
    assert hedef["fields"].get("araci_kurum") == "X Yatırım"


def test_slug_key_matches_when_names_are_written_differently():
    """Aynı şirket iki sitede farklı yazılıyor; detay URL slug'ı aynı."""
    ha = {
        "name": "Bewen Enerji A.Ş.",
        "url": "https://halkarz.com/bewen-enerji-a-s/",
        "ticker": "",
    }
    dv = {
        "name": "BEWEN Enerji Anonim Şirketi",
        "url": "https://borsa.doviz.com/halka-arz/bewen-enerji-a-s/196",
        "ticker": "",
    }
    assert ipo._score_pair(ha, dv) >= 0.97
    ha2 = {
        "name": "Teknika Plast Teknik Kalıp Plastik San. ve Tic. A.Ş.",
        "url": "https://halkarz.com/teknika-plast-teknik-kalip-plastik-san-ve-tic-a-s/",
    }
    dv2 = {
        "name": "Teknika Plast",
        "url": "https://borsa.doviz.com/halka-arz/teknika-plast-teknik-kalip-plastik-san-ve-tic-a-s/206",
    }
    assert ipo._score_pair(ha2, dv2) >= 0.97


def test_rare_word_overlap_matches_reordered_names():
    """Kelime sırası/yazımı farklı, ayırt edici kelimeler aynı."""
    ha = {"name": "Cms Jant ve Makina Sanayii A.Ş."}
    dv = {"name": "CMS Jant Makina San. ve Tic. A.Ş."}
    corpus_h = [ha, {"name": "Başka Bir Şirket A.Ş."}]
    corpus_d = [dv, {"name": "Cevher Jant Sanayii A.Ş."}]
    weights = ipo._token_weights(corpus_h, corpus_d)
    assert ipo._score_pair(ha, dv, weights) >= 0.84
    # jenerik kelimeler tek başına eşleştirmez
    a = {"name": "Bulls Yatırım Menkul Değerler A.Ş."}
    b = {"name": "Alnus Yatırım Menkul Değerler A.Ş."}
    weights2 = ipo._token_weights([a], [b])
    assert ipo._score_pair(a, b, weights2) < 0.84


def test_conflicting_tickers_never_match():
    a = {"name": "Aynı İsim A.Ş.", "ticker": "AAAAA"}
    b = {"name": "Aynı İsim A.Ş.", "ticker": "BBBBB"}
    assert ipo._score_pair(a, b) == 0.0


def test_short_page_falls_back_to_last_good_copy(monkeypatch):
    """Kaynak sayfa boş/kısmi gelirse 'karşı tarafta yok' demeyiz."""
    ipo._page_cache.clear()
    ipo._cache["payload"] = None
    ipo._cache["ts"] = 0.0
    pages = {
        ipo.HALKARZ_HOME: HALKARZ_HOME,
        ipo.DOVIZ_IPO: DOVIZ_HOME,
        ipo.DOVIZ_TASLAK: DOVIZ_TASLAK,
        ipo.DOVIZ_GECMIS: DOVIZ_GECMIS,
    }
    monkeypatch.setattr(ipo, "_MIN_ROWS", {"doviz_taslak": 2})
    monkeypatch.setattr(ipo, "_http_get", lambda url: pages[url])
    first = ipo.fetch_compare(force=True, details=False)
    assert not first["errors"]
    doviz_first = first["counts"]["doviz"]

    def broken(url):
        if url == ipo.DOVIZ_TASLAK:
            return "<html><body>bakim</body></html>"
        return pages[url]

    monkeypatch.setattr(ipo, "_http_get", broken)
    second = ipo.fetch_compare(force=True, details=False)
    assert "doviz_taslak" in second["errors"]
    assert "son başarılı kopya" in second["errors"]["doviz_taslak"]
    assert second["counts"]["doviz"] == doviz_first
    assert second["counts"]["missing"] == first["counts"]["missing"]
    ipo._page_cache.clear()
    ipo._cache["payload"] = None


def test_payload_exposes_sort_and_filter_fields():
    payload = ipo.build_payload(
        halkarz_home_html=HALKARZ_HOME,
        doviz_home_html=DOVIZ_HOME,
        doviz_taslak_html=DOVIZ_TASLAK,
        doviz_gecmis_html=DOVIZ_GECMIS,
        halkarz_details={
            "https://halkarz.com/turker-vangolu-enerji-yatirim-a-s/": ipo.parse_halkarz_detail(HALKARZ_DETAIL),
        },
    )
    assert payload["today"]
    rows = [r for rows in payload["buckets"].values() for r in rows]
    assert payload["counts"]["total"] == len(rows)
    veyas = next(r for r in rows if r["ticker"] == "VEYAS")
    # halkarz listesinde en üstteki kayıt -> sıralama için order 0
    assert veyas["ha_order"] == 0
    assert veyas["dv_order"] == 0
    assert veyas["date_iso"] == "2026-08-12"
    assert veyas["date_source"].startswith("halkarz")
    assert veyas["is_new"] is True
    assert veyas["gap_count"] == len(veyas["missing_on_doviz"]) + len(veyas["mismatch"])
    # yalnız doviz'de olan kayıtta halkarz sırası yok
    solo = next(r for r in rows if "Sadece Doviz" in r["name"])
    assert solo["ha_order"] is None
    assert solo["dv_order"] is not None
    orders = [r["ha_order"] for r in rows if r["ha_order"] is not None]
    assert len(orders) == len(set(orders))
    snap = next(x for x in payload["halkarz_snapshot"] if x["ticker"] == "VEYAS")
    assert snap["date_iso"] == "2026-08-12"
    assert snap["order"] == 0


def test_iso_date_parsing_handles_tr_ranges():
    assert ipo._iso_dates("15-16-17 Eylül 2025") == ["2025-09-15", "2025-09-16", "2025-09-17"]
    assert ipo._iso_dates("02.09.2025") == ["2025-09-02"]
    assert ipo._iso_dates("yakında") == []
    assert ipo._first_iso_date("", "1 Ekim 2025") == "2025-10-01"


def test_halkarz_visit_delta_marks_doviz():
    prev = [
        ipo.snapshot_company(
            {"name": "Asos Proses Makina San. ve Tic. A.Ş.", "ticker": "", "url": "https://halkarz.com/asos/", "status": "Taslak", "section": "taslak", "fields": {"durum": "Taslak"}}
        )
    ]
    curr = [
        ipo.snapshot_company(
            {"name": "Asos Proses Makina San. ve Tic. A.Ş.", "ticker": "", "url": "https://halkarz.com/asos/", "status": "Başvuru Sürecinde", "section": "taslak", "date_label": "", "fields": {"durum": "Başvuru Sürecinde"}}
        ),
        ipo.snapshot_company(
            {"name": "Hedef Grup Satış Dağıtım San. ve Tic. A.Ş.", "ticker": "", "url": "https://halkarz.com/hedef/", "status": "Taslak", "section": "taslak", "fields": {}}
        ),
    ]
    lookup = {
        curr[0]["id"]: {"on_doviz": True, "doviz_url": "https://borsa.doviz.com/x", "doviz_status": "Taslak"},
        curr[1]["id"]: {"on_doviz": False, "doviz_url": "", "doviz_status": ""},
    }
    delta = ipo.diff_halkarz_snapshots(prev, curr, lookup)
    assert len(delta["new"]) == 1
    assert delta["new"][0]["name"].startswith("Hedef")
    assert delta["new"][0]["on_doviz"] is False
    assert len(delta["changed"]) == 1
    assert delta["changed"][0]["on_doviz"] is True
    assert any(c["field"] == "status" for c in delta["changed"][0]["changes"])


def test_next_visit_slot_is_0909_or_1414():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    tr = ZoneInfo("Europe/Istanbul")
    morning = ipo.next_visit_slot(datetime(2026, 8, 19, 8, 0, tzinfo=tr))
    assert morning["slot"] == "09:09"
    noon = ipo.next_visit_slot(datetime(2026, 8, 19, 10, 0, tzinfo=tr))
    assert noon["slot"] == "14:14"
    evening = ipo.next_visit_slot(datetime(2026, 8, 19, 15, 0, tzinfo=tr))
    assert evening["slot"] == "09:09"
    assert evening["at"].startswith("2026-08-20")

    root = Path(__file__).resolve().parents[2]
    base = (root / "templates/base.html").read_text(encoding="utf-8")
    main = (root / "backend/main.py").read_text(encoding="utf-8")
    html = (root / "templates/ipo.html").read_text(encoding="utf-8")
    assert 'href="/ipo" data-nav-match="/ipo"' in base
    assert 'ipo_menu_visible(request)' in base
    assert '@app.get("/ipo")' in main
    assert '@app.get("/api/ipo/compare")' in main
    assert 'ipo-halkarz-0909' in main
    assert 'ipo-halkarz-1414' in main
    assert 'hour=9, minute=9' in main
    assert 'hour=14, minute=14' in main
    assert 'data-header-title="IPO"' in html
    assert 'data-tab="yeni"' in html
    log = (root / "backend/services/admin_access_log.py").read_text(encoding="utf-8")
    assert '("/ipo", "IPO / Halka Arz")' in log


def test_hidden_companies_roundtrip():
    """«Bunu bir daha gösterme» kaydı kalıcı; geri alınca listeden düşer."""
    from backend.database import Base, SessionLocal, engine
    from backend.models import IpoHiddenCompany

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        db.query(IpoHiddenCompany).delete()
        db.commit()
        key = ipo.row_key(
            {"name": "Volta Motor San. ve Tic. A.Ş.", "url": "https://halkarz.com/volta-motor-san-ve-tic-a-s/"},
            None,
        )
        assert key.startswith("s:")
        out = ipo.set_hidden(db, key=key, hidden=True, name="Volta Motor", ticker="", by="x@y.com")
        assert out["hidden"] == [key]
        # aynı anahtar iki kez gizlenince tek kayıt kalır
        ipo.set_hidden(db, key=key, hidden=True, name="Volta Motor")
        assert db.query(IpoHiddenCompany).count() == 1
        rows = ipo.hidden_rows_public(db)
        assert rows[0]["name"] == "Volta Motor"
        assert rows[0]["hidden_by"] == "x@y.com"
        ipo.set_hidden(db, key=key, hidden=False)
        assert ipo.hidden_keys(db) == []
    finally:
        db.query(IpoHiddenCompany).delete()
        db.commit()
        db.close()


def test_row_key_is_stable_across_scans():
    """Aynı şirket her taramada aynı anahtarı üretmeli."""
    ha = {"name": "Bewen Enerji A.Ş.", "url": "https://halkarz.com/bewen-enerji-a-s/", "ticker": "BEWEN"}
    dv = {"name": "Bewen Enerji A.Ş.", "url": "https://borsa.doviz.com/halka-arz/bewen-enerji-a-s/196"}
    key = ipo.row_key(ha, dv)
    assert key.startswith("s:") and "bewen" in key
    assert ipo.row_key(ha, None) == key
    # halkarz tarafı yoksa doviz slug'ı kullanılır (slug'lar aynı)
    assert ipo.row_key(None, dv) == key
    # URL yoksa BIST koduna, o da yoksa ada düşer
    assert ipo.row_key({"name": "X A.Ş.", "ticker": "XXXXX"}, None) == "t:XXXXX"
    assert ipo.row_key({"name": "Şirket Adı A.Ş."}, None).startswith("n:")


def test_compare_rows_carry_row_key():
    payload = ipo.build_payload(
        halkarz_home_html=HALKARZ_HOME,
        doviz_home_html=DOVIZ_HOME,
        doviz_taslak_html=DOVIZ_TASLAK,
        doviz_gecmis_html=DOVIZ_GECMIS,
    )
    rows = [r for rs in payload["buckets"].values() for r in rs]
    keys = [r["row_key"] for r in rows]
    assert all(keys)
    assert len(keys) == len(set(keys)), "row_key'ler benzersiz olmalı"
