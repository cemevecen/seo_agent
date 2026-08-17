"""Doviz News · Latest content — platform sütunları (Android / iOS / Web / mWeb).

Eşleştirme haber ID'si üzerinden:
  · Web / mWeb → haber detay sayfa yolundan ID
  · Android    → `news_detail_opened` olayı + `news_id` parametresi
  · iOS        → `screen_view` olayı + `news_id` parametresi
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.services import doviz_news_traffic as dnt

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "templates/doviz_news.html"

ROWS = [
    {"id": "1234567", "title": "Dolar bugün"},
    {"id": "7654321", "title": "Altın rekor"},
]


class _Status(dict):
    pass


@pytest.fixture()
def ga4_ready(monkeypatch):
    monkeypatch.setattr(
        dnt,
        "get_ga4_connection_status",
        lambda db, site_id: {
            "connected": True,
            "properties": {"web": "1", "mweb": "2", "android": "3", "ios": "4"},
        },
    )


def _patch_collectors(monkeypatch, *, app_rows=None, pages=None):
    import backend.collectors.ga4 as ga4

    monkeypatch.setattr(
        ga4, "fetch_ga4_event_param_breakdown", lambda **kw: list(app_rows or []), raising=False
    )
    monkeypatch.setattr(
        ga4, "fetch_ga4_news_detail_pages_metrics", lambda **kw: list(pages or []), raising=False
    )


def test_app_rows_match_by_news_id(monkeypatch, ga4_ready):
    _patch_collectors(
        monkeypatch,
        app_rows=[{"value": "1234567", "count": 120}, {"value": "9999999", "count": 5}],
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    assert out["ok"] is True
    android = out["by_article"]["1234567"]["android"]
    assert android["d1"] == 120 and android["d7"] == 120
    # Listede olmayan ID sızmamalı
    assert "9999999" not in out["by_article"]


def test_web_rows_match_by_article_path(monkeypatch, ga4_ready):
    _patch_collectors(
        monkeypatch,
        pages=[
            {"page": "/haber/1234567/dolar-bugun", "views": 300, "sessions": 200},
            {"page": "/haber/2222222/baska", "views": 10, "sessions": 5},
        ],
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    web = out["by_article"]["1234567"]["web"]
    assert web["d1"] == 300 and web["d7"] == 300
    assert out["by_article"]["1234567"]["mweb"]["d7"] == 300


def test_totals_and_matched_count(monkeypatch, ga4_ready):
    _patch_collectors(
        monkeypatch,
        app_rows=[{"value": "1234567", "count": 10}],
        pages=[{"page": "/haber/7654321/x", "views": 40, "sessions": 4}],
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    assert out["matched"] == 2
    assert out["totals"]["android"]["d7"] == 10
    assert out["totals"]["web"]["d7"] == 40


def test_no_ids_returns_empty_without_calling_ga4(monkeypatch):
    called = {"n": 0}

    def _boom(*a, **k):
        called["n"] += 1
        raise AssertionError("GA4'e gidilmemeliydi")

    monkeypatch.setattr(dnt, "get_ga4_connection_status", _boom)
    out = dnt.fetch_news_platform_breakdown(None, rows=[{"id": ""}])
    assert out["by_article"] == {}
    assert called["n"] == 0


def test_ga4_disconnected_is_reported_not_raised(monkeypatch):
    monkeypatch.setattr(
        dnt,
        "get_ga4_connection_status",
        lambda db, site_id: {"connected": False, "label": "GA4 not connected"},
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    assert out["ok"] is False
    assert "GA4" in (out["error"] or "")
    assert out["by_article"] == {}


def test_one_platform_failure_does_not_kill_the_others(monkeypatch, ga4_ready):
    import backend.collectors.ga4 as ga4

    def _app(**kw):
        raise RuntimeError("quota")

    monkeypatch.setattr(ga4, "fetch_ga4_event_param_breakdown", _app, raising=False)
    monkeypatch.setattr(
        ga4,
        "fetch_ga4_news_detail_pages_metrics",
        lambda **kw: [{"page": "/haber/1234567/x", "views": 7}],
        raising=False,
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    assert out["ok"] is True
    assert out["by_article"]["1234567"]["web"]["d7"] == 7
    assert "android" not in out["by_article"]["1234567"]


def test_window_dates_end_yesterday():
    from backend.services.timezone_utils import report_calendar_yesterday

    y = report_calendar_yesterday()
    s1, e1 = dnt._platform_window_dates(1)
    s7, e7 = dnt._platform_window_dates(7)
    assert e1 == y.isoformat() and s1 == y.isoformat()
    assert e7 == y.isoformat() and s7 < e7


def test_page_keeps_existing_columns_and_renames_views_to_realtime():
    html = PAGE.read_text(encoding="utf-8")
    assert 'key: "rt_views", label: "Realtime"' in html
    assert 'label: "Views"' not in html
    # Mevcut sütunlar duruyor
    for needle in ('key: "source"', 'key: "category"', 'key: "date"',
                   'label: "GA4 views"', 'label: "Sessions"', 'label: "GSC clicks"'):
        assert needle in html, needle


def test_page_adds_four_platform_columns():
    html = PAGE.read_text(encoding="utf-8")
    for label in ('"Android"', '"iOS"', '"Web"', '"mWeb"'):
        assert 'platformCol("' in html and label in html
    assert "platformCols" in html
    assert ".concat(platformCols).concat(metricCols)" in html


def test_platform_columns_do_not_depend_on_show_traffic():
    """Sütunlar «Show traffic» olmadan da görünmeli (konu 2)."""
    html = PAGE.read_text(encoding="utf-8")
    block = html.split("var platformCols = [", 1)[1].split("];", 1)[0]
    assert "state.trafficShown" not in block, block[:200]


def test_day_window_switch_exists_and_drives_rendering():
    """1 gün / 7 gün anahtarı (konu 3) — hem hücre hem sıralama etkilenir."""
    html = PAGE.read_text(encoding="utf-8")
    assert 'id="dn-pf-window"' in html
    assert 'data-dn-window="d1"' in html and 'data-dn-window="d7"' in html
    assert "bindPlatformWindowSwitch" in html
    assert 'state.pfWindow === "d7" ? d7 : d1' in html
    assert 'var sel = state.pfWindow === "d7" ? v["d7" + sfx] : v["d1" + sfx];' in html


def test_empty_platform_data_explains_itself():
    """Boş kalırsa sebebi ekranda yazsın — sessiz boşluk teşhis edilemiyor."""
    html = PAGE.read_text(encoding="utf-8")
    assert "renderPlatformMeta" in html
    # Platform başına: kaç satır geldi / kaçı eşleşti / hata
    assert "satır geldi, 0 eşleşti" in html
    assert "Platform trafiği alınamadı" in html


def test_payload_exposes_platform_traffic_outside_traffic_block():
    src = (ROOT / "backend/services/doviz_news_sheet.py").read_text(encoding="utf-8")
    assert '"platform_traffic": platform_matrix,' in src
    # Trafik bloğunun içinde çağrılmamalı
    traffic_block = src.split("if include_traffic and db is not None:", 1)[1].split("items = []", 1)[0]
    assert "fetch_news_platform_breakdown" not in traffic_block


def test_source_and_category_are_the_last_columns():
    """Sütun sırası: … platform · metrikler · Source · Category."""
    html = PAGE.read_text(encoding="utf-8")
    tail = html.split(".concat(platformCols).concat(metricCols).concat([", 1)[1].split("])", 1)[0]
    assert 'key: "source"' in tail
    assert 'key: "category"' in tail
    items_table = html.split('mountInteractiveTable("dn-table-items"', 1)[1]
    fixed = items_table.split("columns: [", 1)[1].split(".concat(platformCols)", 1)[0]
    assert 'key: "source"' not in fixed
    assert 'key: "category"' not in fixed


def test_title_is_clipped_at_80_chars_with_full_text_in_tooltip():
    html = PAGE.read_text(encoding="utf-8")
    assert "full.length > 80" in html
    assert "dn-title-text" in html
    assert "esc(full)" in html
    assert ".dn-title-cell { width: 22rem;" in html


# ── iOS: özel boyut adı property'ye göre değişebiliyor ───────────────────────

def _rows(*pairs):
    return [{"value": v, "count": c} for v, c in pairs]


def _pf(views, sessions=0.0):
    """`_fetch_app_platform_counts` çıktısındaki tek ID hücresi."""
    return {"views": float(views), "sessions": float(sessions)}


def test_ios_falls_back_to_id_plus_title_when_plain_id_is_empty():
    calls = []

    def fake(**kw):
        calls.append(kw)
        if kw.get("param_key_2"):
            return _rows(("1234567 · Dolar bugün", 42))
        return []

    out, diag = dnt._fetch_app_platform_counts(
        fake, property_id="4", platform="ios", days=1, id_index={"1234567": ROWS[0]}
    )
    assert out == {"1234567": _pf(42.0)}
    assert diag["strategy"] == "news_id+title"
    assert len(calls) == 2  # önce sade ID denendi


def test_ios_falls_back_to_title_matching_as_last_resort():
    def fake(**kw):
        if kw.get("param_key") == "news_title":
            return _rows(("Dolar bugün", 7), ("Alakasız haber", 3))
        return []

    out, diag = dnt._fetch_app_platform_counts(
        fake, property_id="4", platform="ios", days=7, id_index={"1234567": ROWS[0]}
    )
    assert out == {"1234567": _pf(7.0)}
    assert diag["strategy"] == "news_title"


def test_plain_id_is_preferred_and_stops_early():
    calls = []

    def fake(**kw):
        calls.append(kw.get("param_key"))
        return _rows(("1234567", 99))

    out, diag = dnt._fetch_app_platform_counts(
        fake, property_id="3", platform="android", days=1, id_index={"1234567": ROWS[0]}
    )
    assert out == {"1234567": _pf(99.0)}
    assert diag["strategy"] == "news_id"
    assert calls == ["news_id"]  # gereksiz ek sorgu yok


def test_title_matching_ignores_punctuation_and_case():
    def fake(**kw):
        if kw.get("param_key") == "news_title":
            return _rows(("DOLAR BUGÜN!", 5))
        return []

    out, _ = dnt._fetch_app_platform_counts(
        fake, property_id="4", platform="ios", days=1, id_index={"1234567": {"title": "Dolar bugün"}}
    )
    assert out == {"1234567": _pf(5.0)}


def test_every_strategy_failing_reports_what_was_tried():
    def fake(**kw):
        raise RuntimeError("dimension not found")

    out, diag = dnt._fetch_app_platform_counts(
        fake, property_id="4", platform="ios", days=1, id_index={"1234567": ROWS[0]}
    )
    assert out == {}
    assert diag["strategy"] is None
    assert len(diag["tried"]) == 3
    assert all("error" in t for t in diag["tried"])


def test_breakdown_exposes_per_platform_diagnostics(monkeypatch, ga4_ready):
    _patch_collectors(monkeypatch, app_rows=[{"value": "1234567", "count": 3}],
                      pages=[{"page": "/haber/1234567/x", "views": 9}])
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    diag = out["diagnostics"]
    assert "ios:d1" in diag and "web:d7" in diag
    assert diag["web:d7"]["strategy"] == "page_path"


# ── Session metriği (view'ın yanına gerçek sessions — tahmin değil) ──────────

def test_web_sessions_come_from_ga4_not_estimated(monkeypatch, ga4_ready):
    """Web/mWeb session'ı zaten çekilen rapordan okunur, view'dan türetilmez."""
    _patch_collectors(
        monkeypatch,
        pages=[{"page": "/haber/1234567/dolar-bugun", "views": 300, "sessions": 187}],
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    web = out["by_article"]["1234567"]["web"]
    assert web["d1"] == 300 and web["d7"] == 300
    assert web["d1_sessions"] == 187 and web["d7_sessions"] == 187
    assert out["totals"]["web"]["d7_sessions"] == 187


def test_app_sessions_are_carried_when_ga4_returns_them(monkeypatch, ga4_ready):
    _patch_collectors(
        monkeypatch,
        app_rows=[{"value": "1234567", "count": 120, "sessions": 74}],
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    android = out["by_article"]["1234567"]["android"]
    assert android["d1"] == 120 and android["d1_sessions"] == 74
    assert out["diagnostics"]["android:d1"]["sessions_metric"] is True


def test_app_without_sessions_metric_keeps_views_and_flags_diagnostics(monkeypatch, ga4_ready):
    """GA4 sessions vermezse view kaybolmamalı; eksiklik teşhiste görünmeli."""
    _patch_collectors(monkeypatch, app_rows=[{"value": "1234567", "count": 120}])
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    android = out["by_article"]["1234567"]["android"]
    assert android["d1"] == 120
    assert android["d1_sessions"] == 0
    assert out["diagnostics"]["android:d1"]["sessions_metric"] is False


def test_app_counts_request_sessions_from_the_collector():
    seen = []

    def fake(**kw):
        seen.append(kw.get("with_sessions"))
        return [{"value": "1234567", "count": 9, "sessions": 4}]

    out, diag = dnt._fetch_app_platform_counts(
        fake, property_id="3", platform="android", days=1, id_index={"1234567": ROWS[0]}
    )
    assert seen == [True]
    assert out == {"1234567": _pf(9.0, 4.0)}
    assert diag["sessions_metric"] is True


def test_row_with_only_sessions_is_not_dropped(monkeypatch, ga4_ready):
    """View 0 ama session varsa hücre yine dolmalı (aksi halde sessizce kaybolur)."""
    _patch_collectors(
        monkeypatch,
        pages=[{"page": "/haber/1234567/x", "views": 0, "sessions": 12}],
    )
    dnt._PLATFORM_CACHE.clear()
    out = dnt.fetch_news_platform_breakdown(None, rows=ROWS)
    web = out["by_article"]["1234567"]["web"]
    assert web["d7"] == 0 and web["d7_sessions"] == 12


def test_metric_switch_exists_and_drives_cells_and_sorting():
    html = PAGE.read_text(encoding="utf-8")
    assert 'id="dn-pf-metric"' in html
    assert 'data-dn-metric="views"' in html and 'data-dn-metric="sessions"' in html
    assert "bindPlatformMetricSwitch" in html
    # Hem hücre hem sıralama seçili metriğe bakar
    assert 'var sfx = state.pfMetric === "sessions" ? "_sessions" : "";' in html
    assert 'pfMetric: "views",' in html


def test_tooltip_always_shows_both_metrics_for_both_windows():
    """Anahtar hangi konumda olursa olsun dört sayı tooltip'te bulunmalı."""
    html = PAGE.read_text(encoding="utf-8")
    block = html.split("function platformCol(key, label)", 1)[1].split("var platformCols", 1)[0]
    for needle in ("pf.d1 || 0", "pf.d1_sessions || 0", "pf.d7 || 0", "pf.d7_sessions || 0"):
        assert needle in block, needle
