"""x-ga4 — GA4'ün kullanılmayan boyut/metrikleri.

Sözleşme: yalnızca GA4 Data API kullanılır, bir blok düşerse diğerleri ayakta
kalır, `(not set)` satırları listeyi yutmaz ve web/mWeb'de crash-free yazılmaz.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.services import x_ga4 as X

ROOT = Path(__file__).resolve().parents[2]


class _Row:
    def __init__(self, dims, mets):
        self.dimension_values = [type("V", (), {"value": d})() for d in dims]
        self.metric_values = [type("V", (), {"value": str(m)})() for m in mets]


class _Resp:
    def __init__(self, rows):
        self.rows = rows


class _Client:
    """Sahte GA4 client — istekleri kaydeder, sabit yanıt döndürür."""

    def __init__(self, handler=None):
        self.requests = []
        self._handler = handler

    def run_report(self, request):
        self.requests.append(request)
        if self._handler:
            return self._handler(request)
        return _Resp([_Row(["x"], [1])])


@pytest.fixture(autouse=True)
def _clear_cache():
    X._CACHE.clear()
    yield
    X._CACHE.clear()


def _props(**kw):
    base = {"web": "1", "mweb": "2", "android": "3", "ios": "4"}
    base.update(kw)
    return base


# ── Blok yalıtımı ───────────────────────────────────────────────────────────

def test_a_failing_block_does_not_take_down_the_others(monkeypatch):
    def handler(req):
        names = [d.name for d in req.dimensions]
        if "customEvent:asset_key" in names:
            raise RuntimeError("boyut yok")
        return _Resp([_Row(["v"] * len(names), [5, 5, 5])])

    monkeypatch.setattr(
        X, "get_ga4_connection_status", lambda db, site_id: {"connected": True, "properties": _props()},
        raising=False,
    )
    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": True, "properties": _props()},
    )
    monkeypatch.setattr("backend.collectors.ga4._client", lambda: _Client(handler))

    out = X.build_x_ga4_report(None, days=7)
    assert out["ok"] is True
    # Varlık bloğu ayakta kalır ama hata profil bazında raporlanır (daha ince izolasyon)
    assets = out["blocks"]["assets"]
    assert assets["ok"] is True
    assert assets["combined"] == []
    errors = [v for v in assets["per_profile"].values() if isinstance(v, dict) and v.get("error")]
    assert len(errors) == 4 and all("boyut yok" in e["error"] for e in errors)
    # Diğer bloklar etkilenmez
    assert out["blocks"]["user_stability"]["ok"] is True
    assert out["blocks"]["hourly"]["ok"] is True


def test_ga4_disconnected_is_reported_not_raised(monkeypatch):
    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": False, "label": "GA4 bağlı değil"},
    )
    out = X.build_x_ga4_report(None)
    assert out["ok"] is False
    assert "GA4" in out["error"]
    assert out["blocks"] == {}


# ── Sorgu şekli ─────────────────────────────────────────────────────────────

def test_empty_values_are_filtered_server_side():
    """(not set) satırı listeyi yutmamalı — filtre istekte olmalı."""
    client = _Client()
    X._asset_interest(client, _props(), list(X.PROFILES), "7daysAgo", "yesterday", 10)
    assert client.requests, "istek yapılmadı"
    req = client.requests[0]
    assert req.dimension_filter is not None
    values = list(req.dimension_filter.not_expression.filter.in_list_filter.values)
    assert "(not set)" in values and "" in values


def test_crash_free_is_not_fetched_at_all():
    """Crash-free /firebase sayfasinda zaten var; burada istek bile atilmaz."""
    seen = []

    def handler(req):
        seen.append([m.name for m in req.metrics])
        return _Resp([_Row([], [1, 1, 1])])

    out = X._user_stability(_Client(handler), _props(), list(X.PROFILES))
    assert all("crashFreeUsersRate" not in mets for mets in seen)
    assert len(seen) == 4  # profil basina tek istek
    assert all("crashFreeUsersRate" not in r for r in out["rows"])
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    assert "Crash-free" not in page and "crashFreeUsersRate" not in page


def test_header_clutter_is_removed():
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    assert "Pencere " not in page
    assert ">Dönem:<" not in page and "Dönem:" not in page
    assert "BigQuery" not in page


def test_page_column_is_capped_and_overflow_moves_to_a_sub_row():
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    assert "var PAGE_MAX = 50;" in page
    assert "full.slice(0, PAGE_MAX)" in page
    assert "full.slice(PAGE_MAX)" in page
    assert 'colspan="5"' in page          # artan kisim alt satirda
    assert "table-layout: fixed" in page  # sayisal sutunlar ezilmesin


def test_content_depth_uses_engagement_not_scroll():
    """scrolledUsers bu property'de veri döndürmüyor; süre üzerinden hesaplanır."""
    assert "userEngagementDuration" in X.DEPTH_METRICS
    assert "scrolledUsers" not in X.DEPTH_METRICS


def test_seconds_per_view_is_computed_and_zero_safe():
    def handler(req):
        return _Resp([_Row(["/a"], [100, 500, 10]), _Row(["/b"], [0, 0, 0])])

    out = X._content_depth(_Client(handler), {"web": "1"}, ["web"], "7daysAgo", "yesterday", 10)
    a = next(r for r in out["rows"] if r["page"] == "/a")
    assert a["seconds_per_view"] == 5.0
    b = next(r for r in out["rows"] if r["page"] == "/b")
    assert b["seconds_per_view"] == 0.0  # sıfıra bölme yok


def test_hourly_keeps_the_other_bucket_visible():
    """GA4'ün (other) kovası sessizce yutulmamalı."""
    def handler(req):
        return _Resp([_Row(["9"], [10, 12]), _Row(["(other)"], [999, 1000])])

    out = X._hourly(_Client(handler), {"web": "1"}, ["web"], "7daysAgo", "yesterday")
    s = out["series"]["web"]
    assert [h["hour"] for h in s["hours"]] == [9]
    assert s["other_users"] == 999


def test_hours_are_sorted_numerically():
    def handler(req):
        return _Resp([_Row(["22"], [1, 1]), _Row(["3"], [1, 1]), _Row(["11"], [1, 1])])

    out = X._hourly(_Client(handler), {"web": "1"}, ["web"], "7daysAgo", "yesterday")
    assert [h["hour"] for h in out["series"]["web"]["hours"]] == [3, 11, 22]


def test_missing_property_is_skipped_without_calling_ga4():
    client = _Client()
    out = X._asset_interest(client, {"web": "", "ios": "4"}, ["web", "ios"], "7daysAgo", "yesterday", 5)
    assert client.requests and len(client.requests) == 1  # yalnızca ios
    assert "web" not in out["per_profile"]


def test_cache_prevents_a_second_round_trip(monkeypatch):
    calls = {"n": 0}

    def _mk():
        calls["n"] += 1
        return _Client()

    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": True, "properties": _props()},
    )
    monkeypatch.setattr("backend.collectors.ga4._client", _mk)
    first = X.build_x_ga4_report(None, days=7)
    second = X.build_x_ga4_report(None, days=7)
    assert first["cached"] is False
    assert second["cached"] is True
    assert calls["n"] == 1


# ── Yalnızca GA4: başka kaynak sızmamalı ────────────────────────────────────

def test_module_uses_only_the_ga4_data_api():
    src = (ROOT / "backend/services/x_ga4.py").read_text(encoding="utf-8")
    for forbidden in ("bigquery", "google.cloud", "selenium", "requests.get", "httpx"):
        assert forbidden not in src.lower(), forbidden


def test_page_and_tab_are_registered():
    base = (ROOT / "templates/base.html").read_text(encoding="utf-8")
    assert 'href="/x-ga4"' in base and ">x-ga4<" in base
    main = (ROOT / "backend/main.py").read_text(encoding="utf-8")
    assert '@app.get("/x-ga4")' in main
    assert "x_ga4_router" in main
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    assert "/api/x-ga4/report" in page


# ── Özel boyut property başına tanımlı: eksiklik gizlenmemeli ───────────────

def test_undefined_dimension_is_reported_as_setup_gap_not_an_error():
    """GA4'ün «is not a valid dimension» 400'ü arıza değil, ölçüm eksiği."""
    def handler(req):
        if req.property == "properties/2":  # mweb
            raise RuntimeError(
                "400 Did you mean customEvent:card_name? "
                "Field customEvent:asset_key is not a valid dimension."
            )
        return _Resp([_Row(["gram-altin"], [100])])

    out = X._asset_interest(_Client(handler), _props(), list(X.PROFILES), "7daysAgo", "yesterday", 10)
    assert out["undefined_profiles"] == ["mweb"]
    assert out["per_profile"]["mweb"] == {"undefined": True}
    assert "error" not in out["per_profile"]["mweb"]
    # Kapsanan profiller ayrıca bildirilir ki toplam yanlış okunmasın
    assert out["covered_profiles"] == ["web", "android", "ios"]


def test_a_real_failure_is_still_reported_as_an_error():
    def handler(req):
        raise RuntimeError("503 backend unavailable")

    out = X._asset_interest(_Client(handler), {"web": "1"}, ["web"], "7daysAgo", "yesterday", 5)
    assert out["undefined_profiles"] == []
    assert "503" in out["per_profile"]["web"]["error"]


def test_dimension_missing_detector():
    assert X._dimension_missing(RuntimeError("Field customEvent:x is not a valid dimension")) is True
    assert X._dimension_missing(RuntimeError("400 Did you mean customEvent:card_name?")) is True
    assert X._dimension_missing(RuntimeError("503 backend unavailable")) is False
    assert X._dimension_missing(RuntimeError("quota exhausted")) is False


def test_mweb_own_dimensions_are_collected():
    """mWeb'de menu_item web'dekinden yüksek hacimli; sorgulanmadan bırakılmamalı."""
    pairs = {(pf, dim) for pf, dim, _ in X.BEHAVIOR_DIMENSIONS}
    assert ("mweb", "customEvent:menu_item") in pairs
    assert ("mweb", "customEvent:card_name") in pairs


def test_behavior_undefined_dimension_is_flagged_not_errored():
    def handler(req):
        raise RuntimeError("Field customEvent:from is not a valid dimension")

    out = X._behavior(_Client(handler), _props(), list(X.PROFILES), "7daysAgo", "yesterday", 5)
    g = out["groups"][0]
    assert g.get("undefined") is True
    assert "error" not in g


def test_ui_explains_the_undefined_case_and_warns_about_the_total():
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    assert "undefined_profiles" in page
    assert "tanımlı değil" in page
    assert "Toplam " in page  # eksik profil uyarısı


# ── Platform filtresi ───────────────────────────────────────────────────────

def test_resolve_profiles_filters_and_defaults_to_all():
    props = _props()
    assert X.resolve_profiles(props, "hepsi") == ["web", "mweb", "android", "ios"]
    assert X.resolve_profiles(props, None) == ["web", "mweb", "android", "ios"]
    assert X.resolve_profiles(props, "ios") == ["ios"]
    assert X.resolve_profiles(props, "IOS") == ["ios"]
    # Tanımsız property filtreden düşer
    assert X.resolve_profiles({"web": "1", "ios": ""}, "ios") == []


def test_filter_limits_the_number_of_requests(monkeypatch):
    """Tek platform seçilince kota da o oranda az harcanmalı."""
    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": True, "properties": _props()},
    )
    monkeypatch.setattr("backend.collectors.ga4._client", lambda: _Client())

    every = X.build_x_ga4_report(None, days=7, profile="hepsi")
    X._CACHE.clear()
    only_ios = X.build_x_ga4_report(None, days=7, profile="ios")
    assert only_ios["requests"] < every["requests"]
    assert only_ios["profiles"] == ["ios"]
    assert every["profiles"] == ["web", "mweb", "android", "ios"]


def test_unknown_profile_is_reported_not_silently_empty(monkeypatch):
    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": True, "properties": {"web": "1"}},
    )
    out = X.build_x_ga4_report(None, profile="android")
    assert out["ok"] is False
    assert "android" in out["error"]


def test_cache_key_separates_profiles(monkeypatch):
    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": True, "properties": _props()},
    )
    monkeypatch.setattr("backend.collectors.ga4._client", lambda: _Client())
    X.build_x_ga4_report(None, days=7, profile="web")
    other = X.build_x_ga4_report(None, days=7, profile="ios")
    assert other["cached"] is False  # farklı filtre farklı önbellek


# ── Bildirimsel kırılımlar ──────────────────────────────────────────────────

def test_breakdowns_respect_their_profile_scope():
    plan = X._plan_breakdowns(_props(), list(X.PROFILES))
    pairs = {(spec["key"], pf) for spec, pf, _ in plan}
    # appVersion / deviceModel yalnızca uygulamalarda
    assert ("app_version", "web") not in pairs
    assert ("app_version", "ios") in pairs
    assert ("device", "mweb") not in pairs
    # landing yalnızca site yüzeylerinde
    assert ("landing", "android") not in pairs
    assert ("landing", "web") in pairs
    # events her yerde
    assert all(("events", p) in pairs for p in X.PROFILES)


def test_weekday_numbers_become_names():
    assert X._label_value("dayOfWeek", "0") == "Pazar"
    assert X._label_value("dayOfWeek", "5") == "Cuma"
    assert X._label_value("dayOfWeek", "(other)") == "(other)"
    assert X._label_value("country", "3") == "3"  # yalnızca gün dönüştürülür


def test_breakdown_task_reports_undefined_dimension():
    def handler(req):
        raise RuntimeError("Field appVersion is not a valid dimension")

    spec = next(s for s in X.BREAKDOWNS if s["key"] == "app_version")
    out = X._breakdown_task(_Client(handler), spec, "ios", "4", "7daysAgo", "yesterday", 10)
    assert out["undefined"] is True and out["rows"] == []


def test_breakdown_rows_carry_value_and_metric():
    def handler(req):
        return _Resp([_Row(["screen_view"], [42])])

    spec = next(s for s in X.BREAKDOWNS if s["key"] == "events")
    out = X._breakdown_task(_Client(handler), spec, "ios", "4", "7daysAgo", "yesterday", 10)
    assert out["rows"] == [{"value": "screen_view", "raw": "screen_view", "metric": 42.0}]


def test_page_has_the_platform_filter():
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    for p in ("hepsi", "web", "mweb", "android", "ios"):
        assert 'data-xg-profile="' + p + '"' in page
    assert "profile=" in page          # istekte gönderiliyor
    assert "renderBreakdown" in page   # kırılımlar çiziliyor
