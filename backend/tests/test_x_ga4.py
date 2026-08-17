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
    X._asset_interest(client, _props(), "7daysAgo", "yesterday", 10)
    assert client.requests, "istek yapılmadı"
    req = client.requests[0]
    assert req.dimension_filter is not None
    values = list(req.dimension_filter.not_expression.filter.in_list_filter.values)
    assert "(not set)" in values and "" in values


def test_crash_free_is_only_requested_for_apps():
    """Web'de GA4 sabit 1.0 döndürüyor; ölçülmemiş değeri yazmayız."""
    seen = []

    def handler(req):
        seen.append((req.property, [m.name for m in req.metrics]))
        return _Resp([_Row([], [1, 1, 1])])

    out = X._user_stability(_Client(handler), _props())
    crash_props = {p for p, mets in seen if "crashFreeUsersRate" in mets}
    assert crash_props == {"properties/3", "properties/4"}  # yalnızca android + ios
    web = next(r for r in out["rows"] if r["profile"] == "web")
    assert "crashFreeUsersRate" not in web


def test_content_depth_uses_engagement_not_scroll():
    """scrolledUsers bu property'de veri döndürmüyor; süre üzerinden hesaplanır."""
    assert "userEngagementDuration" in X.DEPTH_METRICS
    assert "scrolledUsers" not in X.DEPTH_METRICS


def test_seconds_per_view_is_computed_and_zero_safe():
    def handler(req):
        return _Resp([_Row(["/a"], [100, 500, 10]), _Row(["/b"], [0, 0, 0])])

    out = X._content_depth(_Client(handler), {"web": "1"}, "7daysAgo", "yesterday", 10)
    a = next(r for r in out["rows"] if r["page"] == "/a")
    assert a["seconds_per_view"] == 5.0
    b = next(r for r in out["rows"] if r["page"] == "/b")
    assert b["seconds_per_view"] == 0.0  # sıfıra bölme yok


def test_hourly_keeps_the_other_bucket_visible():
    """GA4'ün (other) kovası sessizce yutulmamalı."""
    def handler(req):
        return _Resp([_Row(["9"], [10, 12]), _Row(["(other)"], [999, 1000])])

    out = X._hourly(_Client(handler), {"web": "1"}, "7daysAgo", "yesterday")
    s = out["series"]["web"]
    assert [h["hour"] for h in s["hours"]] == [9]
    assert s["other_users"] == 999


def test_hours_are_sorted_numerically():
    def handler(req):
        return _Resp([_Row(["22"], [1, 1]), _Row(["3"], [1, 1]), _Row(["11"], [1, 1])])

    out = X._hourly(_Client(handler), {"web": "1"}, "7daysAgo", "yesterday")
    assert [h["hour"] for h in out["series"]["web"]["hours"]] == [3, 11, 22]


def test_missing_property_is_skipped_without_calling_ga4():
    client = _Client()
    out = X._asset_interest(client, {"web": "", "ios": "4"}, "7daysAgo", "yesterday", 5)
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

    out = X._asset_interest(_Client(handler), _props(), "7daysAgo", "yesterday", 10)
    assert out["undefined_profiles"] == ["mweb"]
    assert out["per_profile"]["mweb"] == {"undefined": True}
    assert "error" not in out["per_profile"]["mweb"]
    # Kapsanan profiller ayrıca bildirilir ki toplam yanlış okunmasın
    assert out["covered_profiles"] == ["web", "android", "ios"]


def test_a_real_failure_is_still_reported_as_an_error():
    def handler(req):
        raise RuntimeError("503 backend unavailable")

    out = X._asset_interest(_Client(handler), {"web": "1"}, "7daysAgo", "yesterday", 5)
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

    out = X._behavior(_Client(handler), _props(), "7daysAgo", "yesterday", 5)
    g = out["groups"][0]
    assert g.get("undefined") is True
    assert "error" not in g


def test_ui_explains_the_undefined_case_and_warns_about_the_total():
    page = (ROOT / "templates/x_ga4.html").read_text(encoding="utf-8")
    assert "undefined_profiles" in page
    assert "tanımlı değil" in page
    assert "Toplam " in page  # eksik profil uyarısı
