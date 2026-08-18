"""x-ga4 — GA4'ün kullanılmayan boyut/metrikleri.

Sözleşme: yalnızca GA4 Data API kullanılır, bir blok düşerse diğerleri ayakta
kalır, `(not set)` satırları listeyi yutmaz ve web/mWeb'de crash-free yazılmaz.
"""

from __future__ import annotations

import re
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
        return _Resp([_Row(["v"] * max(1, len(names)), [5, 5, 5])])

    monkeypatch.setattr(
        "backend.services.ga4_auth.get_ga4_connection_status",
        lambda db, site_id: {"connected": True, "properties": _props()},
    )
    monkeypatch.setattr("backend.collectors.ga4._client", lambda: _Client(handler))

    out = X.build_x_ga4_report(None, days=7)
    assert out["ok"] is True
    assets = next(b for b in out["breakdowns"] if b["key"] == "asset_key")
    assert all(v.get("error") for v in assets["per_profile"].values())
    # Diger container'lar etkilenmez
    assert out["blocks"]["user_stability"]["ok"] is True
    events = next(b for b in out["breakdowns"] if b["key"] == "events")
    assert any(v.get("rows") for v in events["per_profile"].values())


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
    """(not set) satiri listeyi yutmamali — filtre istekte olmali."""
    client = _Client()
    spec = next(s for s in X.BREAKDOWNS if s["key"] == "asset_key")
    X._breakdown_task(client, spec, "web", "1", "7daysAgo", "yesterday", 10)
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
    page = _dlab_ui()
    assert "Crash-free" not in page and "crashFreeUsersRate" not in page


def test_header_clutter_is_removed():
    page = _dlab_ui()
    assert "Pencere " not in page
    assert ">Dönem:<" not in page and "Dönem:" not in page
    assert "BigQuery" not in page


def test_page_column_is_capped_and_overflow_moves_to_a_sub_row():
    page = _dlab_ui()
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


def test_missing_property_is_skipped_in_the_plan():
    plan = X._plan_breakdowns({"web": "", "ios": "4"}, ["web", "ios"])
    assert plan and all(pid == "4" and pf == "ios" for _, pf, pid in plan)


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


def _dlab_ui() -> str:
    """d-lab arayüzünün tamamı — sayfa + ortak CSS + ortak JS.

    Kart çizimi android/ios sekmeleriyle paylaşıldığı için üç dosyaya bölündü.
    Testler davranışı sınıyor, dosya yerleşimini değil; hepsi birlikte okunur.
    """
    return "\n".join(
        (ROOT / part).read_text(encoding="utf-8")
        for part in (
            "templates/x_ga4.html",
            "static/css/dlab.css",
            "static/js/dlab_cards.js",
        )
    )


# ── Yalnızca GA4: başka kaynak sızmamalı ────────────────────────────────────

def test_module_uses_only_the_ga4_data_api():
    src = (ROOT / "backend/services/x_ga4.py").read_text(encoding="utf-8")
    for forbidden in ("bigquery", "google.cloud", "selenium", "requests.get", "httpx"):
        assert forbidden not in src.lower(), forbidden


def test_page_and_tab_are_registered():
    base = (ROOT / "templates/base.html").read_text(encoding="utf-8")
    assert 'href="/d-lab"' in base and ">d-lab<" in base
    main = (ROOT / "backend/main.py").read_text(encoding="utf-8")
    assert '@app.get("/d-lab")' in main
    assert "x_ga4_router" in main
    page = _dlab_ui()
    # Kart çizimi android/ios sekmeleriyle paylaşıldığı için ortak dosyaya taşındı
    assert "/static/js/dlab_cards.js" in page
    cards = (ROOT / "static/js/dlab_cards.js").read_text(encoding="utf-8")
    assert "/api/x-ga4/report" in cards


def test_android_and_ios_tabs_embed_their_own_lab_section():
    """android-lab / ios-lab: sekmenin en altında, kapalı, yalnızca kendi yüzeyi."""
    partial = (ROOT / "templates/partials/dlab_section.html").read_text(encoding="utf-8")
    # Kapalı dropdown olmalı: <details> «open» ile başlamamalı
    assert "<details" in partial and "<details open" not in partial
    # Kapalıyken istek atılmamalı; ilk açılışta yüklenmeli
    assert "autoload: false" in partial
    assert "loadOnce" in partial and 'addEventListener("toggle"' in partial
    # Sunucuya yüzey filtresi gitmeli (istemcide ayıklamak kota harcardı)
    cards = (ROOT / "static/js/dlab_cards.js").read_text(encoding="utf-8")
    assert '"&profile=" + encodeURIComponent(fixedProfile)' in cards

    for tab, profile in (("android", "android"), ("ios", "ios")):
        page = (ROOT / f"templates/{tab}.html").read_text(encoding="utf-8")
        assert 'partials/dlab_section.html' in page, tab
        assert f'lab_profile="{profile}"' in page, tab
        assert f'lab_title="{profile}-lab"' in page, tab
        # En altta olmalı: include'dan sonra jinja etiketi dışında içerik kalmamalı
        tail = page.rsplit('{% include "partials/dlab_section.html" %}', 1)[1]
        rest = re.sub(r"\{%.*?%\}", "", tail).strip()
        assert rest == "", f"{tab}: lab bölümünden sonra içerik var → {rest[:80]}"


def test_old_url_still_resolves():
    """Paylasilmis /x-ga4 linki kirilmamali."""
    main = (ROOT / "backend/main.py").read_text(encoding="utf-8")
    chunk = main.split('@app.get("/x-ga4")', 1)[1][:400]
    assert "RedirectResponse" in chunk and "/d-lab" in chunk


# ── Özel boyut property başına tanımlı: eksiklik gizlenmemeli ───────────────

def test_undefined_dimension_is_reported_as_setup_gap_not_an_error():
    """GA4 «is not a valid dimension» 400'u ariza degil, olcum eksigi."""
    def handler(req):
        raise RuntimeError(
            "400 Did you mean customEvent:card_name? "
            "Field customEvent:asset_key is not a valid dimension."
        )

    spec = next(s for s in X.BREAKDOWNS if s["key"] == "asset_key")
    out = X._breakdown_task(_Client(handler), spec, "mweb", "2", "7daysAgo", "yesterday", 10)
    assert out["undefined"] is True
    assert "error" not in out


def test_a_real_failure_is_still_reported_as_an_error():
    def handler(req):
        raise RuntimeError("503 backend unavailable")

    spec = next(s for s in X.BREAKDOWNS if s["key"] == "events")
    out = X._breakdown_task(_Client(handler), spec, "web", "1", "7daysAgo", "yesterday", 5)
    assert "undefined" not in out
    assert "503" in out["error"]


def test_dimension_missing_detector():
    assert X._dimension_missing(RuntimeError("Field customEvent:x is not a valid dimension")) is True
    assert X._dimension_missing(RuntimeError("400 Did you mean customEvent:card_name?")) is True
    assert X._dimension_missing(RuntimeError("503 backend unavailable")) is False
    assert X._dimension_missing(RuntimeError("quota exhausted")) is False


def test_mweb_own_dimensions_are_collected():
    """mWeb'de menu_item web'dekinden yuksek hacimli; sorgulanmadan birakilmamali."""
    plan = X._plan_breakdowns(_props(), list(X.PROFILES))
    pairs = {(spec["key"], pf) for spec, pf, _ in plan}
    assert ("menu_item", "mweb") in pairs
    assert ("card_name", "mweb") in pairs
    assert ("nav_from", "ios") in pairs
    # from yalnizca iOS'ta gecerli — Android'e bosuna istek atilmaz
    assert ("nav_from", "android") not in pairs


def test_custom_dimensions_are_containers_too():
    """Davranis boyutlari ayri blok degil, ayni bildirimsel listede."""
    keys = {s["key"] for s in X.BREAKDOWNS}
    assert {"asset_key", "nav_from", "search_text", "menu_item", "card_name"} <= keys


def test_ui_reports_gaps_outside_the_filter():
    page = _dlab_ui()
    assert "Kapsam disi" in page or "Kapsam d" in page
    assert "tanimli degil" in page or "tanml" in page or "tanımlı değil" in page


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


def test_global_platform_filter_is_gone():
    """Filtre artik container basina; sayfa ustunde toplu filtre yok."""
    page = _dlab_ui()
    assert "data-xg-profile" not in page


def test_each_container_has_its_own_filter():
    page = _dlab_ui()
    assert 'data-card="' in page and 'data-p="' in page
    assert "paintCard" in page
    # Filtre yalnizca verisi olan profilleri listeler
    assert "withData" in page
    assert "(pp[p].rows || []).length > 0" in page


def test_containers_flow_without_row_gaps():
    """Grid satirlari en uzun karta hizalayip bosluk birakiyordu; sutun akisi."""
    page = _dlab_ui()
    assert "column-width" in page
    assert "break-inside: avoid" in page


# ── Responsive tablolar ─────────────────────────────────────────────────────

def test_tables_are_responsive():
    page = _dlab_ui()
    assert "@media (max-width: 640px)" in page
    assert ".xg-table thead { display: none; }" in page
    assert "content: attr(data-label)" in page
    assert 'data-label="' in page
    assert "white-space: nowrap" in page
    assert "width: 45%" in page
    assert ".xg-scroll { overflow-x: auto" in page


