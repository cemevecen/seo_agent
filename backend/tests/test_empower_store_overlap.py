"""Play / ASC scrape skip — Metrik (Empower) örtüşmesi."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from backend.services.empower_intel_config import (
    PLAY_CONSOLE_SKIP_METRIC_KEYS,
    STORE_EMPOWER_OVERLAP,
    play_console_skip_metric_keys,
    xdata_column_key,
    xdata_dropdown_options,
)
from backend.services.empower_intel_store import _metric_number, query_series

ROOT = Path(__file__).resolve().parents[2]


def test_overlap_rows_cover_skip_keys():
    play_keys = {row["play_key"] for row in STORE_EMPOWER_OVERLAP if row.get("play_key")}
    assert play_keys == set(PLAY_CONSOLE_SKIP_METRIC_KEYS)
    assert play_console_skip_metric_keys() == PLAY_CONSOLE_SKIP_METRIC_KEYS


def test_play_statistics_views_skip_dau_family():
    spec = importlib.util.spec_from_file_location(
        "play_console_scrape", ROOT / "scripts" / "play_console_scrape.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    skip = play_console_skip_metric_keys()
    ids = {
        str(v.get("id"))
        for v in mod.STATISTICS_VIEWS
        if str(v.get("metric_key") or "") in skip
    }
    assert ids == {"dau", "dau_mau", "active_users"}
    kept = {str(v.get("metric_key")) for v in mod.STATISTICS_VIEWS if str(v.get("metric_key") or "") not in skip}
    assert "revenue" in kept
    assert "crashes" in kept
    assert "dau" not in kept


def test_play_known_drops_dau_titles():
    spec = importlib.util.spec_from_file_location(
        "play_console_scrape_known", ROOT / "scripts" / "play_console_scrape.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    known = mod._known(mod._KNOWN_DASHBOARD, mod._KNOWN_GROW, mod._KNOWN_STATISTICS)
    folded = {t.casefold() for t in known}
    assert "günlük etkin kullanıcı sayısı" not in folded
    assert "dau/mau" not in folded
    assert "etkin cihazlar" in folded


def test_asc_batches_drop_sessions():
    spec = importlib.util.spec_from_file_location(
        "asc_console_scrape", ROOT / "scripts" / "asc_console_scrape.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mmap = mod._asc_measure_map()
    assert "sessions" not in mmap
    assert "activeDevices" in mmap
    assert "crashes" in mmap
    batches = mod._asc_measure_batches()
    assert all("sessions" not in b for b in batches)
    assert "sessions" not in mod._asc_required_metrics()
    dropped = mod._asc_drop_overlap_facts(
        [
            {"metric": "sessions", "view_id": "sessions", "date": "2026-08-01"},
            {"metric": "installs", "view_id": "installs", "date": "2026-08-01"},
        ]
    )
    assert [f["metric"] for f in dropped] == ["installs"]


def test_xdata_dropdown_covers_app_columns_minus_version():
    opts = xdata_dropdown_options("android")
    values = {o["value"] for o in opts}
    labels = {o["label"] for o in opts}
    assert "xdata:active1DayUsers" in values
    assert "xdata:dauPerMau" in values
    assert "xdata:sessions" in values
    assert "xdata:totalUsers" in values
    assert "DAU (1 Day)" in labels
    assert "xdata:appVersion" not in values
    ios_opts = {o["value"] for o in xdata_dropdown_options("ios")}
    assert ios_opts == values


def test_xdata_column_key_and_metric_number():
    assert xdata_column_key("xdata:sessions") == "sessions"
    assert xdata_column_key("active1DayUsers") == "active1DayUsers"
    assert _metric_number(True) == 1.0
    assert _metric_number("1,5") == 1.5
    assert _metric_number(None) is None


def test_xdata_dropdown_web_has_web_columns_not_app_only():
    web = {o["value"] for o in xdata_dropdown_options("web")}
    mweb = {o["value"] for o in xdata_dropdown_options("mweb")}
    android = {o["value"] for o in xdata_dropdown_options("android")}
    assert web == mweb
    assert "xdata:organicGoogleSearchClicks" in web
    assert "xdata:screenPageViews" in web
    assert "xdata:appVersion" not in web
    assert "xdata:crashAffectedUsers" not in web
    assert "xdata:organicGoogleSearchClicks" not in android
    assert "xdata:crashAffectedUsers" in android


def test_play_metric_overlay_js_has_xdata_and_drops_overlap():
    text = (ROOT / "static/js/play_metric_overlay.js").read_text(encoding="utf-8")
    assert "fetchXdataSeries" in text
    assert "/api/empower-intel/series" in text
    assert "DROPPED_OVERLAY_KEYS" in text
    assert 'key: "dau"' not in text
    assert 'key: "dau_mau"' not in text
    assert 'key: "active_users"' not in text
    android_block = text.split("var METRIC_GROUPS_IOS")[0]
    ios_block = text.split("var METRIC_GROUPS_IOS")[1].split("var GA4_OVERLAY_ITEMS")[0]
    assert 'key: "sessions"' not in ios_block
    assert 'key: "active_devices"' in android_block
    assert 'key: "crashes"' in ios_block
    assert "METRIC_GROUPS_WEB" in text
    assert "METRIC_GROUPS_MWEB" in text
    assert "GA4 (Web)" in text
    assert "Virgül (MWeb)" in text
    assert 'XDATA_PLATFORMS = ["android", "ios", "web", "mweb"]' in text
    ad = (ROOT / "templates" / "ad.html").read_text(encoding="utf-8")
    assert "function mzMetricOverlayPlatform" in ad
    assert 'toLowerCase() !== "doviz"' in ad
    assert 'branch === "mweb"' in ad
    assert "function mzPeerOverlayPlatform" in ad
    assert "mz-cross-metric-overlay-root" in ad
    assert "Android metrics" in ad
    assert "MWeb metrics" in ad
    assert "fetchSelectedSeries" in text
    assert "data-overlay-label-prefix" in text
    android_html = (ROOT / "templates" / "android.html").read_text(encoding="utf-8")
    assert 'id="pa-compare"' not in android_html
    assert "pa-cross-metric-overlay-root" in android_html
    assert "iOS metrics" in android_html
    assert "var paCrossSeq" in android_html
    assert "seq !== paCrossSeq" in android_html
    assert "function dedupePaCrossOverlays" in android_html
    ios_html = (ROOT / "templates" / "ios.html").read_text(encoding="utf-8")
    assert "ia-cross-metric-overlay-root" in ios_html
    assert "Android metrics" in ios_html
    assert "var iaCrossSeq" in ios_html
    assert "seq !== iaCrossSeq" in ios_html
    assert "onChange !== named" in text
    assert "emitChange" in text
    partial = (ROOT / "templates" / "partials" / "play_metric_overlay_select.html").read_text(encoding="utf-8")
    assert "data-overlay-label-prefix" in partial
    assert "MARKET_OVERLAY_ITEMS" in text
    chrome = (ROOT / "templates" / "partials" / "ga4_global_filter_chrome.html").read_text(encoding="utf-8")
    assert "ga4-play-metric-overlay-root" in chrome
    assert "market_overlay_select" not in chrome
    assert "app_empower_overlay_select" not in chrome
    ga4 = (ROOT / "templates" / "ga4.html").read_text(encoding="utf-8")
    assert "function ga4OnPlayMetricOverlayChange" in ga4
    assert "PlayMetricOverlay.apply" in ga4
    assert "function ga4ChartOverlayPlatform" in ga4
    assert "function ga4SyncPlayMetricOverlay" in ga4


def test_query_series_rejects_unknown_without_db():
    out = query_series(None, platform="android", metric="xdata:notAMetric")  # type: ignore[arg-type]
    assert out["ok"] is False
    skipped = query_series(None, platform="android", metric="xdata:appVersion")  # type: ignore[arg-type]
    assert skipped["ok"] is False

