"""Dönem karşılaştırma KPI ızgarası — toplam sayı + 3/4 kolon."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_period_compare_has_toplam_sayi_and_fixed_grids():
    js = (ROOT / "static/js/notification_extras.js").read_text(encoding="utf-8")
    assert 'periodKpiCard("nt-spark-rows", "Total count"' in js
    assert "curStats.rows" in js
    assert "sparkForKey(curDaily, prevDaily, \"rows\")" in js
    assert "nt-cmp-grid-totals" in js
    assert "nt-cmp-grid-plats" in js
    assert "Total clicks" in js
    assert "Total impressions" in js


def test_notification_css_keeps_3_and_4_cols_at_all_widths():
    html = (ROOT / "templates/notification.html").read_text(encoding="utf-8")
    assert "nt-cmp-grid-totals" in html
    assert "repeat(3, minmax(0, 1fr))" in html
    assert "nt-cmp-grid-plats" in html
    assert "repeat(4, minmax(0, 1fr))" in html
    assert "notification_extras.js?v=9" in html
    # Eski kırılımlı 1/2/3 kolon grid'i geri gelmesin
    assert "grid-template-columns: 1fr;" not in html.split(".nt-cmp-grid {")[1].split("}")[0]


def test_period_compare_uses_merged_trend_and_weight():
    html = (ROOT / "templates/notification.html").read_text(encoding="utf-8")
    js = (ROOT / "static/js/notification_extras.js").read_text(encoding="utf-8")
    assert 'id="nt-trend-clicks"' in html
    assert 'id="nt-weight-clicks"' in html
    assert "Platform weight (click share)" in html
    assert "nt-trend-clicks-app" not in html
    assert "nt-trend-clicks-web" not in html
    assert "Günlük click endeksi" not in html
    assert "Günlük click endeksi" not in js
    assert "Platform % değişim" not in js
    assert "nt-period-trend-chart" not in js
    assert 'plotTrendChart("nt-trend-clicks", PLATFORM_KEYS, rows)' in html
    assert 'uniformtext: { minsize: 10, mode: "hide" }' in html
    assert "textposition: \"inside\"" in html


def test_notification_boot_loads_db_without_blocking_sync():
    html = (ROOT / "templates/notification.html").read_text(encoding="utf-8")
    boot = html.split('status("Loading data…");', 1)[1].split(
        'window.addEventListener("resize"', 1
    )[0]
    assert "loadServerState()" in boot
    assert "syncSheetFromServer" not in boot


def test_raw_list_date_shows_hour_and_minute():
    html = (ROOT / "templates/notification.html").read_text(encoding="utf-8")
    assert "function dateTimeLabel(iso)" in html
    assert 'if (key === "date") return dateTimeLabel(row.date);' in html


def test_extras_redraw_is_not_bound_twice():
    js = (ROOT / "static/js/notification_extras.js").read_text(encoding="utf-8")
    assert 'addEventListener("nt-redraw", onRedraw)' in js
    assert 'addEventListener("nt-data-ready", bootInitialRender)' not in js
    assert "periodCompareReq" in js
