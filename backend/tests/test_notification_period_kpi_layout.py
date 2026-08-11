"""Dönem karşılaştırma KPI ızgarası — toplam sayı + 3/4 kolon."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_period_compare_has_toplam_sayi_and_fixed_grids():
    js = (ROOT / "static/js/notification_extras.js").read_text(encoding="utf-8")
    assert 'periodKpiCard("nt-spark-rows", "Toplam sayı"' in js
    assert "curStats.rows" in js
    assert "sparkForKey(curDaily, prevDaily, \"rows\")" in js
    assert "nt-cmp-grid-totals" in js
    assert "nt-cmp-grid-plats" in js
    assert "Toplam click" in js
    assert "Toplam impression" in js


def test_notification_css_keeps_3_and_4_cols_at_all_widths():
    html = (ROOT / "templates/notification.html").read_text(encoding="utf-8")
    assert "nt-cmp-grid-totals" in html
    assert "repeat(3, minmax(0, 1fr))" in html
    assert "nt-cmp-grid-plats" in html
    assert "repeat(4, minmax(0, 1fr))" in html
    assert "notification_extras.js?v=6" in html
    # Eski kırılımlı 1/2/3 kolon grid'i geri gelmesin
    assert "grid-template-columns: 1fr;" not in html.split(".nt-cmp-grid {")[1].split("}")[0]
