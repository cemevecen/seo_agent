"""Sinemalar Datas — metrik ekle/çıkar senkron sözleşmesi (spark/chart/table)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATAS_JS = ROOT / "static" / "js" / "sinemalar_datas.js"
MTUX_JS = ROOT / "static" / "js" / "metric_table_ux.js"
PARTIAL = ROOT / "templates" / "partials" / "sinemalar_datas_content.html"


def test_toggle_metric_schedules_reload():
    text = DATAS_JS.read_text(encoding="utf-8")
    assert "function toggleMetric(" in text
    assert "function scheduleReload(" in text
    # Toggle / clear must refresh KPI + chart + table (not only the dropdown label)
    assert "scheduleReload();" in text
    toggle_block = text.split("function toggleMetric(", 1)[1].split("function clearMetrics(", 1)[0]
    assert "scheduleReload()" in toggle_block
    clear_block = text.split("function clearMetrics(", 1)[1].split("function positionMetricList(", 1)[0]
    assert "scheduleReload()" in clear_block


def test_table_remove_wired_for_metric_and_overlay():
    text = DATAS_JS.read_text(encoding="utf-8")
    assert "data-sd-col-remove" in text
    assert "function removeSeriesFromUi(" in text
    assert "function uncheckOverlayMetric(" in text
    assert "renderStandardHeaderCell" in text
    assert "renderTransposedMetricLabel" in text
    assert "metricRemoveButtonHtml" in text


def test_mtux_supports_transposed_metric_label_hook():
    text = MTUX_JS.read_text(encoding="utf-8")
    assert "renderTransposedMetricLabel" in text


def test_datas_partial_uses_bumped_cache_bust():
    text = PARTIAL.read_text(encoding="utf-8")
    assert "sinemalar_datas.js?v=" in text
    assert "metric_table_ux.js?v=" in text


def test_keep_at_least_one_primary_metric():
    text = DATAS_JS.read_text(encoding="utf-8")
    assert "Keep at least one metric." in text
    assert "state.selected.length <= 1" in text


def test_legend_toggle_mutes_chart_kpi_and_table():
    text = DATAS_JS.read_text(encoding="utf-8")
    assert "legendMuted" in text
    assert "function toggleLegendMuted(" in text
    assert "function bindLegendEvents(" in text
    assert "data-sd-legend-key" in text
    assert "function isLegendMuted(" in text
    assert "function visibleSeriesList(" in text
    assert "renderLegend(seriesAll)" in text
    assert "visibleSeriesList(seriesAll)" in text
    # KPI / chart / table must respect mute (not only legend styling)
    assert "selectedMetrics().filter(function (key) {\n      return !isLegendMuted(key);" in text
    assert "return !s.dashed && !isLegendMuted(s.key);" in text
    assert 'renderMetricKpis();\n    renderChart();\n    renderTable();' in text


def test_datas_partial_legend_host():
    text = PARTIAL.read_text(encoding="utf-8")
    assert 'id="sd-legend"' in text
    assert "sinemalar_datas.js?v=9" in text
