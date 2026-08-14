#!/usr/bin/env python3
"""Local Playwright smoke: Datas metric add/remove syncs KPI + chart + table ×."""

from __future__ import annotations

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
DATAS = (ROOT / "static/js/sinemalar_datas.js").read_text(encoding="utf-8")
MTUX = (ROOT / "static/js/metric_table_ux.js").read_text(encoding="utf-8")


def main() -> None:
    opts = {
        "web": [
            {"value": "xdata:sessions", "label": "Sessions"},
            {"value": "xdata:active1DayUsers", "label": "DAU"},
            {"value": "xdata:usdSpent", "label": "Revenue"},
            {"value": "xdata:impressions", "label": "Impression"},
        ],
        "mweb": [],
    }
    series_json = json.dumps(
        {
            "ok": True,
            "label": "Metric",
            "series": [
                {"key": "2026-08-01", "value": 10},
                {"key": "2026-08-02", "value": 20},
                {"key": "2026-08-03", "value": 30},
            ],
        }
    )
    html = """<!doctype html><html><body>
<section id="sd-datas-root" data-platform="web">
  <select id="sd-metric-catalog" multiple>
    <option value="xdata:sessions">Sessions</option>
    <option value="xdata:active1DayUsers">DAU</option>
    <option value="xdata:usdSpent">Revenue</option>
    <option value="xdata:impressions">Impression</option>
  </select>
  <button id="sd-metric-trigger"><span id="sd-metric-label"></span></button>
  <div id="sd-metric-list" class="hidden"><div id="sd-metric-list-scroll"></div></div>
  <input id="sd-start" value="2026-08-01"/><input id="sd-end" value="2026-08-03"/>
  <select id="sd-preset"><option value="custom">custom</option></select>
  <input id="sd-compare" type="checkbox"/>
  <button id="sd-run">Apply</button>
  <div id="sd-platform-toggle"><button data-sd-platform="web" class="is-active">Web</button></div>
  <div id="sd-status"></div>
  <div id="sd-metric-kpis"></div>
  <div id="sd-chart-style"><button data-sd-chart-style="area" class="is-active">area</button></div>
  <div id="sd-chart-height"></div><div id="sd-chart-compress"></div>
  <div id="sd-legend"></div>
  <div id="sd-chart-card"><div id="sd-chart-wrap"><svg id="sd-chart"></svg>
    <div id="sd-tooltip" class="hidden"><p id="sd-tip-title"></p><div id="sd-tip-body"></div></div>
  </div></div>
  <div id="sd-table-shell"><div id="sd-table-wrap"><table>
    <thead><tr id="sd-thead-row"></tr></thead><tbody id="sd-table"></tbody>
  </table></div></div>
  <div id="sd-cross-metric-overlay-root" data-play-metric-overlay-root>
    <div data-play-metric-overlay-panel data-play-metric-overlay-for="sd-cross-metric-overlay-root">
      <input type="checkbox" value="xdata:impressions"/>
    </div>
  </div>
</section>
<script>
window.SEO_XDATA_METRIC_OPTIONS = __OPTS__;
window.SD_XDATA_AVG_KEYS = [];
window.fetch = async function (url) {
  const u = String(url || "");
  if (u.indexOf("/api/empower-intel/series") >= 0) {
    return new Response(JSON.stringify(__SERIES__), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  }
  return new Response(JSON.stringify({ ok: false }), { status: 404 });
};
</script>
<script>__MTUX__</script>
<script>__DATAS__</script>
</body></html>"""
    html = (
        html.replace("__OPTS__", json.dumps(opts))
        .replace("__SERIES__", series_json)
        .replace("__MTUX__", MTUX)
        .replace("__DATAS__", DATAS)
    )

    with sync_playwright() as p:
        browser = p.firefox.launch()
        page = browser.new_page()
        page.set_content(html, wait_until="domcontentloaded")
        page.wait_for_timeout(900)

        kpi0 = page.locator("[data-metric-kpi-card]").count()
        chart0 = page.locator("#sd-chart .chart-series-g").count()
        remove0 = page.locator("[data-sd-col-remove]").count()
        status0 = page.locator("#sd-status").inner_text()
        assert kpi0 >= 1, f"expected initial KPIs, got {kpi0} status={status0}"
        assert chart0 == kpi0, f"chart/kpi mismatch {chart0}/{kpi0} status={status0}"
        assert remove0 >= kpi0, f"missing table remove buttons {remove0}/{kpi0}"

        page.click("#sd-metric-trigger")
        page.click('[data-sd-metric-pick="xdata:impressions"]')
        page.wait_for_timeout(700)
        kpi_add = page.locator("[data-metric-kpi-card]").count()
        chart_add = page.locator("#sd-chart .chart-series-g").count()
        assert kpi_add == kpi0 + 1, f"add KPI {kpi_add} vs {kpi0}"
        assert chart_add == kpi_add, f"add chart {chart_add}/{kpi_add}"
        assert page.locator("[data-sd-col-remove]").count() >= kpi_add
        assert page.locator(".metric-kpi-spark").count() == kpi_add

        page.click("#sd-metric-trigger")
        page.click('[data-sd-metric-pick="xdata:usdSpent"]')
        page.wait_for_timeout(700)
        kpi_rm = page.locator("[data-metric-kpi-card]").count()
        chart_rm = page.locator("#sd-chart .chart-series-g").count()
        assert kpi_rm == kpi_add - 1, f"deselect KPI {kpi_rm} vs {kpi_add}"
        assert chart_rm == kpi_rm

        page.locator('[data-sd-col-remove][data-sd-remove-kind="metric"]').first.click()
        page.wait_for_timeout(700)
        kpi_x = page.locator("[data-metric-kpi-card]").count()
        chart_x = page.locator("#sd-chart .chart-series-g").count()
        assert kpi_x == kpi_rm - 1, f"table × KPI {kpi_x} vs {kpi_rm}"
        assert chart_x == kpi_x
        assert page.locator(".metric-kpi-spark").count() == kpi_x

        legend_btns = page.locator("#sd-legend [data-sd-legend-key]")
        assert legend_btns.count() >= kpi_x, "legend buttons missing"
        mute_key = legend_btns.first.get_attribute("data-sd-legend-key")
        legend_btns.first.click()
        page.wait_for_timeout(200)
        assert page.locator(f'#sd-legend [data-sd-legend-key="{mute_key}"].is-off').count() == 1
        kpi_mute = page.locator("[data-metric-kpi-card]").count()
        chart_mute = page.locator("#sd-chart .chart-series-g").count()
        table_cols = page.locator("[data-sd-col-remove]").count()
        assert kpi_mute == kpi_x - 1, f"legend mute KPI {kpi_mute} vs {kpi_x}"
        assert chart_mute == kpi_mute, f"legend mute chart {chart_mute}/{kpi_mute}"
        assert table_cols == kpi_mute, f"legend mute table {table_cols}/{kpi_mute}"
        # unmute restores
        page.locator(f'#sd-legend [data-sd-legend-key="{mute_key}"]').click()
        page.wait_for_timeout(200)
        assert page.locator(f'#sd-legend [data-sd-legend-key="{mute_key}"].is-off').count() == 0
        assert page.locator("[data-metric-kpi-card]").count() == kpi_x

        browser.close()
        print(
            json.dumps(
                {
                    "ok": True,
                    "kpi0": kpi0,
                    "kpi_add": kpi_add,
                    "kpi_rm": kpi_rm,
                    "kpi_x": kpi_x,
                    "kpi_mute": kpi_mute,
                    "mute_key": mute_key,
                }
            )
        )


if __name__ == "__main__":
    main()
