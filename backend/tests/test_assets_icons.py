"""Assets — doviz.com CDN ikon eşlemesi."""

from backend.services.market_sheets_config import (
    MARKET_SHEET_SERIES,
    icon_url_for,
    series_public_dict,
)


def test_every_market_series_has_a_cdn_icon_url():
    assert MARKET_SHEET_SERIES
    for series in MARKET_SHEET_SERIES:
        url = icon_url_for(series.key)
        assert url.startswith("https://cdn.doviz.com/images/")
        assert url.endswith((".png", ".svg", ".webp", ".jpg"))
        pub = series_public_dict(series)
        assert pub["icon_url"] == url
        assert pub["key"] == series.key


def test_known_asset_icons_point_to_expected_cdn_paths():
    assert icon_url_for("usd_try").endswith("/flags/usd.png")
    assert icon_url_for("bitcoin").endswith("/coin/bitcoin.png")
    assert icon_url_for("gram_altin").endswith("/other-assets/altin.png")
    assert icon_url_for("asels").endswith("/stock/ASELS.png")
    assert icon_url_for("bist100").endswith("/other-assets/bist.png")
    assert icon_url_for("xrp").endswith("/coin/ripple.png")
    assert "placeholder" in icon_url_for("not_a_real_key")


def test_assets_ui_renders_icons_and_clips_chart():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    page = (root / "templates/assets.html").read_text(encoding="utf-8")
    js = (root / "static/js/assets_page.js").read_text(encoding="utf-8")
    assert "as-icon" in page
    assert "iconHtml" in js
    assert "clip-path" in js or "clipPath" in js
    assert "as-plot-clip" in js
    assert "icon_url" in js or "iconHtml(spec" in js
    # Pasif UI temizliği
    assert "as-kpi-card__chev" not in js
    assert 'id="as-dim"' not in page
    assert 'id="as-segment"' not in page
    assert "document.body.appendChild(el.metricList)" in js
    assert "z-[10200]" in page
    # Android-like chart + table (always visible; no table-only view toggle)
    assert 'id="as-table-shell"' in page
    assert "metric_table_ux.js" in page
    assert "metric_period_compare.js" in page
    assert "pa_chart_height.js" in page
    assert "resizable_data_list.js" in page
    assert 'id="as-view-toggle"' not in page
    assert 'data-as-view="table"' not in page
    assert "SeoMetricTableUx" in js
    assert "renderHeatGrid" in js
    assert "seriesSparkSvg" in js
    assert "asChartStyle" in js
    assert 'id="as-chart-height"' in page
    assert "chart_style_buttons" in page or "as-chart-style" in page
