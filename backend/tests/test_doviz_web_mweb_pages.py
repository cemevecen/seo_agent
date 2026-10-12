"""Döviz /web · /mweb — android/ios Metrics kabuğu."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_routes_registered():
    src = (ROOT / "backend" / "main.py").read_text(encoding="utf-8")
    assert '@app.get("/web")' in src
    assert '@app.get("/mweb")' in src
    assert "doviz_web_metrics_page" in src
    assert "doviz_mweb_metrics_page" in src


def test_header_links_at_end():
    base = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
    web_i = base.find('href="/web"')
    mweb_i = base.find('href="/mweb"')
    settings_i = base.find('id="header-settings-dropdown"')
    assert web_i > 0 and mweb_i > web_i
    assert settings_i > mweb_i
    assert 'data-nav-match="/web"' in base
    assert 'data-nav-match="/mweb"' in base


def test_templates_and_partial():
    assert (ROOT / "templates" / "web.html").is_file()
    assert (ROOT / "templates" / "mweb.html").is_file()
    partial = (ROOT / "templates" / "partials" / "doviz_web_metrics_content.html").read_text(
        encoding="utf-8"
    )
    assert 'data-project="doviz"' in partial
    assert "data-fixed-platform" in partial
    assert "ga4:sessions" in partial
    assert "virgul:net_revenue" in partial
    assert "sinemalar_datas.js?v=11" in partial
    assert 'data-google-updates="1"' in partial
    assert "sd-google-updates" in partial


def test_datas_js_supports_doviz_project():
    js = (ROOT / "static" / "js" / "sinemalar_datas.js").read_text(encoding="utf-8")
    assert "readProject" in js
    assert "data-fixed-platform" in js
    assert "fetchMetricSeries" in js
    assert "alignPrevMap" in js
    assert "loadGoogleUpdates" in js
    assert "google-search-status/updates" in js


def test_overlay_blocks_app_metrics_on_web():
    js = (ROOT / "static" / "js" / "play_metric_overlay.js").read_text(encoding="utf-8")
    assert "Web/mweb only" in js
    assert "webSurface" in js


def test_page_tarama_web_mweb():
    src = (ROOT / "backend" / "services" / "page_tarama.py").read_text(encoding="utf-8")
    assert '"web": ["empower_intel"' in src or '"web": [' in src
    assert "mweb" in src
