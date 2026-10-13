"""Döviz /web · /mweb — android/ios Metrics kabuğu."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_routes_registered():
    src = (ROOT / "backend" / "main.py").read_text(encoding="utf-8")
    assert '@app.get("/web")' in src
    assert '@app.get("/mweb")' in src
    assert "doviz_web_metrics_page" in src
    assert "doviz_mweb_metrics_page" in src


def test_header_links_before_ios_android():
    base = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
    console_i = base.find('href="/search-console"')
    web_i = base.find('href="/web"')
    mweb_i = base.find('href="/mweb"')
    ios_i = base.find('href="/ios"')
    android_i = base.find('href="/android"')
    assert console_i > 0 and web_i > console_i and mweb_i > web_i
    assert ios_i > mweb_i and android_i > ios_i
    assert 'data-nav-match="/web"' in base
    assert 'data-nav-match="/mweb"' in base


def test_templates_and_partial():
    assert (ROOT / "templates" / "web.html").is_file()
    assert (ROOT / "templates" / "mweb.html").is_file()
    partial = (ROOT / "templates" / "partials" / "doviz_web_metrics_content.html").read_text(
        encoding="utf-8"
    )
    assert 'data-project="{{ dw_project }}"' in partial
    assert "data-fixed-platform" in partial
    assert "dw_project" in partial
    assert "ga4:sessions" in partial
    assert "virgul:net_revenue" in partial
    assert "sinemalar_datas.js?v=20" in partial
    assert 'value="180d" selected' in partial
    assert 'data-google-updates="1"' in partial
    assert "sd-google-updates" in partial
    assert "market:gram_altin" in partial
    assert "market:usd_try" not in partial
    assert "market:bitcoin" not in partial
    assert 'value="180d"' in partial
    assert 'value="last_year_h1"' in partial
    assert 'value="all"' in partial


def test_datas_js_supports_doviz_project():
    js = (ROOT / "static" / "js" / "sinemalar_datas.js").read_text(encoding="utf-8")
    assert "readProject" in js
    assert "data-fixed-platform" in js
    assert "fetchMetricSeries" in js
    assert "alignPrevMap" in js
    assert "loadGoogleUpdates" in js
    assert "google-search-status/updates" in js
    assert "DEFAULT_METRICS_DOVIZ" in js
    assert "market:gram_altin" in js
    assert js.count('"market:gram_altin"') >= 1
    # Top-10 default for doviz open
    _doviz_defaults = js.split("DEFAULT_METRICS_DOVIZ")[1].split("];")[0]
    assert "organicGoogleSearchClicks" not in _doviz_defaults
    assert "organicGoogleSearchClickThroughRate" not in _doviz_defaults
    assert "screenPageViews" in _doviz_defaults
    assert "engagementRate" in _doviz_defaults
    assert "virgul:net_revenue" in js
    # Preset set mirrors /ad-virgul richness
    assert 'v === "180d"' in js
    assert 'v === "last_year_h1"' in js
    assert 'v === "this_month"' in js
    assert "defaultPresetForProject" in js
    assert "isFixedPlatform() || PROJECT === \"doviz\"" in js


def test_overlay_blocks_app_metrics_on_web():
    js = (ROOT / "static" / "js" / "play_metric_overlay.js").read_text(encoding="utf-8")
    assert "Web/mweb only" in js
    assert "webSurface" in js
    assert "MARKET_OVERLAY_ITEMS_WEB" in js
    assert "marketItemsForPlatform" in js


def test_page_tarama_web_mweb():
    src = (ROOT / "backend" / "services" / "page_tarama.py").read_text(encoding="utf-8")
    assert '"web": ["empower_intel"' in src or '"web": [' in src
    assert "mweb" in src
    assert '"sinemalar-web"' in src
    assert '"sinemalar-mweb"' in src


def test_sinemalar_web_mweb_mirrors_doviz_shell():
    main = (ROOT / "backend" / "main.py").read_text(encoding="utf-8")
    assert '@app.get("/sinemalar-web")' in main
    assert '@app.get("/sinemalar-mweb")' in main
    partial = (ROOT / "templates" / "partials" / "doviz_web_metrics_content.html").read_text(
        encoding="utf-8"
    )
    assert 'dw_project|default' in partial or "dw_project" in partial
    assert "sinemalar" in partial
    web = (ROOT / "templates" / "sinemalar_web.html").read_text(encoding="utf-8")
    mweb = (ROOT / "templates" / "sinemalar_mweb.html").read_text(encoding="utf-8")
    assert 'dw_project=\'sinemalar\'' in web or 'dw_project="sinemalar"' in web
    assert 'dw_project=\'sinemalar\'' in mweb or 'dw_project="sinemalar"' in mweb
    assert 'data-page-tarama="sinemalar-web"' in web
    assert 'data-page-tarama="sinemalar-mweb"' in mweb
    js = (ROOT / "static" / "js" / "sinemalar_datas.js").read_text(encoding="utf-8")
    assert "isFixedPlatform() || PROJECT === \"doviz\"" in js
    overlay = (ROOT / "static" / "js" / "play_metric_overlay.js").read_text(encoding="utf-8")
    assert "fetchGa4Series(metricKey, startIso, endIso, plat, project)" in overlay
    assert "fetchVirgulSeries(metricKey, startIso, endIso, plat, project)" in overlay
