"""Sayfa tarama — üst «Sayfayı güncelle» yuvası ve katalog."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

PAGES = {
    "home": "templates/partials/dashboard_content.html",
    "android": "templates/android.html",
    "ios": "templates/ios.html",
    "news": "templates/doviz_news.html",
    "virgul": "templates/ad.html",
    "notification": "templates/notification.html",
    "firebase": "templates/partials/firebase_content.html",
    "app": "templates/partials/app_content.html",
    "vitals": "templates/web_vitals.html",
    "alerts": "templates/partials/alerts_content.html",
    "seo": "templates/seo_audit.html",
    "s-firebase": "templates/s_firebase.html",
    "backlinks": "templates/backlinks.html",
    "policy": "templates/partials/policy_content.html",
    "errors": "templates/errors.html",
}


def test_base_has_overlay_and_script():
    text = (ROOT / "templates/base.html").read_text(encoding="utf-8")
    assert "pc-page-tarama-overlay" in text
    assert "/static/js/page_tarama.js" in text
    assert "pc-page-tarama-bar" in text


def test_all_listed_pages_have_slot_and_key():
    for key, rel in PAGES.items():
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert f'data-page-tarama="{key}"' in text, rel
        assert "data-page-tarama-slot" in text, rel


def test_js_catalog_covers_all_pages():
    js = (ROOT / "static/js/page_tarama.js").read_text(encoding="utf-8")
    assert "Sayfayı güncelle" in js
    assert "127.0.0.1:18765" in js
    for key in PAGES:
        assert f"{key}:" in js or f'"{key}":' in js, key
    for path in (
        "/sync-play",
        "/sync-asc",
        "/sync-firebase",
        "/sync-gsc-cwv",
        "/sync-news",
        "/sync-virgul",
        "/sync-market",
        "/sync-gsc-links",
        "/sync-policy",
        "/sync-noads",
        "/sync-seo-audit",
        "/api/errors/refresh-all/start",
        "/alerts/refresh",
    ):
        assert path in js, path
