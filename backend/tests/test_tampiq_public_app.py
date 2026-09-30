"""TAMPIQ public multilingual Help / Privacy center."""
from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from backend.tampiq_i18n import get_catalog
from backend.tampiq_i18n.locales import (
    DEFAULT_LOCALE,
    NATIVE_NAMES,
    SUPPORTED_LOCALES,
    normalize_locale,
    parse_accept_language,
    resolve_locale,
)
from backend.tampiq_public_app import app

ROOT = Path(__file__).resolve().parents[2]
CLIENT = TestClient(app)


def test_http_core_routes_200():
    for path in ("/", "/support", "/privacy", "/health", "/robots.txt"):
        assert CLIENT.get(path).status_code == 200, path


def test_robots_disallow_all():
    body = CLIENT.get("/robots.txt").text
    assert "Disallow: /" in body
    assert "User-agent: *" in body


def test_pages_are_noindex():
    for path in ("/", "/support", "/privacy"):
        html = CLIENT.get(path).text
        assert "noindex" in html
        assert "nofollow" in html


def test_no_auth_and_no_projectcontrol_chrome():
    for path in ("/", "/support", "/privacy"):
        html = CLIENT.get(path).text.lower()
        assert "projectcontrol" not in html
        assert "admin/login" not in html
        assert ">dashboard<" not in html
        assert "seo agent" not in html


def test_all_locales_query_param():
    assert len(SUPPORTED_LOCALES) == 11
    client = TestClient(app)
    for loc in SUPPORTED_LOCALES:
        for path in ("/", "/support", "/privacy"):
            res = client.get(path, params={"lang": loc})
            assert res.status_code == 200, (path, loc)
            assert f'lang="{loc}"' in res.text
            assert f'data-locale="{loc}"' in res.text
            if loc in {"ar", "ur"}:
                assert 'dir="rtl"' in res.text
            else:
                assert 'dir="ltr"' in res.text
            assert NATIVE_NAMES[loc] in res.text


def test_locale_normalization_examples():
    assert normalize_locale("tr-TR") == "tr"
    assert normalize_locale("en-GB") == "en-US"
    assert normalize_locale("zh-TW") == "zh-Hant"
    assert normalize_locale("zh-HK") == "zh-Hant"
    assert normalize_locale("pt") == "pt-BR"
    assert normalize_locale("es-MX") == "es-ES"
    assert normalize_locale("ar-SA") == "ar"


def test_accept_language_and_priority():
    assert parse_accept_language("tr-TR,tr;q=0.9,en;q=0.8") == "tr"
    assert resolve_locale(query_lang="de", cookie_lang="tr", accept_language="ja") == "de"
    assert resolve_locale(query_lang=None, cookie_lang="ja", accept_language="tr") == "ja"
    assert resolve_locale(query_lang=None, cookie_lang=None, accept_language="de-DE,de;q=0.9") == "de"
    assert resolve_locale(query_lang=None, cookie_lang=None, accept_language=None) == DEFAULT_LOCALE


def test_browser_auto_detect_header():
    client = TestClient(app)
    res = client.get("/support", headers={"Accept-Language": "ja-JP,ja;q=0.9"})
    assert res.status_code == 200
    assert 'data-locale="ja"' in res.text


def test_cookie_locale_used_without_query():
    client = TestClient(app)
    client.cookies.set("tampiq_public_locale", "ar")
    res = client.get("/privacy")
    assert res.status_code == 200
    assert 'data-locale="ar"' in res.text
    assert 'dir="rtl"' in res.text


def test_support_contact_card_content():
    html = CLIENT.get("/support", params={"lang": "en-US"}).text
    assert "TAMPIQ" in html
    assert "Cem Gürsoy EVECEN" not in html
    assert "cemevecen@gmail.com" not in html.lower()
    assert "netbaboli@gmail.com" in html
    assert "2 business days" in html
    assert "Getting Started" in html
    assert "Bug Report" in html


def test_public_identity_has_no_personal_name():
    for path in ("/", "/support", "/privacy"):
        for loc in SUPPORTED_LOCALES:
            html = CLIENT.get(path, params={"lang": loc}).text
            assert "Cem Gürsoy EVECEN" not in html, (path, loc)
            assert "cemevecen@gmail.com" not in html.lower(), (path, loc)
            assert "netbaboli@gmail.com" in html or path == "/"


def test_privacy_factual_claims():
    html = CLIENT.get("/privacy", params={"lang": "en-US"}).text
    assert "on your device" in html.lower() or "on-device" in html.lower()
    assert "Face ID biometric" in html or "biometric" in html.lower()
    assert "Lottie" in html
    assert "30 September 2026" in html


def test_catalogs_exist_for_all_locales():
    catalog_dir = ROOT / "backend" / "tampiq_i18n" / "catalogs"
    missing = []
    for loc in SUPPORTED_LOCALES:
        path = catalog_dir / f"{loc}.json"
        if not path.is_file():
            missing.append(loc)
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for key in ("meta", "nav", "footer", "home", "support", "privacy"):
            assert key in data, (loc, key)
        # Merged catalog must resolve
        merged = get_catalog(loc)
        assert merged["support"]["title"]
        assert merged["privacy"]["title"]
    assert missing == [], f"Missing locale catalogs: {missing}"


def test_projectcontrol_templates_have_zero_tampiq_discovery():
    banned = ("tampiq", "TAMPIQ", "/_public/tampiq", "tampiq-support", "tampiq_public")
    skip = ROOT / "templates" / "tampiq"
    hits = []
    for path in (ROOT / "templates").rglob("*"):
        if not path.is_file() or str(path).startswith(str(skip)):
            continue
        if path.suffix.lower() not in {".html", ".js", ".css"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for token in banned:
            if token in text:
                hits.append(f"{path.relative_to(ROOT)}:{token}")
    assert hits == []


def test_main_has_no_tampiq_routes():
    main_src = (ROOT / "backend" / "main.py").read_text(encoding="utf-8")
    assert "tampiq" not in main_src.lower()
    assert "TAMPIQ" not in main_src
