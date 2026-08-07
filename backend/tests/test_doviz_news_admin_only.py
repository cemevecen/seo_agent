"""Doviz News: Google Sheet asla çekilmez; kaynak admin/DB."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.services import doviz_news_sheet as dns


def test_news_list_url_matches_admin_guide():
    from backend.services.doviz_news_admin import news_list_url

    url = news_list_url(1)
    assert "www.doviz.com/admin/news" in url
    assert "type=N" in url
    assert "status=1" in url
    assert "is_advertorial=0" in url
    assert "source=all" in url
    assert "sort=id_desc" in url


def test_fetch_never_calls_google_sheet(monkeypatch):
    dns._CACHE = None

    def boom(*_a, **_k):
        raise AssertionError("Google Sheet fetch must not run")

    monkeypatch.setattr(
        "backend.services.backlink_csv.fetch_public_sheet_csv",
        boom,
        raising=False,
    )

    with patch.object(dns, "_load_doviz_news_rows_from_db", return_value=[]):
        with patch("backend.config.settings") as settings:
            settings.doviz_admin_news_direct_scrape = False
            settings.doviz_admin_notification_sync_enabled = True
            with pytest.raises(ValueError, match="Tek kaynak admin"):
                dns.fetch_doviz_news_rows(force=True, prefer_sheet=True)


def test_fetch_keeps_admin_db_snapshot_without_sheet():
    dns._CACHE = None
    rows = [
        {
            "id": "910489",
            "title": "Test",
            "date": "2026-08-04T14:54:00",
            "category": "Gündem",
            "active": True,
            "source": "",
            "source_key": "",
            "is_own": True,
        }
    ]
    with patch.object(dns, "_load_doviz_news_rows_from_db", return_value=rows):
        with patch.object(dns, "_db_snapshot_source", return_value="doviz_admin_bridge"):
            with patch("backend.config.settings") as settings:
                settings.doviz_admin_news_direct_scrape = False
                settings.doviz_admin_notification_sync_enabled = True
                with patch(
                    "backend.services.doviz_notification_admin.admin_http_proxy",
                    return_value="",
                ):
                    with patch(
                        "backend.services.doviz_notification_admin.admin_credentials_configured",
                        return_value=False,
                    ):
                        got = dns.fetch_doviz_news_rows(force=True, prefer_sheet=True)
    assert got == rows


def test_fetch_tries_admin_when_proxy_configured():
    dns._CACHE = None
    admin_rows = [{"id": "1", "title": "A", "date": "2026-08-04T10:00:00", "category": "X", "active": True}]
    with patch.object(dns, "_load_doviz_news_rows_from_db", return_value=[]):
        with patch("backend.config.settings") as settings:
            settings.doviz_admin_news_direct_scrape = False
            settings.doviz_admin_notification_sync_enabled = True
            with patch(
                "backend.services.doviz_notification_admin.admin_credentials_configured",
                return_value=True,
            ):
                with patch(
                    "backend.services.doviz_notification_admin.admin_http_proxy",
                    return_value="http://127.0.0.1:8888",
                ):
                    with patch(
                        "backend.services.doviz_news_admin.fetch_active_news_rows_from_admin",
                        return_value={"rows": admin_rows, "source_url": "https://www.doviz.com/admin/news"},
                    ):
                        with patch.object(dns, "set_doviz_news_rows_cache") as set_cache:
                            got = dns.fetch_doviz_news_rows(force=True)
    assert got == admin_rows
    set_cache.assert_called_once()
