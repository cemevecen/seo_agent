# -*- coding: utf-8 -*-
"""/ad erisim allowlist testleri."""

from backend.services.ad_page_access import (
    is_ad_page_allowed_email,
    is_ad_page_path,
    member_denied_ad_access,
    resolve_ad_menu_visible,
)


def test_ad_page_allowed_emails():
    assert is_ad_page_allowed_email("cemevecen@nokta.com")
    assert is_ad_page_allowed_email("CemEvecen@Gmail.com")
    assert not is_ad_page_allowed_email("onurtorun@nokta.com")
    assert not is_ad_page_allowed_email("gozdeunaldi@nokta.com")
    assert not is_ad_page_allowed_email("")


def test_ad_page_paths():
    assert is_ad_page_path("/ad")
    assert is_ad_page_path("/ad/app-banner")
    assert is_ad_page_path("/ad-virgul/app-banner")
    assert is_ad_page_path("/api/mz-analytics/summary")
    assert is_ad_page_path("/api/mz-analytics/ga4-app-banner")
    assert is_ad_page_path("/api/mz-analytics/app-empower/overlay")
    assert not is_ad_page_path("/")
    assert not is_ad_page_path("/realtime")
    assert not is_ad_page_path("/api/home/realtime")
    # Virgul ana sayfa / API Sheets /ad gate'ine girmemeli
    assert not is_ad_page_path("/ad-virgul")
    assert not is_ad_page_path("/api/virgul-analytics/summary")
    assert not is_ad_page_path("/api/virgul-analytics/ingest")


def test_ad_menu_visible_and_denied():
    assert resolve_ad_menu_visible(member_email="cemevecen@nokta.com") is True
    assert resolve_ad_menu_visible(member_email="other@nokta.com") is False
    assert resolve_ad_menu_visible(member_email=None) is False
    assert member_denied_ad_access("other@nokta.com") is True
    assert member_denied_ad_access("cemevecen@gmail.com") is False
