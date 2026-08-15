# -*- coding: utf-8 -*-
"""/x-data sayfa erisim — yalnizca admin e-postalar; metrik API acik."""

from backend.services.xdata_page_access import (
    is_xdata_page_allowed_email,
    is_xdata_page_path,
    member_denied_xdata_access,
    resolve_xdata_menu_visible,
)


def test_xdata_page_allowed_emails():
    assert is_xdata_page_allowed_email("cemevecen@nokta.com")
    assert is_xdata_page_allowed_email("CemEvecen@Gmail.com")
    assert not is_xdata_page_allowed_email("onurtorun@nokta.com")
    assert not is_xdata_page_allowed_email("melihengin@nokta.com")
    assert not is_xdata_page_allowed_email("gozdeunaldi@nokta.com")
    assert not is_xdata_page_allowed_email("outsider@gmail.com")
    assert not is_xdata_page_allowed_email("")


def test_xdata_page_paths_exclude_metric_apis():
    assert is_xdata_page_path("/x-data")
    assert is_xdata_page_path("/x-data/")
    assert is_xdata_page_path("/metrik")
    assert is_xdata_page_path("/metrik?x=1")
    assert not is_xdata_page_path("/api/empower-intel/series")
    assert not is_xdata_page_path("/api/empower-intel/meta")
    assert not is_xdata_page_path("/sinemalar")
    assert not is_xdata_page_path("/sinemalar?tab=datas")
    assert not is_xdata_page_path("/")


def test_xdata_menu_visible_and_denied():
    assert resolve_xdata_menu_visible(member_email="cemevecen@nokta.com") is True
    assert resolve_xdata_menu_visible(member_email="cemevecen@gmail.com") is True
    assert resolve_xdata_menu_visible(member_email="other@nokta.com") is False
    assert resolve_xdata_menu_visible(member_email=None) is False
    assert member_denied_xdata_access("other@nokta.com") is True
    assert member_denied_xdata_access("cemevecen@gmail.com") is False
