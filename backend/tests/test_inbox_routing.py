"""Inbox sekme yönlendirme birim testleri."""

from backend.services.inbox_sync import (
    _finalize_route_tag,
    _route_tag_from_addrs,
    _route_tag_from_thread,
    normalize_inbox_route_tag,
)


def test_info_sinemalar_goes_to_sinemalar_not_info():
    src = "Delivered-To: info@sinemalar.com To: info@sinemalar.com From: noreply@instagram.com"
    assert _route_tag_from_addrs(src) == "sinemalar"


def test_info_doviz_goes_to_info():
    src = "To: info@doviz.com Delivered-To: cemevecen@nokta.com"
    assert _route_tag_from_addrs(src) == "info"


def test_feedback_doviz_goes_to_info():
    src = "X-Original-To: feedback@doviz.com Delivered-To: cemevecen@nokta.com"
    assert _route_tag_from_addrs(src) == "info"


def test_feedback_sinemalar_goes_to_sinemalar():
    src = "to: feedback@sinemalar.com"
    assert _route_tag_from_addrs(src) == "sinemalar"


def test_both_info_addresses_prefers_sinemalar_when_sinemalar_present():
    src = "to: info@doviz.com cc: info@sinemalar.com"
    assert _route_tag_from_addrs(src) == "sinemalar"


def test_finalize_uses_sync_hint_when_headers_missing():
    assert _finalize_route_tag("tome", "cemevecen@nokta.com", "info") == "info"
    assert _finalize_route_tag("tome", "cemevecen@nokta.com", "feedback") == "info"


def test_finalize_prefers_header_over_hint():
    src = "to: info@doviz.com"
    assert _finalize_route_tag("tome", src, "sinemalar") == "info"


def test_thread_route_from_headers():
    msgs = [
        {
            "payload": {
                "headers": [
                    {"name": "From", "value": "sinemalarcom <mail.instagram.com>"},
                    {"name": "To", "value": "info@sinemalar.com"},
                    {"name": "Delivered-To", "value": "cemevecen@nokta.com"},
                ]
            }
        }
    ]
    route_src = "info@sinemalar.com cemevecen@nokta.com"
    assert _route_tag_from_thread(msgs, route_src, "cemevecen@nokta.com") == "sinemalar"


def test_normalize_legacy_feedback_tag():
    assert normalize_inbox_route_tag("feedback") == "info"
    assert normalize_inbox_route_tag("info") == "info"
