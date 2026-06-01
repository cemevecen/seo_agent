"""Inbox sekme yönlendirme birim testleri."""

from backend.services.inbox_sync import (
    INBOX_DEFAULT_TAB,
    INBOX_ROUTE_ALL,
    INBOX_ROUTE_DOVIZ,
    INBOX_ROUTE_NSTAT,
    INBOX_ROUTE_REKLAM,
    INBOX_ROUTE_SINEMALAR,
    INBOX_TAB_ORDER,
    _finalize_route_tag,
    _route_tag_from_addrs,
    _route_tag_from_thread,
    normalize_inbox_route_tag,
)


def test_inbox_tab_order_and_default():
    assert INBOX_DEFAULT_TAB == "doviz"
    assert INBOX_TAB_ORDER == ("doviz", "sinemalar", "nstat", "firebase", "reklam", "all")


def test_info_sinemalar_goes_to_sinemalar_not_doviz():
    src = "Delivered-To: info@sinemalar.com To: info@sinemalar.com From: noreply@instagram.com"
    assert _route_tag_from_addrs(src) == INBOX_ROUTE_SINEMALAR


def test_info_doviz_goes_to_doviz():
    src = "To: info@doviz.com Delivered-To: cemevecen@nokta.com"
    assert _route_tag_from_addrs(src) == INBOX_ROUTE_DOVIZ


def test_feedback_doviz_goes_to_doviz():
    src = "X-Original-To: feedback@doviz.com Delivered-To: cemevecen@nokta.com"
    assert _route_tag_from_addrs(src) == INBOX_ROUTE_DOVIZ


def test_feedback_sinemalar_goes_to_sinemalar():
    src = "to: feedback@sinemalar.com"
    assert _route_tag_from_addrs(src) == INBOX_ROUTE_SINEMALAR


def test_both_info_addresses_prefers_sinemalar_when_sinemalar_present():
    src = "to: info@doviz.com cc: info@sinemalar.com"
    assert _route_tag_from_addrs(src) == INBOX_ROUTE_SINEMALAR


def test_finalize_uses_sync_hint_when_headers_missing():
    assert _finalize_route_tag(INBOX_ROUTE_ALL, "cemevecen@nokta.com", "doviz") == INBOX_ROUTE_DOVIZ
    assert _finalize_route_tag(INBOX_ROUTE_ALL, "cemevecen@nokta.com", "info") == INBOX_ROUTE_DOVIZ


def test_finalize_prefers_header_over_hint():
    src = "to: info@doviz.com"
    assert _finalize_route_tag(INBOX_ROUTE_ALL, src, "sinemalar") == INBOX_ROUTE_DOVIZ


def test_thread_route_nstat_from_noreply_ziyaret_subject():
    msgs = [
        {
            "payload": {
                "headers": [
                    {"name": "From", "value": "Doviz <noreply@doviz.com>"},
                    {"name": "Subject", "value": "En çok ziyaret edilen sayfalar - 23.05.2026 20:00"},
                    {"name": "To", "value": "cemevecen@nokta.com"},
                ]
            }
        }
    ]
    assert _route_tag_from_thread(msgs, "noreply@doviz.com", "cemevecen@nokta.com") == INBOX_ROUTE_NSTAT


def test_thread_route_noreply_other_subject_goes_to_all():
    msgs = [
        {
            "payload": {
                "headers": [
                    {"name": "From", "value": "Doviz <noreply@doviz.com>"},
                    {"name": "Subject", "value": "Günlük özet raporu"},
                    {"name": "To", "value": "cemevecen@nokta.com"},
                ]
            }
        }
    ]
    assert _route_tag_from_thread(msgs, "noreply@doviz.com", "cemevecen@nokta.com") == INBOX_ROUTE_ALL


def test_reklam_goes_to_reklam_tab():
    src = "Delivered-To: reklam@nokta.com To: reklam@nokta.com"
    assert _route_tag_from_addrs(src) == INBOX_ROUTE_REKLAM


def test_finalize_reklam_uses_sync_hint():
    assert _finalize_route_tag(INBOX_ROUTE_ALL, "cemevecen@nokta.com", "reklam") == INBOX_ROUTE_REKLAM


def test_finalize_reklam_prefers_header_over_hint():
    src = "to: reklam@nokta.com"
    assert _finalize_route_tag(INBOX_ROUTE_ALL, src, "all") == INBOX_ROUTE_REKLAM


def test_shared_all_addresses_do_not_route_to_doviz():
    for addr in ("info@blogcu.com", "info@izlesene.com", "medya@nokta.com"):
        assert _route_tag_from_addrs(f"to: {addr}") is None


def test_normalize_legacy_tags():
    assert normalize_inbox_route_tag("feedback") == INBOX_ROUTE_DOVIZ
    assert normalize_inbox_route_tag("info") == INBOX_ROUTE_DOVIZ
    assert normalize_inbox_route_tag("ziyaret") == INBOX_ROUTE_NSTAT
    assert normalize_inbox_route_tag("tome") == INBOX_ROUTE_ALL
