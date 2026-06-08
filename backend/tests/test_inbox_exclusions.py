"""Inbox — Instagram sosyal özet filtreleri."""

from backend.services.inbox_sync import inbox_thread_is_excluded


def test_instagram_digest_snippet_excluded():
    snippet = (
        "sinemalarcom, see what's been happening on Instagram, "
        "others recently added to their stories, others started following you, "
        "unread messages and more in your feed"
    )
    assert inbox_thread_is_excluded(snippet=snippet)


def test_sinemalarcom_instagram_opener_excluded():
    assert inbox_thread_is_excluded(
        subject="sinemalarcom",
        snippet="See what's been happening on Instagram",
    )


def test_normal_support_mail_not_excluded():
    assert not inbox_thread_is_excluded(
        subject="Reklam teklifi",
        snippet="Merhaba info@sinemalar.com ekibi, web sitenizde banner…",
        from_addrs="musteri@example.com",
    )


def test_marker_alone_excluded():
    assert inbox_thread_is_excluded(snippet="… and more in your feed")
