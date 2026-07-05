"""Inbox send-as alias selection helpers."""

import pytest
from email.utils import parseaddr

from backend.services import inbox_sync


def test_normalize_requested_send_as_accepts_allowed_choices():
    assert inbox_sync.normalize_requested_send_as("Cem <cemevecen@nokta.com>") == "cemevecen@nokta.com"
    assert inbox_sync.normalize_requested_send_as("info@doviz.com") == "info@doviz.com"
    assert inbox_sync.normalize_requested_send_as("info@sinemalar.com") == "info@sinemalar.com"


def test_normalize_requested_send_as_rejects_unknown_sender():
    with pytest.raises(RuntimeError, match="Gönderen adresi izinli değil"):
        inbox_sync.normalize_requested_send_as("someone@example.com")


def test_resolve_requested_send_as_uses_verified_alias_display_name():
    class _SendAs:
        def list(self, userId):
            return self

        def execute(self):
            return {
                "sendAs": [
                    {
                        "sendAsEmail": "info@doviz.com",
                        "displayName": "Döviz Destek",
                        "verificationStatus": "accepted",
                    }
                ]
            }

    class _Settings:
        def sendAs(self):
            return _SendAs()

    class _Users:
        def settings(self):
            return _Settings()

    class _Service:
        def users(self):
            return _Users()

    resolved = inbox_sync._resolve_requested_send_as(
        _Service(),
        "info@doviz.com",
        account_email="cemevecen@nokta.com",
    )
    display_name, email = parseaddr(resolved)
    assert display_name
    assert email == "info@doviz.com"


def test_resolve_requested_send_as_requires_gmail_alias():
    class _SendAs:
        def list(self, userId):
            return self

        def execute(self):
            return {"sendAs": [{"sendAsEmail": "cemevecen@nokta.com", "isPrimary": True}]}

    class _Settings:
        def sendAs(self):
            return _SendAs()

    class _Users:
        def settings(self):
            return _Settings()

    class _Service:
        def users(self):
            return _Users()

    with pytest.raises(RuntimeError, match="alias olarak tanımlı"):
        inbox_sync._resolve_requested_send_as(
            _Service(),
            "info@doviz.com",
            account_email="cemevecen@nokta.com",
        )
