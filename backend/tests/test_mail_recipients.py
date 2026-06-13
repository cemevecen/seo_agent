"""Giden posta alıcı normalizasyonu — Gmail hariç."""

from unittest.mock import patch

from backend.services.mailer import (
    DEFAULT_MAIL_RECIPIENT,
    default_mail_recipients,
    normalize_outbound_recipients,
)


def test_normalize_outbound_recipients_strips_gmail():
    out = normalize_outbound_recipients(
        ["cemevecen@gmail.com", "cemevecen@nokta.com", "ops@nokta.com"]
    )
    assert out == ["cemevecen@nokta.com", "ops@nokta.com"]


def test_normalize_outbound_recipients_gmail_only_falls_back():
    out = normalize_outbound_recipients(["cemevecen@gmail.com", "other@gmail.com"])
    assert out == [DEFAULT_MAIL_RECIPIENT]


def test_default_mail_recipients_from_settings():
    with patch("backend.services.mailer.settings") as mock_settings:
        mock_settings.mail_to = "cemevecen@gmail.com, cemevecen@nokta.com"
        assert default_mail_recipients() == ["cemevecen@nokta.com"]


def test_operations_recipients_filters_gmail():
    from backend.services.operations_notifier import operations_recipients

    with patch("backend.services.operations_notifier.settings") as mock_settings:
        mock_settings.operations_mail_to = "cemevecen@gmail.com, cemevecen@nokta.com"
        assert operations_recipients() == ["cemevecen@nokta.com"]
