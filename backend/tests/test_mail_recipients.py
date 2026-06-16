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


def test_sanitize_message_recipients_strips_gmail_from_to_header():
    from email.message import EmailMessage

    from backend.services.mailer import _sanitize_message_recipients

    msg = EmailMessage()
    msg["To"] = "cemevecen@gmail.com, cemevecen@nokta.com"
    safe = _sanitize_message_recipients(msg)
    assert safe == ["cemevecen@nokta.com"]
    assert msg["To"] == "cemevecen@nokta.com"


def test_outbound_recipients_ready_never_rewrites_to_header():
    from email.message import EmailMessage

    from backend.services.mailer import _outbound_recipients_ready, _set_message_to_header

    msg = EmailMessage()
    _set_message_to_header(msg, ["ops@nokta.com", "medya@nokta.com"])
    before = msg["To"]
    assert _outbound_recipients_ready(msg) is not None
    assert _outbound_recipients_ready(msg) is not None
    assert msg["To"] == before


def test_sanitize_after_gmail_then_smtp_path():
    """Gmail + SMTP yolu To'ya ikinci kez yazmaz; yalnızca doğrular."""
    from email.message import EmailMessage

    from backend.services.mailer import (
        _outbound_recipients_ready,
        _sanitize_message_recipients,
        _set_message_to_header,
    )

    msg = EmailMessage()
    msg["Subject"] = "t"
    msg["From"] = "noreply@nokta.com"
    _set_message_to_header(msg, ["cemevecen@nokta.com"])
    assert _outbound_recipients_ready(msg) == ["cemevecen@nokta.com"]
    assert _outbound_recipients_ready(msg) == ["cemevecen@nokta.com"]
    assert msg["To"] == "cemevecen@nokta.com"

    msg2 = EmailMessage()
    msg2["To"] = "cemevecen@gmail.com, cemevecen@nokta.com"
    first = _sanitize_message_recipients(msg2)
    assert first == ["cemevecen@nokta.com"]
    second = _sanitize_message_recipients(msg2)
    assert second == ["cemevecen@nokta.com"]
    assert msg2["To"] == "cemevecen@nokta.com"


def test_default_mail_recipients_from_settings():
    with patch("backend.services.mailer.settings") as mock_settings:
        mock_settings.mail_to = "cemevecen@gmail.com, cemevecen@nokta.com"
        assert default_mail_recipients() == ["cemevecen@nokta.com"]


def test_operations_recipients_filters_gmail():
    from backend.services.operations_notifier import operations_recipients

    with patch("backend.services.operations_notifier.settings") as mock_settings:
        mock_settings.operations_mail_to = "cemevecen@gmail.com, cemevecen@nokta.com"
        assert operations_recipients() == ["cemevecen@nokta.com"]
