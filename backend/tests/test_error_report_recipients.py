"""404 rapor e-posta alıcıları."""

from unittest.mock import patch

from backend.services.mailer import DEFAULT_ERROR_REPORT_RECIPIENT, error_report_recipients


def test_error_report_recipients_filters_gmail():
    with patch("backend.services.mailer.settings") as mock_settings:
        mock_settings.error_report_mail_to = ""
        mock_settings.operations_mail_to = "cemevecen@gmail.com, cemevecen@nokta.com"
        assert error_report_recipients() == ["cemevecen@nokta.com"]


def test_error_report_recipients_explicit_override():
    with patch("backend.services.mailer.settings") as mock_settings:
        mock_settings.error_report_mail_to = "cemevecen@gmail.com, ops@nokta.com"
        mock_settings.operations_mail_to = ""
        assert error_report_recipients() == ["ops@nokta.com"]


def test_error_report_recipients_default_when_no_nokta():
    with patch("backend.services.mailer.settings") as mock_settings:
        mock_settings.error_report_mail_to = "cemevecen@gmail.com"
        mock_settings.operations_mail_to = ""
        assert error_report_recipients() == [DEFAULT_ERROR_REPORT_RECIPIENT]
