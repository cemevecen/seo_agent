"""SMTP üzerinden alarm e-postaları gönderen servis."""

from __future__ import annotations

import smtplib
from email.message import EmailMessage

from backend.config import settings


def is_mail_configured() -> bool:
    # SMTP alanları hazır değilse mail gönderimi sessizce pas geçilir.
    required = [settings.smtp_host, settings.smtp_user, settings.smtp_password, settings.mail_from, settings.mail_to]
    return all(value and value.strip() and not value.startswith("local-") for value in required)


def send_email(subject: str, html_body: str, recipients: list[str] | None = None) -> bool:
    """SMTP ile HTML e-posta gönderir."""
    if not is_mail_configured():
        return False

    recipient_list = recipients or [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    if not recipient_list:
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipient_list)
    message.set_content("Bu e-posta HTML içerik taşır.")
    message.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(settings.smtp_user, settings.smtp_password)
        smtp.send_message(message)
    return True