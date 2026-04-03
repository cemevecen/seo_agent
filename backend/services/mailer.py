"""SMTP üzerinden alarm e-postaları gönderen servis."""

from __future__ import annotations

import logging
import random
import smtplib
import time
from email.message import EmailMessage

from backend.config import settings


def _smtp_configured() -> bool:
    required = [settings.smtp_host, settings.smtp_user, settings.smtp_password, settings.mail_from]
    return all(value and value.strip() and not value.startswith("local-") for value in required)


def is_mail_configured() -> bool:
    # Varsayilan alicilar ile SMTP alanlari hazir degilse mail gönderimi sessizce pas geçilir.
    default_recipient_list = [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    return _smtp_configured() and bool(default_recipient_list)


def send_email(subject: str, html_body: str, recipients: list[str] | None = None) -> bool:
    """
    SMTP ile HTML e-posta gönderir.
    Geçici hatalarda (4xx) yeniden deneme mekanizması içerir.
    """
    recipient_list = recipients or [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    if not _smtp_configured() or not recipient_list:
        if not _smtp_configured():
            logging.warning("SMTP is not configured. Skipping email sending.")
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipient_list)
    message.set_content("This is a plain-text fallback for the HTML email.")
    message.add_alternative(html_body, subtype="html")

    MAX_RETRIES = 3
    INITIAL_BACKOFF_S = 15  # 15 saniye ile başla

    for attempt in range(MAX_RETRIES):
        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=45) as smtp:
                smtp.starttls()
                smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(message)
            logging.info(f"Email with subject '{subject}' sent successfully to {', '.join(recipient_list)}.")
            return True
        except smtplib.SMTPException as e:
            # Hatanın geçici (4xx) olup olmadığını kontrol et
            is_temporary_error = isinstance(e, smtplib.SMTPResponseException) and 400 <= e.smtp_code < 500

            if is_temporary_error and (attempt < MAX_RETRIES - 1):
                # Exponential backoff + jitter
                backoff_time = INITIAL_BACKOFF_S * (2**attempt) + random.uniform(0, 5)
                logging.warning(
                    f"Temporary SMTP error (Code: {e.smtp_code}). "
                    f"Retrying in {backoff_time:.2f} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(backoff_time)
            else:
                # Ya kalıcı hata ya da son deneme de başarısız oldu
                logging.error(
                    f"Failed to send email with subject '{subject}' after {attempt + 1} attempts. "
                    f"Final error: {e}"
                )
                return False
        except OSError as e:
            logging.error(f"SMTP bağlantı hatası (host: {settings.smtp_host}): {e}")
            return False
    return False
