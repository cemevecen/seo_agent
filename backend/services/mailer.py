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


def is_realtime_mail_ready() -> bool:
    """GA4 Realtime alarm postası gönderilebilir mi (SMTP + alıcı + realtime posta bayrakları)."""
    if not settings.ga4_realtime_email_enabled:
        return False
    if not settings.ga4_realtime_page_alert_email:
        return False
    default_recipient_list = [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    return _smtp_configured() and bool(default_recipient_list)


def is_mail_configured() -> bool:
    # Varsayilan alicilar ile SMTP alanlari hazir degilse mail gönderimi sessizce pas geçilir.
    if not settings.outbound_email_enabled:
        return False
    default_recipient_list = [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    return _smtp_configured() and bool(default_recipient_list)


def send_email(subject: str, html_body: str, recipients: list[str] | None = None) -> bool:
    """
    SMTP ile HTML e-posta gönderir.
    Geçici hatalarda (4xx) yeniden deneme mekanizması içerir.
    """
    if not settings.outbound_email_enabled:
        logging.debug("outbound_email_enabled=false; e-posta gönderilmedi: %s", subject[:80])
        return False
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


def send_realtime_email(subject: str, html_body: str, recipients: list[str] | None = None) -> bool:
    """
    GA4 Realtime alarm e-postası (site metrikleri ve sayfa listesi alarmları).

    - ``outbound_email_enabled`` ile koşullanmaz (günlük özet / genel dış posta kapalı olsa da çalışır).
    - ``ga4_realtime_email_enabled`` ve ``ga4_realtime_page_alert_email`` açık olmalı.
    - Geçici SMTP hatalarında ``send_email`` ile aynı yeniden deneme mantığı kullanılır.
    """
    if not settings.ga4_realtime_email_enabled:
        logging.debug(
            "GA4 Realtime e-postası atlandı (ga4_realtime_email_enabled=false): %s",
            subject[:120],
        )
        return False
    if not settings.ga4_realtime_page_alert_email:
        logging.debug(
            "GA4 Realtime e-postası atlandı (ga4_realtime_page_alert_email=false): %s",
            subject[:120],
        )
        return False

    recipient_list = recipients or [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    if not _smtp_configured() or not recipient_list:
        if not _smtp_configured():
            logging.warning(
                "GA4 Realtime e-postası gönderilemedi: SMTP alanları eksik veya yerel placeholder şifre (local-). "
                "Konu: %s",
                subject[:120],
            )
        else:
            logging.warning(
                "GA4 Realtime e-postası gönderilemedi: MAIL_TO boş. Konu: %s",
                subject[:120],
            )
        return False

    subj = subject.strip()
    if not subj.startswith("[GA4 Realtime]"):
        subj = f"[GA4 Realtime] {subj}"

    message = EmailMessage()
    message["Subject"] = subj
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipient_list)
    message.set_content("GA4 Realtime alarm — düz metin özet.")
    message.add_alternative(html_body, subtype="html")

    MAX_RETRIES = 3
    INITIAL_BACKOFF_S = 15

    for attempt in range(MAX_RETRIES):
        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=45) as smtp:
                smtp.starttls()
                smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(message)
            logging.info(
                "GA4 Realtime e-postası gönderildi: %s → %s",
                subj[:100],
                ", ".join(recipient_list),
            )
            return True
        except smtplib.SMTPException as e:
            is_temporary_error = isinstance(e, smtplib.SMTPResponseException) and 400 <= e.smtp_code < 500

            if is_temporary_error and (attempt < MAX_RETRIES - 1):
                backoff_time = INITIAL_BACKOFF_S * (2**attempt) + random.uniform(0, 5)
                logging.warning(
                    "GA4 Realtime SMTP geçici hata (kod %s). %.1f sn sonra yeniden denenecek (%d/%d).",
                    getattr(e, "smtp_code", "?"),
                    backoff_time,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(backoff_time)
            else:
                logging.error(
                    "GA4 Realtime e-postası başarısız (%d deneme). Son hata: %s — Konu: %s",
                    attempt + 1,
                    e,
                    subj[:120],
                )
                return False
        except OSError as e:
            logging.error("GA4 Realtime SMTP bağlantı hatası (%s): %s", settings.smtp_host, e)
            return False
    return False
