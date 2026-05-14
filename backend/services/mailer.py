"""SMTP üzerinden alarm e-postaları gönderen servis."""

from __future__ import annotations

import hashlib
import logging
import random
import re
import secrets
import smtplib
import time
import base64
import googleapiclient.discovery
from email.message import EmailMessage
from email.utils import parseaddr
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from backend.config import settings
from backend.services.smtp_quota import (
    smtp_quota_release_one_send,
    smtp_quota_try_reserve_one_send,
    smtp_recipients_allowed,
)


def _smtp_message_id_host() -> str:
    """Message-ID @ sağ tarafı (mail_from içindeki alan adı)."""
    _, addr = parseaddr(settings.mail_from or "")
    addr = (addr or "").strip()
    if "@" in addr:
        return addr.rsplit("@", 1)[-1].lower()
    return "seo-agent.local"


def _realtime_thread_root_message_id(thread_kind: str, thread_key: str) -> str:
    """Aynı iş parçacığında kalması için sabit sanal kök Message-ID (Gmail References)."""
    host = _smtp_message_id_host()
    kind = re.sub(r"[^a-z0-9-]", "", (thread_kind or "rt").lower())[:24] or "rt"
    key = re.sub(r"[^a-z0-9.]", "", (thread_key or "x").lower())[:48]
    if not key:
        key = hashlib.sha256((thread_kind + thread_key).encode()).hexdigest()[:20]
    return f"<ga4rt.{kind}.{key}@{host}>"


def _apply_realtime_thread_headers(message: EmailMessage, thread_kind: str, thread_key: str) -> None:
    root = _realtime_thread_root_message_id(thread_kind, thread_key)
    host = _smtp_message_id_host()
    kind = re.sub(r"[^a-z0-9-]", "", (thread_kind or "rt").lower())[:24] or "rt"
    key = re.sub(r"[^a-z0-9.]", "", (thread_key or "x").lower())[:48] or "x"
    token = secrets.token_hex(6)
    message["Message-ID"] = f"<ga4rt.{kind}.{key}.{token}@{host}>"
    message["In-Reply-To"] = root
    message["References"] = root


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


def is_news_realtime_mail_ready() -> bool:
    """Haberler (Realtime) alarm e-postası gönderilebilir mi."""
    if not settings.ga4_realtime_email_enabled:
        return False
    if not settings.ga4_realtime_news_alert_email:
        return False
    default_recipient_list = [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    return _smtp_configured() and bool(default_recipient_list)


def is_mail_configured() -> bool:
    # Varsayilan alicilar ile SMTP alanlari hazir degilse mail gönderimi sessizce pas geçilir.
    if not settings.outbound_email_enabled:
        return False
    default_recipient_list = [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    return _smtp_configured() and bool(default_recipient_list)


def _smtp_send_message_with_retries(message: EmailMessage) -> bool:
    """SMTP gönderimi (kota rezervasyonu çağıran tarafında yapılmalıdır)."""
    MAX_RETRIES = 3
    INITIAL_BACKOFF_S = 15
    subj = str(message.get("Subject", ""))[:120]

    for attempt in range(MAX_RETRIES):
        try:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=45) as smtp:
                smtp.starttls()
                smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(message)
            return True
        except smtplib.SMTPException as e:
            is_temporary_error = isinstance(e, smtplib.SMTPResponseException) and 400 <= e.smtp_code < 500

            if is_temporary_error and (attempt < MAX_RETRIES - 1):
                backoff_time = INITIAL_BACKOFF_S * (2**attempt) + random.uniform(0, 5)
                logging.warning(
                    "Temporary SMTP error (Code: %s). Retrying in %.2f seconds... (Attempt %d/%d)",
                    getattr(e, "smtp_code", "?"),
                    backoff_time,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(backoff_time)
            else:
                logging.error(
                    "Failed to send email with subject '%s' after %d attempts. Final error: %s",
                    subj,
                    attempt + 1,
                    e,
                )
                return False
        except OSError as e:
            logging.error("SMTP bağlantı hatası (host: %s): %s", settings.smtp_host, e)
            return False
    return False


def _smtp_dispatch_with_daily_quota(message: EmailMessage) -> bool:
    """Günlük kota rezervasyonu + gönderim; tam başarısızlıkta rezervi geri alır."""
    if not smtp_quota_try_reserve_one_send():
        return False
    success = False
    try:
        success = _smtp_send_message_with_retries(message)
        return success
    finally:
        if not success:
            smtp_quota_release_one_send()


def _gmail_api_dispatch(message: EmailMessage, db: Session | None = None) -> bool:
    """Gmail API (OAuth) üzerinden e-posta gönderir — SMTP port kısıtlamalarını aşmak için idealdir."""
    from backend.services.inbox_gmail_auth import load_inbox_credentials
    from backend.database import SessionLocal
    
    # Session yönetimi: dışarıdan db gelmediyse yeni session aç/kapat
    session = db if db is not None else SessionLocal()
    try:
        creds = load_inbox_credentials(session)
        if not creds:
            return False
        
        # API discovery'yi cache_discovery=False ile yapıyoruz (bazı ortamlarda dosya izni hatası vermemesi için)
        service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds, cache_discovery=False)
        raw_msg = base64.urlsafe_b64encode(message.as_bytes()).decode()
        
        sent_msg = service.users().messages().send(userId='me', body={'raw': raw_msg}).execute()
        logging.info("Gmail API ile e-posta gönderildi. Mesaj ID: %s", sent_msg.get('id'))
        return True
    except Exception as e:
        logging.error("Gmail API ile e-posta gönderimi başarısız: %s", e)
        return False
    finally:
        if db is None:
            session.close()


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
    if not smtp_recipients_allowed(len(recipient_list)):
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipient_list)
    message.set_content("This is a plain-text fallback for the HTML email.")
    message.add_alternative(html_body, subtype="html")

    # ÖNCE GMAIL API (OAuth) DENE (Railway SMTP engeline takılmaz)
    if _gmail_api_dispatch(message):
        logging.info("E-posta GMAIL API (OAuth) ile gönderildi: %s", subject[:100])
        return True

    # FALLBACK: SMTP (Eğer Gmail API bağlı değilse veya hata verdiyse)
    ok = _smtp_dispatch_with_daily_quota(message)
    if ok:
        logging.info(
            "Email with subject '%s' sent successfully to %s.",
            subject[:100],
            ", ".join(recipient_list),
        )
    return ok


def send_realtime_email(
    subject: str,
    html_body: str,
    recipients: list[str] | None = None,
    *,
    thread_kind: str | None = None,
    thread_key: str | None = None,
    is_summary: bool = False,
) -> bool:
    """
    GA4 Realtime alarm e-postası (site metrikleri ve sayfa listesi alarmları).

    - ``outbound_email_enabled`` ile koşullanmaz (günlük özet / genel dış posta kapalı olsa da çalışır).
    - ``ga4_realtime_email_enabled`` açık olmalı.
    - ``ga4_realtime_page_alert_email`` ise sadece bireysel (is_summary=False) maillerde zorunludur.
    - Haber başlığı alarmları: ``send_realtime_news_email`` ve ``ga4_realtime_news_alert_email``.
    - Geçici SMTP hatalarında ``send_email`` ile aynı yeniden deneme mantığı kullanılır.
    """
    if not settings.ga4_realtime_email_enabled:
        logging.warning("GA4 Realtime e-postası gönderilemedi: ga4_realtime_email_enabled=False")
        return False
    if not is_summary and not settings.ga4_realtime_page_alert_email:
        logging.warning("GA4 Realtime e-postası gönderilemedi: ga4_realtime_page_alert_email=False")
        return False

    recipient_list = recipients or [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    if not _smtp_configured():
        logging.warning("GA4 Realtime e-postası gönderilemedi: SMTP yapılandırması eksik (Host/User/Pass/From kontrol edin)")
        return False
    if not recipient_list:
        logging.warning("GA4 Realtime e-postası gönderilemedi: Alıcı listesi (MAIL_TO) boş")
        return False
    if not smtp_recipients_allowed(len(recipient_list)):
        logging.warning("GA4 Realtime e-postası gönderilemedi: Alıcı sayısı sınırı aşıldı")
        return False

    subj = subject.strip()

    message = EmailMessage()
    message["Subject"] = subj
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipient_list)
    message.set_content("GA4 Realtime alarm — düz metin özet.")
    message.add_alternative(html_body, subtype="html")
    if thread_kind and thread_key:
        _apply_realtime_thread_headers(message, thread_kind, thread_key)

    # ÖNCE GMAIL API (OAuth) DENE (Railway SMTP engeline takılmaz)
    if _gmail_api_dispatch(message):
        logging.info("GA4 Realtime e-postası GMAIL API (OAuth) ile gönderildi: %s", subj[:100])
        return True

    # FALLBACK: SMTP (Eğer Gmail API bağlı değilse veya hata verdiyse)
    ok = _smtp_dispatch_with_daily_quota(message)
    if ok:
        logging.info(
            "GA4 Realtime e-postası SMTP ile gönderildi: %s → %s",
            subj[:100],
            ", ".join(recipient_list),
        )
    return ok


def send_realtime_news_email(
    subject: str,
    html_body: str,
    recipients: list[str] | None = None,
    *,
    thread_kind: str | None = None,
    thread_key: str | None = None,
) -> bool:
    """GA4 Realtime «Haberler» alarm e-postası (sayfa postasından bağımsız bayrak)."""
    if not settings.ga4_realtime_email_enabled:
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: ga4_realtime_email_enabled=False")
        return False
    if not settings.ga4_realtime_news_alert_email:
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: ga4_realtime_news_alert_email=False")
        return False

    recipient_list = recipients or [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    if not _smtp_configured():
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: SMTP yapılandırması eksik")
        return False
    if not recipient_list:
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: Alıcı listesi boş")
        return False
    if not smtp_recipients_allowed(len(recipient_list)):
        return False

    subj = subject.strip()

    message = EmailMessage()
    message["Subject"] = subj
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipient_list)
    message.set_content("GA4 Realtime haber alarmı — düz metin özet.")
    message.add_alternative(html_body, subtype="html")
    if thread_kind and thread_key:
        _apply_realtime_thread_headers(message, thread_kind, thread_key)

    # ÖNCE GMAIL API (OAuth) DENE
    if _gmail_api_dispatch(message):
        logging.info("GA4 Realtime haber e-postası GMAIL API (OAuth) ile gönderildi: %s", subj[:100])
        return True

    # FALLBACK: SMTP
    ok = _smtp_dispatch_with_daily_quota(message)
    if ok:
        logging.info(
            "GA4 Realtime haber e-postası SMTP ile gönderildi: %s → %s",
            subj[:100],
            ", ".join(recipient_list),
        )
    return ok
