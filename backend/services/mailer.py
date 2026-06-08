"""SMTP üzerinden alarm e-postaları gönderen servis."""

from __future__ import annotations

import hashlib
import html as _html_mod
import logging
import random
import re
import secrets
import smtplib
import threading
import time
import base64
import googleapiclient.discovery
from email.message import EmailMessage
from email.utils import parseaddr

# ── Realtime e-posta batch modu ──────────────────────────────────────────────
# Bir job döngüsü içinde gönderilecek tüm realtime mailleri biriktirip
# tek bir mail olarak gönderir. Alarm tespiti / DB mantığına dokunulmaz.
_batch_ctx = threading.local()
_last_realtime_batch_sent_at: float | None = None


_SEO_REALTIME_SUBJECT_PREFIX = "seo realtime"


def _is_seo_realtime_subject(subject: str) -> bool:
    """Only the consolidated SEO Realtime thread is allowed to send realtime mail."""
    return (subject or "").strip().lower().startswith(_SEO_REALTIME_SUBJECT_PREFIX)


def _compact_realtime_batch_chip(raw_subject: str) -> str:
    """Bölüm konu satırını telefon önizlemesi için kısa özete çevirir."""
    s = (raw_subject or "").strip()
    if not s:
        return ""

    low = s.lower()
    if "404 spike" in low or "404" in low and "spike" in low:
        dom = ""
        for part in re.split(r"\s*·\s*", s):
            p = part.strip()
            if "." in p and "404" not in p.lower():
                dom = p.replace("www.", "").split(".")[0]
                break
        spike = re.search(r"(\d+)\s*→\s*(\d+)", s)
        if dom and spike:
            return f"{dom} 404 {spike.group(1)}→{spike.group(2)} kul"
        return "404 spike"

    if "🚨" in s or "kritik" in low:
        inner = _compact_realtime_batch_chip(re.sub(r"🚨\s*KRİTİK\s*·\s*", "", s, flags=re.I))
        return f"KRİTİK {inner}" if inner else "KRİTİK"

    prof = ""
    m_prof = re.search(r"\[([a-z]+)\]\s*$", s, re.I)
    if m_prof:
        prof = (m_prof.group(1) or "").lower()
        s = s[: m_prof.start()].strip()

    site = ""
    tail = s
    for sep in (" — ", " - ", " — "):
        if sep in s:
            site, tail = s.split(sep, 1)
            site = site.strip()
            tail = tail.strip()
            break
    if not site:
        site = s.split(" · ", 1)[0].strip()

    first = tail.split(" · ")[0].strip() if tail else ""
    if first.startswith("+"):
        pass
    rest_n = re.search(r"\s+\+(\d+)\s*$", first)
    if rest_n:
        first = first[: rest_n.start()].strip()

    if len(first) > 34:
        first = first[:32] + "…"

    loc = f"{site}/{prof}" if prof and prof not in ("web", "") else site
    if first:
        return f"{loc} {first}".strip()
    return loc or "alarm"


def _combined_realtime_subject(items: list[tuple[str, str]]) -> str:
    """Önizlemede site + olay okunur; «SEO Realtime: N alarm» öneki yok."""
    n = len(items)
    chips = [_compact_realtime_batch_chip(subj) for subj, _ in items[:4]]
    chips = [c for c in chips if c]
    more = max(0, n - len(chips))
    line = " · ".join(chips)
    if more > 0:
        line = f"{line} +{more}" if line else f"+{more}"
    if n <= 1:
        return (line or "Realtime alarm")[:120]
    prefix = f"{n} alarm · "
    budget = 120 - len(prefix)
    if budget < 20:
        return f"{n} alarm"[:120]
    if len(line) > budget:
        line = line[: budget - 1] + "…"
    return f"{prefix}{line}"


def realtime_email_batch_begin() -> None:
    """Batch toplamayı başlat — bu thread'deki send_realtime_email çağrıları biriktirilir."""
    _batch_ctx.collecting = True
    _batch_ctx.items = []  # (subject, html_body)


def _realtime_batch_is_urgent(items: list[tuple[str, str]]) -> bool:
    for subj, _body in items:
        s = (subj or "").lower()
        if "kritik" in s or "404 spike" in s or "🚨" in subj:
            return True
    return False


def _prioritize_realtime_batch_items(items: list[tuple[str, str]]) -> list[tuple[str, str]]:
    urgent = [it for it in items if _realtime_batch_is_urgent([it])]
    rest = [it for it in items if it not in urgent]
    return urgent + rest


def realtime_email_batch_flush() -> bool:
    """Biriktirilen mailleri tek email olarak gönder; batch'i temizle."""
    global _last_realtime_batch_sent_at

    if not getattr(_batch_ctx, "collecting", False):
        return False
    items: list[tuple[str, str]] = list(getattr(_batch_ctx, "items", []))
    _batch_ctx.collecting = False
    _batch_ctx.items = []
    if not items:
        return False

    from backend.config import settings

    min_gap_min = int(getattr(settings, "ga4_realtime_email_batch_interval_minutes", 60))
    urgent = _realtime_batch_is_urgent(items)
    if min_gap_min > 0 and _last_realtime_batch_sent_at is not None and not urgent:
        elapsed = time.time() - _last_realtime_batch_sent_at
        if elapsed < min_gap_min * 60:
            logging.info(
                "SEO Realtime konsolide mail ertelendi (%d dk minimum aralık, %d bölüm atlandı).",
                min_gap_min,
                len(items),
            )
            return False

    total_sections = len(items)
    max_sections = int(getattr(settings, "ga4_realtime_email_batch_max_sections", 8))
    omitted = 0
    if total_sections > max_sections:
        items = _prioritize_realtime_batch_items(items)[:max_sections]
        omitted = total_sections - len(items)

    combined_subject = _combined_realtime_subject(items)

    sep = '<div style="border-top:2px dashed #e2e8f0;margin:22px 0 18px;"></div>'
    combined_body = (
        '<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;'
        'max-width:620px;margin:0 auto;padding:8px 0;">'
        + sep.join(body for _, body in items)
    )
    if omitted > 0:
        combined_body += (
            f'<p style="font-size:11px;color:#94a3b8;margin-top:14px;">'
            f"+ {omitted} alarm daha — /realtime ve Threshold sekmesinde.</p>"
        )
    combined_body += "</div>"

    ok = send_realtime_email(
        combined_subject,
        combined_body,
        thread_kind="combined",
        thread_key="all_sites_batch",
        is_summary=True,
    )
    if ok:
        _last_realtime_batch_sent_at = time.time()
    return ok


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from backend.config import settings
from backend.services.smtp_quota import (
    smtp_quota_release_one_send,
    smtp_quota_try_reserve_one_send,
    smtp_recipients_allowed,
)

DEFAULT_ERROR_REPORT_RECIPIENT = "cemevecen@nokta.com"


def _is_error_report_allowed_recipient(addr: str) -> bool:
    """404 günlük raporu — yalnızca @nokta.com; Gmail ve diğer alan adları hariç."""
    a = (addr or "").strip()
    if not a or "@" not in a:
        return False
    local, _, domain = a.rpartition("@")
    if not local:
        return False
    dom = domain.lower()
    if dom == "gmail.com" or dom.endswith(".gmail.com"):
        return False
    return dom == "nokta.com" or dom.endswith(".nokta.com")


def error_report_recipients() -> list[str]:
    """404 günlük raporu alıcıları — varsayılan cemevecen@nokta.com; OPERATIONS_MAIL_TO/MAIL_TO kullanılmaz."""
    raw = (settings.error_report_mail_to or "").strip() or DEFAULT_ERROR_REPORT_RECIPIENT
    all_recipients = [item.strip() for item in raw.split(",") if item.strip()]
    allowed = [r for r in all_recipients if _is_error_report_allowed_recipient(r)]
    if allowed:
        if len(allowed) < len(all_recipients):
            logging.info(
                "404 rapor alıcıları @nokta.com ile sınırlandı: %s → %s",
                ", ".join(all_recipients),
                ", ".join(allowed),
            )
        return allowed
    if all_recipients:
        logging.warning(
            "404 rapor alıcılarında geçerli @nokta.com yok (%s); varsayılan: %s",
            ", ".join(all_recipients),
            DEFAULT_ERROR_REPORT_RECIPIENT,
        )
    return [DEFAULT_ERROR_REPORT_RECIPIENT]


def send_error_report_email(subject: str, html_body: str) -> bool:
    """Günlük 404 özeti — yalnızca error_report_recipients() listesine gönderir."""
    recipients = error_report_recipients()
    return send_email(subject, html_body, recipients=recipients)


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
    """GA4 Realtime site/KPI alarm postası gönderilebilir mi (SMTP + alıcı + email bayrağı)."""
    if not settings.ga4_realtime_email_enabled:
        return False
    default_recipient_list = [item.strip() for item in settings.mail_to.split(",") if item.strip()]
    return _smtp_configured() and bool(default_recipient_list)


def is_page_alarm_mail_ready() -> bool:
    """Sayfa bazlı alarm postası gönderilebilir mi."""
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
            port = settings.smtp_port or 587
            if port == 465:
                ctx = __import__("ssl").create_default_context()
                conn = smtplib.SMTP_SSL(settings.smtp_host, port, timeout=45, context=ctx)
            else:
                conn = smtplib.SMTP(settings.smtp_host, port, timeout=45)
                conn.starttls()
            with conn:
                conn.login(settings.smtp_user, settings.smtp_password)
                conn.send_message(message)
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
    from backend.services.inbox_gmail_auth import (
        load_inbox_credentials,
        get_inbox_credential_row,
        persist_credentials_if_refreshed,
        delete_inbox_credentials,
    )
    from backend.database import SessionLocal
    from google.auth.transport.requests import Request as GoogleAuthRequest

    session = db if db is not None else SessionLocal()
    try:
        creds = load_inbox_credentials(session)
        if not creds or not creds.refresh_token:
            return False

        # Token süresi dolmuşsa yenile ve DB'ye kaydet
        if creds.expired:
            try:
                creds.refresh(GoogleAuthRequest())
                row = get_inbox_credential_row(session)
                persist_credentials_if_refreshed(session, creds, row)
                logging.info("Gmail OAuth token yenilendi ve DB'ye kaydedildi.")
            except Exception as ref_err:
                err_str = str(ref_err).lower()
                if "invalid_grant" in err_str or "token has been expired or revoked" in err_str:
                    # Kalıcı hata — token iptal edilmiş, DB'den sil ki UI yeniden bağlan uyarısı göstersin
                    try:
                        delete_inbox_credentials(session)
                        logging.warning("Gmail OAuth token kalıcı olarak geçersiz, silindi. Yeniden bağlanma gerekiyor.")
                    except Exception:
                        pass
                else:
                    logging.error("Gmail OAuth token yenileme başarısız: %s", ref_err)
                return False

        if not creds.valid:
            logging.warning("Gmail OAuth token geçersiz, Gmail API atlanıyor.")
            return False

        service = googleapiclient.discovery.build("gmail", "v1", credentials=creds, cache_discovery=False)
        raw_msg = base64.urlsafe_b64encode(message.as_bytes()).decode()
        sent_msg = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info("Gmail API ile e-posta gönderildi. Mesaj ID: %s", sent_msg.get("id"))
        return True
    except Exception as e:
        logging.error("Gmail API ile e-posta gönderimi başarısız: %s", e)
        return False
    finally:
        if db is None:
            session.close()


def send_admin_security_email(subject: str, html_body: str, recipients: list[str]) -> bool:
    """Admin güvenlik uyarıları — outbound_email_enabled kapalı olsa da SMTP/Gmail ile dener."""
    if not recipients:
        return False
    if not _smtp_configured():
        logging.warning("Admin güvenlik e-postası gönderilemedi: SMTP yapılandırması eksik")
        return False
    if not smtp_recipients_allowed(len(recipients)):
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.mail_from
    message["To"] = ", ".join(recipients)
    from backend.services.inbox_email_render import plain_text_for_mailer

    message.set_content(plain_text_for_mailer(html_body, subject=subject))
    message.add_alternative(html_body, subtype="html")

    if _gmail_api_dispatch(message):
        logging.info("Admin güvenlik e-postası Gmail API ile gönderildi: %s", subject[:100])
        return True
    ok = _smtp_dispatch_with_daily_quota(message)
    if ok:
        logging.info("Admin güvenlik e-postası gönderildi: %s", subject[:100])
    return ok


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
    from backend.services.inbox_email_render import plain_text_for_mailer

    message.set_content(plain_text_for_mailer(html_body, subject=subject))
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
    # ── Batch modu: biriktir, şimdi gönderme ─────────────────────────────────
    if getattr(_batch_ctx, "collecting", False) and not is_summary:
        _batch_ctx.items.append((subject.strip(), html_body))
        return True

    subj = subject.strip()
    if not is_summary and not _is_seo_realtime_subject(subj):
        logging.info("SEO Realtime dışı realtime e-postası iptal edildi: %s", subj[:120])
        return False

    if not settings.ga4_realtime_email_enabled:
        logging.warning("GA4 Realtime e-postası gönderilemedi: ga4_realtime_email_enabled=False")
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
    # ── Batch modu: haber alarmlarını da aynı batch'e ekle ───────────────────
    if getattr(_batch_ctx, "collecting", False):
        _batch_ctx.items.append((subject.strip(), html_body))
        return True

    subj = subject.strip()
    if not _is_seo_realtime_subject(subj):
        logging.info("SEO Realtime dışı realtime haber e-postası iptal edildi: %s", subj[:120])
        return False

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
