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
_pending_realtime_batch_items: list[tuple[str, str]] = []
REALTIME_PERIODIC_DIGEST_NOTIF_TYPE = "realtime_periodic_digest"
REALTIME_PERIODIC_DIGEST_KEY = "all_sites_batch"


def _compact_realtime_batch_chip(raw_subject: str) -> str:
    """Bölüm konu satırını telefon önizlemesi için kısa özete çevirir."""
    s = (raw_subject or "").strip()
    if not s:
        return ""

    low = s.lower()
    if "🚨" in s or "kritik" in low:
        inner = _compact_realtime_batch_chip(re.sub(r"🚨\s*KRİTİK\s*·\s*", "", s, flags=re.I))
        return f"KRİTİK {inner}" if inner else "KRİTİK"

    prof = ""
    m_prof = re.search(r"\[([a-z]+)\]\s*$", s, re.I)
    if m_prof:
        prof = (m_prof.group(1) or "").lower()
        s = s[: m_prof.start()].strip()

    tail = s
    for sep in (" — ", " - ", " — "):
        if sep in s:
            _site, tail = s.split(sep, 1)
            tail = tail.strip()
            break

    first = tail.split(" · ")[0].strip() if tail else ""
    rest_n = re.search(r"\s+\+(\d+)\s*$", first)
    if rest_n:
        first = first[: rest_n.start()].strip()

    if len(first) > 38:
        first = first[:36] + "…"

    if prof and prof not in ("web", "") and first:
        return f"{first} [{prof}]"
    if first:
        return first
    return ""


def _combined_realtime_subject(items: list[tuple[str, str]]) -> str:
    """Konsolide konu: SEO Realtime iş parçacığı + okunabilir olay chip'leri."""
    n = len(items)
    chips = [_compact_realtime_batch_chip(subj) for subj, _ in items[:4]]
    chips = [c for c in chips if c]
    more = max(0, n - len(chips))
    line = " · ".join(chips)
    if more > 0:
        line = f"{line} +{more}" if line else f"+{more}"
    if n <= 1:
        core = line or "RT"
    else:
        prefix = f"{n} · "
        budget = 120 - len("SEO Realtime · ") - len(prefix)
        if budget < 12:
            core = str(n)
        else:
            if len(line) > budget:
                line = line[: budget - 1] + "…"
            core = f"{prefix}{line}"
    if core.lower().startswith("seo realtime"):
        return core[:120]
    return f"SEO Realtime · {core}"[:120]


def realtime_email_batch_begin() -> None:
    """Batch toplamayı başlat — ertelenmiş içerik varsa korunur."""
    global _pending_realtime_batch_items
    if getattr(_batch_ctx, "collecting", False) and getattr(_batch_ctx, "items", None):
        return
    _batch_ctx.collecting = True
    merged = list(_pending_realtime_batch_items)
    _pending_realtime_batch_items = []
    _batch_ctx.items = merged
    if not getattr(_batch_ctx, "pending_marks", None):
        _batch_ctx.pending_marks = []


def realtime_email_batch_note_mark(
    site_id: int,
    rule_ids: list[str],
    *,
    profile: str | None = None,
) -> None:
    """Konsolide mail gerçekten gittikten sonra işaretlenecek alarm kayıtları."""
    if not getattr(_batch_ctx, "collecting", False):
        return
    if not rule_ids:
        return
    marks: list[dict] = getattr(_batch_ctx, "pending_marks", None) or []
    marks.append(
        {
            "site_id": int(site_id),
            "rule_ids": [str(r) for r in rule_ids],
            "profile": profile,
        }
    )
    _batch_ctx.pending_marks = marks


def realtime_email_batch_take_pending_marks() -> list[dict]:
    marks = list(getattr(_batch_ctx, "pending_marks", []) or [])
    _batch_ctx.pending_marks = []
    return marks


def _realtime_digest_local_now():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from backend.config import settings

    tz_name = getattr(settings, "report_calendar_timezone", "Europe/Istanbul")
    return datetime.now(ZoneInfo(tz_name))


def _realtime_digest_in_quiet_hours() -> bool:
    """06:30 öncesi ve 23:00 sonrası TR — konsolide SEO Realtime maili gönderilmez."""
    now = _realtime_digest_local_now()
    minutes = now.hour * 60 + now.minute
    start = 6 * 60 + 30   # 06:30
    end = 23 * 60         # 23:00 (dahil değil)
    return minutes < start or minutes >= end


def _realtime_digest_interval_due(min_gap_min: int, db=None) -> bool:
    """Periyodik özet aralığı — önce DB (çoklu replika), sonra bellek."""
    global _last_realtime_batch_sent_at
    if min_gap_min <= 0:
        return True
    if db is not None:
        from datetime import datetime, timedelta, timezone

        from backend.models import NotificationDeliveryLog
        from sqlalchemy import desc

        last_sent = (
            db.query(NotificationDeliveryLog.sent_at)
            .filter(
                NotificationDeliveryLog.notification_type == REALTIME_PERIODIC_DIGEST_NOTIF_TYPE,
                NotificationDeliveryLog.notification_key == REALTIME_PERIODIC_DIGEST_KEY,
            )
            .order_by(desc(NotificationDeliveryLog.sent_at))
            .limit(1)
            .scalar()
        )
        if last_sent is not None:
            if last_sent.tzinfo is None:
                last_sent = last_sent.replace(tzinfo=timezone.utc)
            elapsed = datetime.now(timezone.utc) - last_sent.astimezone(timezone.utc)
            return elapsed >= timedelta(minutes=min_gap_min)
    if _last_realtime_batch_sent_at is None:
        return True
    elapsed = time.time() - _last_realtime_batch_sent_at
    return elapsed >= min_gap_min * 60


def _log_realtime_periodic_digest_sent(db, subject: str, recipient: str) -> None:
    from backend.models import NotificationDeliveryLog

    db.add(
        NotificationDeliveryLog(
            notification_type=REALTIME_PERIODIC_DIGEST_NOTIF_TYPE,
            notification_key=REALTIME_PERIODIC_DIGEST_KEY,
            subject=(subject or "")[:255],
            recipient=(recipient or "")[:255],
        )
    )
    db.commit()


def realtime_email_batch_is_collecting() -> bool:
    return bool(getattr(_batch_ctx, "collecting", False))


def realtime_email_batch_flush() -> bool:
    """Biriktirilen alarm işaretleri + periyodik SEO Realtime özet maili gönder."""
    global _last_realtime_batch_sent_at, _pending_realtime_batch_items

    if not getattr(_batch_ctx, "collecting", False):
        return False
    items: list[tuple[str, str]] = list(getattr(_batch_ctx, "items", []))

    from backend.config import settings

    min_gap_min = int(getattr(settings, "ga4_realtime_email_batch_interval_minutes", 90))

    if _realtime_digest_in_quiet_hours():
        if items:
            logging.info(
                "SEO Realtime özet maili gece penceresinde — %d bölüm ertelendi.",
                len(items),
            )
        return False

    from backend.database import SessionLocal
    from backend.services.ga4_realtime import (
        build_realtime_periodic_digest_html,
        realtime_periodic_digest_subject,
    )

    with SessionLocal() as db:
        if not _realtime_digest_interval_due(min_gap_min, db=db):
            if items:
                logging.info(
                    "SEO Realtime özet maili ertelendi (%d dk minimum aralık, %d alarm kuyrukta).",
                    min_gap_min,
                    len(items),
                )
            return False

        try:
            combined_body = build_realtime_periodic_digest_html(
                db, queued_alarm_sections=len(items)
            )
        except Exception:
            logging.exception("SEO Realtime özet maili HTML üretilemedi")
            _batch_ctx.collecting = True
            _batch_ctx.items = items
            return False

    _batch_ctx.collecting = False
    _batch_ctx.items = []

    combined_subject = realtime_periodic_digest_subject()

    ok = send_realtime_email(
        combined_subject,
        combined_body,
        thread_kind="combined",
        thread_key="all_sites_batch",
        is_summary=True,
    )
    if ok:
        _last_realtime_batch_sent_at = time.time()
        try:
            with SessionLocal() as db:
                from backend.config import settings as _settings

                recips = normalize_outbound_recipients(None, raw_setting=_settings.mail_to)
                _log_realtime_periodic_digest_sent(
                    db, combined_subject, recips[0] if recips else ""
                )
        except Exception:
            logging.exception("SEO Realtime özet maili gönderim kaydı yazılamadı")
    else:
        _batch_ctx.collecting = True
        _batch_ctx.items = items
        _pending_realtime_batch_items.extend(items)
        logging.warning(
            "SEO Realtime özet maili gönderilemedi; %d bölüm sonraki döngüye bırakıldı.",
            len(items),
        )
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

DEFAULT_MAIL_RECIPIENT = "cemevecen@nokta.com"
DEFAULT_OUTBOUND_FROM = "SEO Agent <projectcontrol@nokta.com>"


def _recipient_domain(addr: str) -> str:
    a = (addr or "").strip()
    if "@" not in a:
        return ""
    return a.rpartition("@")[2].lower()


def _is_gmail_recipient(addr: str) -> bool:
    dom = _recipient_domain(addr)
    return dom == "gmail.com" or dom.endswith(".gmail.com") or dom == "googlemail.com"


def _is_nokta_recipient(addr: str) -> bool:
    dom = _recipient_domain(addr)
    return dom == "nokta.com" or dom.endswith(".nokta.com")


def _canonical_email(addr: str) -> str:
    _, parsed = parseaddr((addr or "").strip())
    return (parsed or (addr or "").strip()).strip()


def normalize_outbound_recipients(
    recipients: list[str] | None = None,
    *,
    raw_setting: str | None = None,
    default: str = DEFAULT_MAIL_RECIPIENT,
) -> list[str]:
    """Yalnızca @nokta.com alıcıları; Gmail ve diğer alan adları çıkarılır."""
    src: list[str] = []
    if recipients:
        src.extend(item.strip() for item in recipients if item and str(item).strip())
    elif raw_setting:
        src.extend(item.strip() for item in str(raw_setting).split(",") if item.strip())

    out: list[str] = []
    seen: set[str] = set()
    dropped: list[str] = []
    for item in src:
        addr = _canonical_email(item)
        if not addr or "@" not in addr:
            continue
        if _is_gmail_recipient(addr) or not _is_nokta_recipient(addr):
            dropped.append(addr)
            continue
        key = addr.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(addr)

    if dropped:
        logging.warning(
            "Giden posta alıcıları @nokta.com dışı bırakıldı: %s",
            ", ".join(dropped),
        )

    if not out:
        return [default]
    return out


def effective_mail_from(recipient_list: list[str] | None = None) -> str:
    """Gönderen: MAIL_FROM veya varsayılan servis adresi; alıcı ile aynı posta kutusu olmaz."""
    raw = (settings.mail_from or "").strip()
    from_hdr = raw if raw else DEFAULT_OUTBOUND_FROM
    from_addr = _canonical_email(from_hdr).lower()
    recs = recipient_list or []
    if len(recs) == 1 and from_addr and from_addr == _canonical_email(recs[0]).lower():
        return DEFAULT_OUTBOUND_FROM
    return from_hdr


def _set_message_to_header(message: EmailMessage, recipients: list[str]) -> None:
    """Tek To başlığı — ikinci doğrudan atama Python 3.12+ ValueError verir."""
    value = ", ".join(recipients)
    while message.get_all("To"):
        del message["To"]
    message["To"] = value


def _recipient_addrs_from_message(message: EmailMessage) -> list[str]:
    to_raw = str(message.get("To", "") or "")
    out: list[str] = []
    for part in to_raw.split(","):
        addr = _canonical_email(part.strip())
        if addr:
            out.append(addr)
    return out


def _header_matches_recipient_list(message: EmailMessage, recipients: list[str]) -> bool:
    addrs = _recipient_addrs_from_message(message)
    if len(message.get_all("To") or []) != 1:
        return False
    if any(_is_gmail_recipient(a) for a in addrs):
        return False
    return [a.lower() for a in addrs] == [a.lower() for a in recipients]


def _outbound_recipients_ready(message: EmailMessage) -> list[str] | None:
    """Gönderim öncesi alıcı doğrula; To başlığına dokunmaz (çift To ataması yok)."""
    addrs = _recipient_addrs_from_message(message)
    safe = normalize_outbound_recipients(addrs)
    if not safe:
        logging.error(
            "E-posta gönderimi iptal: Gmail hariç geçerli alıcı yok (To=%s)",
            str(message.get("To", ""))[:160],
        )
        return None
    return safe


def _sanitize_message_recipients(message: EmailMessage) -> list[str] | None:
    """To başlığını güvenli alıcı listesine çeker (yalnızca açıkça gerekince kullanın)."""
    safe = _outbound_recipients_ready(message)
    if safe is None:
        return None
    if _header_matches_recipient_list(message, safe):
        return safe
    try:
        _set_message_to_header(message, safe)
    except ValueError:
        while message.get_all("To"):
            del message["To"]
        message["To"] = ", ".join(safe)
    return safe


def default_mail_recipients() -> list[str]:
    """MAIL_TO — Gmail hariç; boş/ yalnız Gmail ise cemevecen@nokta.com."""
    return normalize_outbound_recipients(raw_setting=settings.mail_to)


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


def _gmail_oauth_outbound_ready() -> bool:
    """Inbox OAuth bağlıysa SMTP olmadan da giden posta mümkün."""
    try:
        from backend.database import SessionLocal
        from backend.services.inbox_gmail_auth import load_inbox_credentials

        with SessionLocal() as db:
            creds = load_inbox_credentials(db)
            return bool(creds and creds.refresh_token)
    except Exception:
        return False


def _realtime_outbound_transport_ready() -> bool:
    return _outbound_transport_ready()


def is_realtime_mail_ready() -> bool:
    """GA4 Realtime site/KPI alarm postası gönderilebilir mi (SMTP veya Gmail OAuth + alıcı)."""
    if not settings.ga4_realtime_email_enabled:
        return False
    default_recipient_list = default_mail_recipients()
    return _realtime_outbound_transport_ready() and bool(default_recipient_list)


def is_page_alarm_mail_ready() -> bool:
    """Sayfa bazlı alarm postası gönderilebilir mi."""
    if not settings.ga4_realtime_email_enabled:
        return False
    if not settings.ga4_realtime_page_alert_email:
        return False
    default_recipient_list = default_mail_recipients()
    return _realtime_outbound_transport_ready() and bool(default_recipient_list)


def is_news_realtime_mail_ready() -> bool:
    """Haberler (Realtime) alarm e-postası gönderilebilir mi."""
    if not settings.ga4_realtime_email_enabled:
        return False
    if not settings.ga4_realtime_news_alert_email:
        return False
    default_recipient_list = default_mail_recipients()
    return _realtime_outbound_transport_ready() and bool(default_recipient_list)


def is_mail_configured() -> bool:
    # Varsayilan alicilar ile SMTP alanlari hazir degilse mail gönderimi sessizce pas geçilir.
    if not settings.outbound_email_enabled:
        return False
    default_recipient_list = default_mail_recipients()
    return _smtp_configured() and bool(default_recipient_list)


def _smtp_send_message_with_retries(message: EmailMessage) -> bool:
    """SMTP gönderimi (kota rezervasyonu çağıran tarafında yapılmalıdır)."""
    if _outbound_recipients_ready(message) is None:
        return False
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
                        try:
                            from backend.services.connection_alerts import notify_oauth_connection_event

                            notify_oauth_connection_event(
                                session,
                                notification_key="inbox:gmail",
                                integration="Gmail Inbox",
                                title="Gelen kutusu",
                                detail="Gmail OAuth token iptal edildi veya süresi doldu.",
                                action="https://projectcontrol.up.railway.app/inbox — Gmail yeniden bağla",
                            )
                        except Exception:
                            logging.exception("Gmail OAuth kopma maili gönderilemedi")
                    except Exception:
                        pass
                else:
                    logging.error("Gmail OAuth token yenileme başarısız: %s", ref_err)
                return False

        if not creds.valid:
            logging.warning("Gmail OAuth token geçersiz, Gmail API atlanıyor.")
            return False

        safe = _outbound_recipients_ready(message)
        if safe is None:
            return False
        _set_message_to_header(message, safe)

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


def _outbound_transport_ready() -> bool:
    if _smtp_configured():
        return True
    return bool(settings.outbound_gmail_api_enabled and _gmail_oauth_outbound_ready())


def _dispatch_outbound_message(message: EmailMessage) -> bool:
    """SMTP önce; Gmail Inbox OAuth yalnızca OUTBOUND_GMAIL_API_ENABLED=true iken yedek."""
    safe = _outbound_recipients_ready(message)
    if safe is None:
        return False
    _set_message_to_header(message, safe)

    if _smtp_configured():
        if _smtp_dispatch_with_daily_quota(message):
            return True
        if not settings.outbound_gmail_api_enabled:
            return False

    if settings.outbound_gmail_api_enabled and _gmail_oauth_outbound_ready():
        return _gmail_api_dispatch(message)
    return False


def send_admin_security_email(subject: str, html_body: str, recipients: list[str]) -> bool:
    """Admin güvenlik uyarıları — outbound_email_enabled kapalı olsa da SMTP/Gmail ile dener."""
    recipient_list = normalize_outbound_recipients(recipients)
    if not recipient_list:
        return False
    if not _outbound_transport_ready():
        logging.warning(
            "Admin güvenlik e-postası gönderilemedi: SMTP veya (OUTBOUND_GMAIL_API_ENABLED + Gmail OAuth) gerekli"
        )
        return False
    if not smtp_recipients_allowed(len(recipient_list)):
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = effective_mail_from(recipient_list)
    _set_message_to_header(message, recipient_list)
    from backend.services.inbox_email_render import plain_text_for_mailer

    message.set_content(plain_text_for_mailer(html_body, subject=subject))
    message.add_alternative(html_body, subtype="html")

    ok = _dispatch_outbound_message(message)
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
    recipient_list = normalize_outbound_recipients(recipients, raw_setting=settings.mail_to)
    if not recipient_list:
        return False
    if not _outbound_transport_ready():
        if not _smtp_configured():
            logging.warning("SMTP is not configured. Skipping email sending.")
        return False
    if not smtp_recipients_allowed(len(recipient_list)):
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = effective_mail_from(recipient_list)
    _set_message_to_header(message, recipient_list)
    from backend.services.inbox_email_render import plain_text_for_mailer

    message.set_content(plain_text_for_mailer(html_body, subject=subject))
    message.add_alternative(html_body, subtype="html")

    ok = _dispatch_outbound_message(message)
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

    if not settings.ga4_realtime_email_enabled:
        logging.warning("GA4 Realtime e-postası gönderilemedi: ga4_realtime_email_enabled=False")
        return False

    recipient_list = normalize_outbound_recipients(recipients, raw_setting=settings.mail_to)
    if not _realtime_outbound_transport_ready():
        logging.warning(
            "GA4 Realtime e-postası gönderilemedi: SMTP yapılandırması veya Gmail OAuth (inbox) gerekli"
        )
        return False
    if not recipient_list:
        logging.warning("GA4 Realtime e-postası gönderilemedi: Alıcı listesi (MAIL_TO) boş")
        return False
    if not smtp_recipients_allowed(len(recipient_list)):
        logging.warning("GA4 Realtime e-postası gönderilemedi: Alıcı sayısı sınırı aşıldı")
        return False

    message = EmailMessage()
    message["Subject"] = subj
    message["From"] = effective_mail_from(recipient_list)
    _set_message_to_header(message, recipient_list)
    from backend.services.inbox_email_render import plain_text_for_mailer

    message.set_content(plain_text_for_mailer(html_body, subject=subj))
    message.add_alternative(html_body, subtype="html")
    if thread_kind and thread_key:
        _apply_realtime_thread_headers(message, thread_kind, thread_key)

    ok = _dispatch_outbound_message(message)
    if ok:
        logging.info(
            "GA4 Realtime e-postası gönderildi: %s → %s",
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

    if not settings.ga4_realtime_email_enabled:
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: ga4_realtime_email_enabled=False")
        return False
    if not settings.ga4_realtime_news_alert_email:
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: ga4_realtime_news_alert_email=False")
        return False

    recipient_list = normalize_outbound_recipients(recipients, raw_setting=settings.mail_to)
    if not _realtime_outbound_transport_ready():
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: SMTP veya Gmail OAuth gerekli")
        return False
    if not recipient_list:
        logging.warning("GA4 Realtime haber e-postası gönderilemedi: Alıcı listesi boş")
        return False
    if not smtp_recipients_allowed(len(recipient_list)):
        return False

    message = EmailMessage()
    message["Subject"] = subj
    message["From"] = effective_mail_from(recipient_list)
    _set_message_to_header(message, recipient_list)
    from backend.services.inbox_email_render import plain_text_for_mailer

    message.set_content(plain_text_for_mailer(html_body, subject=subj))
    message.add_alternative(html_body, subtype="html")
    if thread_kind and thread_key:
        _apply_realtime_thread_headers(message, thread_kind, thread_key)

    ok = _dispatch_outbound_message(message)
    if ok:
        logging.info(
            "GA4 Realtime haber e-postası gönderildi: %s → %s",
            subj[:100],
            ", ".join(recipient_list),
        )
    return ok
