"""OAuth / entegrasyon bağlantı kopması — admin e-posta uyarıları."""

from __future__ import annotations

import logging
from html import escape

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import NotificationDeliveryLog, Site, SupportInboxThread
from backend.services.email_templates import note_box, render_email_shell, section
from backend.services.ga4_auth import get_ga4_connection_status
from backend.services.inbox_gmail_auth import get_inbox_credential_row, inbox_oauth_is_configured
from backend.services.mailer import normalize_outbound_recipients, send_admin_security_email
from backend.services.search_console_auth import get_search_console_connection_status
from backend.services.timezone_utils import format_local_datetime, now_local

LOGGER = logging.getLogger(__name__)

NOTIFICATION_TYPE = "oauth_connection"
PANEL_URL = "https://projectcontrol.up.railway.app/settings"


def _alert_recipients() -> list[str]:
    primary = (settings.admin_login_alert_email or "").strip()
    ops = str(settings.operations_mail_to or "").strip()
    raw = primary or ops or "cemevecen@nokta.com"
    return normalize_outbound_recipients(raw)


def _delivery_exists(db: Session, *, notification_key: str) -> bool:
    return (
        db.query(NotificationDeliveryLog.id)
        .filter(
            NotificationDeliveryLog.notification_type == NOTIFICATION_TYPE,
            NotificationDeliveryLog.notification_key == notification_key,
        )
        .first()
        is not None
    )


def _record_delivery(db: Session, *, notification_key: str, subject: str, recipient: str) -> None:
    db.add(
        NotificationDeliveryLog(
            notification_type=NOTIFICATION_TYPE,
            notification_key=notification_key,
            subject=subject,
            recipient=recipient,
        )
    )
    db.commit()


def clear_oauth_connection_alert(db: Session, notification_key: str) -> None:
    """Bağlantı yeniden kurulunca aynı kopma için tekrar mail gidebilsin."""
    db.query(NotificationDeliveryLog).filter(
        NotificationDeliveryLog.notification_type == NOTIFICATION_TYPE,
        NotificationDeliveryLog.notification_key == notification_key,
    ).delete(synchronize_session=False)
    db.commit()


def collect_broken_connections(db: Session) -> list[dict[str, str]]:
    """Periyodik tarama: kopmuş OAuth / servis hesabı bağlantıları."""
    broken: list[dict[str, str]] = []

    active_sites = db.query(Site).filter(Site.is_active.is_(True)).order_by(Site.id.asc()).all()
    for site in active_sites:
        sc = get_search_console_connection_status(db, site.id)
        if sc.get("requires_reauth"):
            broken.append(
                {
                    "notification_key": f"search_console:site:{site.id}",
                    "integration": "Search Console",
                    "title": site.domain or f"Site #{site.id}",
                    "detail": str(sc.get("label") or "OAuth yeniden bağlanmalı"),
                    "action": f"{PANEL_URL} — Search Console OAuth",
                }
            )

        ga4 = get_ga4_connection_status(db, site.id)
        props = ga4.get("properties") or {}
        if props and not ga4.get("connected"):
            diag = str(ga4.get("diagnostic") or ga4.get("label") or "Service account okunamadı")
            broken.append(
                {
                    "notification_key": f"ga4:site:{site.id}",
                    "integration": "GA4",
                    "title": site.domain or f"Site #{site.id}",
                    "detail": diag,
                    "action": f"{PANEL_URL} — GA4 service account",
                }
            )

    if inbox_oauth_is_configured() and get_inbox_credential_row(db) is None:
        has_threads = db.query(SupportInboxThread.id).limit(1).first() is not None
        if has_threads:
            broken.append(
                {
                    "notification_key": "inbox:gmail",
                    "integration": "Gmail Inbox",
                    "title": "Gelen kutusu",
                    "detail": "Gmail OAuth bağlantısı yok; daha önce senkronize edilmiş konuşmalar DB'de.",
                    "action": "https://projectcontrol.up.railway.app/inbox — Gmail bağla",
                }
            )

    return broken


def _render_alert_html(items: list[dict[str, str]]) -> str:
    sections: list[str] = []
    for item in items:
        detail = escape(item.get("detail") or "")
        action = escape(item.get("action") or PANEL_URL)
        sections.append(
            section(
                f"{escape(item.get('integration') or '')} — {escape(item.get('title') or '')}",
                note_box(
                    "Durum",
                    f"<p><strong>{detail}</strong></p><p style='margin-top:8px;font-size:13px;color:#64748b'>{action}</p>",
                    tone="rose",
                ),
            )
        )
    when = format_local_datetime(now_local())
    sections.append(section("Tespit zamanı", note_box("Kayıt", f"<p>{escape(when)}</p>", tone="slate")))
    return render_email_shell(
        eyebrow="Project Control",
        title="Bağlantı / yetki kopması",
        intro=f"{len(items)} entegrasyon yeniden bağlanmayı bekliyor.",
        tone="rose",
        status_label="Yeniden bağlan",
        sections=sections,
    )


def _send_connection_alert(db: Session, items: list[dict[str, str]]) -> list[str]:
    if not settings.oauth_connection_alert_enabled:
        return []
    recipients = _alert_recipients()
    if not recipients:
        return []

    sent_subjects: list[str] = []
    for item in items:
        key = str(item.get("notification_key") or "").strip()
        if not key or _delivery_exists(db, notification_key=key):
            continue
        subject = f"[Project Control] {item.get('integration')} bağlantısı kopuk — {item.get('title')}"
        html = _render_alert_html([item])
        if send_admin_security_email(subject, html, recipients):
            _record_delivery(
                db,
                notification_key=key,
                subject=subject,
                recipient=",".join(recipients),
            )
            sent_subjects.append(subject)
            LOGGER.warning("OAuth connection alert sent: %s", key)
    return sent_subjects


def notify_oauth_connection_event(
    db: Session,
    *,
    notification_key: str,
    integration: str,
    title: str,
    detail: str,
    action: str = "",
) -> bool:
    """Anlık kopma (token revoke, collector reauth_required vb.)."""
    if not settings.oauth_connection_alert_enabled:
        return False
    item = {
        "notification_key": notification_key,
        "integration": integration,
        "title": title,
        "detail": detail,
        "action": action or PANEL_URL,
    }
    return bool(_send_connection_alert(db, [item]))


def notify_oauth_connection_broken_scan(db: Session) -> list[str]:
    """Zamanlanmış tarama."""
    return _send_connection_alert(db, collect_broken_connections(db))
