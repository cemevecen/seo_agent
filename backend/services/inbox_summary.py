"""Gelen kutusu senkronu ve 5 sekmeli özet e-postası."""

from __future__ import annotations

import html
import logging
import os
from collections import defaultdict
from datetime import datetime

from sqlalchemy.orm import Session

from backend.models import SupportInboxMessage, SupportInboxThread
from backend.services import inbox_gmail_auth, inbox_sync, mailer

logger = logging.getLogger(__name__)

# UI sekmeleriyle aynı sıra
INBOX_SUMMARY_SECTIONS: tuple[tuple[str, str, str, str], ...] = (
    ("firebase", "Firebase", "#b45309", "#fffbeb"),
    ("info", "info@doviz (+ feedback@doviz)", "#1d4ed8", "#eff6ff"),
    ("sinemalar", "sinemalar@", "#4338ca", "#eef2ff"),
    ("ziyaret", "Ziyaret", "#047857", "#ecfdf5"),
    ("tome", "to:me", "#475569", "#f8fafc"),
)


def _inbox_summary_email_disabled() -> bool:
    """Varsayılan açık; INBOX_SUMMARY_EMAIL_ENABLED=false ile kapatılır."""
    raw = (os.getenv("INBOX_SUMMARY_EMAIL_ENABLED") or "true").strip().lower()
    return raw in ("0", "false", "no", "off")


def _normalize_summary_route(route_tag: str | None) -> str:
    tag = (route_tag or "").strip().lower()
    if tag == "feedback":
        return "info"
    if tag in {s[0] for s in INBOX_SUMMARY_SECTIONS}:
        return tag
    return "tome"


def run_inbox_scheduled_sync(db: Session) -> None:
    """10 dk job: Gmail → DB senkronu (e-posta göndermez)."""
    if inbox_gmail_auth.get_inbox_credential_row(db) is None:
        logger.info("Inbox sync atlandı: Gmail henüz bağlı değil.")
        return
    logger.info("Starting scheduled inbox sync...")
    try:
        inbox_sync.sync_scheduled_inbox_threads(db, max_threads=inbox_sync.INBOX_SYNC_MAX_THREADS)
    except Exception as exc:
        logger.warning("Inbox sync failed: %s", exc)


def _latest_inbound_message(db: Session, thread_id: int) -> SupportInboxMessage | None:
    return (
        db.query(SupportInboxMessage)
        .filter(SupportInboxMessage.thread_id == thread_id, SupportInboxMessage.is_outbound.is_(False))
        .order_by(SupportInboxMessage.internal_ms.desc())
        .first()
    )


def _format_thread_date(internal_ms: int) -> str:
    if not internal_ms:
        return "—"
    try:
        return datetime.fromtimestamp(internal_ms / 1000.0).strftime("%d.%m %H:%M")
    except (OSError, OverflowError, ValueError):
        return "—"


def _thread_preview_text(thread: SupportInboxThread, latest: SupportInboxMessage | None) -> str:
    raw = ""
    if latest and (latest.body_text or "").strip():
        raw = latest.body_text.strip()
    elif thread.snippet:
        raw = thread.snippet.strip()
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    if len(raw) > 480:
        raw = raw[:477] + "…"
    return html.escape(raw).replace("\n", "<br/>")


def _render_thread_item(thread: SupportInboxThread, latest: SupportInboxMessage | None) -> str:
    sender = html.escape((latest.from_addr if latest else "") or "Bilinmiyor")
    date_str = _format_thread_date(latest.internal_ms if latest else thread.last_internal_ms)
    subject = html.escape(thread.subject or "(konu yok)")
    preview = _thread_preview_text(thread, latest)
    return (
        "<li style='border-bottom:1px solid #e2e8f0;padding:14px 0;margin:0;list-style:none;'>"
        f"<div style='color:#64748b;font-size:12px;margin-bottom:6px;'>{date_str}</div>"
        f"<div style='font-size:15px;font-weight:800;color:#1e293b;margin-bottom:6px;'>{subject}</div>"
        f"<div style='color:#475569;font-size:13px;margin-bottom:8px;'><b>Kimden:</b> {sender}</div>"
        f"<div style='color:#334155;font-size:13px;line-height:1.55;padding:10px 12px;"
        f"background:#f1f5f9;border-radius:6px;border-left:4px solid #94a3b8;'>{preview}</div>"
        "</li>"
    )


def build_inbox_summary_html(
    grouped: dict[str, list[SupportInboxThread]],
    db: Session,
) -> str:
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    total = sum(len(v) for v in grouped.values())
    parts = [
        "<div style='font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#1e293b;"
        "max-width:680px;margin:0 auto;'>",
        f"<h2 style='color:#1d4ed8;margin:0 0 6px;'>Gelen Kutusu Özeti</h2>",
        f"<p style='color:#64748b;font-size:13px;margin:0 0 20px;'>{now_str} · "
        f"<b>{total}</b> okunmamış konuşma</p>",
    ]

    for route_key, title, accent, bg in INBOX_SUMMARY_SECTIONS:
        threads = grouped.get(route_key) or []
        count = len(threads)
        parts.append(
            f"<section style='margin-bottom:28px;border:1px solid #e2e8f0;border-radius:10px;"
            f"overflow:hidden;background:{bg};'>"
            f"<h3 style='margin:0;padding:14px 16px;font-size:15px;font-weight:800;"
            f"color:{accent};border-bottom:2px solid {accent};background:#fff;'>"
            f"{html.escape(title)}"
            f"<span style='float:right;font-size:13px;font-weight:700;color:#64748b;'>"
            f"{count} okunmamış</span></h3>"
        )
        if not threads:
            parts.append(
                "<p style='margin:0;padding:16px;color:#64748b;font-size:13px;'>"
                "Bu sekmede okunmamış mesaj yok.</p>"
            )
        else:
            parts.append("<ul style='margin:0;padding:0 16px 8px;'>")
            for thread in threads[:15]:
                latest = _latest_inbound_message(db, thread.id)
                parts.append(_render_thread_item(thread, latest))
            if count > 15:
                parts.append(
                    f"<li style='list-style:none;padding:10px 0;color:#64748b;font-size:12px;'>"
                    f"+ {count - 15} konuşma daha…</li>"
                )
            parts.append("</ul>")
        parts.append("</section>")

    parts.append(
        "<p style='margin-top:8px;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0;"
        "padding-top:12px;'>SEO Agent · 2 saatte bir otomatik özet · "
        "<a href='https://projectcontrol.up.railway.app/inbox'>Gelen kutusunu aç</a></p>"
    )
    parts.append("</div>")
    return "\n".join(parts)


def run_inbox_summary_email(db: Session) -> bool:
    """Senkron sonrası 5 sekmeli okunmamış özet e-postası gönderir."""
    if _inbox_summary_email_disabled():
        logger.info("Inbox summary email disabled (INBOX_SUMMARY_EMAIL_ENABLED=false).")
        return False

    if inbox_gmail_auth.get_inbox_credential_row(db) is None:
        logger.info("Inbox summary email atlandı: Gmail bağlı değil.")
        return False

    try:
        inbox_sync.sync_scheduled_inbox_threads(db, max_threads=inbox_sync.INBOX_SYNC_MAX_THREADS)
    except Exception as exc:
        logger.warning("Inbox sync before summary failed (continuing): %s", exc)

    unread_threads = (
        db.query(SupportInboxThread)
        .filter(SupportInboxThread.gmail_unread.is_(True))
        .order_by(SupportInboxThread.last_internal_ms.desc())
        .all()
    )
    logger.info("Unread threads for summary: %d", len(unread_threads))

    grouped: dict[str, list[SupportInboxThread]] = defaultdict(list)
    for thread in unread_threads:
        grouped[_normalize_summary_route(thread.route_tag)].append(thread)

    total = len(unread_threads)
    section_counts = {key: len(grouped.get(key) or []) for key, *_ in INBOX_SUMMARY_SECTIONS}
    chips = " · ".join(f"{k}:{v}" for k, v in section_counts.items() if v > 0)
    subject = f"Inbox özeti — {total} okunmamış" + (f" ({chips})" if chips else "")

    html_body = build_inbox_summary_html(grouped, db)
    ok = mailer.send_email(subject, html_body)
    if ok:
        logger.info("Inbox summary email sent (%d unread).", total)
    else:
        logger.error("Failed to send inbox summary email.")
    return ok


def run_inbox_summary_job(db: Session) -> None:
    """Geriye uyumluluk: admin tetikleme → özet maili."""
    run_inbox_summary_email(db)
