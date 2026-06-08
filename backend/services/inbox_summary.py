"""Gelen kutusu senkronu ve 6 sekmeli özet e-postası."""

from __future__ import annotations

import html
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import SupportInboxMessage, SupportInboxThread
from backend.services import inbox_gmail_auth, inbox_sync, mailer
from backend.services.inbox_email_render import effective_plain_text
from backend.services.inbox_visit_report import is_ziyaret_report_subject, ziyaret_thread_preview

logger = logging.getLogger(__name__)

# UI sekmeleriyle aynı sıra (inbox_sync.INBOX_TAB_ORDER): doviz → sinemalar → nstat → firebase → reklam → all
# (key, başlık, kısa açıklama, vurgu rengi, arka plan)
INBOX_SUMMARY_SECTIONS: tuple[tuple[str, str, str, str, str], ...] = (
    ("doviz", "doviz", "info@doviz.com · feedback@doviz.com", "#1d4ed8", "#eff6ff"),
    ("sinemalar", "sinemalar", "info@sinemalar.com · feedback@sinemalar.com", "#4338ca", "#eef2ff"),
    ("nstat", "nstat", "En çok ziyaret edilen sayfalar (noreply@doviz.com)", "#047857", "#ecfdf5"),
    ("firebase", "firebase", "Firebase Crashlytics uyarıları", "#b45309", "#fffbeb"),
    ("reklam", "reklam", "reklam@nokta.com", "#c026d3", "#fdf4ff"),
    (
        "all",
        "all",
        "to:me · info@blogcu.com · info@izlesene.com · medya@nokta.com",
        "#475569",
        "#f8fafc",
    ),
)


def _inbox_summary_email_disabled() -> bool:
    """Varsayılan açık; INBOX_SUMMARY_EMAIL_ENABLED=false ile kapatılır."""
    raw = (os.getenv("INBOX_SUMMARY_EMAIL_ENABLED") or "true").strip().lower()
    return raw in ("0", "false", "no", "off")


def _normalize_summary_route(route_tag: str | None) -> str:
    return inbox_sync.normalize_inbox_route_tag(route_tag)


def run_inbox_scheduled_sync(db: Session) -> dict[str, Any] | None:
    """Zamanlanmış job: Gmail → DB senkronu (e-posta göndermez). Başarıda özet dict döner."""
    if inbox_gmail_auth.get_inbox_credential_row(db) is None:
        logger.info("Inbox sync atlandı: Gmail henüz bağlı değil.")
        return None
    logger.info("Scheduled inbox sync başladı.")
    try:
        out = inbox_sync.sync_scheduled_inbox_threads(db, max_threads=inbox_sync.INBOX_SYNC_MAX_THREADS)
        logger.info(
            "Scheduled inbox sync tamamlandı: synced=%s mode=%s",
            out.get("synced_threads"),
            out.get("sync_mode"),
        )
        return out
    except RuntimeError as exc:
        if "bağlı değil" in str(exc).lower():
            logger.info("Inbox sync atlandı: %s", exc)
            return None
        logger.warning("Inbox sync failed: %s", exc)
        raise
    except Exception as exc:
        logger.exception("Inbox sync failed: %s", exc)
        raise


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


def _thread_preview_text(
    thread: SupportInboxThread,
    latest: SupportInboxMessage | None,
    *,
    route_key: str,
) -> str:
    raw = ""
    if latest:
        raw = effective_plain_text(latest.body_text, getattr(latest, "body_html", None))
    elif thread.snippet:
        raw = thread.snippet.strip()
    if route_key == "nstat" and raw:
        preview = ziyaret_thread_preview(raw, max_rows=2)
        return html.escape(preview).replace("\n", "<br/>")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    if len(raw) > 480:
        raw = raw[:477] + "…"
    return html.escape(raw).replace("\n", "<br/>")


def _render_thread_item(thread: SupportInboxThread, latest: SupportInboxMessage | None, *, route_key: str) -> str:
    sender = html.escape((latest.from_addr if latest else "") or "Bilinmiyor")
    date_str = _format_thread_date(latest.internal_ms if latest else thread.last_internal_ms)
    subject = html.escape(thread.subject or "(konu yok)")
    preview = _thread_preview_text(thread, latest, route_key=route_key)
    accent = next((s[3] for s in INBOX_SUMMARY_SECTIONS if s[0] == route_key), "#94a3b8")
    return (
        "<li style='border-bottom:1px solid #e2e8f0;padding:14px 0;margin:0;list-style:none;'>"
        "<div style='display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin-bottom:6px;'>"
        f"<span style='font-size:15px;font-weight:800;color:#1e293b;'>{subject}</span>"
        f"<span style='color:#64748b;font-size:12px;white-space:nowrap;'>{date_str}</span>"
        "</div>"
        f"<div style='color:#475569;font-size:13px;margin-bottom:8px;'><b>Kimden:</b> {sender}</div>"
        f"<div style='color:#334155;font-size:13px;line-height:1.55;padding:10px 12px;"
        f"background:#fff;border-radius:6px;border-left:4px solid {accent};'>{preview}</div>"
        "</li>"
    )


def _render_overview_table(grouped: dict[str, list[SupportInboxThread]]) -> str:
    rows = []
    for route_key, title, subtitle, accent, _bg in INBOX_SUMMARY_SECTIONS:
        count = len(grouped.get(route_key) or [])
        rows.append(
            f"<tr>"
            f"<td style='padding:8px 12px;font-weight:700;color:{accent};'>{html.escape(title)}</td>"
            f"<td style='padding:8px 12px;color:#64748b;font-size:12px;'>{html.escape(subtitle)}</td>"
            f"<td style='padding:8px 12px;text-align:right;font-weight:800;color:#1e293b;'>{count}</td>"
            f"</tr>"
        )
    return (
        "<table style='width:100%;border-collapse:collapse;margin:0 0 24px;background:#fff;"
        "border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;'>"
        "<thead><tr style='background:#f1f5f9;'>"
        "<th style='padding:10px 12px;text-align:left;font-size:11px;color:#64748b;'>Sekme</th>"
        "<th style='padding:10px 12px;text-align:left;font-size:11px;color:#64748b;'>Kaynak</th>"
        "<th style='padding:10px 12px;text-align:right;font-size:11px;color:#64748b;'>Konuşma</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
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
        "<h2 style='color:#1d4ed8;margin:0 0 6px;'>Gelen Kutusu Özeti</h2>",
        f"<p style='color:#64748b;font-size:13px;margin:0 0 16px;'>{now_str} · "
        f"<b>{total}</b> konuşma · sıra: "
        f"{' → '.join(inbox_sync.INBOX_TAB_ORDER)}</p>",
        _render_overview_table(grouped),
    ]

    for route_key, title, subtitle, accent, bg in INBOX_SUMMARY_SECTIONS:
        threads = grouped.get(route_key) or []
        count = len(threads)
        parts.append(
            f"<section style='margin-bottom:24px;border:1px solid #e2e8f0;border-radius:10px;"
            f"overflow:hidden;background:{bg};'>"
            f"<div style='padding:14px 16px;background:#fff;border-bottom:2px solid {accent};'>"
            f"<h3 style='margin:0;font-size:15px;font-weight:800;color:{accent};'>"
            f"{html.escape(title)}"
            f"<span style='float:right;font-size:13px;font-weight:700;color:#64748b;'>"
            f"{count} konuşma</span></h3>"
            f"<p style='margin:6px 0 0;font-size:12px;color:#64748b;'>{html.escape(subtitle)}</p>"
            f"</div>"
        )
        if not threads:
            parts.append(
                "<p style='margin:0;padding:16px;color:#64748b;font-size:13px;'>"
                "Bu sekmede konuşma yok.</p>"
            )
        else:
            parts.append("<ul style='margin:0;padding:0 16px 8px;'>")
            for thread in threads[:15]:
                latest = _latest_inbound_message(db, thread.id)
                parts.append(_render_thread_item(thread, latest, route_key=route_key))
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


def _group_unread_threads(unread_threads: list[SupportInboxThread]) -> dict[str, list[SupportInboxThread]]:
    grouped: dict[str, list[SupportInboxThread]] = defaultdict(list)
    for thread in unread_threads:
        if inbox_sync.inbox_thread_is_excluded(
            subject=thread.subject or "",
            snippet=thread.snippet or "",
        ):
            continue
        route = _normalize_summary_route(thread.route_tag)
        if route == "nstat" and not is_ziyaret_report_subject(thread.subject or ""):
            continue
        grouped[route].append(thread)
    return grouped


def run_inbox_summary_email(db: Session) -> bool:
    """Senkron sonrası 6 sekmeli gelen kutusu özet e-postası gönderir."""
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

    grouped = _group_unread_threads(unread_threads)
    total = sum(len(v) for v in grouped.values())
    section_counts = {key: len(grouped.get(key) or []) for key, *_ in INBOX_SUMMARY_SECTIONS}
    chips = " · ".join(f"{k}:{v}" for k, v in section_counts.items() if v > 0)
    subject = f"Inbox özeti — {total} konuşma" + (f" ({chips})" if chips else "")

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
