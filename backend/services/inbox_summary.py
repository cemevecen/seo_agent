"""Gelen kutusu senkronu ve 5 sekmeli özet e-postası (doviz → sinemalar → medya → nstat → firebase)."""

from __future__ import annotations

import html
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.orm import Session

from backend.models import SupportInboxMessage, SupportInboxThread
from backend.services import inbox_gmail_auth, inbox_sync, mailer
from backend.services.inbox_email_render import effective_plain_text
from backend.services.inbox_medya import (
    MEDYA_KIND_ISBIRLIGI,
    MEDYA_KIND_REKLAM,
    classify_medya_thread_kind,
    medya_kind_label,
)
from backend.services.inbox_visit_report import is_ziyaret_report_subject, ziyaret_thread_preview

logger = logging.getLogger(__name__)

# Özet mailinde yalnızca bu sekmeler (sıra sabit)
# (key, başlık, kısa açıklama, vurgu rengi, arka plan)
INBOX_SUMMARY_SECTIONS: tuple[tuple[str, str, str, str, str], ...] = (
    ("doviz", "doviz", "info@doviz.com · feedback@doviz.com", "#1d4ed8", "#eff6ff"),
    ("sinemalar", "sinemalar", "info@sinemalar.com · feedback@sinemalar.com", "#4338ca", "#eef2ff"),
    (
        "medya",
        "medya",
        "medya@nokta.com · işbirliği / reklam ayrımı",
        "#0f766e",
        "#ecfdf5",
    ),
    ("nstat", "nstat", "En çok ziyaret edilen sayfalar (noreply@doviz.com)", "#047857", "#ecfdf5"),
    ("firebase", "firebase", "Firebase Crashlytics uyarıları", "#b45309", "#fffbeb"),
)

INBOX_SUMMARY_TAB_ORDER: tuple[str, ...] = tuple(s[0] for s in INBOX_SUMMARY_SECTIONS)
_SUMMARY_ROUTE_KEYS = frozenset(INBOX_SUMMARY_TAB_ORDER)
SUMMARY_DETAIL_MAX_AGE_DAYS = 7


def _inbox_summary_email_disabled() -> bool:
    """Varsayılan açık; INBOX_SUMMARY_EMAIL_ENABLED=false ile kapatılır."""
    raw = (os.getenv("INBOX_SUMMARY_EMAIL_ENABLED") or "true").strip().lower()
    return raw in ("0", "false", "no", "off")


def _normalize_summary_route(route_tag: str | None) -> str:
    return inbox_sync.normalize_inbox_route_tag(route_tag)


def _summary_cutoff_ms() -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=SUMMARY_DETAIL_MAX_AGE_DAYS)
    return int(cutoff.timestamp() * 1000)


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


def _inbound_messages_for_summary(
    db: Session,
    thread_id: int,
    *,
    cutoff_ms: int,
) -> list[SupportInboxMessage]:
    return (
        db.query(SupportInboxMessage)
        .filter(
            SupportInboxMessage.thread_id == thread_id,
            SupportInboxMessage.is_outbound.is_(False),
            SupportInboxMessage.internal_ms >= cutoff_ms,
        )
        .order_by(SupportInboxMessage.internal_ms.desc())
        .all()
    )


def _format_message_date(internal_ms: int) -> str:
    if not internal_ms:
        return "—"
    try:
        return datetime.fromtimestamp(internal_ms / 1000.0).strftime("%d.%m %H:%M")
    except (OSError, OverflowError, ValueError):
        return "—"


def _message_preview_text(
    message: SupportInboxMessage,
    *,
    route_key: str,
) -> str:
    raw = effective_plain_text(message.body_text, getattr(message, "body_html", None))
    if route_key == "nstat" and raw:
        preview = ziyaret_thread_preview(raw, max_rows=4)
        return html.escape(preview).replace("\n", "<br/>")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    if len(raw) > 800:
        raw = raw[:797] + "…"
    return html.escape(raw).replace("\n", "<br/>")


def _medya_kind_badge_html(kind: str) -> str:
    label = html.escape(medya_kind_label(kind))
    colors = {
        MEDYA_KIND_ISBIRLIGI: ("#047857", "#ecfdf5"),
        MEDYA_KIND_REKLAM: ("#b45309", "#fffbeb"),
    }
    fg, bg = colors.get(kind, ("#64748b", "#f1f5f9"))
    return (
        f"<span style='display:inline-block;margin-left:8px;padding:2px 8px;border-radius:999px;"
        f"font-size:10px;font-weight:800;color:{fg};background:{bg};'>{label}</span>"
    )


def _medya_section_subtitle(threads: list[SupportInboxThread], base_subtitle: str) -> str:
    if not threads:
        return base_subtitle
    counts = {MEDYA_KIND_ISBIRLIGI: 0, MEDYA_KIND_REKLAM: 0}
    for thread in threads:
        kind = classify_medya_thread_kind(
            subject=thread.subject or "",
            snippet=thread.snippet or "",
        )
        if kind in counts:
            counts[kind] += 1
    extra = (
        f" · {counts[MEDYA_KIND_ISBIRLIGI]} işbirliği · {counts[MEDYA_KIND_REKLAM]} reklam"
    )
    return base_subtitle + extra


def _render_message_item(
    thread: SupportInboxThread,
    message: SupportInboxMessage,
    *,
    route_key: str,
    medya_kind: str | None = None,
) -> str:
    sender = html.escape((message.from_addr or "").strip() or "Bilinmiyor")
    date_str = _format_message_date(message.internal_ms)
    subject = html.escape((message.subject or thread.subject or "").strip() or "(konu yok)")
    if route_key == "medya" and medya_kind:
        subject += _medya_kind_badge_html(medya_kind)
    preview = _message_preview_text(message, route_key=route_key)
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
    cutoff_ms = _summary_cutoff_ms()
    total = sum(len(grouped.get(key) or []) for key in INBOX_SUMMARY_TAB_ORDER)
    order_label = " → ".join(INBOX_SUMMARY_TAB_ORDER)
    parts = [
        "<div style='font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#1e293b;"
        "max-width:680px;margin:0 auto;'>",
        "<h2 style='color:#1d4ed8;margin:0 0 6px;'>Gelen Kutusu Özeti</h2>",
        f"<p style='color:#64748b;font-size:13px;margin:0 0 16px;'>{now_str} · "
        f"<b>{total}</b> konuşma (son {SUMMARY_DETAIL_MAX_AGE_DAYS} gün) · sıra: "
        f"{order_label}</p>",
        _render_overview_table(grouped),
    ]

    for route_key, title, subtitle, accent, bg in INBOX_SUMMARY_SECTIONS:
        threads = grouped.get(route_key) or []
        count = len(threads)
        section_subtitle = subtitle
        if route_key == "medya":
            section_subtitle = _medya_section_subtitle(threads, subtitle)
        parts.append(
            f"<section style='margin-bottom:24px;border:1px solid #e2e8f0;border-radius:10px;"
            f"overflow:hidden;background:{bg};'>"
            f"<div style='padding:14px 16px;background:#fff;border-bottom:2px solid {accent};'>"
            f"<h3 style='margin:0;font-size:15px;font-weight:800;color:{accent};'>"
            f"{html.escape(title)}"
            f"<span style='float:right;font-size:13px;font-weight:700;color:#64748b;'>"
            f"{count} konuşma</span></h3>"
            f"<p style='margin:6px 0 0;font-size:12px;color:#64748b;'>{html.escape(section_subtitle)}</p>"
            f"</div>"
        )
        if not threads:
            parts.append(
                "<p style='margin:0;padding:16px;color:#64748b;font-size:13px;'>"
                "Bu sekmede konuşma yok.</p>"
            )
        else:
            parts.append("<ul style='margin:0;padding:0 16px 8px;'>")
            message_count = 0
            for thread in threads:
                messages = _inbound_messages_for_summary(db, thread.id, cutoff_ms=cutoff_ms)
                if not messages:
                    continue
                medya_kind = None
                if route_key == "medya":
                    body_hint = effective_plain_text(
                        messages[0].body_text,
                        getattr(messages[0], "body_html", None),
                    )
                    medya_kind = classify_medya_thread_kind(
                        subject=thread.subject or messages[0].subject or "",
                        snippet=thread.snippet or "",
                        body=body_hint,
                    )
                for message in messages:
                    parts.append(
                        _render_message_item(
                            thread,
                            message,
                            route_key=route_key,
                            medya_kind=medya_kind,
                        )
                    )
                    message_count += 1
            if message_count == 0:
                parts.append(
                    "<li style='list-style:none;padding:10px 0;color:#64748b;font-size:13px;'>"
                    f"Son {SUMMARY_DETAIL_MAX_AGE_DAYS} günde gösterilecek ileti yok.</li>"
                )
            parts.append("</ul>")
        parts.append("</section>")

    parts.append(
        "<p style='margin-top:8px;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0;"
        "padding-top:12px;'>SEO Agent · 2 saatte bir otomatik özet · "
        f"Detay: son {SUMMARY_DETAIL_MAX_AGE_DAYS} gün, ileti bazında · "
        "<a href='https://projectcontrol.up.railway.app/inbox'>Gelen kutusunu aç</a></p>"
    )
    parts.append("</div>")
    return "\n".join(parts)


def _group_unread_threads(
    unread_threads: list[SupportInboxThread],
    *,
    cutoff_ms: int,
) -> dict[str, list[SupportInboxThread]]:
    grouped: dict[str, list[SupportInboxThread]] = defaultdict(list)
    for thread in unread_threads:
        if inbox_sync.inbox_thread_is_excluded(
            subject=thread.subject or "",
            snippet=thread.snippet or "",
        ):
            continue
        route = _normalize_summary_route(thread.route_tag)
        if route not in _SUMMARY_ROUTE_KEYS:
            continue
        if route == "nstat" and not is_ziyaret_report_subject(thread.subject or ""):
            continue
        if (thread.last_internal_ms or 0) < cutoff_ms:
            continue
        grouped[route].append(thread)
    for route_key in grouped:
        grouped[route_key].sort(key=lambda t: t.last_internal_ms or 0, reverse=True)
    return grouped


def run_inbox_summary_email(db: Session) -> bool:
    """Senkron sonrası 4 sekmeli gelen kutusu özet e-postası gönderir."""
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

    cutoff_ms = _summary_cutoff_ms()
    grouped = _group_unread_threads(unread_threads, cutoff_ms=cutoff_ms)
    total = sum(len(grouped.get(key) or []) for key in INBOX_SUMMARY_TAB_ORDER)
    section_counts = {key: len(grouped.get(key) or []) for key in INBOX_SUMMARY_TAB_ORDER}
    chips = " · ".join(f"{k}:{v}" for k, v in section_counts.items() if v > 0)
    subject = f"Inbox özeti — {total} konuşma" + (f" ({chips})" if chips else "")

    html_body = build_inbox_summary_html(grouped, db)
    ok = mailer.send_email(subject, html_body)
    if ok:
        logger.info("Inbox summary email sent (%d unread in summary window).", total)
    else:
        logger.error("Failed to send inbox summary email.")
    return ok


def run_inbox_summary_job(db: Session) -> None:
    """Geriye uyumluluk: admin tetikleme → özet maili."""
    run_inbox_summary_email(db)
