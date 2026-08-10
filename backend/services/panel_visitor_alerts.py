"""Owner-only (cem) ziyaretçi uyarıları ve çıkış kullanım özeti."""

from __future__ import annotations

import html
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")

OWNER_ALERT_TO = "cemevecen@nokta.com"
_ALERT_COOLDOWN_SEC = 20 * 60
_lock = threading.Lock()
_last_visitor_alert_at: dict[str, float] = {}


def _norm(email: str) -> str:
    return str(email or "").strip().lower()


def is_owner_email(email: str, owner_emails: frozenset[str] | set[str] | None = None) -> bool:
    em = _norm(email)
    if not em or "@" not in em:
        return False
    # Yerel kısım cemevecen → her zaman owner (gmail/nokta vb.)
    local = em.split("@", 1)[0]
    if local == "cemevecen":
        return True
    if owner_emails is None:
        try:
            from backend.services.app_member_auth import ADMIN_MEMBER_EMAILS

            owner_emails = ADMIN_MEMBER_EMAILS
        except Exception:  # noqa: BLE001
            owner_emails = frozenset()
    return em in {_norm(e) for e in owner_emails}


def _fmt_tr(dt: datetime | None) -> str:
    if not dt:
        return "—"
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return aware.astimezone(_TR).strftime("%d.%m.%Y %H:%M:%S")


def _duration_tr(start: datetime | None, end: datetime | None) -> str:
    if not start or not end:
        return "—"
    sec = max(0, int((end - start).total_seconds()))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h} sa {m} dk {s} sn"
    if m:
        return f"{m} dk {s} sn"
    return f"{s} sn"


def owners_currently_online(
    sessions: dict[str, dict[str, Any]] | list[dict[str, Any]],
    owner_emails: frozenset[str] | set[str],
    *,
    idle_minutes: int = 30,
) -> list[str]:
    owners = {_norm(e) for e in owner_emails}
    now = datetime.utcnow()
    cutoff = now.timestamp() - idle_minutes * 60
    found: set[str] = set()
    rows = sessions.values() if isinstance(sessions, dict) else sessions
    for s in rows:
        em = _norm(str(s.get("email") or ""))
        if em not in owners:
            continue
        last = s.get("last_seen")
        if isinstance(last, datetime):
            if last.timestamp() < cutoff:
                continue
        found.add(em)
    return sorted(found)


def maybe_alert_visitor_joined(
    sessions: dict[str, dict[str, Any]],
    *,
    email: str,
    session: dict[str, Any],
    owner_emails: frozenset[str] | set[str],
) -> bool:
    """Owner çevrimiçiyken başka e-posta oturumu açılırsa nokta.com’a mail."""
    em = _norm(email)
    if not em or "@" not in em:
        return False
    if is_owner_email(em, owner_emails):
        return False
    if not owners_currently_online(sessions, owner_emails):
        return False

    now = time.time()
    with _lock:
        prev = _last_visitor_alert_at.get(em, 0.0)
        if now - prev < _ALERT_COOLDOWN_SEC:
            return False
        _last_visitor_alert_at[em] = now

    label = str(session.get("label") or em).strip() or em
    ip = str(session.get("ip") or "—")
    device = str(session.get("device") or "—")
    ua = str(session.get("user_agent") or "—")
    first = session.get("first_seen")
    first_tr = _fmt_tr(first if isinstance(first, datetime) else None)
    owners = owners_currently_online(sessions, owner_emails)

    body = (
        '<div style="font-family:system-ui,-apple-system,sans-serif;font-size:14px;line-height:1.5;color:#0f172a;">'
        '<p style="margin:0 0 12px;font-size:16px;font-weight:700;color:#047857;">Panele başka kullanıcı bağlandı</p>'
        f'<p style="margin:0 0 8px;"><strong>E-posta:</strong> {html.escape(em)}</p>'
        f'<p style="margin:0 0 8px;"><strong>Görünen ad:</strong> {html.escape(label)}</p>'
        f'<p style="margin:0 0 8px;"><strong>Bağlantı:</strong> {html.escape(first_tr)}</p>'
        f'<p style="margin:0 0 8px;"><strong>IP:</strong> {html.escape(ip)}</p>'
        f'<p style="margin:0 0 8px;"><strong>Cihaz:</strong> {html.escape(device)}</p>'
        f'<p style="margin:0 0 8px;font-size:12px;color:#64748b;"><strong>User-Agent:</strong><br>{html.escape(ua[:500])}</p>'
        f'<p style="margin:12px 0 0;font-size:12px;color:#64748b;">O anda çevrimiçi owner: {html.escape(", ".join(owners) or "—")}</p>'
        "</div>"
    )
    try:
        from backend.services.mailer import send_admin_security_email

        ok = send_admin_security_email(
            "PC panel bildirimi",
            body,
            [OWNER_ALERT_TO],
        )
        if ok:
            LOGGER.info("Visitor join alert mailed for %s", em)
        return bool(ok)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Visitor join alert failed: %s", exc)
        return False


def send_usage_summary_email(
    *,
    email: str,
    display_name: str = "",
    session: dict[str, Any] | None = None,
    paths: list[dict[str, Any]] | None = None,
) -> bool:
    """Çıkış sonrası kullanım özeti — konu sabit, gövdede tüm detay."""
    em = _norm(email)
    if not em or "@" not in em:
        return False
    sess = session or {}
    first = sess.get("first_seen")
    last = sess.get("last_seen") or datetime.utcnow()
    if not isinstance(first, datetime):
        first = None
    if not isinstance(last, datetime):
        last = datetime.utcnow()

    nav = paths if paths is not None else (sess.get("paths") or [])
    if nav:
        items = []
        for hit in nav:
            at = html.escape(str(hit.get("at_tr") or hit.get("at") or ""))
            path = html.escape(str(hit.get("path") or ""))
            label = html.escape(str(hit.get("label") or ""))
            items.append(
                f"<li style=\"margin:0 0 4px;\"><strong>{at}</strong> {label} "
                f"<code style=\"font-size:11px;color:#64748b;\">{path}</code></li>"
            )
        nav_html = (
            '<ol style="margin:0;padding-left:20px;font-size:13px;">'
            + "".join(items)
            + "</ol>"
        )
    else:
        nav_html = (
            '<p style="margin:0;font-size:12px;color:#64748b;">'
            "Oturumda sayfa gezintisi kaydı yok.</p>"
        )

    label = (display_name or str(sess.get("label") or em)).strip() or em
    body = (
        '<div style="font-family:system-ui,-apple-system,sans-serif;font-size:14px;line-height:1.5;color:#0f172a;">'
        '<p style="margin:0 0 12px;font-size:16px;font-weight:700;color:#0369a1;">PC kullanım özeti</p>'
        f'<p style="margin:0 0 8px;"><strong>E-posta:</strong> {html.escape(em)}</p>'
        f'<p style="margin:0 0 8px;"><strong>Görünen ad:</strong> {html.escape(label)}</p>'
        f'<p style="margin:0 0 8px;"><strong>Giriş:</strong> {html.escape(_fmt_tr(first))}</p>'
        f'<p style="margin:0 0 8px;"><strong>Çıkış:</strong> {html.escape(_fmt_tr(last))}</p>'
        f'<p style="margin:0 0 8px;"><strong>Süre:</strong> {html.escape(_duration_tr(first, last))}</p>'
        f'<p style="margin:0 0 8px;"><strong>IP:</strong> {html.escape(str(sess.get("ip") or "—"))}</p>'
        f'<p style="margin:0 0 8px;"><strong>Cihaz:</strong> {html.escape(str(sess.get("device") or "—"))}</p>'
        f'<p style="margin:0 0 8px;font-size:12px;color:#64748b;"><strong>User-Agent:</strong><br>'
        f'{html.escape(str(sess.get("user_agent") or "—")[:500])}</p>'
        '<p style="margin:16px 0 6px;font-weight:700;">Gezilen sayfalar</p>'
        + nav_html
        + "</div>"
    )
    try:
        from backend.services.mailer import send_admin_security_email

        ok = send_admin_security_email("PC kullanım özeti", body, [OWNER_ALERT_TO])
        if ok:
            LOGGER.info("Usage summary mailed for %s", em)
        return bool(ok)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Usage summary mail failed: %s", exc)
        return False
