"""Admin giriş geçmişi, tanıdık cihazlar ve tanınmayan giriş uyarıları."""

from __future__ import annotations

import hashlib
import html
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import AdminLoginEvent, AdminTrustedDevice

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")
_LOGIN_HISTORY_LIMIT = 10


def device_fingerprint(ip: str, user_agent: str) -> str:
    """IP + User-Agent ile cihaz parmak izi (dinamik IP'de tarayıcı sabit kalır)."""
    raw = f"{(ip or '').strip()}|{(user_agent or '').strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def parse_device_label(user_agent: str) -> str:
    ua_l = (user_agent or "").lower()
    if "mobile" in ua_l or "android" in ua_l or "iphone" in ua_l:
        device = "Mobil"
    elif "tablet" in ua_l or "ipad" in ua_l:
        device = "Tablet"
    else:
        device = "Masaüstü"
    if "chrome" in ua_l and "edg" not in ua_l and "opr" not in ua_l:
        browser = "Chrome"
    elif "firefox" in ua_l:
        browser = "Firefox"
    elif "safari" in ua_l and "chrome" not in ua_l:
        browser = "Safari"
    elif "edg" in ua_l:
        browser = "Edge"
    elif "opr" in ua_l or "opera" in ua_l:
        browser = "Opera"
    else:
        browser = "Tarayıcı"
    return f"{device} / {browser}"


def format_tr(dt: datetime | None) -> str:
    if not dt:
        return "—"
    aware = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return aware.astimezone(_TR).strftime("%d.%m.%Y %H:%M")


def trusted_fingerprints(db: Session) -> set[str]:
    rows = db.query(AdminTrustedDevice.fingerprint).all()
    return {str(r[0]) for r in rows if r and r[0]}


def is_trusted(db: Session, fingerprint: str) -> bool:
    if not fingerprint:
        return False
    return (
        db.query(AdminTrustedDevice.id)
        .filter(AdminTrustedDevice.fingerprint == fingerprint)
        .first()
        is not None
    )


def trust_fingerprint(
    db: Session,
    fingerprint: str,
    *,
    label: str = "",
    ip_hint: str = "",
) -> None:
    fp = (fingerprint or "").strip()
    if not fp:
        return
    existing = db.query(AdminTrustedDevice).filter(AdminTrustedDevice.fingerprint == fp).first()
    if existing:
        if label and not existing.label:
            existing.label = label[:120]
        if ip_hint:
            existing.ip_hint = ip_hint[:64]
    else:
        db.add(
            AdminTrustedDevice(
                fingerprint=fp,
                label=(label or "Tanıdık cihaz")[:120],
                ip_hint=(ip_hint or "")[:64],
            )
        )
    db.query(AdminLoginEvent).filter(AdminLoginEvent.fingerprint == fp).update(
        {AdminLoginEvent.is_trusted: True},
        synchronize_session=False,
    )
    db.commit()


def _trim_old_events(db: Session) -> None:
    keep_ids = [
        row[0]
        for row in db.query(AdminLoginEvent.id)
        .order_by(AdminLoginEvent.created_at.desc(), AdminLoginEvent.id.desc())
        .limit(100)
        .all()
    ]
    if len(keep_ids) < 100:
        return
    db.query(AdminLoginEvent).filter(AdminLoginEvent.id.notin_(keep_ids)).delete(
        synchronize_session=False
    )
    db.commit()


def _event_label(event_type: str) -> str:
    return {
        "login_ok": "Admin girişi",
        "login_fail": "Başarısız giriş",
        "settings_ok": "Settings erişimi",
        "settings_fail": "Settings — hatalı şifre",
    }.get(event_type, event_type)


def _send_unknown_login_alert(
    *,
    ip: str,
    device_label: str,
    user_agent: str,
    fingerprint: str,
    event_type: str,
) -> bool:
    recipient = (settings.admin_login_alert_email or "").strip()
    if not recipient or not settings.admin_login_alert_enabled:
        return False
    try:
        from backend.services.mailer import send_admin_security_email

        when = format_tr(datetime.utcnow())
        et = _event_label(event_type)
        body = (
            '<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;max-width:560px;">'
            f"<h2 style=\"color:#b91c1c;margin:0 0 12px;\">Tanınmayan admin erişimi</h2>"
            f"<p style=\"margin:0 0 8px;\"><strong>Olay:</strong> {html.escape(et)}</p>"
            f"<p style=\"margin:0 0 8px;\"><strong>Zaman:</strong> {html.escape(when)} (TR)</p>"
            f"<p style=\"margin:0 0 8px;\"><strong>IP:</strong> {html.escape(ip or '—')}</p>"
            f"<p style=\"margin:0 0 8px;\"><strong>Cihaz:</strong> {html.escape(device_label)}</p>"
            f"<p style=\"margin:0 0 8px;font-size:12px;color:#64748b;\"><strong>UA:</strong> "
            f"{html.escape((user_agent or '')[:200])}</p>"
            f"<p style=\"margin:12px 0 0;font-size:12px;color:#64748b;\">Parmak izi: "
            f"<code>{html.escape(fingerprint[:16])}…</code></p>"
            "<p style=\"margin:16px 0 0;font-size:13px;\">Bu sizseniz Settings → "
            "«Bu cihaz benim» ile tanıdık olarak işaretleyin; bir daha uyarı gelmez.</p>"
            "</div>"
        )
        subject = f"SEO Agent — Tanınmayan admin girişi ({ip or '?'})"
        return send_admin_security_email(subject, body, [recipient])
    except Exception as exc:
        LOGGER.warning("Admin giriş uyarı e-postası gönderilemedi: %s", exc)
        return False


def record_access_event(
    db: Session,
    *,
    event_type: str,
    ip: str,
    user_agent: str,
) -> AdminLoginEvent:
    """Giriş veya settings erişimini kalıcı kaydet; gerekirse e-posta gönder."""
    ua = (user_agent or "")[:512]
    fp = device_fingerprint(ip, ua)
    device = parse_device_label(ua)
    trusted = is_trusted(db, fp)

    row = AdminLoginEvent(
        event_type=event_type,
        ip=(ip or "")[:64],
        device_label=device[:120],
        user_agent=ua,
        fingerprint=fp,
        is_trusted=trusted,
    )
    db.add(row)
    db.flush()

    should_alert = event_type in ("login_ok", "settings_ok") and not trusted
    if should_alert:
        has_any_trusted = db.query(AdminTrustedDevice.id).first() is not None
        if not has_any_trusted:
            trust_fingerprint(db, fp, label=device, ip_hint=ip)
            row.is_trusted = True
        else:
            sent = _send_unknown_login_alert(
                ip=ip,
                device_label=device,
                user_agent=ua,
                fingerprint=fp,
                event_type=event_type,
            )
            row.alert_sent = sent

    db.commit()
    _trim_old_events(db)
    return row


def recent_login_history(db: Session, *, limit: int = _LOGIN_HISTORY_LIMIT) -> list[dict]:
    rows = (
        db.query(AdminLoginEvent)
        .order_by(AdminLoginEvent.created_at.desc(), AdminLoginEvent.id.desc())
        .limit(limit)
        .all()
    )
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "event_type": r.event_type,
                "event_label": _event_label(r.event_type),
                "ip": r.ip,
                "device": r.device_label,
                "fingerprint": r.fingerprint,
                "is_trusted": bool(r.is_trusted),
                "alert_sent": bool(r.alert_sent),
                "created_at": r.created_at,
                "created_at_tr": format_tr(r.created_at),
                "is_success": r.event_type.endswith("_ok"),
            }
        )
    return out


def enrich_active_session(
    session: dict,
    *,
    trusted_fps: set[str],
    current_key: str,
    session_key: str,
) -> dict:
    ua = session.get("user_agent") or ""
    ip = session.get("ip") or ""
    fp = device_fingerprint(ip, ua)
    first_seen = session.get("first_seen")
    last_seen = session.get("last_seen")
    return {
        **session,
        "fingerprint": fp,
        "is_trusted": fp in trusted_fps,
        "is_current": session_key == current_key,
        "first_seen_tr": format_tr(first_seen if isinstance(first_seen, datetime) else None),
        "last_seen_tr": format_tr(last_seen if isinstance(last_seen, datetime) else None),
    }
