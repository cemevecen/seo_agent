"""Admin giriş geçmişi, tanıdık cihazlar ve tanınmayan giriş uyarıları."""

from __future__ import annotations

import hashlib
import html
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import AdminLoginEvent, AdminTrustedDevice

if TYPE_CHECKING:
    from starlette.requests import Request

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")
_LOGIN_HISTORY_LIMIT = 10
_nav_lock = threading.Lock()
_nav_watch: dict[str, dict[str, Any]] = {}


def client_ip_from_request(request: Request) -> str:
    """Proxy arkasında gerçek istemci IP (member OAuth vb.)."""
    if settings.trust_proxy_headers:
        forwarded = (request.headers.get("x-forwarded-for") or "").strip()
        if forwarded:
            return forwarded.split(",")[0].strip()
    return (request.client.host if request.client else "") or ""


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


def parse_browser_short(user_agent: str) -> str:
    """Konu satırı için yalnızca tarayıcı adı."""
    ua_l = (user_agent or "").lower()
    if "chrome" in ua_l and "edg" not in ua_l and "opr" not in ua_l:
        return "Chrome"
    if "firefox" in ua_l:
        return "Firefox"
    if "safari" in ua_l and "chrome" not in ua_l:
        return "Safari"
    if "edg" in ua_l:
        return "Edge"
    if "opr" in ua_l or "opera" in ua_l:
        return "Opera"
    return "Tarayıcı"


def parse_ua_details(user_agent: str) -> dict[str, str]:
    ua = user_agent or ""
    ua_l = ua.lower()
    os_label = "Bilinmiyor"
    m = re.search(r"Mac OS X ([\d_]+)", ua)
    if m:
        os_label = "macOS " + m.group(1).replace("_", ".")
    elif "windows nt 10" in ua_l:
        os_label = "Windows 10/11"
    elif "windows nt" in ua_l:
        os_label = "Windows"
    elif m := re.search(r"Android ([\d.]+)", ua):
        os_label = f"Android {m.group(1)}"
    elif "iphone" in ua_l or "ipad" in ua_l:
        if m := re.search(r"OS ([\d_]+)", ua):
            os_label = "iOS " + m.group(1).replace("_", ".")
        else:
            os_label = "iOS"
    elif "linux" in ua_l:
        os_label = "Linux"
    browser_ver = ""
    for pat, name in (
        (r"Chrome/([\d.]+)", "Chrome"),
        (r"Firefox/([\d.]+)", "Firefox"),
        (r"Version/([\d.]+).*Safari", "Safari"),
        (r"Edg/([\d.]+)", "Edge"),
    ):
        m = re.search(pat, ua)
        if m:
            browser_ver = f"{name} {m.group(1)}"
            break
    arch = "ARM64" if "arm64" in ua_l or "aarch64" in ua_l else ("x64" if "x86_64" in ua_l or "win64" in ua_l else "")
    return {
        "os": os_label,
        "browser_version": browser_ver or parse_browser_short(ua),
        "arch": arch or "—",
    }


_ADMIN_PATH_LABELS: list[tuple[str, str]] = [
    ("/ad/app-banner", "Ad · GA4 banner"),
    ("/data-explorer", "Speed / Data Explorer"),
    ("/search-console", "Search Console"),
    ("/seo-audit", "SEO Audit"),
    ("/tmdb-upcoming", "Movie / TMDB"),
    ("/intelligence", "News / Intelligence"),
    ("/external-explorer", "External Explorer"),
    ("/public-sites", "Public Sites"),
    ("/realtime", "Realtime"),
    ("/firebase", "Firebase"),
    ("/notification", "Notification"),
    ("/backlinks", "Backlinks"),
    ("/settings", "Settings"),
    ("/external", "External"),
    ("/errors", "Errors"),
    ("/alerts", "Alerts"),
    ("/boards", "GitLab / Boards"),
    ("/policy", "Policy"),
    ("/inbox", "Inbox"),
    ("/ga4", "GA4"),
    ("/admin/login", "Admin Login"),
    ("/ad", "Ad / Monetizasyon"),
    ("/app", "App"),
    ("/ai", "AI Talk"),
    ("/", "Home / Günün Özeti"),
]


def admin_path_label(path: str) -> str:
    p = (path or "/").split("?")[0].rstrip("/") or "/"
    for prefix, label in _ADMIN_PATH_LABELS:
        if prefix == "/":
            if p == "/":
                return label
            continue
        if p == prefix or p.startswith(prefix + "/"):
            return label
    if p.startswith("/api/"):
        return "API " + p[:48]
    return p[:80] or "/"


def should_track_admin_path(path: str) -> bool:
    p = (path or "").split("?")[0]
    if not p or p.startswith(("/static/", "/health", "/favicon", "/apple-touch-icon")):
        return False
    if p.startswith("/api/"):
        return False
    if p in ("/admin/login", "/admin/auth/login", "/admin/settings-login"):
        return False
    return True


def begin_nav_watch(
    fingerprint: str,
    *,
    meta: dict[str, Any],
) -> None:
    fp = (fingerprint or "").strip()
    if not fp:
        return
    with _nav_lock:
        _nav_watch[fp] = {
            "meta": dict(meta),
            "paths": [],
            "started": time.time(),
        }


def record_admin_nav(fingerprint: str, path: str) -> None:
    fp = (fingerprint or "").strip()
    if not fp or not should_track_admin_path(path):
        return
    label = admin_path_label(path)
    p = (path or "").split("?")[0]
    with _nav_lock:
        bucket = _nav_watch.get(fp)
        if not bucket:
            return
        paths: list[dict[str, str]] = bucket["paths"]
        if paths and paths[-1].get("path") == p:
            return
        paths.append(
            {
                "path": p,
                "label": label,
                "at_tr": datetime.now(_TR).strftime("%H:%M:%S"),
            }
        )
        if len(paths) > 40:
            del paths[:-40]


def _pop_nav_watch(fingerprint: str) -> dict[str, Any] | None:
    fp = (fingerprint or "").strip()
    with _nav_lock:
        return _nav_watch.pop(fp, None)


def _lookup_ip_geo(ip: str) -> dict[str, Any]:
    ip = (ip or "").strip()
    if not ip or ip.startswith(("127.", "10.", "192.168.", "172.16.", "::1", "localhost")):
        return {}
    try:
        url = (
            f"http://ip-api.com/json/{urllib.parse.quote(ip)}"
            "?fields=status,message,country,regionName,city,isp,org,as,proxy,hosting,mobile,timezone"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "seo-agent-admin-alert/1.0"})
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if data.get("status") != "success":
            return {}
        return data
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        LOGGER.debug("IP geo lookup atlandı (%s): %s", ip, exc)
        return {}


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
        "member_login_ok": "Google üye girişi",
        "login_fail": "Başarısız giriş",
        "settings_ok": "Settings erişimi",
        "settings_fail": "Settings — hatalı şifre",
    }.get(event_type, event_type)


def _prior_login_rows(db: Session, *, fingerprint: str, ip: str, limit: int = 6) -> list[AdminLoginEvent]:
    q = (
        db.query(AdminLoginEvent)
        .filter(AdminLoginEvent.event_type.in_(("login_ok", "member_login_ok", "settings_ok", "login_fail")))
        .order_by(AdminLoginEvent.created_at.desc(), AdminLoginEvent.id.desc())
        .limit(limit + 3)
    )
    rows = q.all()
    out: list[AdminLoginEvent] = []
    for r in rows:
        if r.fingerprint == fingerprint or r.ip == ip:
            out.append(r)
        if len(out) >= limit:
            break
    return out


def _active_sessions_for_fingerprint(fingerprint: str) -> list[dict[str, Any]]:
    try:
        from backend import main as main_mod

        now = datetime.utcnow()
        cutoff = now - __import__("datetime").timedelta(minutes=main_mod._SESSION_IDLE_MINUTES)
        out: list[dict[str, Any]] = []
        for _key, sess in main_mod._active_sessions.items():
            if sess.get("last_seen", now) < cutoff:
                continue
            ua = sess.get("user_agent") or ""
            ip = sess.get("ip") or ""
            fp = device_fingerprint(ip, ua)
            if fp == fingerprint:
                out.append(dict(sess))
        return out
    except Exception:
        return []


def _html_section(title: str, inner: str) -> str:
    return (
        f"<h3 style=\"margin:20px 0 8px;font-size:14px;color:#0f172a;\">{html.escape(title)}</h3>"
        f"{inner}"
    )


def _deliver_unknown_login_alert(
    *,
    ip: str,
    device_label: str,
    user_agent: str,
    fingerprint: str,
    event_type: str,
    referer: str = "",
    accept_language: str = "",
    nav_paths: list[dict[str, str]] | None = None,
    actor_email: str = "",
) -> bool:
    from backend.services.mailer import normalize_outbound_recipients, send_admin_security_email

    recipients = normalize_outbound_recipients([(settings.admin_login_alert_email or "").strip()])
    if not recipients or not settings.admin_login_alert_enabled:
        return False
    try:
        from backend.database import SessionLocal

        when = format_tr(datetime.utcnow())
        et = _event_label(event_type)
        actor_line = ""
        em = (actor_email or "").strip()
        if em:
            actor_line = (
                f"<p style=\"margin:0 0 8px;\"><strong>Kullanıcı e-posta:</strong> "
                f"{html.escape(em)}</p>"
            )
        ua_details = parse_ua_details(user_agent)
        geo = _lookup_ip_geo(ip)
        geo_line = "—"
        if geo:
            parts = [
                geo.get("city"),
                geo.get("regionName"),
                geo.get("country"),
            ]
            loc = ", ".join(p for p in parts if p)
            flags = []
            if geo.get("hosting"):
                flags.append("hosting/VPS")
            if geo.get("proxy"):
                flags.append("proxy")
            if geo.get("mobile"):
                flags.append("mobil ağ")
            geo_line = loc or "—"
            if geo.get("isp"):
                geo_line += f" · ISP: {geo['isp']}"
            if geo.get("org") and geo.get("org") != geo.get("isp"):
                geo_line += f" · {geo['org']}"
            if geo.get("timezone"):
                geo_line += f" · TZ: {geo['timezone']}"
            if flags:
                geo_line += f" · ({', '.join(flags)})"

        prior_html = "<p style=\"margin:0;color:#64748b;font-size:12px;\">Kayıt yok.</p>"
        trusted_count = 0
        with SessionLocal() as db:
            trusted_count = db.query(AdminTrustedDevice.id).count()
            prior = _prior_login_rows(db, fingerprint=fingerprint, ip=ip, limit=5)
            if prior:
                rows = []
                for r in prior:
                    rows.append(
                        "<tr>"
                        f"<td style=\"padding:4px 8px;border-bottom:1px solid #e2e8f0;\">{html.escape(format_tr(r.created_at))}</td>"
                        f"<td style=\"padding:4px 8px;border-bottom:1px solid #e2e8f0;\">{html.escape(_event_label(r.event_type))}</td>"
                        f"<td style=\"padding:4px 8px;border-bottom:1px solid #e2e8f0;\">{html.escape(r.ip or '—')}</td>"
                        f"<td style=\"padding:4px 8px;border-bottom:1px solid #e2e8f0;\">{html.escape(r.device_label or '—')}</td>"
                        f"<td style=\"padding:4px 8px;border-bottom:1px solid #e2e8f0;\">"
                        f"{'✓ tanıdık' if r.is_trusted else '—'}</td>"
                        "</tr>"
                    )
                prior_html = (
                    "<table style=\"border-collapse:collapse;width:100%;font-size:12px;\">"
                    "<thead><tr style=\"background:#f1f5f9;\">"
                    "<th style=\"text-align:left;padding:4px 8px;\">Zaman</th>"
                    "<th style=\"text-align:left;padding:4px 8px;\">Olay</th>"
                    "<th style=\"text-align:left;padding:4px 8px;\">IP</th>"
                    "<th style=\"text-align:left;padding:4px 8px;\">Cihaz</th>"
                    "<th style=\"text-align:left;padding:4px 8px;\">Tanıdık</th>"
                    "</tr></thead><tbody>"
                    + "".join(rows)
                    + "</tbody></table>"
                )

        nav_paths = nav_paths or []
        if nav_paths:
            nav_items = []
            for i, hit in enumerate(nav_paths, 1):
                nav_items.append(
                    f"<li style=\"margin:0 0 4px;\"><strong>{html.escape(hit.get('at_tr') or '')}</strong> "
                    f"{html.escape(hit.get('label') or '')} "
                    f"<code style=\"font-size:11px;color:#64748b;\">{html.escape(hit.get('path') or '')}</code></li>"
                )
            nav_html = "<ol style=\"margin:0;padding-left:20px;font-size:13px;\">" + "".join(nav_items) + "</ol>"
        else:
            nav_html = (
                "<p style=\"margin:0;font-size:12px;color:#64748b;\">"
                "İzleme penceresinde sayfa gezintisi kaydedilmedi (hemen çıkış veya yalnızca API).</p>"
            )

        sessions = _active_sessions_for_fingerprint(fingerprint)
        sess_html = "<p style=\"margin:0;font-size:12px;color:#64748b;\">Eşleşen aktif oturum yok.</p>"
        if sessions:
            bits = []
            for s in sessions:
                bits.append(
                    f"<li>IP {html.escape(s.get('ip') or '—')} · "
                    f"ilk {html.escape(format_tr(s.get('first_seen')))} · "
                    f"son {html.escape(format_tr(s.get('last_seen')))}</li>"
                )
            sess_html = "<ul style=\"margin:0;padding-left:18px;font-size:12px;\">" + "".join(bits) + "</ul>"

        delay = int(settings.admin_login_alert_nav_delay_seconds or 0)
        nav_note = (
            f"Girişten sonra ~{delay} sn içinde ziyaret edilen menüler/sayfalar."
            if delay > 0
            else "Anlık uyarı (gezinti penceresi kapalı)."
        )

        body = (
            '<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',sans-serif;max-width:640px;line-height:1.45;">'
            f"<h2 style=\"color:#b91c1c;margin:0 0 12px;\">Tanınmayan admin erişimi</h2>"
            f"<p style=\"margin:0 0 8px;\"><strong>Olay:</strong> {html.escape(et)}</p>"
            + actor_line
            + f"<p style=\"margin:0 0 8px;\"><strong>Zaman:</strong> {html.escape(when)} (TR)</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>IP:</strong> {html.escape(ip or '—')}</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>Konum / ağ:</strong> {html.escape(geo_line)}</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>Cihaz:</strong> {html.escape(device_label)}</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>İşletim sistemi:</strong> {html.escape(ua_details.get('os') or '—')}</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>Tarayıcı:</strong> {html.escape(ua_details.get('browser_version') or '—')} "
            + f"· <strong>Mimari:</strong> {html.escape(ua_details.get('arch') or '—')}</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>Dil (Accept-Language):</strong> {html.escape(accept_language or '—')}</p>"
            + f"<p style=\"margin:0 0 8px;\"><strong>Referer (giriş):</strong> {html.escape(referer or '—')}</p>"
            + f"<p style=\"margin:0 0 8px;font-size:12px;color:#64748b;\"><strong>User-Agent:</strong><br>"
            + f"{html.escape(user_agent or '')}</p>"
            + f"<p style=\"margin:0 0 8px;font-size:12px;color:#64748b;\"><strong>Parmak izi:</strong> "
            + f"<code>{html.escape(fingerprint)}</code></p>"
            + f"<p style=\"margin:0 0 8px;font-size:12px;\"><strong>Tanıdık cihaz sayısı (DB):</strong> {trusted_count}</p>"
            + _html_section("Menü / sayfa gezintisi", f"<p style=\"margin:0 0 6px;font-size:11px;color:#64748b;\">{html.escape(nav_note)}</p>{nav_html}")
            + _html_section("Bu IP / cihaz — son kayıtlar", prior_html)
            + _html_section("Aktif oturum (bellek)", sess_html)
            + "<p style=\"margin:20px 0 0;font-size:13px;\">Bu sizseniz Settings → "
            "«Bu cihaz benim» ile tanıdık olarak işaretleyin; bir daha uyarı gelmez.</p>"
            "</div>"
        )
        browser = parse_browser_short(user_agent)
        ip_disp = (ip or "?").strip() or "?"
        if em:
            subject = f"panel girişi - '{em}' - '{ip_disp}'"
        else:
            subject = f"admin girişi - '{browser}' - '{ip_disp}'"
        return send_admin_security_email(subject, body, recipients)
    except Exception as exc:
        LOGGER.warning("Admin giriş uyarı e-postası gönderilemedi: %s", exc)
        return False


def schedule_unknown_login_alert(
    *,
    ip: str,
    device_label: str,
    user_agent: str,
    fingerprint: str,
    event_type: str,
    referer: str = "",
    accept_language: str = "",
    actor_email: str = "",
) -> bool:
    """Gezinti toplamak için gecikmeli gönderim; delay=0 ise anında."""
    delay = max(0, int(settings.admin_login_alert_nav_delay_seconds or 0))
    meta = {
        "ip": ip,
        "device_label": device_label,
        "user_agent": user_agent,
        "fingerprint": fingerprint,
        "event_type": event_type,
        "referer": referer,
        "accept_language": accept_language,
        "actor_email": (actor_email or "").strip(),
    }
    begin_nav_watch(fingerprint, meta=meta)

    def _run() -> None:
        if delay > 0:
            time.sleep(delay)
        bucket = _pop_nav_watch(fingerprint) or {"meta": meta, "paths": []}
        m = bucket.get("meta") or meta
        paths = bucket.get("paths") or []
        _deliver_unknown_login_alert(
            ip=str(m.get("ip") or ""),
            device_label=str(m.get("device_label") or ""),
            user_agent=str(m.get("user_agent") or ""),
            fingerprint=str(m.get("fingerprint") or fingerprint),
            event_type=str(m.get("event_type") or event_type),
            referer=str(m.get("referer") or ""),
            accept_language=str(m.get("accept_language") or ""),
            nav_paths=paths,
            actor_email=str(m.get("actor_email") or ""),
        )

    if delay <= 0:
        bucket = _pop_nav_watch(fingerprint) or {"meta": meta, "paths": []}
        paths = bucket.get("paths") or []
        return _deliver_unknown_login_alert(
            ip=ip,
            device_label=device_label,
            user_agent=user_agent,
            fingerprint=fingerprint,
            event_type=event_type,
            referer=referer,
            accept_language=accept_language,
            nav_paths=paths,
            actor_email=actor_email,
        )

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"admin-alert-{fingerprint[:8]}",
    ).start()
    return True


def _send_unknown_login_alert(**kwargs: Any) -> bool:
    """Geriye dönük test uyumu."""
    return schedule_unknown_login_alert(**kwargs)


def record_access_event(
    db: Session,
    *,
    event_type: str,
    ip: str,
    user_agent: str,
    referer: str = "",
    accept_language: str = "",
    actor_email: str = "",
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

    em = (actor_email or "").strip()
    if event_type == "member_login_ok" and em:
        sent = schedule_unknown_login_alert(
            ip=ip,
            device_label=device,
            user_agent=ua,
            fingerprint=fp,
            event_type=event_type,
            referer=referer,
            accept_language=accept_language,
            actor_email=em,
        )
        row.alert_sent = sent
    elif event_type in ("login_ok", "settings_ok") and not trusted:
        has_any_trusted = db.query(AdminTrustedDevice.id).first() is not None
        if not has_any_trusted:
            trust_fingerprint(db, fp, label=device, ip_hint=ip)
            row.is_trusted = True
        else:
            sent = schedule_unknown_login_alert(
                ip=ip,
                device_label=device,
                user_agent=ua,
                fingerprint=fp,
                event_type=event_type,
                referer=referer,
                accept_language=accept_language,
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
