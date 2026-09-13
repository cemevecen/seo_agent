"""Alan bazlı realtime trafik farkı postası.

Yalnızca doviz (web, mweb, android, ios) ve sinemalar (web, mweb).
Karşılaştırma GA4 realtime önceki 15 dk ile sonraki 15 dk. Eşik %70.
Eski SEO özet / haber / sayfa postaları bu kanaldan gitmez.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

SWING_PCT = 70.0
# 2→4 gibi küçük sıçramalar %100 görünür; gerçek trafik için taban.
MIN_USERS = 20
COOLDOWN_MINUTES = 50
NOTIF_TYPE = "realtime_traffic_swing"

_PROFILE_LABEL = {
    "web": "web",
    "mweb": "mweb",
    "android": "android",
    "ios": "ios",
}


def _bare_domain(domain: str | None) -> str:
    d = (domain or "").strip().lower()
    if d.startswith("www."):
        d = d[4:]
    return d


def swing_profiles_for_domain(domain: str | None) -> tuple[str, ...] | None:
    d = _bare_domain(domain)
    if d == "doviz.com" or d.endswith(".doviz.com"):
        return ("web", "mweb", "android", "ios")
    if d == "sinemalar.com" or d.endswith(".sinemalar.com"):
        return ("web", "mweb")
    return None


def evaluate_traffic_swing(
    previous: float,
    current: float,
    *,
    threshold_pct: float = SWING_PCT,
    min_users: int = MIN_USERS,
) -> dict[str, Any] | None:
    """Önceki ve sonraki pencere. İkisi de yoksa veya taban altındaysa mail yok."""
    prev = float(previous or 0)
    cur = float(current or 0)
    if prev <= 0:
        return None
    if max(prev, cur) < min_users:
        return None
    pct = ((cur - prev) / prev) * 100.0
    if abs(pct) < threshold_pct:
        return None
    direction = "up" if pct > 0 else "down"
    return {
        "previous": int(round(prev)),
        "current": int(round(cur)),
        "change_pct": round(pct, 1),
        "direction": direction,
    }


def _site_label(domain: str) -> str:
    d = _bare_domain(domain)
    if d.endswith(".doviz.com"):
        return "doviz"
    if d.endswith(".sinemalar.com"):
        return "sinemalar"
    if d.endswith(".com"):
        return d[: -len(".com")]
    return d or "site"


def swing_subject(domain: str, profile: str, change_pct: float) -> str:
    sign = "+" if change_pct > 0 else "−"
    label = _site_label(domain)
    area = _PROFILE_LABEL.get(profile, profile or "web")
    return f"{label} {area} {sign}{abs(change_pct):.0f}%"[:120]


def _swing_html(domain: str, profile: str, hit: dict[str, Any]) -> str:
    from backend.services.email_templates import note_box, render_email_shell, section

    area = _PROFILE_LABEL.get(profile, profile or "web")
    label = _site_label(domain)
    pct = float(hit["change_pct"])
    prev = int(hit["previous"])
    cur = int(hit["current"])
    tone = "emerald" if pct > 0 else "rose"
    sign = "+" if pct > 0 else ""
    body = (
        f"{prev:,} → {cur:,} ({sign}{pct:.1f}%)\n"
        "Önceki 15 dk ile sonraki 15 dk · yalnız bu alan"
    )
    return render_email_shell(
        eyebrow="Realtime",
        title=f"{label} {area}",
        intro="Aktif kullanıcı farkı eşik üzerinde.",
        tone=tone,
        status_label=f"{sign}{pct:.0f}%",
        sections=[section("Trafik", note_box("15 dk", body, tone=tone))],
    )


def _recently_sent(db: Session, key: str) -> bool:
    from backend.models import NotificationDeliveryLog

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=COOLDOWN_MINUTES)
    row = (
        db.query(NotificationDeliveryLog.sent_at)
        .filter(
            NotificationDeliveryLog.notification_type == NOTIF_TYPE,
            NotificationDeliveryLog.notification_key == key,
        )
        .order_by(desc(NotificationDeliveryLog.sent_at))
        .limit(1)
        .first()
    )
    if not row or row[0] is None:
        return False
    sent = row[0]
    if sent.tzinfo is None:
        sent = sent.replace(tzinfo=timezone.utc)
    return sent >= cutoff


def _mark_sent(db: Session, key: str, subject: str) -> None:
    from backend.models import NotificationDeliveryLog

    db.add(
        NotificationDeliveryLog(
            notification_type=NOTIF_TYPE,
            notification_key=key[:255],
            subject=subject[:255],
            recipient="",
        )
    )
    db.commit()


def _users_from_result(result: dict[str, Any]) -> tuple[float, float] | None:
    if not isinstance(result, dict) or result.get("error"):
        return None
    comp = (result.get("comparison") or {}).get("activeUsers") or {}
    if "previous" not in comp and "current" not in comp:
        total = result.get("total") or result.get("current") or {}
        prev = result.get("previous") or {}
        if "activeUsers" not in total and "activeUsers" not in prev:
            return None
        return float(prev.get("activeUsers") or 0), float(total.get("activeUsers") or 0)
    return float(comp.get("previous") or 0), float(comp.get("current") or 0)


def notify_traffic_swings(
    db: Session,
    fetched: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Eşik aşan her alan için ayrı posta. Veri yoksa yazılmaz."""
    from backend.models import Site
    from backend.services.ga4_realtime import check_site_realtime
    from backend.services.mailer import send_area_realtime_email

    by_key: dict[tuple[int, str], dict[str, Any]] = {}
    for row in fetched or []:
        if not isinstance(row, dict) or row.get("error"):
            continue
        sid = row.get("site_id")
        prof = str(row.get("profile") or "")
        if sid is None or not prof:
            continue
        by_key[(int(sid), prof)] = row

    sent: list[dict[str, Any]] = []
    sites = db.query(Site).filter(Site.is_active.is_(True)).all()
    for site in sites:
        profiles = swing_profiles_for_domain(site.domain)
        if not profiles:
            continue
        for profile in profiles:
            result = by_key.get((int(site.id), profile))
            if result is None:
                try:
                    result = check_site_realtime(
                        db,
                        site,
                        profile=profile,
                        skip_alarms=True,
                        skip_emails=True,
                    )
                except Exception as exc:
                    logger.warning(
                        "trafik farkı çekilemedi %s %s: %s",
                        site.domain,
                        profile,
                        exc,
                    )
                    continue
            pair = _users_from_result(result or {})
            if pair is None:
                continue
            hit = evaluate_traffic_swing(pair[0], pair[1])
            if not hit:
                continue
            key = f"{_bare_domain(site.domain)}:{profile}:{hit['direction']}"
            if _recently_sent(db, key):
                logger.info("trafik farkı cooldown %s", key)
                continue
            subject = swing_subject(site.domain, profile, float(hit["change_pct"]))
            html = _swing_html(site.domain, profile, hit)
            if not send_area_realtime_email(subject, html):
                logger.warning("trafik farkı postası gitmedi: %s", subject)
                continue
            _mark_sent(db, key, subject)
            sent.append(
                {
                    "domain": site.domain,
                    "profile": profile,
                    "subject": subject,
                    **hit,
                }
            )
            logger.info("trafik farkı postası: %s", subject)
    return sent
