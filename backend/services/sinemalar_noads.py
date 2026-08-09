"""Sinemalar management/noAds listesi ↔ Ad Manager policy URL eşleştirmesi."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlparse

from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

_ENTITY_RE = re.compile(
    r"(?:mobileweb/movieInfo|mobileweb/person(?:Movies)?|/film/[^/]+|/sanatci/[^/]+|"
    r"management/movie|management/person)/(\d+)",
    re.I,
)
_ID_ANY_RE = re.compile(r"(?:^|[^\d])(\d{4,})(?:[^\d]|$)")


def _utcnow() -> datetime:
    return datetime.utcnow()


def normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    raw = unquote(raw)
    if "://" not in raw and raw.startswith("www."):
        raw = "https://" + raw
    if "://" not in raw and "sinemalar.com" in raw:
        raw = "https://" + raw.lstrip("/")
    try:
        p = urlparse(raw)
    except Exception:
        return raw.lower().rstrip("/")
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host.startswith("m."):
        host = host[2:]
    path = (p.path or "").rstrip("/")
    return f"{host}{path}".lower()


def entity_keys_from_text(text: str) -> set[str]:
    keys: set[str] = set()
    s = (text or "").strip()
    if not s:
        return keys
    nu = normalize_url(s)
    if nu:
        keys.add(f"url:{nu}")
    for m in _ENTITY_RE.finditer(s):
        eid = m.group(1)
        keys.add(f"id:{eid}")
        if "person" in m.group(0).lower() or "sanatci" in m.group(0).lower():
            keys.add(f"person:{eid}")
        else:
            keys.add(f"movie:{eid}")
    return keys


def build_noads_keyset(entries: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for e in entries or []:
        if not isinstance(e, dict):
            if isinstance(e, str):
                keys |= entity_keys_from_text(e)
            continue
        for field in ("url", "href", "path", "label", "title", "text", "entity_id", "id"):
            val = e.get(field)
            if val is None:
                continue
            keys |= entity_keys_from_text(str(val))
            if field in ("entity_id", "id") and str(val).isdigit():
                keys.add(f"id:{val}")
                keys.add(f"movie:{val}")
                keys.add(f"person:{val}")
    return keys


def violation_matches(url: str, noads_keys: set[str]) -> bool:
    if not noads_keys:
        return False
    vkeys = entity_keys_from_text(url)
    if not vkeys:
        return False
    return bool(vkeys & noads_keys)


def ingest_noads_payload(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    """noAds entry listesini kaydet, policy satırlarını işaretle, gerekirse alarm maili."""
    from backend.models import AdPolicyViolation, SinemalarNoAdsSnapshot
    from backend.services.policy_csv import is_sinemalar_url

    entries_raw = payload.get("entries") or payload.get("urls") or payload.get("rows") or []
    entries: list[dict[str, Any]] = []
    for item in entries_raw:
        if isinstance(item, str):
            entries.append({"url": item})
        elif isinstance(item, dict):
            entries.append(item)

    if not entries:
        return {"ok": False, "message": "noAds listesi boş — admin oturumu veya sayfa seçicileri kontrol et"}

    keys = build_noads_keyset(entries)
    now = _utcnow()
    scraped_at = payload.get("scraped_at") or now.isoformat()

    snap = db.get(SinemalarNoAdsSnapshot, 1)
    if snap is None:
        snap = SinemalarNoAdsSnapshot(id=1)
        db.add(snap)
    snap.entries_json = json.dumps(entries, ensure_ascii=False)[:2_000_000]
    snap.entry_count = len(entries)
    snap.key_count = len(keys)
    snap.scraped_at = now
    snap.message = str(payload.get("message") or "")[:500]
    snap.source = str(payload.get("source") or "sinemalar_noads")[:64]
    snap.updated_at = now

    matched = 0
    missing = 0
    unchecked = 0
    missing_rows: list[dict[str, Any]] = []

    rows = (
        db.query(AdPolicyViolation)
        .order_by(AdPolicyViolation.ad_requests_7d.desc())
        .limit(8000)
        .all()
    )
    for row in rows:
        if not is_sinemalar_url(row.url or ""):
            continue
        hit = violation_matches(row.url or "", keys)
        row.in_noads = hit
        row.noads_checked_at = now
        if hit:
            matched += 1
        else:
            missing += 1
            missing_rows.append(
                {
                    "id": row.id,
                    "url": row.url,
                    "issue_type": row.issue_type,
                    "enforcement": row.enforcement,
                    "ad_requests_7d": int(row.ad_requests_7d or 0),
                    "page_title": row.page_title or "",
                }
            )

    snap.matched_count = matched
    snap.missing_count = missing
    db.commit()

    email_sent = False
    email_skipped = ""
    try:
        email_sent, email_skipped = maybe_send_noads_alarm(
            db,
            snap=snap,
            missing_rows=missing_rows,
            entry_count=len(entries),
            scraped_at=str(scraped_at),
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("noAds alarm maili hata: %s", exc)
        email_skipped = str(exc)

    return {
        "ok": True,
        "entry_count": len(entries),
        "key_count": len(keys),
        "matched": matched,
        "missing": missing,
        "unchecked": unchecked,
        "scraped_at": scraped_at,
        "email_sent": email_sent,
        "email_skipped": email_skipped,
        "message": f"noAds {len(entries)} kayıt · policy eşleşen {matched} · eksik {missing}",
    }


def get_noads_summary(db: Session) -> dict[str, Any]:
    from backend.models import SinemalarNoAdsSnapshot

    snap = db.get(SinemalarNoAdsSnapshot, 1)
    if not snap:
        return {
            "has_data": False,
            "entry_count": 0,
            "matched": 0,
            "missing": 0,
            "scraped_at": None,
            "message": "",
        }
    return {
        "has_data": True,
        "entry_count": int(snap.entry_count or 0),
        "key_count": int(snap.key_count or 0),
        "matched": int(snap.matched_count or 0),
        "missing": int(snap.missing_count or 0),
        "scraped_at": snap.scraped_at.isoformat() if snap.scraped_at else None,
        "message": snap.message or "",
        "last_email_at": snap.last_email_at.isoformat() if snap.last_email_at else None,
    }


def maybe_send_noads_alarm(
    db: Session,
    *,
    snap: Any,
    missing_rows: list[dict[str, Any]],
    entry_count: int,
    scraped_at: str,
) -> tuple[bool, str]:
    """Bariz eksikler için alarm: yüksek istekli veya çok sayıda noAds dışı policy URL."""
    from backend.config import settings
    from backend.services.mailer import send_email

    if not missing_rows:
        return False, "eksik yok"

    if not bool(getattr(settings, "policy_noads_email_enabled", True)):
        return False, "POLICY_NOADS_EMAIL_ENABLED=false"

    cooldown_h = float(getattr(settings, "policy_noads_email_cooldown_hours", 6) or 6)
    if snap.last_email_at:
        age_h = (datetime.utcnow() - snap.last_email_at).total_seconds() / 3600.0
        if age_h < cooldown_h:
            return False, f"cooldown ({age_h:.1f}h < {cooldown_h}h)"

    req_floor = int(getattr(settings, "policy_noads_alarm_min_ad_requests", 500) or 500)
    count_floor = int(getattr(settings, "policy_noads_alarm_min_missing", 15) or 15)

    high = [r for r in missing_rows if int(r.get("ad_requests_7d") or 0) >= req_floor]
    restricted = [
        r
        for r in missing_rows
        if "kısıt" in str(r.get("enforcement") or "").lower()
        or "restrict" in str(r.get("enforcement") or "").lower()
    ]

    obvious = False
    reasons: list[str] = []
    if len(missing_rows) >= count_floor:
        obvious = True
        reasons.append(f"{len(missing_rows)} policy URL noAds'te yok (≥{count_floor})")
    if len(high) >= 3:
        obvious = True
        reasons.append(f"{len(high)} URL ≥{req_floor} reklam isteği ile noAds dışı")
    if len(restricted) >= 5 and len(high) >= 1:
        obvious = True
        reasons.append(f"{len(restricted)} kısıtlı reklam URL'si noAds dışı")

    if not obvious:
        return False, "eşik altı (bariz değil)"

    top = sorted(missing_rows, key=lambda r: int(r.get("ad_requests_7d") or 0), reverse=True)[:25]
    rows_html = "".join(
        (
            "<tr>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #e2e8f0'>{int(r.get('ad_requests_7d') or 0):,}</td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #e2e8f0'>{_esc(r.get('enforcement'))}</td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #e2e8f0'>{_esc(r.get('issue_type'))}</td>"
            f"<td style='padding:6px 8px;border-bottom:1px solid #e2e8f0;word-break:break-all'>"
            f"<a href='{_esc(r.get('url'))}'>{_esc(r.get('page_title') or r.get('url'))}</a></td>"
            "</tr>"
        )
        for r in top
    )
    subject = f"Policy · noAds eksik · {len(missing_rows)} URL"
    body = f"""
    <div style="font-family:system-ui,sans-serif;font-size:14px;color:#0f172a">
      <p><b>Sinemalar Policy ↔ noAds uyumsuzluğu</b></p>
      <p>{_esc(' · '.join(reasons))}</p>
      <p>noAds kayıt: <b>{entry_count}</b> · eksik policy: <b>{len(missing_rows)}</b> · tarama: {_esc(scraped_at)}</p>
      <p>Panel: <a href="https://projectcontrol.up.railway.app/policy">/policy</a>
         · noAds: <a href="https://www.sinemalar.com/management/noAds">management/noAds</a></p>
      <table style="border-collapse:collapse;width:100%;margin-top:12px">
        <thead>
          <tr style="background:#f1f5f9;text-align:left">
            <th style="padding:6px 8px">İstek</th>
            <th style="padding:6px 8px">Durum</th>
            <th style="padding:6px 8px">Sorun</th>
            <th style="padding:6px 8px">URL</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """
    ok = bool(send_email(subject, body))
    if ok:
        snap.last_email_at = datetime.utcnow()
        db.commit()
        return True, ""
    return False, "send_email false (SMTP/OUTBOUND kapalı olabilir)"


def _esc(v: Any) -> str:
    s = str(v or "")
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
