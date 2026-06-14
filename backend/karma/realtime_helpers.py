"""Trend modülü — paylaşımlı realtime veri katmanı."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc
from sqlalchemy.orm import Session

from backend.models import NewsIntelligenceItem, RealtimeAlarmLog, RealtimePageSnapshot, RealtimeSnapshot, Site


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def age_minutes(dt: datetime | None, *, now: datetime | None = None) -> float:
    if not dt:
        return 9999.0
    now = now or utcnow()
    pub = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    return max(0.0, (now - pub).total_seconds() / 60.0)


def tokenize(text: str) -> list[str]:
    words = re.findall(r"[a-zA-ZçğıöşüÇĞİÖŞÜ0-9]{4,}", (text or "").lower())
    stop = {
        "için", "olarak", "daha", "sonra", "haber", "günü", "bugün", "yeni", "ile", "veya",
        "this", "that", "with", "olan", "gibi", "kadar", "son", "dakika",
    }
    return [w for w in words if w not in stop]


def intel_recent(
    db: Session,
    *,
    hours: int = 12,
    minutes: int | None = None,
    limit: int = 200,
) -> list[NewsIntelligenceItem]:
    from backend.menu_excluded import is_menu_excluded_label

    if minutes is not None:
        cutoff = utcnow() - timedelta(minutes=minutes)
    else:
        cutoff = utcnow() - timedelta(hours=hours)
    rows = (
        db.query(NewsIntelligenceItem)
        .filter(NewsIntelligenceItem.published_at >= cutoff)
        .order_by(desc(NewsIntelligenceItem.published_at))
        .limit(limit * 2)
        .all()
    )
    return [r for r in rows if not is_menu_excluded_label(r.source_name)][:limit]


def site_pulse(db: Session, site_id: int) -> dict[str, Any]:
    """web + mweb anlık kullanıcı nabzı."""
    out: dict[str, Any] = {"web": None, "mweb": None, "total_current": 0, "total_delta": 0}
    for prof in ("web", "mweb"):
        snap = (
            db.query(RealtimeSnapshot)
            .filter(RealtimeSnapshot.site_id == site_id, RealtimeSnapshot.profile == prof)
            .order_by(desc(RealtimeSnapshot.collected_at))
            .first()
        )
        if not snap:
            continue
        cur = float(snap.active_users_current or 0)
        prev = float(snap.active_users_previous or 0)
        delta = cur - prev
        pct = (delta / prev * 100.0) if prev else (100.0 if cur else 0.0)
        out[prof] = {
            "current": cur,
            "previous": prev,
            "delta": delta,
            "delta_pct": round(pct, 1),
            "collected_at": snap.collected_at.isoformat() if snap.collected_at else "",
            "alarms": int(snap.alarm_count or 0),
        }
        out["total_current"] += cur
        out["total_delta"] += delta
    return out


def top_pages_rt(db: Session, site_id: int, profile: str = "web", limit: int = 25) -> list[dict[str, Any]]:
    latest = (
        db.query(RealtimePageSnapshot.collected_at)
        .filter(RealtimePageSnapshot.site_id == site_id, RealtimePageSnapshot.profile == profile)
        .order_by(desc(RealtimePageSnapshot.collected_at))
        .limit(1)
        .scalar()
    )
    if not latest:
        return []
    rows = (
        db.query(RealtimePageSnapshot)
        .filter(
            RealtimePageSnapshot.site_id == site_id,
            RealtimePageSnapshot.profile == profile,
            RealtimePageSnapshot.collected_at == latest,
        )
        .order_by(desc(RealtimePageSnapshot.active_users))
        .limit(limit)
        .all()
    )
    return [
        {
            "path": r.page_path or "?",
            "users": int(r.active_users or 0),
            "profile": profile,
            "collected_at": latest.isoformat() if latest else "",
        }
        for r in rows
    ]


def alarms_recent(db: Session, site_id: int, *, hours: int = 6, limit: int = 40) -> list[RealtimeAlarmLog]:
    cutoff = utcnow() - timedelta(hours=hours)
    return (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id, RealtimeAlarmLog.triggered_at >= cutoff)
        .order_by(desc(RealtimeAlarmLog.triggered_at))
        .limit(limit)
        .all()
    )


def alarm_spike_patterns(db: Session, site_id: int, *, days: int = 30) -> dict[str, Any]:
    """Geçmiş alarmlardan saat/gün spike pattern."""
    cutoff = utcnow() - timedelta(days=days)
    rows = (
        db.query(RealtimeAlarmLog)
        .filter(RealtimeAlarmLog.site_id == site_id, RealtimeAlarmLog.triggered_at >= cutoff)
        .all()
    )
    by_hour: Counter[int] = Counter()
    by_dow: Counter[int] = Counter()
    for r in rows:
        if not r.triggered_at:
            continue
        by_hour[r.triggered_at.hour] += 1
        by_dow[r.triggered_at.weekday()] += 1
    dow_names = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"]
    top_hours = by_hour.most_common(5)
    top_days = [(dow_names[d], c) for d, c in by_dow.most_common(3)]
    return {"total": len(rows), "top_hours": top_hours, "top_days": top_days}


def score_intel_row(row: NewsIntelligenceItem, topic_counter: Counter[str], *, now: datetime | None = None) -> float:
    now = now or utcnow()
    age_m = age_minutes(row.published_at, now=now)
    recency = max(0.3, 4.0 - age_m / 30.0)
    score = recency
    if not row.is_in_our_site:
        score += 2.5
    topic = (row.topic or row.category or "").strip()
    if topic:
        score += min(topic_counter.get(topic, 0), 8) * 0.35
    if age_m <= 30:
        score += 1.5
    if age_m <= 60 and not row.is_in_our_site:
        score += 1.0
    return score


def drivers_for_profiles(db: Session, site_id: int) -> dict[str, dict[str, Any]]:
    from backend.services.ga4_realtime import fetch_traffic_drivers

    out: dict[str, dict[str, Any]] = {}
    for prof in ("web", "mweb"):
        try:
            out[prof] = fetch_traffic_drivers(db, site_id, prof)
        except Exception:
            out[prof] = {"drivers_increase": [], "drivers_decrease": [], "has_data": False}
    return out


def fmt_driver(d: dict[str, Any]) -> str:
    page = d.get("page") or d.get("path") or d.get("title") or "?"
    delta = d.get("delta_pct") or d.get("change_pct") or d.get("delta") or ""
    users = d.get("activeUsers") or d.get("active_users") or ""
    parts = [str(page)[:70]]
    if delta != "":
        parts.append(f"Δ {delta}%")
    if users != "":
        parts.append(f"{users} aktif")
    return " · ".join(parts)


def gsc_rising_and_decay(queries: list[dict]) -> tuple[list[dict], list[dict]]:
    rising = [q for q in queries if float(q.get("delta") or 0) > 0.5]
    decay = [q for q in queries if float(q.get("delta") or 0) < -0.5]
    rising.sort(key=lambda x: -float(x.get("delta") or 0))
    decay.sort(key=lambda x: float(x.get("delta") or 0))
    return rising, decay


def match_query_intel(query: str, intel_rows: list[NewsIntelligenceItem]) -> tuple[bool, bool, NewsIntelligenceItem | None]:
    """(any_match, in_our_site, best_row)"""
    qlow = query.lower()
    tokens = tokenize(query)[:4]
    best: NewsIntelligenceItem | None = None
    in_site = False
    for row in intel_rows:
        hl = (row.headline or "").lower()
        if qlow in hl or any(t in hl for t in tokens):
            if row.is_in_our_site:
                in_site = True
            if best is None:
                best = row
    hit = best is not None
    return hit, in_site, best


def editorial_calendar_events(domain: str) -> list[tuple[str, str, str]]:
    is_sinemalar = "sinemalar" in (domain or "").lower()
    if is_sinemalar:
        return [
            ("Ocak", "Oscar sezonu, kış filmleri", "Yılın filmleri / ödül listeleri"),
            ("Şubat", "Sevgililer günü vizyon", "Romantik komedi / özel liste"),
            ("Mayıs", "Cannes", "Festival haberleri, fragman"),
            ("Temmuz-Ağu", "Blockbuster sezonu", "Vizyon takvimi yoğun içerik"),
            ("Aralık", "Yıl sonu top 10", "En çok izlenen / beğenilen"),
        ]
    return [
        ("Her Çarşamba", "TCMB faiz", "Canlı sayfa + push hazırlığı"),
        ("Ay başı", "Enflasyon TÜFE/ÜFE", "Veri anı trafik spike"),
        ("Cuma", "ABD tarım dışı istihdam", "Dolar / altın canlı"),
        ("Bayram öncesi", "Altın / döviz talebi", "Rehber + hesaplama"),
        ("Seçim / kriz", "Politika / piyasa", "War room modu"),
    ]
