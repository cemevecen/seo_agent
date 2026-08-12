"""Settings scrape paneli — katalog, ingest log, saatlik özet."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import desc
from sqlalchemy.orm import Session

from backend.models import (
    AscConsoleWorkspace,
    BacklinkImport,
    CollectorRun,
    DovizNewsWorkspace,
    FirebaseConsoleWorkspace,
    NotificationAnalyticsWorkspace,
    PlayConsoleWorkspace,
    ScrapeIngestLog,
    Site,
)

LOGGER = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")
_UTC = timezone.utc

# Mac bridge schedule (Europe/Istanbul) — scripts/doviz_admin_notification_bridge.py ile uyumlu
SCRAPE_CATALOG: list[dict[str, Any]] = [
    {
        "source": "notification_analytics",
        "label": "Notification analytics",
        "targets": ["doviz"],
        "cadence": "Her 30 dk",
        "hours_tr": "*:00 / *:30",
        "method": "Automatic scan",
        "volume_unit": "satır",
    },
    {
        "source": "doviz_news",
        "label": "Döviz news admin",
        "targets": ["doviz"],
        "cadence": "Saatlik",
        "hours_tr": "Her saat",
        "method": "Automatic scan",
        "volume_unit": "haber",
    },
    {
        "source": "play_console",
        "label": "Play Console (Android)",
        "targets": ["doviz · com.Doviz"],
        "cadence": "3 saatte bir",
        "hours_tr": "00/03/06/09/12/15/18/21:00",
        "method": "Automatic scan",
        "volume_unit": "metrik+yorum",
    },
    {
        "source": "asc_console",
        "label": "App Store Connect (iOS)",
        "targets": ["doviz · ASC"],
        "cadence": "3 saatte bir",
        "hours_tr": "00/03/06/09/12/15/18/21:05",
        "method": "Automatic scan",
        "volume_unit": "metrik",
    },
    {
        "source": "firebase_console",
        "label": "Firebase Console (Crashlytics)",
        "targets": ["doviz-android", "doviz-ios"],
        "cadence": "3 saatte bir",
        "hours_tr": "00/03/06/09/12/15/18/21:10",
        "method": "Automatic scan",
        "volume_unit": "crash-free+issues",
    },
    {
        "source": "virgul_analytics",
        "label": "Virgül ads",
        "targets": ["doviz", "sinemalar"],
        "cadence": "6 saatte bir",
        "hours_tr": "00/06/12/18:00",
        "method": "Automatic scan",
        "volume_unit": "satır",
    },
    {
        "source": "gsc_links",
        "label": "GSC Links",
        "targets": ["doviz.com", "sinemalar.com"],
        "cadence": "Günde 2",
        "hours_tr": "01:00 · 13:00",
        "method": "Automatic scan",
        "volume_unit": "link satırı",
    },
    {
        "source": "admanager_policy",
        "label": "Ad Manager Policy",
        "targets": ["sinemalar.com"],
        "cadence": "Günde 2",
        "hours_tr": "01:05 · 13:05",
        "method": "Automatic scan",
        "volume_unit": "ihlal",
    },
    {
        "source": "pagespeed_web",
        "label": "PageSpeed web.dev",
        "targets": ["www.doviz.com", "www.sinemalar.com"],
        "cadence": "Günde 2 · mobil+masaüstü",
        "hours_tr": "01:10 · 13:10",
        "method": "Automatic scan",
        "volume_unit": "snapshot",
    },
    {
        "source": "sinemalar_noads",
        "label": "Sinemalar noAds",
        "targets": ["sinemalar"],
        "cadence": "Günde 2",
        "hours_tr": "01:15 · 13:15",
        "method": "Automatic scan",
        "volume_unit": "URL",
    },
    {
        "source": "seo_audit",
        "label": "SEO audit (HTML meta)",
        "targets": ["doviz", "sinemalar"],
        "cadence": "Günde 2",
        "hours_tr": "02:45 · 14:45",
        "method": "Automatic scan",
        "volume_unit": "URL",
    },
    {
        "source": "gsc_cwv",
        "label": "GSC Core Web Vitals + AMP",
        "targets": ["doviz.com", "sinemalar.com"],
        "cadence": "Günde 2",
        "hours_tr": "03:00 · 15:00",
        "method": "Automatic scan",
        "volume_unit": "URL+KPI",
    },
]

# collector_runs provider → scrape source
_COLLECTOR_PROVIDER_MAP = {
    "pagespeed_web_scrape": "pagespeed_web",
    "gsc_cwv": "gsc_cwv",
    "seo_audit": "seo_audit",
}

_SOURCE_LABELS = {c["source"]: c["label"] for c in SCRAPE_CATALOG}


def _now_utc() -> datetime:
    return datetime.now(_UTC).replace(tzinfo=None)


def _to_tr(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_UTC)
    return dt.astimezone(_TR)


def _parse_iso(raw: str | None) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(_UTC).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def record_scrape_ingest(
    db: Session,
    *,
    source: str,
    target: str = "",
    status: str = "success",
    row_count: int = 0,
    message: str = "",
    detail: dict | None = None,
    site_id: int | None = None,
    scraped_at: datetime | str | None = None,
    commit: bool = True,
) -> ScrapeIngestLog | None:
    """Ingest sonrası append-only log. Hata yutulur — ingest'i bozmaz."""
    try:
        scraped_dt: datetime | None
        if isinstance(scraped_at, datetime):
            scraped_dt = scraped_at
            if scraped_dt.tzinfo is not None:
                scraped_dt = scraped_dt.astimezone(_UTC).replace(tzinfo=None)
        else:
            scraped_dt = _parse_iso(str(scraped_at) if scraped_at else None)
        st = (status or "success").strip().lower()
        if st not in ("success", "error", "partial"):
            st = "success" if st in ("ok", "true", "1") else "error"
        row = ScrapeIngestLog(
            source=(source or "unknown")[:64],
            target=(target or "")[:128],
            status=st[:20],
            row_count=max(0, int(row_count or 0)),
            message=(message or "")[:2000],
            detail_json=json.dumps(detail or {}, ensure_ascii=False, default=str)[:8000],
            site_id=site_id,
            scraped_at=scraped_dt,
            received_at=_now_utc(),
        )
        db.add(row)
        if commit:
            db.commit()
            db.refresh(row)
        else:
            db.flush()
        return row
    except Exception:
        LOGGER.debug("scrape ingest log failed source=%s", source, exc_info=True)
        try:
            db.rollback()
        except Exception:
            pass
        return None


def _workspace_last_syncs(db: Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    def _push(source: str, target: str, ok: bool | None, msg: str, updated: datetime | None, extra: dict | None = None):
        tr = _to_tr(updated)
        out.append(
            {
                "source": source,
                "label": _SOURCE_LABELS.get(source, source),
                "target": target,
                "ok": ok,
                "message": (msg or "")[:240],
                "updated_at": updated.isoformat(sep=" ", timespec="seconds") if updated else "",
                "updated_at_tr": tr.strftime("%d.%m.%Y %H:%M") if tr else "—",
                "extra": extra or {},
            }
        )

    try:
        play = db.query(PlayConsoleWorkspace).order_by(PlayConsoleWorkspace.id.asc()).first()
        if play:
            _push(
                "play_console",
                play.package_name or "com.Doviz",
                bool(play.sync_ok) if play.sync_ok is not None else None,
                play.sync_message or "",
                play.updated_at or play.background_synced_at,
                {"mode": play.sync_mode or ""},
            )
    except Exception:
        LOGGER.debug("play workspace read failed", exc_info=True)

    try:
        asc = db.query(AscConsoleWorkspace).order_by(AscConsoleWorkspace.id.asc()).first()
        if asc:
            _push(
                "asc_console",
                asc.bundle_id or "ASC",
                bool(asc.sync_ok) if asc.sync_ok is not None else None,
                asc.sync_message or "",
                asc.updated_at or asc.background_synced_at,
                {"mode": asc.sync_mode or ""},
            )
    except Exception:
        LOGGER.debug("asc workspace read failed", exc_info=True)

    try:
        fb = db.query(FirebaseConsoleWorkspace).order_by(FirebaseConsoleWorkspace.id.asc()).first()
        if fb:
            _push(
                "firebase_console",
                "doviz-android + doviz-ios",
                bool(fb.sync_ok) if fb.sync_ok is not None else None,
                fb.sync_message or "",
                fb.updated_at or fb.background_synced_at,
                {"mode": fb.sync_mode or "", "days": fb.scrape_days},
            )
    except Exception:
        LOGGER.debug("firebase workspace read failed", exc_info=True)

    try:
        nt = db.query(NotificationAnalyticsWorkspace).order_by(NotificationAnalyticsWorkspace.id.asc()).first()
        if nt:
            n_rows = 0
            try:
                n_rows = len(json.loads(nt.rows_json or "[]") or [])
            except Exception:
                n_rows = 0
            _push(
                "notification_analytics",
                "doviz",
                True,
                f"{n_rows} satır · {nt.source or '—'}",
                nt.updated_at,
            )
    except Exception:
        LOGGER.debug("notification workspace read failed", exc_info=True)

    try:
        news = db.query(DovizNewsWorkspace).order_by(DovizNewsWorkspace.id.asc()).first()
        if news:
            _push(
                "doviz_news",
                "doviz",
                bool(news.sync_ok) if news.sync_ok is not None else None,
                news.sync_message or f"{news.row_count} haber",
                news.background_synced_at or news.updated_at,
            )
    except Exception:
        LOGGER.debug("news workspace read failed", exc_info=True)

    try:
        from backend.models import SinemalarNoAdsSnapshot

        noads = db.query(SinemalarNoAdsSnapshot).filter(SinemalarNoAdsSnapshot.id == 1).first()
        if noads:
            _push(
                "sinemalar_noads",
                "sinemalar",
                True,
                noads.message or "",
                noads.scraped_at,
                {"entry_count": noads.entry_count},
            )
    except Exception:
        LOGGER.debug("noads snapshot read failed", exc_info=True)

    return out


def _events_from_scrape_logs(db: Session, since: datetime) -> list[dict[str, Any]]:
    rows = (
        db.query(ScrapeIngestLog)
        .filter(ScrapeIngestLog.received_at >= since)
        .order_by(desc(ScrapeIngestLog.received_at))
        .limit(5000)
        .all()
    )
    events: list[dict[str, Any]] = []
    for r in rows:
        events.append(
            {
                "source": r.source,
                "target": r.target or "",
                "status": r.status,
                "row_count": int(r.row_count or 0),
                "message": r.message or "",
                "at": r.received_at,
                "origin": "ingest_log",
            }
        )
    return events


def _events_from_collector_runs(db: Session, since: datetime) -> list[dict[str, Any]]:
    providers = list(_COLLECTOR_PROVIDER_MAP.keys())
    rows = (
        db.query(CollectorRun, Site.domain)
        .outerjoin(Site, Site.id == CollectorRun.site_id)
        .filter(
            CollectorRun.provider.in_(providers),
            CollectorRun.requested_at >= since,
        )
        .order_by(desc(CollectorRun.requested_at))
        .limit(5000)
        .all()
    )
    events: list[dict[str, Any]] = []
    for run, domain in rows:
        src = _COLLECTOR_PROVIDER_MAP.get(run.provider, run.provider)
        st = (run.status or "").lower()
        if st == "started":
            continue
        status = "success" if st == "success" else "error"
        events.append(
            {
                "source": src,
                "target": (domain or "") or (run.target_url or "")[:80],
                "status": status,
                "row_count": int(run.row_count or 0),
                "message": (run.error_message or "")[:240],
                "at": run.finished_at or run.requested_at,
                "origin": "collector_run",
            }
        )
    return events


def _events_from_backlink_imports(db: Session, since: datetime) -> list[dict[str, Any]]:
    try:
        rows = (
            db.query(BacklinkImport, Site.domain)
            .outerjoin(Site, Site.id == BacklinkImport.site_id)
            .filter(BacklinkImport.created_at >= since)
            .order_by(desc(BacklinkImport.created_at))
            .limit(2000)
            .all()
        )
    except Exception:
        return []
    events: list[dict[str, Any]] = []
    for imp, domain in rows:
        events.append(
            {
                "source": "gsc_links",
                "target": f"{domain or ''} · {imp.report_type or ''}".strip(" ·"),
                "status": "success",
                "row_count": int(imp.row_count or 0),
                "message": "",
                "at": imp.created_at,
                "origin": "backlink_import",
            }
        )
    return events


def _hour_bucket_tr(dt: datetime) -> str:
    tr = _to_tr(dt)
    if tr is None:
        return ""
    return tr.strftime("%Y-%m-%d %H:00")


def build_hourly_rollup(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """hour × source × target → success/error counts + volume."""
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for ev in events:
        at = ev.get("at")
        if not isinstance(at, datetime):
            continue
        hour = _hour_bucket_tr(at)
        if not hour:
            continue
        key = (hour, ev.get("source") or "?", (ev.get("target") or "—")[:64])
        cell = buckets.get(key)
        if cell is None:
            cell = {
                "hour": hour,
                "source": key[1],
                "label": _SOURCE_LABELS.get(key[1], key[1]),
                "target": key[2],
                "success": 0,
                "error": 0,
                "partial": 0,
                "volume": 0,
                "last_message": "",
            }
            buckets[key] = cell
        st = (ev.get("status") or "").lower()
        if st == "success":
            cell["success"] += 1
        elif st == "partial":
            cell["partial"] += 1
        else:
            cell["error"] += 1
            if ev.get("message"):
                cell["last_message"] = ev["message"][:160]
        cell["volume"] += int(ev.get("row_count") or 0)
    rows = list(buckets.values())
    rows.sort(key=lambda r: (r["hour"], r["source"], r["target"]), reverse=True)
    return rows


def build_recent_events(events: list[dict[str, Any]], limit: int = 80) -> list[dict[str, Any]]:
    sorted_ev = sorted(
        (e for e in events if isinstance(e.get("at"), datetime)),
        key=lambda e: e["at"],
        reverse=True,
    )[:limit]
    out: list[dict[str, Any]] = []
    for ev in sorted_ev:
        tr = _to_tr(ev["at"])
        out.append(
            {
                "source": ev.get("source"),
                "label": _SOURCE_LABELS.get(ev.get("source") or "", ev.get("source") or ""),
                "target": ev.get("target") or "—",
                "status": ev.get("status"),
                "row_count": int(ev.get("row_count") or 0),
                "message": (ev.get("message") or "")[:200],
                "at_tr": tr.strftime("%d.%m %H:%M:%S") if tr else "—",
                "origin": ev.get("origin") or "",
            }
        )
    return out


def build_source_summary(events: list[dict[str, Any]], hours: int) -> list[dict[str, Any]]:
    by_src: dict[str, dict[str, Any]] = {}
    for cat in SCRAPE_CATALOG:
        by_src[cat["source"]] = {
            "source": cat["source"],
            "label": cat["label"],
            "cadence": cat["cadence"],
            "hours_tr": cat["hours_tr"],
            "targets": ", ".join(cat["targets"]),
            "method": cat["method"],
            "volume_unit": cat["volume_unit"],
            "success": 0,
            "error": 0,
            "volume": 0,
            "last_at_tr": "—",
        }
    last_at: dict[str, datetime] = {}
    for ev in events:
        src = ev.get("source") or ""
        if src not in by_src:
            by_src[src] = {
                "source": src,
                "label": _SOURCE_LABELS.get(src, src),
                "cadence": "—",
                "hours_tr": "—",
                "targets": "",
                "method": "",
                "volume_unit": "satır",
                "success": 0,
                "error": 0,
                "volume": 0,
                "last_at_tr": "—",
            }
        st = (ev.get("status") or "").lower()
        if st == "success":
            by_src[src]["success"] += 1
        else:
            by_src[src]["error"] += 1
        by_src[src]["volume"] += int(ev.get("row_count") or 0)
        at = ev.get("at")
        if isinstance(at, datetime):
            prev = last_at.get(src)
            if prev is None or at > prev:
                last_at[src] = at
    for src, at in last_at.items():
        tr = _to_tr(at)
        if tr and src in by_src:
            by_src[src]["last_at_tr"] = tr.strftime("%d.%m %H:%M")
    # Katalog sırası önce
    ordered = []
    seen = set()
    for cat in SCRAPE_CATALOG:
        ordered.append(by_src[cat["source"]])
        seen.add(cat["source"])
    for src, row in by_src.items():
        if src not in seen:
            ordered.append(row)
    for row in ordered:
        row["window_hours"] = hours
    return ordered


def build_scrape_settings_context(db: Session, *, hours: int = 48) -> dict[str, Any]:
    hours = max(6, min(168, int(hours or 48)))
    since = _now_utc() - timedelta(hours=hours)
    events: list[dict[str, Any]] = []
    try:
        events.extend(_events_from_scrape_logs(db, since))
    except Exception:
        LOGGER.debug("scrape_logs query failed", exc_info=True)
    try:
        events.extend(_events_from_collector_runs(db, since))
    except Exception:
        LOGGER.debug("collector_runs scrape query failed", exc_info=True)
    try:
        events.extend(_events_from_backlink_imports(db, since))
    except Exception:
        LOGGER.debug("backlink imports query failed", exc_info=True)

    # Dedup: aynı source+target+minute+status+volume (collector_run + ingest_log çift yazım)
    deduped: list[dict[str, Any]] = []
    seen_keys: set[tuple] = set()
    for ev in events:
        at = ev.get("at")
        if not isinstance(at, datetime):
            continue
        key = (
            ev.get("source"),
            (ev.get("target") or "")[:40],
            at.replace(second=0, microsecond=0),
            ev.get("status"),
            int(ev.get("row_count") or 0),
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(ev)

    now_tr = datetime.now(_TR)
    return {
        "hours": hours,
        "generated_at_tr": now_tr.strftime("%d.%m.%Y %H:%M:%S"),
        "catalog": SCRAPE_CATALOG,
        "sources": build_source_summary(deduped, hours),
        "hourly": build_hourly_rollup(deduped),
        "recent": build_recent_events(deduped, limit=100),
        "workspaces": _workspace_last_syncs(db),
        "event_count": len(deduped),
        "note": (
            "Times are Europe/Istanbul. Volume = rows/URLs/snapshots ingested per source. "
            "Last scan column shows the most recent successful run."
        ),
    }
