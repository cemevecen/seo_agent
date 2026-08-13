"""Firebase Console Crashlytics scrape snapshot — tek paylaşımlı workspace (id=1)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from backend.models import FirebaseConsoleWorkspace

LOGGER = logging.getLogger(__name__)

_WORKSPACE_ID = 1


def _get_or_create(db: Session) -> FirebaseConsoleWorkspace:
    row = db.get(FirebaseConsoleWorkspace, _WORKSPACE_ID)
    if row is None:
        row = FirebaseConsoleWorkspace(id=_WORKSPACE_ID)
        db.add(row)
        db.flush()
    return row


def _pack_blob(metrics: list[dict[str, Any]] | None, panels: dict[str, Any] | None) -> str:
    return json.dumps(
        {
            "version": 1,
            "items": metrics if isinstance(metrics, list) else [],
            "panels": panels if isinstance(panels, dict) else {},
        },
        ensure_ascii=False,
    )


def _unpack_blob(raw: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        data = json.loads(raw or "") if raw else {}
    except Exception:
        return [], {}
    if isinstance(data, dict):
        items = data.get("items") if isinstance(data.get("items"), list) else []
        panels = data.get("panels") if isinstance(data.get("panels"), dict) else {}
        return items, panels
    if isinstance(data, list):
        return data, {}
    return [], {}


def ingest_firebase_console_payload(db: Session, body: dict[str, Any]) -> dict[str, Any]:
    row = _get_or_create(db)
    metrics = body.get("metrics") if isinstance(body.get("metrics"), list) else []
    panels = body.get("panels") if isinstance(body.get("panels"), dict) else {}
    raw_network = body.get("raw_network") if isinstance(body.get("raw_network"), list) else []
    try:
        days = int(body.get("scrape_days") or 365)
    except (TypeError, ValueError):
        days = 365
    days = min(max(days, 1), 365)

    # Kısmi platform taraması (ios-only / android-only): diğer platformu silme
    incoming_panels = panels
    new_plats = (
        incoming_panels.get("platforms")
        if isinstance(incoming_panels.get("platforms"), dict)
        else {}
    )
    merge = bool(body.get("merge_platforms"))
    if not merge and isinstance(new_plats, dict) and 0 < len(new_plats) < 2:
        merge = True
    if merge and new_plats:
        old_metrics, old_panels = _unpack_blob(row.metrics_json or "")
        old_plats = (
            old_panels.get("platforms") if isinstance(old_panels.get("platforms"), dict) else {}
        )
        merged_plats = dict(old_plats)
        merged_plats.update(new_plats)
        panels = dict(old_panels)
        for k, v in incoming_panels.items():
            if k == "platforms":
                continue
            panels[k] = v
        panels["platforms"] = merged_plats
        scraped = {str(k).lower() for k in new_plats.keys()}
        kept = [
            m
            for m in old_metrics
            if isinstance(m, dict) and str(m.get("platform") or "").lower() not in scraped
        ]
        metrics = kept + [m for m in metrics if isinstance(m, dict)]

    row.metrics_json = _pack_blob(metrics, panels)
    row.raw_network_json = json.dumps(raw_network[:200], ensure_ascii=False)
    row.source = str(body.get("source") or "firebase_console_bridge")[:64]
    row.source_url = str(body.get("source_url") or "")[:512]
    row.sync_ok = bool(body.get("sync_ok", True))
    row.sync_message = str(body.get("sync_message") or "")[:512]
    row.sync_mode = str(body.get("sync_mode") or "crashlytics_scrape")[:64]
    row.scrape_days = days
    now = datetime.utcnow()
    row.updated_at = now
    if row.sync_ok:
        row.background_synced_at = now
    db.commit()
    return {
        "ok": True,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "metric_count": len(metrics),
        "scrape_days": days,
        "merged_platforms": bool(merge),
        "platforms": list(new_plats.keys()) if isinstance(new_plats, dict) else [],
    }


def firebase_console_payload(db: Session) -> dict[str, Any]:
    row = db.get(FirebaseConsoleWorkspace, _WORKSPACE_ID)
    if row is None:
        return {
            "ok": True,
            "empty": True,
            "metrics": [],
            "panels": {},
            "message": "Henüz Firebase tarama yok — Mac köprüde oturum açıp senkron çalıştırın",
        }
    metrics, panels = _unpack_blob(row.metrics_json or "")
    try:
        raw_network = json.loads(row.raw_network_json or "[]")
    except Exception:
        raw_network = []
    platforms = panels.get("platforms") if isinstance(panels.get("platforms"), dict) else {}
    return {
        "ok": bool(row.sync_ok),
        "empty": not metrics and not platforms,
        "metrics": metrics,
        "panels": panels,
        "platforms": platforms,
        "raw_network": raw_network if isinstance(raw_network, list) else [],
        "source": row.source,
        "source_url": row.source_url,
        "sync_ok": row.sync_ok,
        "sync_message": row.sync_message,
        "sync_mode": row.sync_mode,
        "scrape_days": int(row.scrape_days or 365),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "background_synced_at": (
            row.background_synced_at.isoformat() if row.background_synced_at else None
        ),
        "message": row.sync_message or None,
    }


def _parse_day(s: str | None) -> datetime | None:
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00")[:19])
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(t[:10], fmt)
        except ValueError:
            continue
    return None


def query_firebase_console(
    db: Session,
    *,
    platform: str = "all",
    days: int = 30,
    version: str | None = None,
    device: str | None = None,
) -> dict[str, Any]:
    """Zaman / sürüm / cihaz filtreli görünüm (scrape snapshot üzerinden)."""
    snap = firebase_console_payload(db)
    try:
        days_i = min(max(int(days or 30), 1), 365)
    except (TypeError, ValueError):
        days_i = 30
    plat = (platform or "all").strip().lower()
    ver = (version or "").strip()
    dev = (device or "").strip().lower()
    cutoff = datetime.utcnow() - timedelta(days=days_i)
    win_key = "24h" if days_i <= 1 else "7d" if days_i <= 7 else "30d" if days_i <= 30 else "90d"

    platforms = snap.get("platforms") if isinstance(snap.get("platforms"), dict) else {}
    selected = {}
    if plat in ("android", "ios"):
        if plat in platforms:
            selected[plat] = platforms[plat]
    else:
        selected = dict(platforms)

    def _keep_fact(f: dict[str, Any]) -> bool:
        if ver:
            fv = str(f.get("version") or f.get("app_version") or "").strip()
            if fv and fv != ver and not fv.startswith(ver):
                return False
        if dev:
            fd = str(f.get("device") or f.get("device_model") or "").lower()
            if fd and dev not in fd:
                return False
        dt = _parse_day(str(f.get("date") or f.get("day") or f.get("date_iso") or ""))
        if dt and dt < cutoff:
            return False
        return True

    out_platforms: dict[str, Any] = {}
    for key, block in selected.items():
        if not isinstance(block, dict):
            continue
        windows = block.get("windows") if isinstance(block.get("windows"), dict) else {}
        win = windows.get(win_key) if isinstance(windows.get(win_key), dict) else {}
        # Filtre penceresi varsa CF/issue/series oradan
        base_issues = win.get("issues") if win.get("issues") else (block.get("issues") or [])
        base_series = win.get("series") if win.get("series") else (block.get("series") or [])
        issues = [i for i in base_issues if isinstance(i, dict) and _keep_fact(i)]
        series = [s for s in base_series if isinstance(s, dict) and _keep_fact(s)]
        by_version = [
            r
            for r in (block.get("by_version") or [])
            if isinstance(r, dict)
            and (
                not ver
                or str(r.get("version") or "") == ver
                or str(r.get("version") or "").startswith(ver)
            )
        ]
        by_device = [
            r
            for r in (block.get("by_device") or [])
            if isinstance(r, dict)
            and (not dev or dev in str(r.get("device") or r.get("label") or "").lower())
        ]
        anr_issues = [
            i for i in (block.get("anr_issues") or []) if isinstance(i, dict) and _keep_fact(i)
        ]
        nonfatal_issues = [
            i for i in (block.get("nonfatal_issues") or []) if isinstance(i, dict) and _keep_fact(i)
        ]
        release = block.get("release_monitoring") if isinstance(block.get("release_monitoring"), dict) else {}
        if ver and release.get("version") and str(release.get("version")) != ver:
            release = {**release, "filter_mismatch": True}
        cf_pct = win.get("crash_free_pct") if win else block.get("crash_free_pct")
        cf_fmt = win.get("crash_free_fmt") if win else block.get("crash_free_fmt")
        sess_pct = win.get("crash_free_sessions_pct") if win else block.get("crash_free_sessions_pct")
        sess_fmt = win.get("crash_free_sessions_fmt") if win else block.get("crash_free_sessions_fmt")
        out_platforms[key] = {
            **{
                k: v
                for k, v in block.items()
                if k
                not in (
                    "issues",
                    "series",
                    "by_version",
                    "by_device",
                    "by_os",
                    "anr_issues",
                    "nonfatal_issues",
                    "crash_free_pct",
                    "crash_free_fmt",
                    "crash_free_sessions_pct",
                    "crash_free_sessions_fmt",
                )
            },
            "crash_free_pct": cf_pct,
            "crash_free_fmt": cf_fmt,
            "crash_free_sessions_pct": sess_pct,
            "crash_free_sessions_fmt": sess_fmt,
            "active_window": win_key,
            "window": win or None,
            "issues": issues[:200],
            "anr_issues": anr_issues[:80],
            "nonfatal_issues": nonfatal_issues[:80],
            "series": series[-days_i:] if series else [],
            "by_version": by_version[:50],
            "by_device": by_device[:50],
            "by_os": [
                r
                for r in (block.get("by_os") or [])
                if isinstance(r, dict)
            ][:50],
            "release_monitoring": release,
            "latest_24h": block.get("latest_24h") or windows.get("24h"),
            "latest_7d": block.get("latest_7d") or windows.get("7d"),
        }

    versions = sorted(
        {
            str(r.get("version") or "").strip()
            for block in out_platforms.values()
            for r in (block.get("by_version") or [])
            if isinstance(r, dict) and r.get("version")
        }
    )
    devices = sorted(
        {
            str(r.get("device") or r.get("label") or "").strip()
            for block in out_platforms.values()
            for r in (block.get("by_device") or [])
            if isinstance(r, dict) and (r.get("device") or r.get("label"))
        }
    )

    return {
        **snap,
        "filter": {
            "platform": plat,
            "days": days_i,
            "version": ver or None,
            "device": device or None,
        },
        "platforms": out_platforms,
        "filter_options": {"versions": versions, "devices": devices},
    }
