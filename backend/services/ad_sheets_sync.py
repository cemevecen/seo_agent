"""Reklam Google Sheets → AdReportRow senkronu (sheet tek kaynak)."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.models import AdReportCatalog, AdReportRow
from backend.services.ad_analytics_store import (
    _STREAM_BY_KEY,
    clear_stream_rows,
    import_upload_file,
    invalidate_facets_cache,
    stream_non_sheet_source_count,
)
from backend.services.ad_sheets_config import AD_SHEET_SOURCES, sheet_catalog_filename
from backend.services.backlink_csv import fetch_public_sheet_csv

LOGGER = logging.getLogger(__name__)

_SHEET_SYNC_TTL_SEC = 180.0
_FETCH_TIMEOUT_SEC = 180
_INCREMENTAL_OVERLAP_DAYS = 21
_last_sheet_sync_mono: float = 0.0
_last_sheet_sync_at: datetime | None = None
_last_sheet_sync_result: dict[str, Any] | None = None


def _short_sync_error(exc: BaseException, *, limit: int = 220) -> str:
    msg = (str(exc) or "Import başarısız").strip()
    if "[SQL:" in msg:
        msg = msg.split("[SQL:", 1)[0].strip()
    low = msg.lower()
    if "cardinalityviolation" in low or "affect row a second time" in low:
        return (
            "Sheet’te aynı gün + reklam birimi + gelir tipi tekrarları vardı; "
            "tekilleştirme sonrası yeniden dene."
        )
    if len(msg) > limit:
        return msg[: limit - 1] + "…"
    return msg


def _stream_date_bounds(db: Session, stream_key: str) -> dict[str, str | None | int]:
    stream = _STREAM_BY_KEY.get(stream_key)
    if stream is None:
        return {"min_date": None, "max_date": None, "rows": 0}
    row = (
        db.query(
            func.min(AdReportRow.report_date),
            func.max(AdReportRow.report_date),
            func.count(AdReportRow.id),
        )
        .filter(
            AdReportRow.project == stream.project,
            AdReportRow.branch == stream.branch,
        )
        .one()
    )
    min_d, max_d, n = row[0], row[1], int(row[2] or 0)
    return {
        "min_date": min_d.isoformat() if min_d else None,
        "max_date": max_d.isoformat() if max_d else None,
        "rows": n,
    }


def sheets_sync_status(db: Session) -> dict[str, Any]:
    streams = []
    for src in AD_SHEET_SOURCES:
        bounds = _stream_date_bounds(db, src.stream_key)
        catalog = sheet_catalog_filename(src.stream_key)
        polluted = stream_non_sheet_source_count(db, src.stream_key, catalog)
        streams.append(
            {
                "stream_key": src.stream_key,
                "label": src.label,
                "sheet_url": src.sheet_url,
                "polluted_rows": polluted,
                **bounds,
            }
        )
    return {
        "ok": True,
        "sources": len(AD_SHEET_SOURCES),
        "last_sync_at": _last_sheet_sync_at.isoformat() + "Z" if _last_sheet_sync_at else None,
        "ttl_seconds": _SHEET_SYNC_TTL_SEC,
        "streams": streams,
        "last_result": _last_sheet_sync_result,
    }


def sync_one_sheet(
    db: Session,
    *,
    stream_key: str,
    commit: bool = True,
    full: bool = False,
) -> dict[str, Any]:
    src = next((s for s in AD_SHEET_SOURCES if s.stream_key == stream_key), None)
    if src is None:
        return {"ok": False, "stream_key": stream_key, "error": "Bilinmeyen stream_key"}
    if stream_key not in _STREAM_BY_KEY:
        return {"ok": False, "stream_key": stream_key, "error": "Stream tanımsız"}

    t0 = time.monotonic()
    before = _stream_date_bounds(db, stream_key)
    catalog = sheet_catalog_filename(stream_key)
    prior_sheet = db.execute(
        select(AdReportCatalog).where(AdReportCatalog.source_file == catalog)
    ).scalars().first()
    polluted = stream_non_sheet_source_count(db, stream_key, catalog)

    min_import: date | None = None
    replace_stream = bool(full) or polluted > 0 or prior_sheet is None
    mode = "full_replace" if replace_stream else "incremental"

    if not replace_stream and before.get("max_date"):
        try:
            max_d = date.fromisoformat(str(before["max_date"])[:10])
            min_import = max_d - timedelta(days=_INCREMENTAL_OVERLAP_DAYS)
            mode = "incremental"
        except ValueError:
            replace_stream = True
            mode = "full_replace"
            min_import = None

    try:
        csv_text = fetch_public_sheet_csv(src.sheet_url, timeout=_FETCH_TIMEOUT_SEC)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Ad sheet fetch failed %s: %s", stream_key, exc)
        return {
            "ok": False,
            "stream_key": stream_key,
            "label": src.label,
            "error": _short_sync_error(exc),
            "mode": mode,
            "elapsed_s": round(time.monotonic() - t0, 1),
        }

    cleared: dict[str, Any] | None = None
    if replace_stream:
        cleared = clear_stream_rows(db, stream_key, commit=False)
        mode = "full_replace"
        min_import = None
        LOGGER.info(
            "Ad sheet replace %s: cleared %s rows (polluted=%s)",
            stream_key,
            cleared.get("deleted_rows"),
            polluted,
        )

    raw = csv_text.encode("utf-8")
    try:
        result = import_upload_file(
            db,
            raw,
            filename=catalog,
            commit=commit,
            stream_key=stream_key,
            min_date=min_import,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Ad sheet import failed %s", stream_key)
        db.rollback()
        return {
            "ok": False,
            "stream_key": stream_key,
            "label": src.label,
            "error": _short_sync_error(exc),
            "mode": mode,
            "elapsed_s": round(time.monotonic() - t0, 1),
        }

    bounds = _stream_date_bounds(db, stream_key)
    parsed = int(result.get("parsed") or 0)
    return {
        "ok": parsed > 0 or not result.get("parse_error"),
        "stream_key": stream_key,
        "label": src.label,
        "mode": mode,
        "replaced": bool(cleared),
        "cleared_rows": int((cleared or {}).get("deleted_rows") or 0),
        "polluted_rows_before": polluted,
        "import_from": min_import.isoformat() if min_import else None,
        "parsed": parsed,
        "inserted": int(result.get("inserted") or 0),
        "updated": int(result.get("updated") or 0),
        "skipped": int(result.get("skipped") or 0),
        "warning": result.get("warning") or result.get("parse_error") or "",
        "max_date": bounds.get("max_date"),
        "min_date": bounds.get("min_date"),
        "rows_in_db": bounds.get("rows"),
        "elapsed_s": round(time.monotonic() - t0, 1),
        "bytes": len(raw),
    }


def sync_from_google_sheets(
    db: Session,
    *,
    force: bool = False,
    stream_key: str | None = None,
    full: bool = False,
) -> dict[str, Any]:
    """Tüm (veya tek) reklam sheet'lerini çekip DB'ye yazar."""
    global _last_sheet_sync_mono, _last_sheet_sync_at, _last_sheet_sync_result

    now = time.monotonic()
    if (
        not force
        and not full
        and stream_key is None
        and _last_sheet_sync_mono > 0
        and (now - _last_sheet_sync_mono) < _SHEET_SYNC_TTL_SEC
    ):
        return {
            "ok": True,
            "synced": False,
            "skipped": True,
            "message": "Son senkron taze; force=1 ile zorla yenile.",
            "status": sheets_sync_status(db),
            "last_result": _last_sheet_sync_result,
        }

    sources = AD_SHEET_SOURCES
    if stream_key:
        sources = tuple(s for s in AD_SHEET_SOURCES if s.stream_key == stream_key)
        if not sources:
            return {"ok": False, "synced": False, "message": f"Bilinmeyen dal: {stream_key}"}

    t0 = time.monotonic()
    per_stream: list[dict[str, Any]] = []
    ok_count = 0
    fail_count = 0
    total_parsed = 0

    for src in sources:
        item = sync_one_sheet(db, stream_key=src.stream_key, commit=True, full=full)
        per_stream.append(item)
        if item.get("ok"):
            ok_count += 1
            total_parsed += int(item.get("parsed") or 0)
        else:
            fail_count += 1

    invalidate_facets_cache()
    _last_sheet_sync_mono = time.monotonic()
    _last_sheet_sync_at = datetime.utcnow()
    message = (
        f"{ok_count}/{len(sources)} sheet senkron · {total_parsed:,} satır"
        + (f" · {fail_count} hata" if fail_count else "")
    )
    result = {
        "ok": fail_count == 0 and ok_count > 0,
        "synced": True,
        "skipped": False,
        "full": full,
        "message": message,
        "ok_count": ok_count,
        "fail_count": fail_count,
        "total_parsed": total_parsed,
        "elapsed_s": round(time.monotonic() - t0, 1),
        "streams": per_stream,
        "status": sheets_sync_status(db),
    }
    _last_sheet_sync_result = {
        "message": message,
        "ok_count": ok_count,
        "fail_count": fail_count,
        "total_parsed": total_parsed,
        "at": _last_sheet_sync_at.isoformat() + "Z",
    }
    return result
