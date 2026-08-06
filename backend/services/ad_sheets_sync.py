"""Reklam Google Sheets → AdReportRow senkronu (sheet tek kaynak)."""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Any, Callable

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
# Döviz Web ~50MB; proxy 502’yi önlemek için fetch uzun, sync arka planda.
_FETCH_TIMEOUT_SEC = 300
_last_sheet_sync_mono: float = 0.0
_last_sheet_sync_at: datetime | None = None
_last_sheet_sync_result: dict[str, Any] | None = None

_job_lock = threading.Lock()
_job: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "detail": "",
    "stream_key": None,
    "stream_label": None,
    "index": 0,
    "total": 0,
    "full": False,
    "ok_count": 0,
    "fail_count": 0,
    "total_parsed": 0,
    "rows_done": 0,
    "rows_total": 0,
    "rows_kept": 0,
    "rows_label": "",
    "streams": [],
    "error": None,
    "message": "",
    "started_at": None,
    "finished_at": None,
    "elapsed_s": None,
}


def _iso_now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _fmt_row_count(n: int | float | None) -> str:
    """Compact: 111234 → 111k."""
    v = max(0, int(n or 0))
    if v >= 1000:
        return f"{v // 1000}k"
    return str(v)


def get_sync_job() -> dict[str, Any]:
    with _job_lock:
        out = dict(_job)
        out["streams"] = list(_job.get("streams") or [])
    return out


def _set_job(**kwargs: Any) -> None:
    with _job_lock:
        _job.update(kwargs)


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
        resume_from = bounds.get("max_date")
        streams.append(
            {
                "stream_key": src.stream_key,
                "label": src.label,
                "sheet_url": src.sheet_url,
                "polluted_rows": polluted,
                "resume_from": resume_from,
                "next_mode": (
                    "full_replace"
                    if polluted > 0 or not resume_from
                    else "incremental_from_last"
                ),
                **bounds,
            }
        )
    # En eski / en yeni “son veri” — UI hint için
    resumes = [s["resume_from"] for s in streams if s.get("resume_from")]
    return {
        "ok": True,
        "sources": len(AD_SHEET_SOURCES),
        "last_sync_at": _last_sheet_sync_at.isoformat() + "Z" if _last_sheet_sync_at else None,
        "ttl_seconds": _SHEET_SYNC_TTL_SEC,
        "resume_from_min": min(resumes) if resumes else None,
        "resume_from_max": max(resumes) if resumes else None,
        "streams": streams,
        "last_result": _last_sheet_sync_result,
        "job": get_sync_job(),
    }


def sync_one_sheet(
    db: Session,
    *,
    stream_key: str,
    commit: bool = True,
    full: bool = False,
    on_phase: Callable[..., None] | None = None,
) -> dict[str, Any]:
    src = next((s for s in AD_SHEET_SOURCES if s.stream_key == stream_key), None)
    if src is None:
        return {"ok": False, "stream_key": stream_key, "error": "Bilinmeyen stream_key"}
    if stream_key not in _STREAM_BY_KEY:
        return {"ok": False, "stream_key": stream_key, "error": "Stream tanımsız"}

    def _phase(phase: str, detail: str = "", **extra: Any) -> None:
        if on_phase:
            on_phase(phase, detail, **extra)

    t0 = time.monotonic()
    before = _stream_date_bounds(db, stream_key)
    catalog = sheet_catalog_filename(stream_key)
    prior_sheet = db.execute(
        select(AdReportCatalog).where(AdReportCatalog.source_file == catalog)
    ).scalars().first()
    polluted = stream_non_sheet_source_count(db, stream_key, catalog)

    # İlk sync / kirli dal / tam değiştir → full. Aksi halde DB’deki son veri gününden
    # (o gün dahil) ileri satırları upsert et — sheet’e eklenen yeni günler gelir.
    min_import: date | None = None
    last_data_date: date | None = None
    if before.get("max_date"):
        try:
            last_data_date = date.fromisoformat(str(before["max_date"])[:10])
        except ValueError:
            last_data_date = None

    replace_stream = bool(full) or polluted > 0 or prior_sheet is None or last_data_date is None
    mode = "full_replace" if replace_stream else "incremental"

    if not replace_stream and last_data_date is not None:
        min_import = last_data_date  # dahil: 05.08 varsa yeniden yaz + 06.08+ ekle
        mode = "incremental"

    _phase("fetch", f"{src.label}: Google’dan CSV çekiliyor…")
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
            "resume_from": last_data_date.isoformat() if last_data_date else None,
            "elapsed_s": round(time.monotonic() - t0, 1),
        }

    nbytes = len(csv_text.encode("utf-8"))
    if min_import is not None:
        _phase(
            "fetch_done",
            f"{src.label}: {nbytes / (1024 * 1024):.1f} MB · "
            f"{min_import.isoformat()} ve sonrası yazılıyor…",
        )
    else:
        _phase(
            "fetch_done",
            f"{src.label}: {nbytes / (1024 * 1024):.1f} MB alındı · yazılıyor…",
        )

    cleared: dict[str, Any] | None = None
    if replace_stream:
        _phase("clear", f"{src.label}: eski satırlar temizleniyor…")
        cleared = clear_stream_rows(db, stream_key, commit=False)
        mode = "full_replace"
        min_import = None
        LOGGER.info(
            "Ad sheet replace %s: cleared %s rows (polluted=%s)",
            stream_key,
            cleared.get("deleted_rows"),
            polluted,
        )
    else:
        LOGGER.info(
            "Ad sheet incremental %s: from %s (inclusive)",
            stream_key,
            min_import.isoformat() if min_import else "?",
        )

    _phase(
        "import",
        (
            f"{src.label}: {min_import.isoformat()} ve sonrası upsert…"
            if min_import
            else f"{src.label}: satırlar DB’ye yazılıyor (büyük sheet birkaç dk sürebilir)…"
        ),
    )
    raw = csv_text.encode("utf-8")

    def _import_progress(ev: dict[str, Any]) -> None:
        scanned = int(ev.get("scanned") or ev.get("parsed") or 0)
        est = int(ev.get("row_estimate") or 0)
        kept = int(ev.get("kept") or ev.get("parsed") or 0)
        if est <= 0 and scanned <= 0:
            return
        label = (
            f"{_fmt_row_count(scanned)}/{_fmt_row_count(est)}"
            if est > 0
            else _fmt_row_count(scanned)
        )
        kept_bit = f" · yazılan {_fmt_row_count(kept)}" if kept else ""
        _phase(
            "import",
            f"{src.label}: {label} taranıyor{kept_bit}",
            rows_done=scanned,
            rows_total=est,
            rows_kept=kept,
            rows_label=label,
        )

    try:
        result = import_upload_file(
            db,
            raw,
            filename=catalog,
            commit=commit,
            stream_key=stream_key,
            min_date=min_import,
            progress_cb=_import_progress,
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
            "resume_from": last_data_date.isoformat() if last_data_date else None,
            "elapsed_s": round(time.monotonic() - t0, 1),
        }

    bounds = _stream_date_bounds(db, stream_key)
    parsed = int(result.get("parsed") or 0)
    return {
        "ok": parsed > 0 or not result.get("parse_error") or mode == "incremental",
        "stream_key": stream_key,
        "label": src.label,
        "mode": mode,
        "replaced": bool(cleared),
        "cleared_rows": int((cleared or {}).get("deleted_rows") or 0),
        "polluted_rows_before": polluted,
        "resume_from": last_data_date.isoformat() if last_data_date and not cleared else None,
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
        "bytes": nbytes,
    }


def sync_from_google_sheets(
    db: Session,
    *,
    force: bool = False,
    stream_key: str | None = None,
    full: bool = False,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
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

    def _emit(extra: dict[str, Any] | None = None) -> None:
        if not on_progress:
            return
        payload = {
            "index": len(per_stream),
            "total": len(sources),
            "ok_count": ok_count,
            "fail_count": fail_count,
            "total_parsed": total_parsed,
            "streams": list(per_stream),
        }
        if extra:
            payload.update(extra)
        on_progress(payload)

    for i, src in enumerate(sources):
        def _on_phase(phase: str, detail: str, *, _i: int = i, _src=src, **extra: Any) -> None:
            payload = {
                "phase": phase,
                "detail": detail,
                "stream_key": _src.stream_key,
                "stream_label": _src.label,
                "index": _i,
                "total": len(sources),
            }
            payload.update(extra)
            _emit(payload)

        _emit(
            {
                "phase": "start_stream",
                "detail": f"{src.label} başlıyor…",
                "stream_key": src.stream_key,
                "stream_label": src.label,
                "index": i,
                "total": len(sources),
                "rows_done": 0,
                "rows_total": 0,
                "rows_kept": 0,
                "rows_label": "",
            }
        )
        item = sync_one_sheet(
            db,
            stream_key=src.stream_key,
            commit=True,
            full=full,
            on_phase=_on_phase,
        )
        per_stream.append(item)
        if item.get("ok"):
            ok_count += 1
            total_parsed += int(item.get("parsed") or 0)
        else:
            fail_count += 1
        _emit(
            {
                "phase": "stream_done",
                "detail": (
                    f"{src.label}: tamam · {int(item.get('parsed') or 0):,} satır"
                    if item.get("ok")
                    else f"{src.label}: hata · {item.get('error') or 'başarısız'}"
                ),
                "stream_key": src.stream_key,
                "stream_label": src.label,
                "index": i + 1,
                "total": len(sources),
                "last_item": item,
            }
        )

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


def start_sync_job(
    *,
    force: bool = True,
    stream_key: str | None = None,
    full: bool = False,
) -> dict[str, Any]:
    """HTTP timeout (502) önlemek için sync’i arka planda çalıştır."""
    with _job_lock:
        if _job.get("running"):
            return {
                "accepted": False,
                "background": True,
                "reason": "already_running",
                "message": "Senkron zaten çalışıyor; bitmesini bekleyin.",
                "job": dict(_job),
            }

        sources = AD_SHEET_SOURCES
        if stream_key:
            sources = tuple(s for s in AD_SHEET_SOURCES if s.stream_key == stream_key)
            if not sources:
                return {
                    "accepted": False,
                    "background": False,
                    "ok": False,
                    "message": f"Bilinmeyen dal: {stream_key}",
                }

        _job.update(
            {
                "running": True,
                "phase": "queued",
                "detail": "Arka planda başlıyor…",
                "stream_key": stream_key,
                "stream_label": sources[0].label if len(sources) == 1 else None,
                "index": 0,
                "total": len(sources),
                "full": bool(full),
                "ok_count": 0,
                "fail_count": 0,
                "total_parsed": 0,
                "rows_done": 0,
                "rows_total": 0,
                "rows_kept": 0,
                "rows_label": "",
                "streams": [],
                "error": None,
                "message": f"{len(sources)} sheet kuyruğa alındı",
                "started_at": _iso_now(),
                "finished_at": None,
                "elapsed_s": None,
            }
        )

    def _worker() -> None:
        from backend.database import SessionLocal

        t0 = time.monotonic()

        def on_progress(payload: dict[str, Any]) -> None:
            update = {
                "phase": payload.get("phase") or "running",
                "detail": payload.get("detail") or "",
                "stream_key": payload.get("stream_key"),
                "stream_label": payload.get("stream_label"),
                "index": int(payload.get("index") or 0),
                "total": int(payload.get("total") or 0),
                "ok_count": int(payload.get("ok_count") or 0),
                "fail_count": int(payload.get("fail_count") or 0),
                "total_parsed": int(payload.get("total_parsed") or 0),
                "streams": list(payload.get("streams") or []),
                "message": payload.get("detail") or _job.get("message") or "",
                "elapsed_s": round(time.monotonic() - t0, 1),
            }
            if "rows_done" in payload:
                update["rows_done"] = int(payload.get("rows_done") or 0)
            if "rows_total" in payload:
                update["rows_total"] = int(payload.get("rows_total") or 0)
            if "rows_kept" in payload:
                update["rows_kept"] = int(payload.get("rows_kept") or 0)
            if "rows_label" in payload:
                update["rows_label"] = str(payload.get("rows_label") or "")
            _set_job(**update)

        try:
            with SessionLocal() as db:
                result = sync_from_google_sheets(
                    db,
                    force=force,
                    stream_key=stream_key,
                    full=full,
                    on_progress=on_progress,
                )
            _set_job(
                running=False,
                phase="done",
                detail=result.get("message") or "Tamamlandı",
                message=result.get("message") or "Tamamlandı",
                ok_count=int(result.get("ok_count") or 0),
                fail_count=int(result.get("fail_count") or 0),
                total_parsed=int(result.get("total_parsed") or 0),
                streams=list(result.get("streams") or []),
                index=int(result.get("ok_count") or 0) + int(result.get("fail_count") or 0),
                finished_at=_iso_now(),
                elapsed_s=round(time.monotonic() - t0, 1),
                error=None,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Ad sheets background sync failed")
            _set_job(
                running=False,
                phase="error",
                detail=_short_sync_error(exc),
                message=_short_sync_error(exc),
                error=_short_sync_error(exc),
                finished_at=_iso_now(),
                elapsed_s=round(time.monotonic() - t0, 1),
            )

    threading.Thread(target=_worker, daemon=True, name="ad-sheets-sync").start()
    return {
        "accepted": True,
        "background": True,
        "ok": True,
        "message": "Senkron arka planda başladı (Döviz Web birkaç dakika sürebilir).",
        "job": get_sync_job(),
    }
