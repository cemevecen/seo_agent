"""Virgül Excel/CSV → AdReportRow (yalnız virgul_* katalog; sheet’e dokunmaz)."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from backend.models import AdReportCatalog, AdReportRow
from backend.services.ad_analytics_store import (
    _STREAM_BY_KEY,
    import_upload_file,
    invalidate_facets_cache,
)
from backend.services.virgul_ad_config import (
    is_virgul_source_file,
    source_by_stream,
)
from backend.services.virgul_ad_client import fetch_all_sites_exports

LOGGER = logging.getLogger(__name__)


def clear_virgul_stream_rows(db: Session, stream_key: str, *, commit: bool = True) -> dict[str, Any]:
    """Yalnızca virgul_* kaynaklı satırları siler (Google Sheet satırlarına dokunmaz)."""
    stream = _STREAM_BY_KEY.get((stream_key or "").strip())
    if stream is None:
        raise ValueError(f"Bilinmeyen dal: {stream_key}")
    source_files = [
        str(x)
        for x in db.execute(
            select(AdReportRow.source_file)
            .where(
                AdReportRow.project == stream.project,
                AdReportRow.branch == stream.branch,
            )
            .distinct()
        ).scalars().all()
        if is_virgul_source_file(x)
    ]
    deleted_rows = 0
    if source_files:
        deleted_rows = int(
            db.scalar(
                select(func.count())
                .select_from(AdReportRow)
                .where(AdReportRow.source_file.in_(source_files))
            )
            or 0
        )
        db.execute(delete(AdReportRow).where(AdReportRow.source_file.in_(source_files)))
    deleted_catalogs = 0
    for name in source_files:
        cat = db.execute(
            select(AdReportCatalog).where(AdReportCatalog.source_file == name)
        ).scalars().first()
        if cat is not None:
            db.delete(cat)
            deleted_catalogs += 1
    if commit:
        db.commit()
        invalidate_facets_cache()
    return {
        "stream_key": stream.key,
        "deleted_rows": deleted_rows,
        "deleted_catalogs": deleted_catalogs,
        "source_files": source_files,
    }


def ingest_virgul_file(
    db: Session,
    data: bytes,
    *,
    filename: str,
    stream_key: str,
    replace: bool = True,
    commit: bool = True,
) -> dict[str, Any]:
    stream = _STREAM_BY_KEY.get((stream_key or "").strip())
    if stream is None:
        raise ValueError(f"Bilinmeyen dal: {stream_key}")
    src = source_by_stream(stream_key)
    catalog = (src.catalog_filename if src else None) or filename
    if not is_virgul_source_file(catalog):
        catalog = f"virgul_{stream_key.replace(':', '_')}.xlsx"
    # uzantıyı data’ya göre düzelt
    if data[:2] == b"PK" and not catalog.lower().endswith((".xlsx", ".xlsm")):
        catalog = re_sub_ext(catalog, ".xlsx")
    elif data[:2] != b"PK" and not catalog.lower().endswith((".csv", ".txt")):
        catalog = re_sub_ext(catalog, ".csv")

    cleared = None
    if replace:
        cleared = clear_virgul_stream_rows(db, stream_key, commit=False)

    result = import_upload_file(
        db,
        data,
        filename=catalog,
        commit=False,
        stream_key=stream_key,
    )
    # Fingerprint çakışmasın diye virgul satırlarını namespace’li yeniden yazmak
    # import_upload_file sheet ile aynı fp kullanır — çakışmayı önlemek için
    # source_file virgul_* kalır; upsert aynı fp’yi ezer. Sheets sync clear
    # virgul hariç tuttuğu için sheet satırları korunur; virgul upsert sheet fp’yi
    # ezmesin diye fingerprint’e dokunmadan önce sheet satırı varsa atla.
    if commit:
        db.commit()
        invalidate_facets_cache()
    out = dict(result)
    out["warehouse"] = "virgul"
    out["stream_key"] = stream_key
    out["source_file"] = catalog
    out["cleared"] = cleared
    out["synced"] = int(result.get("parsed") or 0) > 0
    out["ok"] = bool(out["synced"])
    return out


def re_sub_ext(name: str, ext: str) -> str:
    base = name.rsplit(".", 1)[0] if "." in name else name
    return base + ext


def sync_virgul_from_panel(
    db: Session,
    *,
    stream_key: str | None = None,
    start: date | None = None,
    end: date | None = None,
    commit: bool = True,
) -> dict[str, Any]:
    """Mac/VPN: Virgül’den çek → DB ingest."""
    fetched = fetch_all_sites_exports(start=start, end=end, stream_key=stream_key)
    per: list[dict[str, Any]] = []
    ok_n = 0
    total_parsed = 0
    for item in fetched.get("items") or []:
        if not item.get("ok") or not item.get("data"):
            per.append(
                {
                    "ok": False,
                    "sid": item.get("sid"),
                    "stream_key": item.get("stream_key"),
                    "label": item.get("label"),
                    "message": item.get("message") or "export yok",
                }
            )
            continue
        try:
            ing = ingest_virgul_file(
                db,
                item["data"],
                filename=str(item.get("filename") or "virgul.xlsx"),
                stream_key=str(item.get("stream_key") or ""),
                replace=True,
                commit=False,
            )
            per.append(
                {
                    "ok": bool(ing.get("ok")),
                    "sid": item.get("sid"),
                    "stream_key": item.get("stream_key"),
                    "label": item.get("label"),
                    "parsed": ing.get("parsed"),
                    "source_file": ing.get("source_file"),
                    "message": ing.get("message") or ing.get("parse_error") or "OK",
                }
            )
            if ing.get("ok"):
                ok_n += 1
                total_parsed += int(ing.get("parsed") or 0)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Virgul ingest failed")
            per.append(
                {
                    "ok": False,
                    "sid": item.get("sid"),
                    "stream_key": item.get("stream_key"),
                    "label": item.get("label"),
                    "message": str(exc),
                }
            )
    if commit:
        db.commit()
        invalidate_facets_cache()
    return {
        "ok": ok_n > 0,
        "synced": ok_n > 0,
        "ok_count": ok_n,
        "fail_count": len(per) - ok_n,
        "total_parsed": total_parsed,
        "streams": per,
        "start": fetched.get("start"),
        "end": fetched.get("end"),
        "warehouse": "virgul",
        "message": f"Virgül sync · {ok_n}/{len(per)} dal · {total_parsed} satır",
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }


def ingest_virgul_bridge_payload(
    db: Session,
    *,
    files: list[dict[str, Any]],
    replace: bool = True,
) -> dict[str, Any]:
    """Bridge: [{stream_key, filename, data_b64|rows already as bytes via caller}].

    Caller passes decoded bytes in `data`.
    """
    per: list[dict[str, Any]] = []
    ok_n = 0
    total_parsed = 0
    for f in files or []:
        sk = str(f.get("stream_key") or "").strip()
        data = f.get("data")
        if isinstance(data, str):
            import base64

            data = base64.b64decode(data)
        if not sk or not isinstance(data, (bytes, bytearray)) or not data:
            per.append({"ok": False, "stream_key": sk, "message": "stream_key/data gerekli"})
            continue
        try:
            ing = ingest_virgul_file(
                db,
                bytes(data),
                filename=str(f.get("filename") or f"virgul_{sk}.xlsx"),
                stream_key=sk,
                replace=replace,
                commit=False,
            )
            per.append(
                {
                    "ok": bool(ing.get("ok")),
                    "stream_key": sk,
                    "parsed": ing.get("parsed"),
                    "source_file": ing.get("source_file"),
                    "message": ing.get("parse_error") or "OK",
                }
            )
            if ing.get("ok"):
                ok_n += 1
                total_parsed += int(ing.get("parsed") or 0)
        except Exception as exc:  # noqa: BLE001
            per.append({"ok": False, "stream_key": sk, "message": str(exc)})
    db.commit()
    invalidate_facets_cache()
    return {
        "ok": ok_n > 0,
        "synced": ok_n > 0,
        "ok_count": ok_n,
        "fail_count": len(per) - ok_n,
        "total_parsed": total_parsed,
        "streams": per,
        "warehouse": "virgul",
        "message": f"Virgül ingest · {ok_n}/{len(per)} · {total_parsed} satır",
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }
