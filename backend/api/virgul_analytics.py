"""Virgül reklam analitiği API — yalnızca virgul_* warehouse (sheet/manuel yok)."""

from __future__ import annotations

import base64
import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import ad_analytics_store as store
from backend.services.revenue_targets_sheet import revenue_targets_payload
from backend.services.virgul_ad_config import virgul_sources_payload
from backend.services.virgul_ad_sync import ingest_virgul_bridge_payload, sync_virgul_from_panel

logger = logging.getLogger(__name__)

router = APIRouter(tags=["virgul-analytics"])

_WAREHOUSE = "virgul"


def _check_ingest_token(
    authorization: str | None,
    x_notification_ingest_token: str | None,
) -> None:
    expected = (settings.notification_ingest_token or "").strip()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="NOTIFICATION_INGEST_TOKEN tanımlı değil (Railway Variables).",
        )
    got = (x_notification_ingest_token or "").strip()
    if not got and authorization:
        raw = authorization.strip()
        if raw.lower().startswith("bearer "):
            got = raw[7:].strip()
        else:
            got = raw
    if not got or got != expected:
        raise HTTPException(status_code=401, detail="Geçersiz ingest token.")


def _filter_kwargs(**kwargs: Any) -> dict[str, Any]:
    return {**kwargs, "warehouse": _WAREHOUSE}


class VirgulIngestFile(BaseModel):
    stream_key: str
    filename: str | None = None
    data_b64: str = Field(..., description="Excel/CSV base64")


class VirgulIngestBody(BaseModel):
    files: list[VirgulIngestFile] = Field(default_factory=list)
    replace: bool = True
    source: str | None = "virgul_bridge"


@router.get("/virgul-analytics/sources")
def get_virgul_sources():
    return {"ok": True, "warehouse": _WAREHOUSE, "sources": virgul_sources_payload()}


@router.get("/virgul-analytics/facets")
def get_virgul_facets(
    db: Session = Depends(get_db),
    _: str | None = Query(None, alias="_"),
):
    return store.facets(db, skip_cache=_ is not None, warehouse=_WAREHOUSE)


@router.get("/virgul-analytics/summary")
def get_virgul_summary(
    db: Session = Depends(get_db),
    start: str | None = Query(None),
    end: str | None = Query(None),
    income_types: str | None = Query(None),
    ad_units: str | None = Query(None),
    platforms: str | None = Query(None),
    channels: str | None = Query(None),
    surfaces: str | None = Query(None),
    sources: str | None = Query(None),
    search: str | None = Query(None),
    project: str | None = Query(None),
    branch: str | None = Query(None),
    compare_mode: str | None = Query(None),
    compare_start: str | None = Query(None),
    compare_end: str | None = Query(None),
):
    return store.query_summary(
        db,
        **_filter_kwargs(
            start=start,
            end=end,
            income_types=income_types,
            ad_units=ad_units,
            platforms=platforms,
            channels=channels,
            surfaces=surfaces,
            sources=sources,
            search=search,
            project=project,
            branch=branch,
            compare_mode=compare_mode,
            compare_start=compare_start,
            compare_end=compare_end,
        ),
    )


@router.get("/virgul-analytics/table")
def get_virgul_table(
    db: Session = Depends(get_db),
    start: str | None = Query(None),
    end: str | None = Query(None),
    income_types: str | None = Query(None),
    ad_units: str | None = Query(None),
    platforms: str | None = Query(None),
    channels: str | None = Query(None),
    surfaces: str | None = Query(None),
    sources: str | None = Query(None),
    search: str | None = Query(None),
    project: str | None = Query(None),
    branch: str | None = Query(None),
    breakdown: str = Query("date,ad_unit,income_type"),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    compare_mode: str | None = Query(None),
    compare_start: str | None = Query(None),
    compare_end: str | None = Query(None),
):
    return store.query_table(
        db,
        **_filter_kwargs(
            start=start,
            end=end,
            income_types=income_types,
            ad_units=ad_units,
            platforms=platforms,
            channels=channels,
            surfaces=surfaces,
            sources=sources,
            search=search,
            project=project,
            branch=branch,
            breakdown=breakdown,
            limit=limit,
            offset=offset,
            compare_mode=compare_mode,
            compare_start=compare_start,
            compare_end=compare_end,
        ),
    )


@router.get("/virgul-analytics/suggested-favorites")
def get_virgul_suggested_favorites(
    db: Session = Depends(get_db),
    period_days: int = Query(30, ge=1, le=366),
):
    # Favoriler sheet store fonksiyonu warehouse bilmiyor olabilir — boş güvenli yanıt
    try:
        return store.suggested_detail_favorites(db, period_days=period_days)
    except Exception as exc:  # noqa: BLE001
        logger.warning("virgul suggested favorites: %s", exc)
        return {"items": []}


@router.post("/virgul-analytics/sync")
def post_virgul_sync(
    db: Session = Depends(get_db),
    stream_key: str | None = Query(None),
    force: bool = Query(True),
):
    """Sunucudan doğrudan Virgül çekimi — genelde VPN/Mac’te; Railway’de çoğu zaman fail."""
    _ = force
    try:
        return sync_virgul_from_panel(db, stream_key=stream_key)
    except Exception as exc:  # noqa: BLE001
        logger.exception("virgul sync failed")
        raise HTTPException(status_code=502, detail=str(exc) or "Virgül sync başarısız") from exc


@router.post("/virgul-analytics/ingest")
def post_virgul_ingest(
    body: VirgulIngestBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """Ofis Mac bridge → Railway (sheet yok)."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    files: list[dict[str, Any]] = []
    for f in body.files or []:
        try:
            raw = base64.b64decode(f.data_b64)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=f"data_b64 bozuk: {exc}") from exc
        files.append(
            {
                "stream_key": f.stream_key,
                "filename": f.filename or f"virgul_{f.stream_key}.xlsx",
                "data": raw,
            }
        )
    try:
        result = ingest_virgul_bridge_payload(db, files=files, replace=body.replace)
        try:
            from backend.services.scrape_telemetry import record_scrape_ingest

            keys = [f.get("stream_key") for f in files if f.get("stream_key")]
            record_scrape_ingest(
                db,
                source="virgul_analytics",
                target=", ".join(str(k) for k in keys)[:128] or "virgul",
                status="success",
                row_count=int(result.get("row_count") or result.get("inserted") or len(files) or 0),
                message=str(result.get("message") or ""),
                commit=True,
            )
        except Exception:
            pass
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("virgul ingest failed")
        try:
            from backend.services.scrape_telemetry import record_scrape_ingest

            record_scrape_ingest(
                db,
                source="virgul_analytics",
                target="virgul",
                status="error",
                message=str(exc)[:500],
                commit=True,
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/virgul-analytics/sheets-status")
def virgul_sheets_status(db: Session = Depends(get_db)):
    """UI ‘son sync’ hint — Google Sheets değil; katalog/ingest zamanı."""
    from backend.services.virgul_ad_sync import virgul_sync_status

    return virgul_sync_status(db)


@router.get("/virgul-analytics/revenue-targets")
def get_virgul_revenue_targets(
    project: str | None = Query(None, description="doviz | sinemalar"),
    year: int | None = Query(None, ge=2000, le=2100),
    force: bool = Query(False, description="Hedef tablo önbelleğini atla"),
):
    """Aylık gelir hedef / kazanç tablosu (ad-virgul gelir hedefleri paneli)."""
    try:
        return revenue_targets_payload(project=project, year=year, force=force)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("virgul revenue-targets fetch failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/virgul-analytics/upload")
def virgul_upload_forbidden():
    raise HTTPException(status_code=403, detail="Virgül sekmesinde manuel yükleme kapalı")
