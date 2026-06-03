"""Reklam analitiği API — Excel/CSV yükleme ve filtreli özet."""

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services import ad_analytics_store as store

router = APIRouter(tags=["ad-analytics"])

_MAX_BULK_BYTES = 120 * 1024 * 1024  # 12 dosya × ~10 MB


def _filter_kwargs(
    *,
    start: str | None,
    end: str | None,
    income_types: str | None,
    ad_units: str | None,
    platforms: str | None,
    channels: str | None,
    surfaces: str | None,
    sources: str | None,
    search: str | None,
    project: str | None,
    branch: str | None,
) -> dict:
    return {
        "start": start,
        "end": end,
        "income_types": income_types,
        "ad_units": ad_units,
        "platforms": platforms,
        "channels": channels,
        "surfaces": surfaces,
        "sources": sources,
        "search": search,
        "project": project,
        "branch": branch,
    }


@router.get("/ad-analytics/facets")
def get_ad_analytics_facets(db: Session = Depends(get_db)):
    return store.facets(db)


@router.get("/ad-analytics/summary")
def get_ad_analytics_summary(
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
        ),
    )


@router.get("/ad-analytics/table")
def get_ad_analytics_table(
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
        ),
        breakdown=breakdown,
        limit=limit,
        offset=offset,
    )


@router.post("/ad-analytics/upload")
async def post_ad_analytics_upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    name = (file.filename or "upload").strip()
    low = name.lower()
    try:
        raw = await file.read()
        if not raw:
            raise HTTPException(status_code=400, detail="Boş dosya")
        if low.endswith((".xlsx", ".xlsm", ".csv", ".txt")):
            result = store.import_upload_file(db, raw, filename=name)
        else:
            raise HTTPException(status_code=400, detail="Yalnızca .xlsx veya .csv desteklenir")
        if not result.get("parsed"):
            raise HTTPException(status_code=400, detail="Dosyadan satır okunamadı (başlık/format)")
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ad-analytics/upload-bulk")
async def post_ad_analytics_upload_bulk(
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    """12 xlsx tek seferde: dal başına 2025+2026 birleşir; aynı günler güncellenir (upsert)."""
    if not files:
        raise HTTPException(status_code=400, detail="Dosya seçilmedi")
    payload: list[tuple[bytes, str]] = []
    total_bytes = 0
    for uf in files:
        name = (uf.filename or "upload.xlsx").strip()
        low = name.lower()
        if not low.endswith((".xlsx", ".xlsm", ".csv", ".txt")):
            raise HTTPException(status_code=400, detail=f"Desteklenmeyen format: {name}")
        raw = await uf.read()
        total_bytes += len(raw)
        if total_bytes > _MAX_BULK_BYTES:
            raise HTTPException(status_code=413, detail="Toplam yükleme 120 MB sınırını aşıyor")
        payload.append((raw, name))
    try:
        result = store.import_upload_files_bulk(payload)
        if result.get("parsed", 0) <= 0:
            raise HTTPException(status_code=400, detail="Hiçbir dosyadan satır okunamadı")
        return result
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/ad-analytics/reset")
def post_ad_analytics_reset(db: Session = Depends(get_db)):
    try:
        return store.reset_all(db)
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc
