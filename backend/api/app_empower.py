"""Empower uygulama metrikleri — /ad yükleme, /ga4 overlay okuma."""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services import app_empower_store as store
from backend.services.app_empower_config import SERIES_BY_KEY

router = APIRouter(prefix="/mz-analytics/app-empower", tags=["app-empower"])


@router.get("/imports")
def get_app_empower_imports(db: Session = Depends(get_db)):
    return {"imports": store.list_imports(db)}


@router.get("/meta")
def get_app_empower_meta():
    return {
        "series": [{"key": s.key, "label": s.label, "unit": s.unit} for s in SERIES_BY_KEY.values()],
        "filename_hint": "dovizandroidempower1.xlsx, doviziosempower2.xlsx",
    }


@router.get("/overlay")
def get_app_empower_overlay(
    platform: str = Query(..., description="android veya ios"),
    start: str | None = Query(None),
    end: str | None = Query(None),
    series: str | None = Query(None, description="Virgülle: sessions,dau_7d,..."),
    db: Session = Depends(get_db),
):
    keys = None
    if series:
        keys = [k.strip() for k in series.split(",") if k.strip()]
        bad = [k for k in keys if k not in SERIES_BY_KEY]
        if bad:
            raise HTTPException(status_code=400, detail=f"Bilinmeyen seri: {', '.join(bad)}")
    try:
        return store.query_overlay(db, platform=platform, start=start, end=end, series_keys=keys)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/upload")
async def post_app_empower_upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    raw = await file.read()
    name = (file.filename or "empower.xlsx").strip()
    try:
        return store.import_file(db, raw, name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/upload-bulk")
async def post_app_empower_upload_bulk(
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    payload: list[tuple[bytes, str]] = []
    for uf in files:
        raw = await uf.read()
        name = (uf.filename or "empower.xlsx").strip()
        payload.append((raw, name))
    return store.import_files_bulk(db, payload)


@router.post("/refresh-import")
def post_app_empower_refresh_import(
    db: Session = Depends(get_db),
    body: dict = Body(...),
):
    source_file = str(body.get("source_file") or "").strip()
    if not source_file:
        raise HTTPException(status_code=400, detail="source_file gerekli")
    try:
        return store.refresh_import(db, source_file)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/delete-import")
def post_app_empower_delete_import(
    db: Session = Depends(get_db),
    body: dict = Body(...),
):
    source_file = str(body.get("source_file") or "").strip()
    if not source_file:
        raise HTTPException(status_code=400, detail="source_file gerekli")
    try:
        return store.delete_source_file(db, source_file)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/delete-imports-bulk")
def post_app_empower_delete_imports_bulk(
    db: Session = Depends(get_db),
    body: dict = Body(...),
):
    files = body.get("source_files") or []
    if not isinstance(files, list) or not files:
        raise HTTPException(status_code=400, detail="source_files listesi gerekli")
    return store.delete_source_files_bulk(db, [str(x) for x in files])
