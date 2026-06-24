"""Notification Analytics — paylaşımlı DB API."""

from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services import notification_analytics_store as store
from backend.services.notification_analytics_alerts import evaluate_notification_analytics_alerts

router = APIRouter(tags=["notification-analytics"])


class WorkspaceUpdateBody(BaseModel):
    rows: list[dict] | None = None
    last_id: int | None = None
    start: str | None = None
    end: str | None = None
    preset: str | None = None


class AppendRowsBody(BaseModel):
    rows: list[dict] = Field(default_factory=list)


class UploadCsvBody(BaseModel):
    csv_text: str = ""


@router.get("/notification-analytics/state")
def get_notification_analytics_state(
    include_rows: bool = Query(True, description="false ise yalnızca özet meta (satırlar ayrı chunk ile)"),
    db: Session = Depends(get_db),
):
    return store.workspace_state(db, include_rows=include_rows)


@router.get("/notification-analytics/rows")
def get_notification_analytics_rows(
    offset: int = Query(0, ge=0),
    limit: int = Query(2500, ge=1, le=10000),
    start: str | None = Query(None, description="YYYY-MM-DD (dahil)"),
    end: str | None = Query(None, description="YYYY-MM-DD (dahil)"),
    db: Session = Depends(get_db),
):
    return store.workspace_rows_chunk(db, offset=offset, limit=limit, start=start, end=end)


@router.put("/notification-analytics/state")
def put_notification_analytics_state(body: WorkspaceUpdateBody, db: Session = Depends(get_db)):
    try:
        return store.save_workspace(
            db,
            rows=body.rows,
            last_id=body.last_id,
            start=body.start,
            end=body.end,
            preset=body.preset,
        )
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/append")
def post_notification_analytics_append(body: AppendRowsBody, db: Session = Depends(get_db)):
    try:
        return store.append_rows(db, body.rows or [])
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/upload")
def post_notification_analytics_upload(body: UploadCsvBody, db: Session = Depends(get_db)):
    try:
        return store.upload_csv_text(db, body.csv_text or "")
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/upload-file")
async def post_notification_analytics_upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    try:
        raw = await file.read()
        if not raw:
            raise HTTPException(status_code=400, detail="Boş dosya.")
        return store.upload_file_bytes(db, raw, file.filename or "")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/reset")
def post_notification_analytics_reset(db: Session = Depends(get_db)):
    try:
        return store.reset_workspace(db)
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/notification-analytics/traffic")
def get_notification_analytics_traffic(
    content_id: str = Query(..., description="Bildirim içerik ID"),
    headline: str | None = Query(None, description="Bildirim başlığı (GA4/GSC eşleme yedek)"),
    send_date: str | None = Query(None, description="Gönderim tarihi YYYY-MM-DD"),
    site_id: int = Query(1, ge=1, description="GA4/GSC site ID"),
    days: int = Query(14, ge=1, le=90, description="Gönderim tarihinden itibaren pencere (gün)"),
    live: bool = Query(True, description="GA4 canlı çekim; GSC DB boşsa canlı"),
    db: Session = Depends(get_db),
):
    try:
        from backend.services.notification_content_traffic import resolve_content_traffic

        return resolve_content_traffic(
            db,
            content_id=content_id,
            headline=headline,
            send_date=send_date,
            site_id=site_id,
            days=days,
            live=live,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/notification-analytics/alerts/evaluate")
def get_notification_analytics_alerts_evaluate(
    send_email: bool = Query(False, description="true ise operasyon e-postası gönder"),
    db: Session = Depends(get_db),
):
    try:
        return evaluate_notification_analytics_alerts(db, send_email=send_email)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/alerts/check")
def post_notification_analytics_alerts_check(db: Session = Depends(get_db)):
    """Manuel alarm kontrolü — e-posta + AI Talk alert."""
    try:
        return evaluate_notification_analytics_alerts(db, send_email=True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
