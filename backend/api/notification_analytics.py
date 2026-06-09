"""Notification Analytics — paylaşımlı DB API."""

from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.services import notification_analytics_store as store

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
def get_notification_analytics_state(db: Session = Depends(get_db)):
    return store.workspace_state(db)


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
        text = raw.decode("utf-8-sig", errors="replace")
        return store.upload_csv_text(db, text)
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
