"""Notification Analytics — paylaşımlı DB API."""

from pydantic import BaseModel, Field

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services import notification_analytics_store as store
from backend.services.notification_analytics_alerts import evaluate_notification_analytics_alerts

router = APIRouter(tags=["notification-analytics"])


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


class IngestRowsBody(BaseModel):
    rows: list[dict] = Field(default_factory=list)
    source: str = "doviz_admin_bridge"
    replace: bool | None = None


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


@router.post("/notification-analytics/sync-sheet")
def post_notification_analytics_sync_sheet(
    force: bool = Query(False, description="true ise TTL yok sayılır, Doviz admin yeniden çekilir"),
    db: Session = Depends(get_db),
):
    """Aktif kaynak: Doviz admin tarama / Mac köprüsü ingest. Sheet yedek kapalı."""
    try:
        result = store.sync_notification_analytics(db, force=force)
        if result.get("ok") is False and not result.get("skipped"):
            raise HTTPException(status_code=502, detail=result.get("message") or "Senkron başarısız.")
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/sync-admin")
def post_notification_analytics_sync_admin(
    force: bool = Query(True, description="true ise TTL yok sayılır"),
    db: Session = Depends(get_db),
):
    """Doviz.com admin notifications/stats."""
    try:
        result = store.sync_from_doviz_admin(db, force=force)
        if result.get("ok") is False and not result.get("skipped"):
            raise HTTPException(status_code=502, detail=result.get("message") or "Admin senkronu başarısız.")
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/ingest")
def post_notification_analytics_ingest(
    body: IngestRowsBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
):
    """VPN köprüsü: admin stats satırlarını yazar (UI/manuel yok)."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    try:
        result = store.ingest_notification_rows(
            db,
            body.rows or [],
            source=(body.source or "doviz_admin_bridge").strip() or "doviz_admin_bridge",
            replace=body.replace,
        )
        try:
            from backend.services.scrape_telemetry import record_scrape_ingest

            ok = not (result.get("ok") is False and not result.get("synced"))
            record_scrape_ingest(
                db,
                source="notification_analytics",
                target="doviz",
                status="success" if ok else "error",
                row_count=int(result.get("inserted") or result.get("row_count") or len(body.rows or []) or 0),
                message=str(result.get("message") or ""),
                commit=True,
            )
        except Exception:
            pass
        if result.get("ok") is False and not result.get("synced"):
            raise HTTPException(status_code=422, detail=result.get("message") or "Ingest başarısız.")
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/notification-analytics/sync-sheet-backup")
def post_notification_analytics_sync_sheet_backup(
    force: bool = Query(True, description="true ise TTL yok sayılır"),
    db: Session = Depends(get_db),
):
    """Kapalı — Google Sheet yedek iptal."""
    raise HTTPException(
        status_code=410,
        detail="Google Sheet yedek kapalı. Aktif kaynak: Doviz admin tarama / Mac köprüsü.",
    )


@router.post("/notification-analytics/upload")
def post_notification_analytics_upload(body: UploadCsvBody, db: Session = Depends(get_db)):
    """Kapalı — manuel dosya/sheet yükleme hesaplara dahil değil (çift kaynak engeli)."""
    raise HTTPException(
        status_code=410,
        detail="Manuel CSV yükleme kapalı. Aktif kaynak: Doviz admin tarama / Mac köprüsü.",
    )


@router.post("/notification-analytics/upload-file")
async def post_notification_analytics_upload_file(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """Kapalı — manuel dosya yükleme hesaplara dahil değil."""
    raise HTTPException(
        status_code=410,
        detail="Manuel dosya yükleme kapalı. Aktif kaynak: Doviz admin.",
    )


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
