"""Doviz News — Google Sheets / admin haber yayın raporu API."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.services.doviz_news_sheet import doviz_news_payload, fetch_doviz_news_rows, ingest_doviz_news_rows

logger = logging.getLogger(__name__)

router = APIRouter(tags=["doviz-news"])


def _check_ingest_token(
    authorization: str | None,
    x_notification_ingest_token: str | None,
) -> None:
    """Notification ile aynı NOTIFICATION_INGEST_TOKEN."""
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


class IngestNewsBody(BaseModel):
    rows: list[dict] = Field(default_factory=list)
    source: str | None = "doviz_admin_news_bridge"
    source_url: str | None = None
    merge: bool = True
    sync_mode: str | None = "recent_7d"
    sync_ok: bool | None = True
    sync_message: str | None = None


@router.get("/doviz-news/report")
def get_doviz_news_report(
    category: str | None = Query(None, description="Kategori filtresi (boş = tümü)"),
    period: str | None = Query(
        "last_2d",
        description="Dönem: all | today | yesterday | last_2d | last_7d | prev_week | this_month | last_month | custom",
    ),
    start: str | None = Query(
        None,
        description="Özel aralık başlangıç (YYYY-MM-DD); end ile birlikte period=custom",
    ),
    end: str | None = Query(
        None,
        description="Özel aralık bitiş (YYYY-MM-DD); start ile birlikte period=custom",
    ),
    force: bool = Query(False, description="Önbelleği atla ve yeniden çek (admin → sheet)"),
    items_limit: int = Query(250, ge=1, le=500),
    include_traffic: bool = Query(False, description="GA4 + GSC trafik zenginleştirmesi (varsayılan kapalı)"),
    site_id: int = Query(1, ge=1, description="Site ID (GA4/GSC)"),
    db: Session = Depends(get_db),
):
    try:
        return doviz_news_payload(
            category=category,
            period=period,
            force=force,
            items_limit=items_limit,
            db=db,
            include_traffic=include_traffic,
            site_id=site_id,
            custom_start=start,
            custom_end=end,
        )
    except Exception as exc:
        logger.exception("doviz news report failed")
        raise HTTPException(status_code=400, detail=str(exc) or "Doviz News tablosu yüklenemedi") from exc


@router.post("/doviz-news/ingest")
def post_doviz_news_ingest(
    body: IngestNewsBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
):
    """VPN köprüsü: admin aktif haber satırlarını yazar."""
    _check_ingest_token(authorization, x_notification_ingest_token)

    def _log(status: str, row_count: int = 0, message: str = "") -> None:
        try:
            from backend.database import SessionLocal
            from backend.services.scrape_telemetry import record_scrape_ingest

            with SessionLocal() as db:
                record_scrape_ingest(
                    db,
                    source="doviz_news",
                    target="doviz",
                    status=status,
                    row_count=row_count,
                    message=message,
                    commit=True,
                )
        except Exception:
            pass

    try:
        if body.sync_ok is False:
            from backend.services.doviz_news_sheet import record_doviz_news_sync_failure

            record_doviz_news_sync_failure(
                message=body.sync_message or "Bridge sync başarısız",
                sync_mode=(body.sync_mode or "recent_7d"),
                source=(body.source or "doviz_admin_news_bridge") or "doviz_admin_news_bridge",
            )
            _log("error", 0, body.sync_message or "Bridge sync başarısız")
            return {
                "ok": False,
                "synced": False,
                "sync_ok": False,
                "message": body.sync_message or "Bridge sync başarısız",
            }
        result = ingest_doviz_news_rows(
            body.rows or [],
            source=(body.source or "doviz_admin_news_bridge").strip() or "doviz_admin_news_bridge",
            source_url=body.source_url,
            merge=bool(body.merge),
            sync_mode=(body.sync_mode or ("recent_7d" if body.merge else "full")).strip()
            or "recent_7d",
        )
        ok = not (result.get("ok") is False and not result.get("synced"))
        _log(
            "success" if ok else "error",
            int(result.get("row_count") or len(body.rows or []) or 0),
            str(result.get("message") or ""),
        )
        if result.get("ok") is False and not result.get("synced"):
            raise HTTPException(status_code=422, detail=result.get("message") or "Ingest başarısız.")
        return result
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("doviz news ingest failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/doviz-news/sync")
def post_doviz_news_sync(
    force: bool = Query(True),
    prefer_sheet: bool = Query(
        False,
        description="Yok sayılır — Google Sheet kullanılmaz; tek kaynak admin.",
        deprecated=True,
    ),
):
    """Admin / VPN köprüsü snapshot senkronu (Google Sheet yok)."""
    try:
        rows = fetch_doviz_news_rows(force=force, prefer_sheet=prefer_sheet)
        from backend.services.doviz_news_sheet import _CACHE

        cache = _CACHE or {}
        return {
            "ok": True,
            "synced": True,
            "parsed": len(rows),
            "row_count": len(rows),
            "source": cache.get("source"),
            "source_url": cache.get("source_url"),
            "fetched_at": cache.get("fetched_at"),
            "sheet_skipped": bool(cache.get("sheet_skipped")),
            "message": f"Doviz news sync · {len(rows)} kayıt · {cache.get('source') or '—'}",
            "admin_error": cache.get("admin_error"),
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("doviz news sync failed")
        raise HTTPException(status_code=502, detail=str(exc) or "Doviz News senkronu başarısız") from exc
