"""x-ga4 endpoint'i — GA4'ün kullanılmayan boyut/metrikleri."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.rate_limiter import limiter

router = APIRouter(tags=["x-ga4"])


@router.get("/x-ga4/report")
@limiter.limit("30/minute")
def x_ga4_report(
    request: Request,
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(15, ge=5, le=50),
    profile: str = Query("hepsi", description="hepsi | web | mweb | android | ios"),
    force: bool = Query(False, description="5 dk önbelleği atla"),
    site_id: int = Query(1, ge=1),
    progress: str = Query("", max_length=64, description="İlerleme anahtarı (opsiyonel)"),
    db: Session = Depends(get_db),
):
    from backend.services.x_ga4 import build_x_ga4_report

    return build_x_ga4_report(
        db,
        site_id=site_id,
        days=days,
        limit=limit,
        profile=profile,
        force=force,
        progress_token=progress,
    )


@router.get("/x-ga4/progress")
@limiter.limit("240/minute")
def x_ga4_progress(
    request: Request,
    token: str = Query(..., max_length=64),
):
    """Süren raporun tamamlanan istek sayısı.

    Rapor tek bir uzun HTTP isteği; ilerleme ayrı okunur. Sık yoklandığı için
    hız sınırı rapor ucundan yüksek — aksi halde çubuk kendi kendini kilitler.
    """
    from backend.services.x_ga4 import progress_snapshot

    return progress_snapshot(token)
