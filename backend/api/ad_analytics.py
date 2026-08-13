"""Reklam analitiği API — Excel/CSV yükleme ve filtreli özet.

Sheets /ad yayından kaldırıldı: /api/mz-analytics → 410
(GA4 banner + gelir hedefleri hariç).
Virgül /ad-virgul → /api/virgul-analytics (bu router'a dokunulmaz).
"""

import json
import logging

from fastapi import APIRouter, Body, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.orm import Session

from backend.database import SessionLocal, get_db
from backend.services import ad_analytics_store as store
from backend.services import app_empower_store as empower_store
from backend.services.revenue_targets_sheet import revenue_targets_payload
from backend.api.app_empower import router as app_empower_router

LOGGER = logging.getLogger(__name__)

_SHEETS_AD_GONE = (
    "Google Sheets /ad sekmesi yayından kaldırıldı. "
    "Virgül monetizasyon için /ad-virgul ve /api/virgul-analytics kullanın."
)


def _sheets_ad_gate(request: Request) -> None:
    """Sheets mz-analytics yükünü kes; banner + gelir hedefleri açık kalır."""
    path = request.url.path or ""
    if "/ga4-app-banner" in path or "/revenue-targets" in path:
        return
    raise HTTPException(status_code=410, detail=_SHEETS_AD_GONE)


router = APIRouter(
    tags=["mz-analytics"],
    dependencies=[Depends(_sheets_ad_gate)],
)
router.include_router(app_empower_router)

_MAX_BULK_BYTES = 120 * 1024 * 1024  # 12 dosya × ~10 MB


async def _read_upload_payload(files: list[UploadFile]) -> list[tuple[bytes, str]]:
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
        if not raw:
            LOGGER.warning("Ad bulk upload empty body: %s", name)
        payload.append((raw, name))
    return payload


def _merge_bulk_upload_result(
    ad_result: dict,
    empower_result: dict | None,
) -> dict:
    out = dict(ad_result)
    if empower_result is not None:
        out["empower"] = empower_result
    return out


def _bulk_upload_succeeded(ad_result: dict, empower_result: dict | None) -> bool:
    if int(ad_result.get("parsed") or 0) > 0:
        return True
    if empower_result and int(empower_result.get("ok_count") or 0) > 0:
        return True
    return False


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


@router.get("/mz-analytics/facets")
def get_ad_analytics_facets(
    db: Session = Depends(get_db),
    _: str | None = Query(None, alias="_"),
):
    return store.facets(db, skip_cache=_ is not None)


@router.get("/mz-analytics/suggested-favorites")
def get_ad_analytics_suggested_favorites(
    db: Session = Depends(get_db),
    period_days: int = Query(30, ge=1, le=366),
):
    """Dal başına son dönemde en çok gelir getiren Mx birimleri (favori önerisi)."""
    return store.suggested_detail_favorites(db, period_days=period_days)


@router.get("/mz-analytics/summary")
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
    compare_mode: str | None = Query(
        None,
        description="previous_period | previous_year | custom",
    ),
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
        ),
        compare_mode=compare_mode,
        compare_start=compare_start,
        compare_end=compare_end,
    )


@router.get("/mz-analytics/table")
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
        ),
        breakdown=breakdown,
        limit=limit,
        offset=offset,
        compare_mode=compare_mode,
        compare_start=compare_start,
        compare_end=compare_end,
    )


@router.get("/mz-analytics/sheets-status")
def get_ad_sheets_status(db: Session = Depends(get_db)):
    from backend.services import ad_sheets_sync as sheets_sync

    return sheets_sync.sheets_sync_status(db)


@router.post("/mz-analytics/sync-sheets")
def post_ad_sync_sheets(
    db: Session = Depends(get_db),
    force: bool = Query(True, description="TTL’yi yok say"),
    stream_key: str | None = Query(None, description="Tek dal; boş = hepsi"),
    full: bool = Query(
        False,
        description="True: dalı temizleyip sheet’i baştan yaz. False: kirli/ilk seferde tam, sonra son 21 gün",
    ),
    background: bool = Query(
        True,
        description="True: arka planda çalıştır (Railway 502 önler; Döviz Web ~50MB)",
    ),
):
    from backend.services import ad_sheets_sync as sheets_sync

    key = (stream_key or "").strip() or None
    try:
        if background:
            return sheets_sync.start_sync_job(force=force, stream_key=key, full=full)
        return sheets_sync.sync_from_google_sheets(
            db,
            force=force,
            stream_key=key,
            full=full,
        )
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        LOGGER.exception("ad sync-sheets failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mz-analytics/sync-sheets/job")
def get_ad_sync_sheets_job():
    from backend.services import ad_sheets_sync as sheets_sync

    return sheets_sync.get_sync_job()


@router.post("/mz-analytics/sync-sheets/cancel")
def post_ad_sync_sheets_cancel():
    """Çalışan Sheets senkronunu iptal et; oturum değişiklikleri rollback olur."""
    from backend.services import ad_sheets_sync as sheets_sync

    return sheets_sync.request_cancel_sync_job()


@router.post("/mz-analytics/append")
async def post_ad_analytics_append(
    file: UploadFile = File(...),
    stream_key: str = Query(..., description="Örn. doviz:desktop"),
    db: Session = Depends(get_db),
):
    raise HTTPException(
        status_code=410,
        detail="Manuel dosya yükleme kapatıldı. Google Sheets → Verileri güncelle kullanın.",
    )


@router.post("/mz-analytics/upload")
async def post_ad_analytics_upload(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    raise HTTPException(
        status_code=410,
        detail="Manuel dosya yükleme kapatıldı. Google Sheets → Verileri güncelle kullanın.",
    )


@router.post("/mz-analytics/upload-bulk")
async def post_ad_analytics_upload_bulk(
    files: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
):
    """Yalnızca Empower; reklam xlsx/csv reddedilir."""
    payload = await _read_upload_payload(files)
    ad_files, empower_files = empower_store.partition_mz_upload_files(payload)
    if ad_files:
        raise HTTPException(
            status_code=410,
            detail="Manuel reklam dosyası yükleme kapatıldı. Google Sheets → Verileri güncelle.",
        )
    if not empower_files:
        raise HTTPException(status_code=400, detail="Dosya yok")
    empower_result = empower_store.import_files_bulk(db, empower_files)
    result = {
        "files": [],
        "file_count": 0,
        "inserted": 0,
        "parsed": 0,
        "total": store.count_rows(db),
        "unknown_files": [],
        "summary": store.build_upload_batch_summary([]),
    }
    result = _merge_bulk_upload_result(result, empower_result)
    if not _bulk_upload_succeeded(result, empower_result):
        raise HTTPException(status_code=400, detail="Empower yükleme başarısız")
    return result


@router.post("/mz-analytics/upload-bulk-stream")
async def post_ad_analytics_upload_bulk_stream(
    files: list[UploadFile] = File(...),
):
    """Empower için stream; reklam xlsx reddedilir."""
    payload = await _read_upload_payload(files)
    ad_files, empower_files = empower_store.partition_mz_upload_files(payload)
    if ad_files:
        raise HTTPException(
            status_code=410,
            detail="Manuel reklam dosyası yükleme kapatıldı. Google Sheets → Verileri güncelle.",
        )
    if not empower_files:
        raise HTTPException(status_code=400, detail="Dosya yok")

    def _ndjson_stream():
        try:
            yield json.dumps(
                {"phase": "empower_start", "file_count": len(empower_files), "pct": 2},
                ensure_ascii=False,
            ) + "\n"
            with SessionLocal() as db:
                empower_result = empower_store.import_files_bulk(db, empower_files)
                total = store.count_rows(db)
            yield json.dumps(
                {
                    "phase": "done",
                    "pct": 100,
                    "empower": empower_result,
                    "total": total,
                    "summary": store.build_upload_batch_summary([]),
                },
                ensure_ascii=False,
            ) + "\n"
        except Exception as exc:  # noqa: BLE001
            yield json.dumps(
                {"phase": "batch_error", "error": str(exc), "pct": 0},
                ensure_ascii=False,
            ) + "\n"

    return StreamingResponse(_ndjson_stream(), media_type="application/x-ndjson")


def _mz_ga4_overlay_profiles(branch: str) -> tuple[str, list[str]]:
    """Dal → GA4 profilleri. desktop=web, mweb=mobil web; ios/android=uygulama."""
    br = (branch or "desktop").strip().lower()
    if br == "desktop":
        return "web", ["web"]
    if br == "mweb":
        return "web", ["mweb"]
    if br in ("android", "ios"):
        return "app", ["android", "ios"]
    return "app", ["android", "ios"]


def _mz_ga4_site(db: Session, project: str):
    from sqlalchemy import case

    from backend.models import Site

    pid = (project or "doviz").strip().lower()
    if pid == "sinemalar":
        domain_like = "%sinemalar.com%"
        www_rank = case((Site.domain.ilike("www.sinemalar.com%"), 0), else_=1)
    else:
        domain_like = "%doviz.com%"
        www_rank = case((Site.domain.ilike("www.doviz.com%"), 0), else_=1)
    return (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .filter(Site.domain.ilike(domain_like))
        .order_by(www_rank, Site.id.asc())
        .first()
    )


@router.get("/mz-analytics/ga4-mobile-overlay")
def get_ga4_mobile_overlay(
    db: Session = Depends(get_db),
    project: str = Query("doviz"),
    branch: str = Query("desktop"),
):
    """GA4 günlük trend overlay — web/mweb veya android/ios (/ad drill grafikleri)."""
    from backend.config import settings
    from backend.services.warehouse import get_latest_ga4_report_snapshot

    site = _mz_ga4_site(db, project)
    if site is None:
        kind, _profiles = _mz_ga4_overlay_profiles(branch)
        return {
            "site_id": None,
            "kind": kind,
            "project": (project or "doviz").strip().lower(),
            "branch": (branch or "desktop").strip().lower(),
            "web": None,
            "mweb": None,
            "android": None,
            "ios": None,
        }

    period_days = int(settings.ga4_trend_12m_period_days)
    kind, profiles = _mz_ga4_overlay_profiles(branch)

    def _profile_trend(profile: str) -> dict | None:
        snap = get_latest_ga4_report_snapshot(
            db,
            site_id=site.id,
            profile=profile,
            period_days=period_days,
        )
        if not snap:
            return None
        payload = snap.get("payload") if isinstance(snap.get("payload"), dict) else {}
        dt = payload.get("daily_trend") if isinstance(payload.get("daily_trend"), dict) else {}
        dates = dt.get("dates") or []
        if not dates:
            return None
        return {
            "profile": profile,
            "last_start": snap.get("last_start"),
            "last_end": snap.get("last_end"),
            "collected_at": snap.get("collected_at"),
            "daily_trend": {
                "dates": dates,
                "sessions": dt.get("sessions") or [],
                "activeUsers": dt.get("activeUsers") or [],
            },
        }

    out: dict = {
        "site_id": site.id,
        "domain": site.domain,
        "project": (project or "doviz").strip().lower(),
        "branch": (branch or "desktop").strip().lower(),
        "kind": kind,
        "period_days": period_days,
        "web": None,
        "mweb": None,
        "android": None,
        "ios": None,
    }
    for prof in profiles:
        out[prof] = _profile_trend(prof)
    return out


def _fetch_ga4_app_banner_payload(
    db: Session,
    *,
    project: str,
    profile: str,
    start: str | None,
    end: str | None,
    top_campaigns: int,
) -> dict:
    """GA4 mobil — günlük first_open, kampanya + mweb + (iOS) ASC."""
    from google.api_core import exceptions as ga_exc

    from datetime import date as date_cls

    from backend.services.app_intel import APP_PRODUCTS
    from backend.services.ga4_app_attribution import (
        default_banner_date_range,
        fetch_app_banner_attribution,
        fetch_mweb_banner_events_daily,
        trim_banner_payload_to_observed_start,
    )
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties

    prof = (profile or "android").strip().lower()
    if prof not in ("android", "ios"):
        raise HTTPException(status_code=400, detail="profile android veya ios olmalı.")

    if start and end:
        start_s, end_s = start.strip()[:10], end.strip()[:10]
    else:
        start_s, end_s = default_banner_date_range(days=28)

    site = _mz_ga4_site(db, project)
    if site is None:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    proj_key = (project or "doviz").strip().lower()
    use_ios_manual = prof == "ios" and proj_key == "doviz"

    record = get_ga4_credentials_record(db, site.id)
    properties = load_ga4_properties(record)

    if use_ios_manual:
        from backend.services.doviz_ios_app_banner_manual import (
            fetch_doviz_ios_app_banner_manual,
        )

        try:
            payload = fetch_doviz_ios_app_banner_manual(
                start=start_s,
                end=end_s,
                top_campaigns=top_campaigns,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    else:
        property_id = str(properties.get(prof) or "").strip()
        if not property_id:
            raise HTTPException(
                status_code=404,
                detail=f"GA4 {prof} property tanımlı değil.",
            )
        try:
            payload = fetch_app_banner_attribution(
                property_id,
                start=start_s,
                end=end_s,
                top_campaigns=top_campaigns,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ga_exc.GoogleAPIError as exc:
            raise HTTPException(status_code=502, detail=f"GA4 API: {exc.message}") from exc

    payload["site_id"] = site.id
    payload["domain"] = site.domain
    payload["project"] = proj_key
    payload["profile"] = prof

    start_d = date_cls.fromisoformat(start_s)
    end_d = date_cls.fromisoformat(end_s)
    mweb_pid = str(properties.get("mweb") or "").strip()
    if mweb_pid:
        try:
            payload["mweb_banner"] = fetch_mweb_banner_events_daily(
                mweb_pid,
                start=start_s,
                end=end_s,
                profile=prof,
            )
        except Exception as exc:  # noqa: BLE001
            payload["mweb_banner"] = {"ok": False, "error": str(exc)}

    if prof == "ios":
        bundle = (APP_PRODUCTS.get(proj_key) or {}).get("ios_bundle_id") or ""
        if bundle:
            try:
                from backend.services.asc_campaign_downloads import fetch_banner_campaign_downloads

                payload["app_store_campaign_downloads"] = fetch_banner_campaign_downloads(
                    bundle_id=bundle,
                    start=start_d,
                    end=end_d,
                )
            except Exception as exc:  # noqa: BLE001
                payload["app_store_campaign_downloads"] = {"ok": False, "error": str(exc)}
        else:
            payload["app_store_campaign_downloads"] = {
                "ok": False,
                "message": "iOS bundle tanımlı değil.",
            }

    trim_banner_payload_to_observed_start(payload)
    return payload


@router.get("/mz-analytics/ga4-app-banner")
def get_ga4_app_banner(
    db: Session = Depends(get_db),
    project: str = Query("doviz"),
    profile: str = Query("android", description="android | ios"),
    start: str | None = Query(None),
    end: str | None = Query(None),
    top_campaigns: int = Query(10, ge=1, le=25),
):
    return _fetch_ga4_app_banner_payload(
        db,
        project=project,
        profile=profile,
        start=start,
        end=end,
        top_campaigns=top_campaigns,
    )


@router.get("/mz-analytics/ga4-app-banner/export.xlsx")
def export_ga4_app_banner_xlsx(
    db: Session = Depends(get_db),
    project: str = Query("doviz"),
    profile: str = Query("android", description="android | ios"),
    start: str | None = Query(None),
    end: str | None = Query(None),
    top_campaigns: int = Query(10, ge=1, le=25),
    active_only: bool = Query(
        False,
        description="Yalnızca en az bir metrik > 0 olan günler (paneldeki filtre ile uyumlu).",
    ),
):
    from backend.services.ga4_app_banner_export import build_app_banner_xlsx

    payload = _fetch_ga4_app_banner_payload(
        db,
        project=project,
        profile=profile,
        start=start,
        end=end,
        top_campaigns=top_campaigns,
    )
    blob = build_app_banner_xlsx(payload, active_only=active_only)
    start_s = str(payload.get("chart_start") or payload.get("start") or "")[:10]
    end_s = str(payload.get("chart_end") or payload.get("end") or "")[:10]
    prof = str(payload.get("profile") or "app").strip().lower()
    proj = str(payload.get("project") or "doviz").strip().lower()
    filename = f"app_banner_{proj}_{prof}_{start_s}_{end_s}.xlsx"
    return Response(
        content=blob,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/mz-analytics/app-lab-preview")
def get_app_lab_preview(
    db: Session = Depends(get_db),
    start: str | None = Query(None),
    end: str | None = Query(None),
    project: str | None = Query(None),
    branch: str | None = Query(None),
):
    """Monetizasyon lab önizleme kartları (/ad sayfa altı; aktif stream peer moduna göre)."""
    return store.query_app_lab_preview(
        db,
        start=start,
        end=end,
        project=(project or "doviz").strip().lower(),
        branch=(branch or "desktop").strip().lower(),
    )


@router.get("/mz-analytics/revenue-targets")
def get_revenue_targets(
    project: str | None = Query(None, description="doviz | sinemalar"),
    year: int | None = Query(None, ge=2000, le=2100),
    force: bool = Query(False, description="Google Sheet önbelleğini atla"),
    db: Session = Depends(get_db),
):
    """Google Sheets aylık gelir hedef tablosu (Döviz / Sinemalar)."""
    try:
        return revenue_targets_payload(
            project=project,
            year=year,
            force=force,
            db=db,
            warehouse="sheets",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("revenue-targets fetch failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/mz-analytics/reset")
def post_ad_analytics_reset(db: Session = Depends(get_db)):
    try:
        return store.reset_all(db)
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/mz-analytics/delete-import")
def post_ad_analytics_delete_import(
    db: Session = Depends(get_db),
    body: dict = Body(...),
):
    from backend.services.ad_sheets_config import is_sheet_catalog_filename

    source_file = str(body.get("source_file") or "").strip()
    if not source_file:
        raise HTTPException(status_code=400, detail="source_file gerekli")
    if is_sheet_catalog_filename(source_file):
        raise HTTPException(
            status_code=400,
            detail="Google Sheet katalogları silinmez. «Tüm veriyi sıfırla» veya full sync kullanın.",
        )
    restore = body.get("restore", True)
    if isinstance(restore, str):
        restore = restore.strip().lower() not in ("0", "false", "no")
    try:
        return store.delete_source_file(db, source_file, restore=bool(restore))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/mz-analytics/delete-imports-bulk")
def post_ad_analytics_delete_imports_bulk(
    db: Session = Depends(get_db),
    body: dict = Body(...),
):
    from backend.services.ad_sheets_config import is_sheet_catalog_filename

    raw_files = body.get("source_files") or body.get("files") or []
    if not isinstance(raw_files, list) or not raw_files:
        raise HTTPException(status_code=400, detail="source_files gerekli")
    blocked = [str(x) for x in raw_files if is_sheet_catalog_filename(str(x))]
    if blocked:
        raise HTTPException(
            status_code=400,
            detail="Google Sheet katalogları silinmez: " + ", ".join(blocked[:3]),
        )
    restore = body.get("restore", True)
    if isinstance(restore, str):
        restore = restore.strip().lower() not in ("0", "false", "no")
    try:
        return store.delete_source_files_bulk(
            db,
            [str(x) for x in raw_files],
            restore=bool(restore),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc)) from exc
