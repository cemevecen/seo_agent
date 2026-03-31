"""Site yönetimi API endpoint'leri (JSON)."""

from datetime import datetime
import json

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.collectors.crawler import collect_crawler_metrics
from backend.collectors.crux_history import collect_crux_history
from backend.collectors.pagespeed import collect_pagespeed_metrics
from backend.models import Site, SiteCredential
from backend.rate_limiter import limiter
from backend.services.crypto import encrypt_text
from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties, upsert_ga4_properties
from backend.services.search_console_auth import get_search_console_connection_status

router = APIRouter(tags=["sites"])


def _site_to_dict(site: Site) -> dict:
    # Model nesnesini API için sade JSON çıktısına dönüştürür.
    return {
        "id": site.id,
        "domain": site.domain,
        "display_name": site.display_name,
        "is_active": site.is_active,
        "created_at": site.created_at.isoformat() if isinstance(site.created_at, datetime) else None,
    }


@router.get("/sites")
@limiter.limit("60/minute")
def list_sites(request: Request, db: Session = Depends(get_db)):
    # Tüm siteleri en yeni kayıt üstte olacak şekilde döndürür.
    sites = db.query(Site).order_by(Site.created_at.desc()).all()
    items = []
    for site in sites:
        item = _site_to_dict(site)
        item["search_console"] = get_search_console_connection_status(db, site.id)
        items.append(item)
    return {"items": items}


@router.post("/sites", status_code=status.HTTP_201_CREATED)
@limiter.limit("60/minute")
async def create_site(request: Request, db: Session = Depends(get_db)):
    # Hem JSON hem de form-data isteğini destekleyerek site kaydı oluşturur.
    content_type = (request.headers.get("content-type") or "").lower()

    if "application/json" in content_type:
        payload = await request.json()
        domain = (payload.get("domain") or "").strip().lower()
        display_name = (payload.get("display_name") or "").strip()
        is_active = bool(payload.get("is_active", True))
    else:
        form = await request.form()
        domain = str(form.get("domain", "")).strip().lower()
        display_name = str(form.get("display_name", "")).strip()
        is_active = str(form.get("is_active", "true")).lower() in {"true", "1", "on", "yes"}

    if not domain:
        raise HTTPException(status_code=422, detail="Domain alanı zorunludur.")

    if not display_name:
        display_name = domain

    existing = db.query(Site).filter(Site.domain == domain).first()
    if existing:
        raise HTTPException(status_code=409, detail="Bu domain zaten kayıtlı.")

    site = Site(domain=domain, display_name=display_name, is_active=is_active)
    db.add(site)
    db.commit()
    db.refresh(site)

    bootstrap: dict[str, object] = {}
    if site.is_active:
        try:
            bootstrap["pagespeed"] = collect_pagespeed_metrics(db, site)
        except Exception as exc:  # noqa: BLE001
            bootstrap["pagespeed"] = {"state": "failed", "error": str(exc)}
        try:
            bootstrap["crawler"] = collect_crawler_metrics(db, site)
        except Exception as exc:  # noqa: BLE001
            bootstrap["crawler"] = {"state": "failed", "error": str(exc)}
        try:
            bootstrap["crux_history"] = collect_crux_history(db, site)
        except Exception as exc:  # noqa: BLE001
            bootstrap["crux_history"] = {"state": "failed", "error": str(exc)}
        db.commit()

    return {"item": _site_to_dict(site), "bootstrap": bootstrap}


@router.delete("/sites/{site_id}")
@limiter.limit("60/minute")
def delete_site(request: Request, site_id: int, db: Session = Depends(get_db)):
    # Site kaydını siler; ilişkili kayıtlar foreign key ile temizlenir.
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    db.delete(site)
    db.commit()

    return {"ok": True, "deleted_id": site_id}


@router.post("/sites/{site_id}/credentials", status_code=status.HTTP_201_CREATED)
@limiter.limit("60/minute")
async def create_site_credential(request: Request, site_id: int, db: Session = Depends(get_db)):
    # Google credential verisini düz metin yerine Fernet ile şifreleyerek saklar.
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    payload = await request.json()
    credential_type = str(payload.get("credential_type", "google")).strip().lower()
    credential_data = payload.get("credential_data")

    if credential_data is None:
        raise HTTPException(status_code=422, detail="credential_data alanı zorunludur.")

    serialized = json.dumps(credential_data, ensure_ascii=False)
    encrypted_data = encrypt_text(serialized)

    credential = SiteCredential(
        site_id=site.id,
        credential_type=credential_type,
        encrypted_data=encrypted_data,
    )
    db.add(credential)
    db.commit()
    db.refresh(credential)

    return {
        "item": {
            "id": credential.id,
            "site_id": credential.site_id,
            "credential_type": credential.credential_type,
        }
    }


@router.post("/sites/{site_id}/ga4", status_code=status.HTTP_201_CREATED)
@limiter.limit("60/minute")
async def upsert_site_ga4_property(request: Request, site_id: int, db: Session = Depends(get_db)):
    site = db.query(Site).filter(Site.id == site_id).first()
    if not site:
        raise HTTPException(status_code=404, detail="Site bulunamadı.")

    form = await request.form()
    existing = load_ga4_properties(get_ga4_credentials_record(db, site.id))
    updates: dict[str, str] = {}
    for key in ("web", "mweb", "android", "ios"):
        form_key = f"ga4_property_{key}"
        if form_key in form:
            updates[key] = str(form.get(form_key, "")).strip()

    # Backward compat: tek alan ile "web"e yaz
    if not updates and "ga4_property_id" in form:
        updates["web"] = str(form.get("ga4_property_id", "")).strip()

    merged = dict(existing)
    for k, v in updates.items():
        if v:
            merged[k] = v
        elif k in merged:
            del merged[k]

    if not merged:
        raise HTTPException(status_code=422, detail="En az bir GA4 property ID girmen gerekiyor.")

    record = upsert_ga4_properties(db, site.id, merged)
    return {"ok": True, "item": {"id": record.id, "site_id": record.site_id, "credential_type": record.credential_type}}
