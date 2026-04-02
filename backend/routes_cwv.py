"""Core Web Vitals (CrUX) sayfası — main.py yüklenmeden önce router olarak eklenir (404 önlemi)."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from backend.collectors.crux_history import collect_crux_history
from backend.database import SessionLocal
from backend.models import Site

router = APIRouter(tags=["core-web-vitals"])


@router.get("/cwv", include_in_schema=False)
@router.get("/core-web-vitals")
@router.get("/core-web-vitals/", include_in_schema=False)
def core_web_vitals_page(request: Request):
    import backend.main as main_mod

    return main_mod._core_web_vitals_page_impl(request)


@router.get("/api/core-web-vitals", include_in_schema=False)
def core_web_vitals_api_alias():
    return RedirectResponse("/cwv", status_code=302)


@router.get("/core_web_vitals", include_in_schema=False)
def core_web_vitals_underscore_alias():
    return RedirectResponse("/cwv", status_code=302)


@router.post("/core-web-vitals/refresh/{site_id}")
@router.post("/cwv/refresh/{site_id}", include_in_schema=False)
def core_web_vitals_refresh(site_id: int):
    target = "/cwv"
    with SessionLocal() as db:
        site = db.query(Site).filter(Site.id == site_id).first()
        if site is None:
            return RedirectResponse(target, status_code=302)
        try:
            collect_crux_history(db, site)
            db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
    return RedirectResponse(f"{target}#site-{site_id}", status_code=302)
