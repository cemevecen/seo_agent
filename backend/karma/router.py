from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.karma.config import TREND_BY_SLUG, TREND_GROUPS, TREND_ITEMS
from backend.karma.data import get_trend_data
from backend.models import Site

router = APIRouter(tags=["trend"])


def _trend_sites(db: Session) -> list[dict]:
    rows = (
        db.query(Site)
        .filter(Site.is_active.is_(True))
        .order_by(Site.id.asc())
        .all()
    )
    return [
        {"id": s.id, "domain": s.domain, "display_name": s.display_name or s.domain}
        for s in rows
    ]


def _default_site_id(sites: list[dict]) -> int:
    for s in sites:
        if "doviz" in (s.get("domain") or "").lower():
            return int(s["id"])
    return int(sites[0]["id"]) if sites else 1


def _render_trend_page(slug: str, request: Request, site_id: int | None, db: Session):
    if slug not in TREND_BY_SLUG:
        raise HTTPException(status_code=404, detail="Trend modülü bulunamadı")
    sites = _trend_sites(db)
    sid = site_id if site_id else _default_site_id(sites)
    item = TREND_BY_SLUG[slug]
    from backend.main import templates

    return templates.TemplateResponse(
        request,
        "trend/page.html",
        {
            "request": request,
            "site_name": "TREND",
            "domain": "trend",
            "trend_slug": slug,
            "trend_item": item,
            "trend_items": TREND_ITEMS,
            "trend_groups": TREND_GROUPS,
            "sites": sites,
            "site_id": sid,
            # geriye uyumluluk
            "karma_slug": slug,
            "karma_item": item,
            "karma_items": TREND_ITEMS,
            "karma_groups": TREND_GROUPS,
        },
    )


@router.get("/trend")
def trend_index():
    return RedirectResponse(url=f"/trend/{TREND_ITEMS[0].slug}", status_code=302)


@router.get("/trend/{slug}", response_class=HTMLResponse)
def trend_page(slug: str, request: Request, site_id: int | None = None, db: Session = Depends(get_db)):
    return _render_trend_page(slug, request, site_id, db)


@router.get("/karma")
def karma_index_legacy():
    return RedirectResponse(url="/trend", status_code=301)


@router.get("/karma/{slug}", response_class=HTMLResponse)
def karma_page_legacy(slug: str, request: Request, site_id: int | None = None, db: Session = Depends(get_db)):
    return _render_trend_page(slug, request, site_id, db)


@router.get("/api/trend/{slug}")
def trend_api(slug: str, site_id: int = 1, db: Session = Depends(get_db)):
    if slug not in TREND_BY_SLUG:
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        data = get_trend_data(db, slug, site_id)
        return JSONResponse(data)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/api/karma/{slug}")
def karma_api_legacy(slug: str, site_id: int = 1, db: Session = Depends(get_db)):
    return trend_api(slug, site_id, db)


@router.get("/api/trend")
def trend_meta():
    return JSONResponse(
        {
            "items": [
                {"slug": i.slug, "title": i.title, "group": i.group, "description": i.description, "order": i.order}
                for i in TREND_ITEMS
            ],
            "groups": list(TREND_GROUPS),
        }
    )


@router.get("/api/karma")
def karma_meta_legacy():
    return trend_meta()
