from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.karma.config import KARMA_BY_SLUG, KARMA_GROUPS, KARMA_ITEMS
from backend.karma.data import get_karma_data
from backend.models import Site

router = APIRouter(tags=["karma"])


def _karma_sites(db: Session) -> list[dict]:
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


@router.get("/karma")
def karma_index():
    first = KARMA_ITEMS[0].slug
    return RedirectResponse(url=f"/karma/{first}", status_code=302)


@router.get("/karma/{slug}", response_class=HTMLResponse)
def karma_page(slug: str, request: Request, site_id: int | None = None, db: Session = Depends(get_db)):
    if slug not in KARMA_BY_SLUG:
        raise HTTPException(status_code=404, detail="Karma modülü bulunamadı")
    sites = _karma_sites(db)
    sid = site_id if site_id else _default_site_id(sites)
    item = KARMA_BY_SLUG[slug]
    from backend.main import templates

    return templates.TemplateResponse(
        request,
        "karma/page.html",
        {
            "request": request,
            "site_name": "KARMA",
            "domain": "karma",
            "karma_slug": slug,
            "karma_item": item,
            "karma_items": KARMA_ITEMS,
            "karma_groups": KARMA_GROUPS,
            "sites": sites,
            "site_id": sid,
        },
    )


@router.get("/api/karma/{slug}")
def karma_api(slug: str, site_id: int = 1, db: Session = Depends(get_db)):
    if slug not in KARMA_BY_SLUG:
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        data = get_karma_data(db, slug, site_id)
        return JSONResponse(data)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/api/karma")
def karma_meta():
    return JSONResponse(
        {
            "items": [
                {"slug": i.slug, "title": i.title, "group": i.group, "description": i.description, "order": i.order}
                for i in KARMA_ITEMS
            ],
            "groups": list(KARMA_GROUPS),
        }
    )
