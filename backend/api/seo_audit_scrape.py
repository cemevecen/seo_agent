"""SEO audit Mac bridge scrape — URL listesi + ingest + progress."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.models import Site
from backend.services.seo_audit_runner import collect_seo_audit_url_entries
from backend.services.seo_audit_store import ingest_seo_audit_scrape

router = APIRouter(tags=["seo-audit-scrape"])


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


def _session_ok(request: Request) -> bool:
    try:
        return bool(getattr(request.state, "user", None) or request.session.get("user"))
    except Exception:
        return False


class SeoAuditIngestBody(BaseModel):
    site_id: int | None = None
    domain: str = ""
    rows: list[dict[str, Any]] = Field(default_factory=list)
    replace_all: bool = False
    collected_at: str = ""
    trigger_source: str = "seo_audit_scrape"


class SeoAuditProgressBody(BaseModel):
    site_id: int
    running: bool = True
    total: int = 0
    done: int = 0
    ok: int = 0
    error: int = 0
    current: str = ""
    message: str = ""


@router.get("/seo-audit/urls")
def seo_audit_urls(
    request: Request,
    db: Session = Depends(get_db),
    site_id: int | None = Query(default=None),
    domain: str = Query(default=""),
    limit: int = Query(default=500, ge=50, le=2000),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """GA4 top trafik URL listesi — bridge scrape için."""
    token_ok = False
    try:
        _check_ingest_token(authorization, x_notification_ingest_token)
        token_ok = True
    except HTTPException:
        if not _session_ok(request):
            raise
    site: Site | None = None
    if site_id:
        site = db.query(Site).filter(Site.id == site_id).first()
    elif domain:
        d = domain.strip().lower().removeprefix("https://").removeprefix("http://").strip("/")
        variants = {d, d.removeprefix("www."), f"www.{d.removeprefix('www.')}"}
        site = db.query(Site).filter(Site.domain.in_(list(variants))).first()
    if site is None:
        raise HTTPException(status_code=404, detail="Site not found")
    progress: dict[str, Any] = {}
    entries = collect_seo_audit_url_entries(
        site.id, site.domain or "", progress, limit=limit
    )
    return {
        "ok": True,
        "site_id": site.id,
        "domain": site.domain,
        "limit": limit,
        "count": len(entries),
        "urls": [e["url"] for e in entries],
        "entries": entries,
        "auth": "token" if token_ok else "session",
        "note": progress.get("current") or "",
    }


@router.post("/seo-audit/ingest")
def seo_audit_ingest(
    body: SeoAuditIngestBody,
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    result = ingest_seo_audit_scrape(
        db,
        site_id=body.site_id,
        domain=body.domain,
        rows=body.rows,
        replace_all=body.replace_all,
        collected_at=body.collected_at,
        trigger_source=body.trigger_source,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message") or "ingest failed")
    return result


@router.post("/seo-audit/progress")
def seo_audit_progress_push(
    body: SeoAuditProgressBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Bridge scrape progress → UI polling (/api/seo-audit/{id}/status)."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    from backend.services.seo_audit_store import set_seo_audit_progress

    set_seo_audit_progress(
        int(body.site_id),
        {
            "running": bool(body.running),
            "total": int(body.total or 0),
            "done": int(body.done or 0),
            "ok": int(body.ok or 0),
            "error": int(body.error or 0),
            "current": str(body.current or body.message or ""),
            "source": "seo_audit_scrape",
        },
    )
    return {"ok": True}
