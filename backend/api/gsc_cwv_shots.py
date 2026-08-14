"""GSC CWV screenshot ingest (test / soft-capture)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.models import Site
from backend.services import gsc_cwv_storage as shot_store

router = APIRouter(tags=["gsc-cwv-shots"])


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


def _resolve_site(db: Session, *, site_key: str, site_domain: str) -> Site:
    key = (site_key or "").strip().lower()
    domain = (site_domain or "").strip().lower().removeprefix("www.")
    sites = db.query(Site).all()
    for s in sites:
        d = (s.domain or "").strip().lower()
        bare = d.removeprefix("www.")
        if key and (key in d or key == bare.split(".")[0]):
            return s
        if domain and (domain == bare or domain in d):
            return s
    aliases = {
        "doviz": ("doviz.com",),
        "sinemalar": ("sinemalar.com",),
    }
    for alias, ends in aliases.items():
        if key != alias:
            continue
        for s in sites:
            bare = (s.domain or "").strip().lower().removeprefix("www.")
            if any(bare.endswith(e) for e in ends):
                return s
    raise HTTPException(status_code=404, detail=f"Site bulunamadı: {site_key or site_domain}")


@router.post("/gsc-cwv/shots-ingest")
async def gsc_cwv_shots_ingest(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
    site_key: str = Form(""),
    site_domain: str = Form(""),
    source: str = Form("gsc_cwv_shots"),
    scraped_at: str = Form(""),
    files: list[UploadFile] = File(...),
    variants: list[str] = Form(default=[]),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    if not files:
        raise HTTPException(status_code=400, detail="files gerekli")
    site = _resolve_site(db, site_key=site_key, site_domain=site_domain)
    domain_slug = (site.domain or "site").replace(".", "-")
    # Avoid importing main — use static path convention
    gsc_dir = Path(__file__).resolve().parents[2] / "static" / "gsc"
    saved: list[str] = []
    for idx, upload in enumerate(files):
        variant = ""
        if idx < len(variants):
            variant = (variants[idx] or "").strip().lower()
        if not variant:
            name = (upload.filename or "").lower()
            for cand in ("mobile", "desktop", "full", "extra"):
                if cand in name:
                    variant = cand
                    break
        if variant not in shot_store.CWV_VARIANTS:
            continue
        content = await upload.read()
        if not content or len(content) < 100:
            continue
        shot_store.upsert_screenshot(
            db,
            site_id=int(site.id),
            variant=variant,
            data=content,
            filename=upload.filename or f"{variant}.png",
        )
        shot_store.write_disk_copy(domain_slug, variant, content, gsc_dir=gsc_dir)
        saved.append(variant)
    return {
        "ok": bool(saved),
        "site_id": site.id,
        "domain": site.domain,
        "saved": saved,
        "source": source,
        "scraped_at": scraped_at,
        "message": f"{len(saved)} shot kaydedildi",
    }
