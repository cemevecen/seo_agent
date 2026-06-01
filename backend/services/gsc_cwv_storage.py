"""GSC CWV ekran görüntüleri — disk + Postgres (Railway ephemeral disk için DB birincil)."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from backend.models import GscCwvScreenshot

LOGGER = logging.getLogger(__name__)

CWV_VARIANTS = frozenset({"full", "mobile", "desktop", "extra"})


def _guess_content_type(data: bytes, filename: str) -> str:
    name = (filename or "").lower()
    if name.endswith(".webp") or data.startswith(b"RIFF"):
        return "image/webp"
    if name.endswith(".jpg") or name.endswith(".jpeg") or data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    return "image/png"


def upsert_screenshot(db: Session, *, site_id: int, variant: str, data: bytes, filename: str = "") -> GscCwvScreenshot:
    if variant not in CWV_VARIANTS:
        raise ValueError(f"Geçersiz variant: {variant}")
    ct = _guess_content_type(data, filename)
    row = (
        db.query(GscCwvScreenshot)
        .filter(GscCwvScreenshot.site_id == site_id, GscCwvScreenshot.variant == variant)
        .first()
    )
    now = datetime.utcnow()
    if row is None:
        row = GscCwvScreenshot(
            site_id=site_id,
            variant=variant,
            content_type=ct,
            image_data=data,
            updated_at=now,
        )
        db.add(row)
    else:
        row.content_type = ct
        row.image_data = data
        row.updated_at = now
    db.commit()
    db.refresh(row)
    return row


def delete_screenshot(db: Session, *, site_id: int, variant: str) -> bool:
    if variant not in CWV_VARIANTS:
        raise ValueError(f"Geçersiz variant: {variant}")
    row = (
        db.query(GscCwvScreenshot)
        .filter(GscCwvScreenshot.site_id == site_id, GscCwvScreenshot.variant == variant)
        .first()
    )
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def load_screenshot(db: Session, *, site_id: int, variant: str) -> GscCwvScreenshot | None:
    return (
        db.query(GscCwvScreenshot)
        .filter(GscCwvScreenshot.site_id == site_id, GscCwvScreenshot.variant == variant)
        .first()
    )


def cwv_public_url(site_id: int, variant: str, updated_at: datetime | None) -> str:
    v = int(updated_at.timestamp()) if updated_at else 0
    return f"/search-console/cwv-image/{site_id}/{variant}?v={v}"


def build_gsc_cwv_urls(db: Session, *, site_id: int, domain_for_property: str) -> dict[str, str]:
    from urllib.parse import quote

    resource_id = f"sc-domain:{domain_for_property}" if domain_for_property else ""
    resource_param = quote(resource_id, safe="") if resource_id else ""
    out: dict[str, str] = {
        "resource_url": (
            f"https://search.google.com/search-console/core-web-vitals?resource_id={resource_param}&hl=en"
            if resource_param
            else ""
        ),
        "mobile_url": "",
        "desktop_url": "",
        "full_url": "",
        "extra_url": "",
    }
    for variant in ("mobile", "desktop", "full", "extra"):
        row = load_screenshot(db, site_id=site_id, variant=variant)
        if row and row.image_data:
            out[f"{variant}_url"] = cwv_public_url(site_id, variant, row.updated_at)
    return out


def write_disk_copy(domain_slug: str, variant: str, data: bytes, *, gsc_dir: Path) -> None:
    try:
        gsc_dir.mkdir(parents=True, exist_ok=True)
        (gsc_dir / f"{domain_slug}-cwv-{variant}.png").write_bytes(data)
    except OSError as exc:
        LOGGER.warning("gsc cwv disk write failed %s-%s: %s", domain_slug, variant, exc)


def delete_disk_copy(domain_slug: str, variant: str, *, gsc_dir: Path) -> None:
    path = gsc_dir / f"{domain_slug}-cwv-{variant}.png"
    try:
        if path.exists():
            path.unlink()
    except OSError as exc:
        LOGGER.warning("gsc cwv disk delete failed %s: %s", path, exc)
