"""SEO audit — Mac bridge scrape ingest (Railway HTTP crawl ile karışmaz)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import Site, UrlAuditRecord
from backend.services.seo_audit_runner import _persist_audit_result
from backend.services.warehouse import finish_collector_run, start_collector_run

LOGGER = logging.getLogger(__name__)

SOURCE = "seo_audit_scrape"

# UI polling — Mac bridge scrape progress (site_id → dict)
SEO_AUDIT_PROGRESS: dict[int, dict[str, Any]] = {}


def set_seo_audit_progress(site_id: int, payload: dict[str, Any]) -> None:
    SEO_AUDIT_PROGRESS[int(site_id)] = dict(payload or {})


def get_seo_audit_progress(site_id: int) -> dict[str, Any]:
    return dict(SEO_AUDIT_PROGRESS.get(int(site_id)) or {})


def _site_by_id_or_domain(db: Session, *, site_id: int | None, domain: str) -> Site | None:
    if site_id:
        return db.query(Site).filter(Site.id == int(site_id)).first()
    d = (domain or "").strip().lower().removeprefix("https://").removeprefix("http://").strip("/")
    if not d:
        return None
    variants = {d, d.removeprefix("www."), f"www.{d.removeprefix('www.')}"}
    return db.query(Site).filter(Site.domain.in_(list(variants))).first()


def ingest_seo_audit_scrape(
    db: Session,
    *,
    site_id: int | None = None,
    domain: str = "",
    rows: list[dict[str, Any]],
    replace_all: bool = False,
    collected_at: str = "",
    trigger_source: str = "seo_audit_scrape",
) -> dict[str, Any]:
    """Bridge scrape sonuçlarını url_audit_records'a yazar."""
    site = _site_by_id_or_domain(db, site_id=site_id, domain=domain)
    if site is None:
        return {"ok": False, "message": f"Site bulunamadı: id={site_id} domain={domain}"}

    clean_rows = [r for r in (rows or []) if isinstance(r, dict) and (r.get("url") or "").strip()]
    if not clean_rows:
        return {"ok": False, "message": "rows boş", "site_id": site.id}

    now = datetime.utcnow()
    if collected_at:
        try:
            now = datetime.fromisoformat(collected_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass

    run = start_collector_run(
        db,
        site_id=site.id,
        provider="seo_audit",
        strategy="scrape",
        target_url=f"https://{site.domain}",
        trigger_source=trigger_source or SOURCE,
    )
    ok = 0
    err = 0
    try:
        for row in clean_rows:
            url = str(row.get("url") or "").strip()
            try:
                row = dict(row)
                row.setdefault("source", SOURCE)
                checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
                row["checks"] = checks
                _persist_audit_result(
                    db,
                    site.id,
                    url,
                    row,
                    collected_at=now,
                    sitemap_source="ga4_scrape",
                )
                ok += 1
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("seo audit ingest row fail %s: %s", url, exc)
                err += 1
        deleted_old = 0
        if replace_all:
            deleted_old = (
                db.query(UrlAuditRecord)
                .filter(
                    UrlAuditRecord.site_id == site.id,
                    UrlAuditRecord.collected_at < now,
                )
                .delete(synchronize_session=False)
            )
        finish_collector_run(
            db,
            run,
            status="success",
            row_count=ok,
            summary={
                "source": SOURCE,
                "ok": ok,
                "error": err,
                "deleted_old": deleted_old,
                "replace_all": bool(replace_all),
            },
        )
        db.commit()
        return {
            "ok": True,
            "site_id": site.id,
            "domain": site.domain,
            "saved": ok,
            "error": err,
            "deleted_old": deleted_old,
            "source": SOURCE,
            "message": f"SEO scrape ingest · {ok} kayıt" + (f" · {deleted_old} eski silindi" if deleted_old else ""),
        }
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("seo audit scrape ingest failed")
        try:
            finish_collector_run(db, run, status="error", error_message=str(exc)[:400])
            db.commit()
        except Exception:
            db.rollback()
        return {"ok": False, "message": str(exc)[:240]}
