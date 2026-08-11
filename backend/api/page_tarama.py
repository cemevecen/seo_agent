"""Sayfa tarama — panel kuyruk + Mac daemon claim."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from backend.config import settings
from backend.services import page_tarama as store

router = APIRouter(tags=["page-tarama"])


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


class StartBody(BaseModel):
    page: str = ""


class ResultBody(BaseModel):
    run_id: str = ""
    job_id: str = ""
    ok: bool = False
    message: str = ""
    running: bool = False


@router.get("/page-tarama/catalog")
def catalog() -> dict[str, Any]:
    return {"ok": True, "pages": store.PAGES, "jobs": store.JOBS}


@router.post("/page-tarama/start")
def start(body: StartBody) -> dict[str, Any]:
    page = (body.page or "").strip()
    if page not in store.PAGES:
        raise HTTPException(status_code=400, detail="Bilinmeyen sayfa")
    try:
        run = store.start_run(page)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "no_bridge_jobs") from exc
    return {"ok": True, **run}


@router.get("/page-tarama/progress")
def progress(run_id: str = "") -> dict[str, Any]:
    run = store.get_run((run_id or "").strip())
    if not run:
        raise HTTPException(status_code=404, detail="Kuyruk bulunamadı")
    return {"ok": True, **run}


@router.get("/page-tarama/claim")
def claim(
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    job = store.claim_next()
    return {"ok": True, "job": job}


@router.post("/page-tarama/result")
def result(
    body: ResultBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    store.touch_bridge()
    if body.running:
        store.mark_running(body.run_id, body.job_id, body.message)
        return {"ok": True}
    found = store.record_result(body.run_id, body.job_id, ok=body.ok, message=body.message)
    if not found:
        raise HTTPException(status_code=404, detail="İş bulunamadı")
    return {"ok": True}
