"""Sayfa tarama — panel kuyruk + Mac daemon claim."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse
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


def _manual_limit_response(exc: store.ManualLimitExceeded) -> JSONResponse:
    quota = dict(exc.quota or {})
    retry = int(quota.get("retry_after_sec") or 1)
    return JSONResponse(
        status_code=429,
        content={"ok": False, "detail": quota.get("message") or "Saatte en fazla 3 tarama", **quota},
        headers={"Retry-After": str(max(1, retry))},
    )


def _begin_payload(page: str) -> dict[str, Any]:
    out = store.begin_manual(page)
    payload: dict[str, Any] = {"ok": True, **(out.get("quota") or {})}
    run = out.get("run")
    if run:
        payload.update(run)
    return payload


@router.get("/page-tarama/catalog")
def catalog() -> dict[str, Any]:
    return {"ok": True, "pages": store.PAGES, "jobs": store.JOBS}


@router.get("/page-tarama/quota")
def quota() -> dict[str, Any]:
    return {"ok": True, **store.quota_status()}


@router.post("/page-tarama/manual")
def manual(body: StartBody) -> Any:
    page = (body.page or "").strip()
    if page not in store.PAGES:
        raise HTTPException(status_code=400, detail="Bilinmeyen sayfa")
    try:
        return _begin_payload(page)
    except store.ManualLimitExceeded as exc:
        return _manual_limit_response(exc)


@router.post("/page-tarama/start")
def start(body: StartBody) -> Any:
    page = (body.page or "").strip()
    if page not in store.PAGES:
        raise HTTPException(status_code=400, detail="Bilinmeyen sayfa")
    if not any(s.get("kind") == "bridge" for s in store.jobs_for(page)):
        raise HTTPException(status_code=400, detail="no_bridge_jobs")
    try:
        return _begin_payload(page)
    except store.ManualLimitExceeded as exc:
        return _manual_limit_response(exc)


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
