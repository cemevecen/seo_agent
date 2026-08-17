"""Sayfa tarama — panel kuyruk + Mac daemon claim."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
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


def _caller_quota_context(request: Request) -> tuple[str | None, bool]:
    """(email, unlimited). Owner/admin e-postaları + panel admin şifre oturumu limitsiz."""
    email: str | None = None
    try:
        from backend.services import app_member_auth as ama

        member = ama.member_from_request(request)
        if member and member.email:
            email = str(member.email).strip()
    except Exception:  # noqa: BLE001
        member = None
    if store.is_manual_limit_exempt(email):
        return email, True
    try:
        from backend.main import _is_admin_authenticated

        if _is_admin_authenticated(request):
            return email, True
    except Exception:  # noqa: BLE001
        pass
    return email, False


class StartBody(BaseModel):
    page: str = ""


class ResultBody(BaseModel):
    run_id: str = ""
    job_id: str = ""
    ok: bool = False
    message: str = ""
    running: bool = False
    phase: str = ""
    step: int | None = None
    total_steps: int | None = None
    platform: str = ""
    sub_label: str = ""
    worker: str = ""
    needs_login: bool = False


class RequeueBody(BaseModel):
    run_id: str = ""
    job_id: str = ""
    message: str = ""


class PingBody(BaseModel):
    worker: str = ""
    ready: dict[str, str] | None = None
    current: list[str] | None = None
    version: str = ""


class LeaseBody(BaseModel):
    job: str = ""
    slot: str = ""
    worker: str = ""
    ttl_sec: float = store.LEASE_TTL_SEC


def _manual_limit_response(exc: store.ManualLimitExceeded) -> JSONResponse:
    quota = dict(exc.quota or {})
    retry = int(quota.get("retry_after_sec") or 1)
    return JSONResponse(
        status_code=429,
        content={"ok": False, "detail": quota.get("message") or "At most 3 scans per hour", **quota},
        headers={"Retry-After": str(max(1, retry))},
    )


def _begin_payload(page: str, *, email: str | None = None, unlimited: bool = False) -> dict[str, Any]:
    out = store.begin_manual(page, email=email, unlimited=unlimited)
    payload: dict[str, Any] = {"ok": True, **(out.get("quota") or {})}
    run = out.get("run")
    if run:
        payload.update(run)
    return payload


@router.get("/page-tarama/catalog")
def catalog() -> dict[str, Any]:
    return {"ok": True, "pages": store.PAGES, "jobs": store.JOBS}


@router.get("/page-tarama/quota")
def quota(request: Request) -> dict[str, Any]:
    email, unlimited = _caller_quota_context(request)
    return {"ok": True, **store.quota_status(email=email, unlimited=unlimited)}


@router.post("/page-tarama/manual")
def manual(body: StartBody, request: Request) -> Any:
    page = (body.page or "").strip()
    if page not in store.PAGES:
        raise HTTPException(status_code=400, detail="Bilinmeyen sayfa")
    email, unlimited = _caller_quota_context(request)
    try:
        return _begin_payload(page, email=email, unlimited=unlimited)
    except store.ManualLimitExceeded as exc:
        return _manual_limit_response(exc)


@router.post("/page-tarama/start")
def start(body: StartBody, request: Request) -> Any:
    page = (body.page or "").strip()
    if page not in store.PAGES:
        raise HTTPException(status_code=400, detail="Bilinmeyen sayfa")
    if not any(s.get("kind") == "bridge" for s in store.jobs_for(page)):
        raise HTTPException(status_code=400, detail="no_bridge_jobs")
    email, unlimited = _caller_quota_context(request)
    try:
        return _begin_payload(page, email=email, unlimited=unlimited)
    except store.ManualLimitExceeded as exc:
        return _manual_limit_response(exc)


@router.get("/page-tarama/progress")
def progress(run_id: str = "") -> dict[str, Any]:
    run = store.get_run((run_id or "").strip())
    if not run:
        raise HTTPException(status_code=404, detail="Kuyruk bulunamadı")
    return {"ok": True, **run}


def _parse_ready(raw: str) -> dict[str, str] | None:
    """`play:ready,virgul:no_creds` → {"play": "ready", ...}. Boşsa None (eski worker)."""
    raw = (raw or "").strip()
    if not raw:
        return None
    out: dict[str, str] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" in chunk:
            jid, state = chunk.split(":", 1)
        else:
            jid, state = chunk, store.READY_OK
        jid = jid.strip()[:40]
        if jid:
            out[jid] = (state.strip() or store.READY_OK)[:40]
    return out or None


@router.get("/page-tarama/claim")
def claim(
    worker: str = "",
    ready: str = "",
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    job = store.claim_next(worker=worker, ready=_parse_ready(ready))
    return {"ok": True, "job": job}


@router.get("/page-tarama/workers")
def workers() -> dict[str, Any]:
    """Panel: hangi Mac online, ne yapıyor, hangi iş için giriş gerekiyor."""
    return {"ok": True, "workers": store.workers_public()}


@router.post("/page-tarama/bridge-ping")
def bridge_ping(
    body: PingBody | None = None,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Mac keepalive — bridge canlı; zombie job'ları progress_at ile sonsuza uzatma."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    name = (body.worker if body else "") or ""
    if name.strip():
        store.heartbeat_worker(
            name,
            ready=(body.ready if body else None),
            current=(body.current if body else None),
            version=(body.version if body else "") or "",
        )
    else:
        store.touch_bridge(refresh_inflight=False)
    return {"ok": True}


@router.post("/page-tarama/auto-lease")
def auto_lease(
    body: LeaseBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Zamanlı tarama tek-çalıştırma kirası — iki Mac aynı slotu koşmasın."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    out = store.auto_lease(body.job, body.slot, body.worker, ttl_sec=body.ttl_sec)
    return {"ok": True, **out}


@router.post("/page-tarama/fail-inflight")
def fail_inflight(
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Mac bridge restart — yarım kalan claimed/running işleri kapat."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    store.touch_bridge()
    n = store.fail_inflight_jobs(
        reason="Mac bridge restarted — Update page'i tekrar dene",
    )
    return {"ok": True, "failed": n}


@router.post("/page-tarama/requeue")
def requeue(
    body: RequeueBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Mac tarayıcı kilidi meşgul — işi kuyruğa geri koy (fail değil)."""
    _check_ingest_token(authorization, x_notification_ingest_token)
    store.touch_bridge(refresh_inflight=False)
    ok = store.requeue_claim(
        body.run_id,
        body.job_id,
        detail=body.message or "Waiting for previous scan · back in queue",
    )
    if not ok:
        raise HTTPException(status_code=404, detail="İş kuyruğa alınamadı")
    return {"ok": True}


@router.post("/page-tarama/result")
def result(
    body: ResultBody,
    authorization: str | None = Header(default=None),
    x_notification_ingest_token: str | None = Header(default=None),
) -> dict[str, Any]:
    _check_ingest_token(authorization, x_notification_ingest_token)
    store.touch_bridge()
    if body.running:
        store.mark_running(
            body.run_id,
            body.job_id,
            body.message,
            phase=body.phase or "",
            step=body.step,
            total_steps=body.total_steps,
            platform=body.platform or "",
            sub_label=body.sub_label or "",
            worker=body.worker or "",
        )
        return {"ok": True}
    found = store.record_result(
        body.run_id,
        body.job_id,
        ok=body.ok,
        message=body.message,
        worker=body.worker or "",
        needs_login=bool(body.needs_login),
    )
    if not found:
        raise HTTPException(status_code=404, detail="İş bulunamadı")
    return {"ok": True}
