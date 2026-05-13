"""Gmail gelen kutusu (info@ / feedback@) API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import SupportInboxMessage, SupportInboxThread
from backend.rate_limiter import limiter
from backend.services import inbox_gmail_auth, inbox_llm, inbox_sync

LOGGER = logging.getLogger(__name__)

router = APIRouter(prefix="/inbox", tags=["inbox"])


class ReadBody(BaseModel):
    read: bool = True


class AnsweredBody(BaseModel):
    answered: bool = True


class SendBody(BaseModel):
    to: str = Field(..., min_length=3, max_length=512)
    subject: str = Field("", max_length=998)
    text: str = Field(..., min_length=1, max_length=50_000)


def _thread_or_404(db: Session, thread_id: int) -> SupportInboxThread:
    row = db.query(SupportInboxThread).filter(SupportInboxThread.id == thread_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Konuşma bulunamadı.")
    return row


@router.get("/status")
@limiter.limit("120/minute")
def inbox_status(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    from backend.config import settings

    return {
        "oauth_client_configured": inbox_gmail_auth.inbox_oauth_is_configured(),
        "connected": row is not None,
        "account_email": row.account_email if row else "",
        "query": settings.inbox_gmail_query,
        "openai_ready": bool((settings.openai_api_key or "").strip()),
        "redirect_uri": inbox_gmail_auth.get_inbox_oauth_redirect_uri(),
    }


@router.get("/oauth/start")
def inbox_oauth_start(request: Request, next: str = "/inbox"):
    if not inbox_gmail_auth.inbox_oauth_is_configured():
        return HTMLResponse(
            "Google OAuth istemcisi eksik (GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET).",
            status_code=400,
        )
    safe_next = next if next.startswith("/") else "/inbox"
    state = inbox_gmail_auth.encode_inbox_oauth_state(safe_next)
    flow = inbox_gmail_auth.build_inbox_oauth_flow(state=state)
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )
    return RedirectResponse(authorization_url, status_code=302)


@router.get("/oauth/callback")
def inbox_oauth_callback(request: Request, db: Session = Depends(get_db)):
    from googleapiclient.discovery import build

    err = request.query_params.get("error")
    if err:
        return HTMLResponse(f"Google OAuth reddedildi: {err}", status_code=400)
    state = request.query_params.get("state")
    if not state:
        return HTMLResponse("OAuth state eksik.", status_code=400)
    try:
        payload = inbox_gmail_auth.decode_inbox_oauth_state(state)
    except ValueError as exc:
        return HTMLResponse(str(exc), status_code=400)
    try:
        flow = inbox_gmail_auth.build_inbox_oauth_flow(state=state)
        flow.fetch_token(authorization_response=str(request.url))
        creds = flow.credentials
        svc = build("gmail", "v1", credentials=creds, cache_discovery=False)
        prof = svc.users().getProfile(userId="me").execute()
        email = str(prof.get("emailAddress") or "").strip()
        inbox_gmail_auth.save_inbox_credentials(db, creds, email)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox oauth callback failed")
        return HTMLResponse(f"Gmail bağlantısı tamamlanamadı: {exc}", status_code=500)
    return RedirectResponse(str(payload.get("return_path") or "/inbox"), status_code=302)


@router.delete("/oauth")
@limiter.limit("20/minute")
def inbox_oauth_disconnect(request: Request, db: Session = Depends(get_db)):
    ok = inbox_gmail_auth.delete_inbox_credentials(db)
    return {"disconnected": ok}


@router.post("/sync")
@limiter.limit("12/minute")
def inbox_sync_post(request: Request, db: Session = Depends(get_db)):
    try:
        out = inbox_sync.sync_inbox_threads(db, max_threads=35)
        return JSONResponse(out)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/threads")
@limiter.limit("120/minute")
def inbox_threads_list(
    request: Request,
    db: Session = Depends(get_db),
    route: str | None = Query(None, description="info|feedback|mixed"),
    limit: int = Query(80, ge=1, le=200),
):
    q = db.query(SupportInboxThread).order_by(SupportInboxThread.last_internal_ms.desc())
    if route in ("info", "feedback", "mixed"):
        q = q.filter(SupportInboxThread.route_tag == route)
    rows = q.limit(limit).all()
    items = []
    for t in rows:
        items.append(
            {
                "id": t.id,
                "gmail_thread_id": t.gmail_thread_id,
                "subject": t.subject,
                "snippet": (t.snippet or "")[:240],
                "route_tag": t.route_tag,
                "gmail_unread": t.gmail_unread,
                "answered_flag": t.answered_flag,
                "last_internal_ms": t.last_internal_ms,
                "last_synced_at": t.last_synced_at.isoformat() if t.last_synced_at else None,
                "has_summary": bool((t.ai_summary or "").strip()),
                "has_draft": bool((t.ai_draft_reply or "").strip()),
            }
        )
    return {"items": items}


@router.get("/threads/{thread_id}")
@limiter.limit("120/minute")
def inbox_thread_detail(request: Request, thread_id: int, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    msgs = (
        db.query(SupportInboxMessage)
        .filter(SupportInboxMessage.thread_id == t.id)
        .order_by(SupportInboxMessage.internal_ms.asc())
        .all()
    )
    return {
        "thread": {
            "id": t.id,
            "gmail_thread_id": t.gmail_thread_id,
            "subject": t.subject,
            "snippet": t.snippet,
            "route_tag": t.route_tag,
            "gmail_unread": t.gmail_unread,
            "answered_flag": t.answered_flag,
            "ai_summary": t.ai_summary,
            "ai_draft_reply": t.ai_draft_reply,
            "last_synced_at": t.last_synced_at.isoformat() if t.last_synced_at else None,
        },
        "messages": [
            {
                "id": m.id,
                "from": m.from_addr,
                "to": m.to_addr,
                "subject": m.subject,
                "body_text": m.body_text,
                "internal_ms": m.internal_ms,
                "is_outbound": m.is_outbound,
            }
            for m in msgs
        ],
    }


@router.post("/threads/{thread_id}/read")
@limiter.limit("60/minute")
def inbox_thread_set_read(
    request: Request,
    thread_id: int,
    body: ReadBody,
    db: Session = Depends(get_db),
):
    t = _thread_or_404(db, thread_id)
    try:
        inbox_sync.set_thread_gmail_read(db, gmail_thread_id=t.gmail_thread_id, read=body.read)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    db.refresh(t)
    return {"ok": True, "gmail_unread": t.gmail_unread}


@router.patch("/threads/{thread_id}/answered")
@limiter.limit("60/minute")
def inbox_thread_set_answered(
    request: Request,
    thread_id: int,
    body: AnsweredBody,
    db: Session = Depends(get_db),
):
    t = _thread_or_404(db, thread_id)
    t.answered_flag = bool(body.answered)
    db.commit()
    return {"ok": True, "answered_flag": t.answered_flag}


@router.post("/threads/{thread_id}/summarize")
@limiter.limit("20/minute")
def inbox_thread_summarize(request: Request, thread_id: int, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    msgs = (
        db.query(SupportInboxMessage)
        .filter(SupportInboxMessage.thread_id == t.id)
        .order_by(SupportInboxMessage.internal_ms.asc())
        .all()
    )
    parts = []
    for m in msgs:
        parts.append(f"---\nKimden: {m.from_addr}\nKonu: {m.subject}\n{m.body_text}\n")
    blob = "\n".join(parts)
    try:
        summary = inbox_llm.summarize_thread_tr_tr(blob)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("inbox summarize failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    t.ai_summary = summary
    db.commit()
    return {"summary": summary}


@router.post("/threads/{thread_id}/draft")
@limiter.limit("20/minute")
def inbox_thread_draft(request: Request, thread_id: int, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    msgs = (
        db.query(SupportInboxMessage)
        .filter(SupportInboxMessage.thread_id == t.id)
        .order_by(SupportInboxMessage.internal_ms.asc())
        .all()
    )
    parts = []
    for m in msgs:
        parts.append(f"---\nKimden: {m.from_addr}\nKonu: {m.subject}\n{m.body_text}\n")
    blob = "\n".join(parts)
    try:
        draft = inbox_llm.draft_reply_tr_tr(blob)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("inbox draft failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    t.ai_draft_reply = draft
    db.commit()
    return {"draft": draft}


@router.post("/threads/{thread_id}/send")
@limiter.limit("15/minute")
def inbox_thread_send(request: Request, thread_id: int, body: SendBody, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    subj = body.subject.strip() or (f"Re: {t.subject}" if t.subject else "Re:")
    try:
        mid = inbox_sync.send_reply_plain(
            db,
            gmail_thread_id=t.gmail_thread_id,
            to_email=body.to.strip(),
            subject=subj,
            text=body.text,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox send failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "gmail_message_id": mid}
