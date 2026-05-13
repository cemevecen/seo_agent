"""Gmail gelen kutusu (info@ / feedback@) API."""

from __future__ import annotations

import logging
import re
import json
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from sqlalchemy import func
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
    """Gmail gönder; ``text`` veya ``body`` anahtarı kabul edilir (boşluklar uçta temizlenir)."""

    model_config = ConfigDict(extra="ignore")

    to: str = Field(default="", max_length=512)
    subject: str = Field(default="", max_length=998)
    text: str = Field(
        default="",
        max_length=50_000,
        validation_alias=AliasChoices("text", "body"),
    )
    reply_to_gmail_message_id: str | None = Field(default=None, max_length=128)


class ReplyTemplatesBody(BaseModel):
    """İsteğe bağlı JSON gövde; ``focus_gmail_message_id`` hangi gelen iletiye şablon üretileceğini seçer."""

    model_config = ConfigDict(extra="ignore")

    focus_gmail_message_id: str | None = Field(default=None, max_length=128)


def _thread_or_404(db: Session, thread_id: int) -> SupportInboxThread:
    row = db.query(SupportInboxThread).filter(SupportInboxThread.id == thread_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Konuşma bulunamadı.")
    return row


def _resolve_inbound_focus(
    msgs: list[SupportInboxMessage], focus_gmail_message_id: str | None
) -> SupportInboxMessage | None:
    fid = (focus_gmail_message_id or "").strip()
    if fid:
        for m in msgs:
            if m.is_outbound:
                continue
            if (m.gmail_message_id or "").strip() == fid:
                return m
    for m in reversed(msgs):
        if not m.is_outbound:
            return m
    return None


def _thread_blob_for_reply_templates(msgs: list[SupportInboxMessage], focus: SupportInboxMessage | None) -> str:
    parts: list[str] = []
    for m in msgs:
        direction = "giden" if m.is_outbound else "gelen"
        parts.append(
            f"---\nKimden: {m.from_addr}\nKonu: {m.subject}\nYön: {direction}\n{(m.body_text or '').strip()}\n"
        )
    blob = "\n".join(parts)
    if focus is not None:
        direction = "giden" if focus.is_outbound else "gelen"
        blob += (
            "\n\n=== YANITLANACAK İLETİ (öncelik: buna yanıt ver) ===\n"
            f"Kimden: {focus.from_addr}\nKonu: {focus.subject}\nYön: {direction}\n"
            f"{(focus.body_text or '').strip()}\n=== BİTİŞ ===\n"
        )
    return blob


def _body_preview(text: str | None, *, max_sentences: int = 2, max_chars: int = 320) -> str:
    """İlk bir–iki cümle veya kısa kesit; liste / kart önizlemesi için."""
    raw = (text or "").replace("\r\n", "\n").strip()
    if not raw:
        return ""
    one_line = re.sub(r"\s+", " ", raw).strip()
    chunks = re.split(r"(?<=[.!?…])\s+", one_line)
    picked: list[str] = []
    for ch in chunks:
        c = ch.strip()
        if not c:
            continue
        picked.append(c)
        if len(picked) >= max_sentences:
            break
    s = " ".join(picked).strip() if picked else one_line
    if len(s) > max_chars:
        cut = s[: max_chars - 1]
        s = cut.rsplit(" ", 1)[0] + "…" if " " in cut else cut + "…"
    return s


def _latest_message_body_by_thread(db: Session, thread_ids: list[int]) -> dict[int, str]:
    """Her konuşma için zaman damgası en yeni iletinin düz metin gövdesi."""
    if not thread_ids:
        return {}
    subq = (
        db.query(
            SupportInboxMessage.thread_id.label("tid"),
            func.max(SupportInboxMessage.internal_ms).label("mx"),
        )
        .filter(SupportInboxMessage.thread_id.in_(thread_ids))
        .group_by(SupportInboxMessage.thread_id)
        .subquery()
    )
    rows = (
        db.query(SupportInboxMessage.thread_id, SupportInboxMessage.body_text)
        .join(
            subq,
            (SupportInboxMessage.thread_id == subq.c.tid)
            & (SupportInboxMessage.internal_ms == subq.c.mx),
        )
        .all()
    )
    return {int(tid): (body or "") for tid, body in rows}


@router.get("/status")
@limiter.limit("120/minute")
def inbox_status(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    from backend.config import settings

    return {
        "oauth_client_configured": inbox_gmail_auth.inbox_oauth_is_configured(),
        "connected": row is not None,
        "account_email": row.account_email if row else "",
        "gmail_inbox_query": settings.inbox_gmail_query,
        "openai_ready": bool((settings.openai_api_key or "").strip()),
        "inbox_llm_ready": inbox_llm.inbox_llm_any_configured(),
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
    # include_granted_scopes=True incremental modda önceki izinleri (ör. Search Console
    # webmasters.readonly) yanıta ekler; Flow yalnızca Gmail kapsamları beklediği için
    # "Scope has changed ..." hatasına yol açar. Inbox bu istemciyle aynı Client ID kullanıyor.
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="false",
    )
    return RedirectResponse(authorization_url, status_code=302)


@router.get("/oauth/callback")
def inbox_oauth_callback(request: Request, db: Session = Depends(get_db)):
    import httpx as _httpx

    err = request.query_params.get("error")
    if err:
        return HTMLResponse(f"Google OAuth reddedildi: {err}", status_code=400)
    state = request.query_params.get("state")
    if not state:
        return HTMLResponse("OAuth state eksik.", status_code=400)
    code = request.query_params.get("code")
    if not code:
        return HTMLResponse("OAuth kodu eksik.", status_code=400)
    try:
        payload = inbox_gmail_auth.decode_inbox_oauth_state(state)
    except ValueError as exc:
        return HTMLResponse(str(exc), status_code=400)
    try:
        creds = inbox_gmail_auth.exchange_inbox_authorization_code(code)
        # googleapiclient.build() + execute() google-auth scope validasyonu tetikler.
        # Bunun yerine doğrudan httpx ile Gmail profile endpoint'ini çağırıyoruz.
        prof_resp = _httpx.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/profile",
            headers={"Authorization": f"Bearer {creds.token}"},
            timeout=15.0,
        )
        if prof_resp.status_code != 200:
            raise RuntimeError(f"Gmail profile alınamadı: {prof_resp.status_code} {prof_resp.text[:200]}")
        email = str(prof_resp.json().get("emailAddress") or "").strip()
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
@limiter.limit("30/minute")
def inbox_sync_post(request: Request, db: Session = Depends(get_db)):
    try:
        out = inbox_sync.sync_inbox_threads(db, max_threads=35)
        return JSONResponse(out)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox sync beklenmeyen hata")
        raise HTTPException(status_code=502, detail=f"Senkron hatası: {exc}") from exc


@router.post("/sync-stream")
@limiter.limit("30/minute")
def inbox_sync_stream(request: Request, db: Session = Depends(get_db)):
    """NDJSON satırları: senkron ilerlemesi (gerçek zamanlı progress için)."""

    def ndjson_iter():
        try:
            for evt in inbox_sync.iter_sync_inbox_threads(db, max_threads=35):
                yield (json.dumps(evt, ensure_ascii=False) + "\n").encode("utf-8")
        except RuntimeError as exc:
            err = {"type": "error", "message": str(exc), "pct": 0}
            yield (json.dumps(err, ensure_ascii=False) + "\n").encode("utf-8")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("inbox sync-stream")
            err = {"type": "error", "message": f"Senkron hatası: {exc}", "pct": 0}
            yield (json.dumps(err, ensure_ascii=False) + "\n").encode("utf-8")

    headers = {
        "Cache-Control": "no-store, no-transform",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(ndjson_iter(), media_type="application/x-ndjson", headers=headers)


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
    tid_list = [t.id for t in rows]
    latest_bodies = _latest_message_body_by_thread(db, tid_list)
    items = []
    for t in rows:
        last_body = (latest_bodies.get(t.id) or "").strip()
        preview_src = last_body or (t.snippet or "")
        items.append(
            {
                "id": t.id,
                "gmail_thread_id": t.gmail_thread_id,
                "subject": t.subject,
                "snippet": (t.snippet or "")[:240],
                "message_preview": _body_preview(preview_src),
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
                "gmail_message_id": m.gmail_message_id,
                "from": m.from_addr,
                "to": m.to_addr,
                "subject": m.subject,
                "body_preview": _body_preview(m.body_text),
                "body_text": m.body_text,
                "internal_ms": m.internal_ms,
                "is_outbound": m.is_outbound,
            }
            for m in msgs
        ],
    }


@router.post("/threads/{thread_id}/refresh-bodies")
@limiter.limit("30/minute")
def inbox_thread_refresh_bodies(request: Request, thread_id: int, db: Session = Depends(get_db)):
    """İleti gövdelerini Gmail'den tekrar çeker (attachmentId / büyük gövde)."""
    try:
        out = inbox_sync.refresh_thread_bodies_from_gmail(db, thread_id=thread_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(out)


@router.post("/threads/{thread_id}/read")
@limiter.limit("60/minute")
def inbox_thread_set_read(
    request: Request,
    thread_id: int,
    read_payload: ReadBody = Body(default_factory=ReadBody),
    db: Session = Depends(get_db),
):
    t = _thread_or_404(db, thread_id)
    try:
        inbox_sync.set_thread_gmail_read(db, gmail_thread_id=t.gmail_thread_id, read=read_payload.read)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    db.refresh(t)
    return {"ok": True, "gmail_unread": t.gmail_unread}


@router.patch("/threads/{thread_id}/answered")
@limiter.limit("60/minute")
def inbox_thread_set_answered(
    request: Request,
    thread_id: int,
    payload: AnsweredBody,
    db: Session = Depends(get_db),
):
    t = _thread_or_404(db, thread_id)
    try:
        inbox_sync.set_thread_gmail_answered(
            db, gmail_thread_id=t.gmail_thread_id, answered=bool(payload.answered)
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    db.refresh(t)
    return {"ok": True, "answered_flag": t.answered_flag}


@router.delete("/threads/{thread_id}")
@limiter.limit("30/minute")
def inbox_thread_delete(request: Request, thread_id: int, db: Session = Depends(get_db)):
    _thread_or_404(db, thread_id)
    try:
        inbox_sync.trash_thread_gmail_and_delete_local(db, thread_id=thread_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


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


@router.post("/threads/{thread_id}/reply-templates")
@limiter.limit("20/minute")
def inbox_thread_reply_templates(
    request: Request,
    thread_id: int,
    provider: str | None = Query(default=None, max_length=12, description="groq|gemini|openai"),
    payload: ReplyTemplatesBody = Body(default_factory=ReplyTemplatesBody),
    db: Session = Depends(get_db),
):
    """Bağlı LLM (Groq / Gemini / OpenAI) ile 3 Türkçe yanıt şablonu üretir."""
    t = _thread_or_404(db, thread_id)
    msgs = (
        db.query(SupportInboxMessage)
        .filter(SupportInboxMessage.thread_id == t.id)
        .order_by(SupportInboxMessage.internal_ms.asc())
        .all()
    )
    pref = (provider or "").strip().lower() or None
    if pref and pref not in ("groq", "gemini", "openai"):
        raise HTTPException(
            status_code=400,
            detail="Geçersiz provider. Kullanın: groq, gemini veya openai.",
        )
    fid = payload.focus_gmail_message_id
    focus = _resolve_inbound_focus(msgs, (fid or "").strip() or None)
    blob = _thread_blob_for_reply_templates(msgs, focus)
    try:
        templates, used = inbox_llm.reply_templates_three_tr_tr(blob, preferred_provider=pref)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox reply-templates failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    focus_payload: dict[str, Any] | None = None
    if focus is not None:
        focus_payload = {
            "gmail_message_id": focus.gmail_message_id,
            "from": focus.from_addr,
            "subject": (focus.subject or t.subject or "").strip(),
        }
    return {"templates": templates, "provider": used, "focus": focus_payload}


@router.post("/threads/{thread_id}/send")
@limiter.limit("15/minute")
def inbox_thread_send(request: Request, thread_id: int, payload: SendBody, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    to_email = payload.to.strip()
    if len(to_email) < 3 or "@" not in to_email:
        raise HTTPException(status_code=400, detail="Alıcı e-posta adresi geçersiz veya boş.")
    body_text = (payload.text or "").strip()
    if not body_text:
        raise HTTPException(
            status_code=400,
            detail="Gövde (metin) boş. Göndermeden önce ileti metnini doldurun veya `text` / `body` alanını JSON’da gönderin.",
        )
    subj = payload.subject.strip() or (f"Re: {t.subject}" if t.subject else "Re:")
    try:
        mid = inbox_sync.send_reply_plain(
            db,
            gmail_thread_id=t.gmail_thread_id,
            to_email=to_email,
            subject=subj,
            text=body_text,
            reply_to_gmail_message_id=(payload.reply_to_gmail_message_id or "").strip() or None,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox send failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "gmail_message_id": mid}
