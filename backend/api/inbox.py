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

from backend.config import settings
from backend.database import get_db
from backend.models import SupportInboxMessage, SupportInboxThread
from backend.rate_limiter import limiter
from backend.services import inbox_gmail_auth, inbox_llm, inbox_sync
from backend.services.inbox_visit_report import render_ziyaret_message_html, ziyaret_thread_preview

_INBOX_ACTION_AUTH_COOKIE = "seo_inbox_action_auth"


def _require_inbox_action_auth(request: Request) -> None:
    """Inbox aksiyon koruması — INBOX_ACTION_PASSWORD tanımlıysa cookie doğrula."""
    import hashlib, hmac as _hmac
    from backend.config import settings as _settings
    raw_pwd = (getattr(_settings, "inbox_action_password", "") or "").strip()
    if not raw_pwd:
        return  # Şifre tanımlanmamışsa herkese açık
    token = str(request.cookies.get(_INBOX_ACTION_AUTH_COOKIE) or "")
    if not token:
        raise HTTPException(status_code=403, detail="inbox_action_auth_required")
    secret = str(getattr(_settings, "secret_key", "") or "").encode("utf-8")
    expected = _hmac.new(secret, ("inbox_action:" + raw_pwd).encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    if not _hmac.compare_digest(token, expected):
        raise HTTPException(status_code=403, detail="inbox_action_auth_required")

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


def _extract_sender_email_from_any_body(msgs: list[SupportInboxMessage]) -> str | None:
    """Feedback formları gibi gövde içinde 'Email: ...' geçen iletilerde gerçek adresi bulur."""
    # Pattern: Email: veya E-posta: veya Mail:
    pattern = re.compile(
        r"(?:Email|E-posta|Mail)\s*[:：]\s*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})",
        re.IGNORECASE,
    )
    for m in reversed(msgs):
        if m.is_outbound:
            continue
        text = m.body_text or ""
        match = pattern.search(text)
        if match:
            return match.group(1).strip()
    return None


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

    return {
        "oauth_client_configured": inbox_gmail_auth.inbox_oauth_is_configured(),
        "connected": row is not None,
        "account_email": row.account_email if row else "",
        "gmail_inbox_query": settings.inbox_gmail_query,
        "openai_ready": bool((settings.openai_api_key or "").strip()),
        "inbox_llm_ready": inbox_llm.inbox_llm_any_configured(),
        "redirect_uri": inbox_gmail_auth.get_inbox_oauth_redirect_uri(request=request),
        "oauth_login_hint": (getattr(settings, "inbox_oauth_login_hint", "") or "").strip(),
        "client_id_suffix": (
            settings.google_client_id.strip()[-12:] if settings.google_client_id.strip() else ""
        ),
    }


@router.get("/oauth/start")
def inbox_oauth_start(request: Request, next: str = "/inbox"):
    if not inbox_gmail_auth.inbox_oauth_is_configured():
        return HTMLResponse(
            "Google OAuth istemcisi eksik (GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET).",
            status_code=400,
        )
    try:
        safe_next = next if next.startswith("/") else "/inbox"
        state = inbox_gmail_auth.encode_inbox_oauth_state(safe_next, request=request)
        flow = inbox_gmail_auth.build_inbox_oauth_flow(state=state, request=request)
        auth_kwargs: dict[str, str] = {
            "access_type": "offline",
            "prompt": "select_account consent",
            "include_granted_scopes": "false",
        }
        hint = (settings.inbox_oauth_login_hint or "").strip()
        if hint and "@" in hint:
            auth_kwargs["login_hint"] = hint
        authorization_url, _ = flow.authorization_url(**auth_kwargs)
        return RedirectResponse(authorization_url, status_code=302)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox oauth start failed")
        from urllib.parse import quote
        return RedirectResponse(
            url=f"/inbox?oauth_error={quote(str(exc)[:160])}",
            status_code=302,
        )


@router.get("/oauth/callback")
def inbox_oauth_callback(request: Request, db: Session = Depends(get_db)):
    import httpx as _httpx

    err = request.query_params.get("error")
    if err:
        if err == "access_denied":
            return RedirectResponse(url="/inbox?oauth_error=access_denied", status_code=302)
        return RedirectResponse(url=f"/inbox?oauth_error={err}", status_code=302)
    state = request.query_params.get("state")
    if not state:
        return HTMLResponse("OAuth state eksik.", status_code=400)
    code = request.query_params.get("code")
    if not code:
        return HTMLResponse("OAuth kodu eksik.", status_code=400)
    try:
        payload = inbox_gmail_auth.decode_inbox_oauth_state(state, request=request)
    except ValueError as exc:
        from urllib.parse import quote
        return RedirectResponse(url=f"/inbox?oauth_error={quote(str(exc)[:120])}", status_code=302)
    try:
        creds = inbox_gmail_auth.exchange_inbox_authorization_code(code, request=request)
        # googleapiclient.build() + execute() google-auth scope validasyonu tetikler.
        # Bunun yerine doğrudan httpx ile Gmail profile endpoint'ini çağırıyoruz.
        prof_resp = _httpx.get(
            "https://gmail.googleapis.com/gmail/v1/users/me/profile",
            headers={"Authorization": f"Bearer {creds.token}"},
            timeout=15.0,
        )
        if prof_resp.status_code != 200:
            raise RuntimeError(f"Gmail profil alınamadı: {prof_resp.status_code}")
        email = str(prof_resp.json().get("emailAddress") or "").strip()
        inbox_gmail_auth.save_inbox_credentials(db, creds, email)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox oauth callback failed")
        from urllib.parse import quote
        return RedirectResponse(url=f"/inbox?oauth_error={quote(str(exc)[:120])}", status_code=302)
    return RedirectResponse(str(payload.get("return_path") or "/inbox"), status_code=302)


@router.get("/action-auth/status")
@limiter.limit("120/minute")
def inbox_action_auth_status(request: Request):
    """Inbox aksiyon şifresinin cookie'de geçerli olup olmadığını döndürür."""
    import hashlib, hmac as _hmac
    from backend.config import settings as _settings
    raw_pwd = (getattr(_settings, "inbox_action_password", "") or "").strip()
    if not raw_pwd:
        return {"authenticated": True, "required": False}
    token = str(request.cookies.get(_INBOX_ACTION_AUTH_COOKIE) or "")
    if not token:
        return {"authenticated": False, "required": True}
    secret = str(getattr(_settings, "secret_key", "") or "").encode("utf-8")
    expected = _hmac.new(secret, ("inbox_action:" + raw_pwd).encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    ok = _hmac.compare_digest(token, expected)
    return {"authenticated": ok, "required": True}


@router.post("/action-auth")
@limiter.limit("20/minute")
async def inbox_action_auth_set(request: Request):
    """Inbox aksiyon şifresini doğrula ve oturum cookie'si yaz."""
    import hashlib, hmac as _hmac
    from backend.config import settings as _settings
    raw_pwd = (getattr(_settings, "inbox_action_password", "") or "").strip()
    if not raw_pwd:
        resp = JSONResponse({"ok": True})
        return resp
    form = await request.form()
    submitted = str(form.get("password") or "").strip()
    if submitted != raw_pwd:
        return JSONResponse({"ok": False, "error": "Yanlış şifre"})
    secret = str(getattr(_settings, "secret_key", "") or "").encode("utf-8")
    token = _hmac.new(secret, ("inbox_action:" + raw_pwd).encode("utf-8"), digestmod=hashlib.sha256).hexdigest()
    secure = request.url.scheme == "https"
    resp = JSONResponse({"ok": True})
    resp.set_cookie(
        key=_INBOX_ACTION_AUTH_COOKIE,
        value=token,
        httponly=True,
        secure=secure,
        samesite="lax",
        max_age=86400 * 7,
        path="/",
    )
    return resp


@router.delete("/oauth")
@limiter.limit("20/minute")
def inbox_oauth_disconnect(request: Request, db: Session = Depends(get_db)):
    _require_inbox_action_auth(request)
    ok = inbox_gmail_auth.delete_inbox_credentials(db)
    return {"disconnected": ok}


@router.post("/repair-route-tags")
@limiter.limit("10/minute")
def inbox_repair_route_tags(request: Request, db: Session = Depends(get_db)):
    """Mevcut konuşmaların sekme etiketlerini To/Delivered-To alanlarından yeniden hesaplar."""
    _require_inbox_action_auth(request)
    out = inbox_sync.repair_misrouted_inbox_threads(db)
    return JSONResponse(out)


@router.post("/sync")
@limiter.limit("30/minute")
def inbox_sync_post(request: Request, db: Session = Depends(get_db)):
    try:
        out = inbox_sync.sync_inbox_threads(
            db,
            max_threads=inbox_sync.INBOX_SYNC_MAX_THREADS,
            lookback_days=90,
        )
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

    _require_inbox_action_auth(request)
    def ndjson_iter():
        try:
            for evt in inbox_sync.iter_sync_inbox_all_routes(
                db,
                max_threads_per_route=inbox_sync.INBOX_SYNC_MAX_THREADS,
                lookback_days=90,
            ):
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
    route: str | None = Query(None, description="info|feedback|sinemalar|firebase|ziyaret|tome"),
    limit: int = Query(inbox_sync.INBOX_LIST_LIMIT, ge=1, le=200),
):
    q = db.query(SupportInboxThread).order_by(SupportInboxThread.last_internal_ms.desc())
    if route in ("info", "feedback", "sinemalar", "firebase", "ziyaret", "tome"):
        q = q.filter(SupportInboxThread.route_tag == route)
    rows = q.limit(limit).all()
    tid_list = [t.id for t in rows]
    latest_bodies = _latest_message_body_by_thread(db, tid_list)
    items = []
    for t in rows:
        last_body = (latest_bodies.get(t.id) or "").strip()
        preview_src = last_body or (t.snippet or "")
        if t.route_tag == "ziyaret" and last_body:
            message_preview = ziyaret_thread_preview(last_body)
        else:
            message_preview = _body_preview(preview_src)
        items.append(
            {
                "id": t.id,
                "gmail_thread_id": t.gmail_thread_id,
                "subject": t.subject,
                "snippet": (t.snippet or "")[:240],
                "message_preview": message_preview,
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
                "body_display_html": (
                    render_ziyaret_message_html(
                        body_html=getattr(m, "body_html", "") or "",
                        body_text=m.body_text or "",
                    )
                    if t.route_tag == "ziyaret"
                    else None
                ),
                "internal_ms": m.internal_ms,
                "is_outbound": m.is_outbound,
            }
            for m in msgs
        ],
    }


@router.post("/threads/{thread_id}/refresh-bodies")
@limiter.limit("30/minute")
def inbox_thread_refresh_bodies(request: Request, thread_id: int, db: Session = Depends(get_db)):
    """İleti gövdelerini Gmail'den tekrar çeker (attachmentI / büyük gövde)."""
    try:
        out = inbox_sync.refresh_thread_bodies_from_gmail(db, thread_id=thread_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(out)


@router.post("/threads/{thread_id}/read")
@limiter.limit("60/minute")
async def inbox_thread_set_read(
    request: Request,
    thread_id: int,
    db: Session = Depends(get_db),
):
    _require_inbox_action_auth(request)
    # Manual parse to avoid Pydantic forward ref issues
    read_val: bool = True
    try:
        raw = await request.body()
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                read_val = bool(parsed.get("read", True))
    except Exception:  # noqa: BLE001
        pass
    t = _thread_or_404(db, thread_id)
    try:
        inbox_sync.set_thread_gmail_read(db, gmail_thread_id=t.gmail_thread_id, read=read_val)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    db.refresh(t)
    return {"ok": True, "gmail_unread": t.gmail_unread}


@router.patch("/threads/{thread_id}/answered")
@limiter.limit("60/minute")
async def inbox_thread_set_answered(
    request: Request,
    thread_id: int,
    db: Session = Depends(get_db),
):
    _require_inbox_action_auth(request)
    ans_val: bool = True
    try:
        raw = await request.body()
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                ans_val = bool(parsed.get("answered", True))
    except Exception:  # noqa: BLE001
        pass
    t = _thread_or_404(db, thread_id)
    try:
        inbox_sync.set_thread_gmail_answered(
            db, gmail_thread_id=t.gmail_thread_id, answered=ans_val
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    db.refresh(t)
    return {"ok": True, "answered_flag": t.answered_flag}


@router.delete("/threads/{thread_id}")
@limiter.limit("30/minute")
def inbox_thread_delete(request: Request, thread_id: int, db: Session = Depends(get_db)):
    _thread_or_404(db, thread_id)
    _require_inbox_action_auth(request)
    try:
        inbox_sync.trash_thread_gmail_and_delete_local(db, thread_id=thread_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True}


@router.post("/threads/{thread_id}/summarize")
@limiter.limit("20/minute")
def inbox_thread_summarize(request: Request, thread_id: int, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    _require_inbox_action_auth(request)
    if t.route_tag in ("firebase", "ziyaret"):
        raise HTTPException(
            status_code=400,
            detail="Firebase ve Ziyaret için «Durum analizi» kullanın.",
        )
    _, blob = _thread_messages_blob(db, thread_id)
    try:
        summary = inbox_llm.summarize_thread_tr_tr(blob)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("inbox summarize failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    t.ai_summary = summary
    db.commit()
    return {"summary": summary}


def _thread_messages_blob(db: Session, thread_id: int) -> tuple[SupportInboxThread, str]:
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
    return t, "\n".join(parts)


@router.post("/threads/{thread_id}/analyze-alert")
@limiter.limit("15/minute")
def inbox_thread_analyze_alert(request: Request, thread_id: int, db: Session = Depends(get_db)):
    """Firebase / Ziyaret uyarıları için manuel AI durum analizi (≥15 cümle)."""
    _require_inbox_action_auth(request)
    t, blob = _thread_messages_blob(db, thread_id)
    if t.route_tag not in ("firebase", "ziyaret"):
        raise HTTPException(
            status_code=400,
            detail="Durum analizi yalnızca Firebase veya Ziyaret sekmelerindeki iletiler için kullanılabilir.",
        )
    try:
        analysis = inbox_llm.analyze_alert_thread_tr_tr(blob, route_tag=t.route_tag)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("inbox analyze-alert failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    t.ai_summary = analysis
    db.commit()
    return {"analysis": analysis, "route_tag": t.route_tag}


@router.post("/threads/{thread_id}/draft")
@limiter.limit("20/minute")
def inbox_thread_draft(request: Request, thread_id: int, db: Session = Depends(get_db)):
    t = _thread_or_404(db, thread_id)
    _require_inbox_action_auth(request)
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


@router.post("/generate-from-prompt")
@limiter.limit("30/minute")
async def inbox_generate_from_prompt(request: Request):
    """Kabaca yazılmış talimatı profesyonel Türkçe e-postaya çevirir."""
    _require_inbox_action_auth(request)
    body = await request.json()
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt boş olamaz.")
    if len(prompt) > 2000:
        raise HTTPException(status_code=422, detail="prompt en fazla 2000 karakter olabilir.")
    try:
        text, provider = inbox_llm.generate_email_from_prompt(prompt)
        return {"text": text, "provider": provider}
    except Exception as exc:
        LOGGER.exception("inbox generate-from-prompt failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/threads/{thread_id}/reply-templates")
@limiter.limit("20/minute")
async def inbox_thread_reply_templates(
    request: Request,
    thread_id: int,
    provider: str | None = Query(default=None, max_length=12, description="groq|gemini|openai"),
    db: Session = Depends(get_db),
):
    """Bağlı LLM (Groq / Gemini / OpenAI) ile 3 Türkçe yanıt şablonu üretir."""
    # Body() + Pydantic forward ref (from __future__ import annotations) çakışmasını
    # önlemek için JSON gövde manuel parse edilir.
    focus_gmail_message_id: str | None = None
    try:
        raw = await request.body()
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                fid_raw = parsed.get("focus_gmail_message_id")
                if fid_raw and isinstance(fid_raw, str):
                    focus_gmail_message_id = fid_raw.strip()[:128] or None
    except Exception:  # noqa: BLE001
        pass  # gövde yoksa veya JSON değilse varsayılan None
    t = _thread_or_404(db, thread_id)
    if t.route_tag in ("firebase", "ziyaret"):
        raise HTTPException(
            status_code=400,
            detail="Firebase ve Ziyaret iletilerinde yanıt şablonu üretilmez.",
        )
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
    focus = _resolve_inbound_focus(msgs, focus_gmail_message_id)
    blob = _thread_blob_for_reply_templates(msgs, focus)
    try:
        templates, used = inbox_llm.reply_templates_three_tr_tr(blob, preferred_provider=pref)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox reply-templates failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    focus_payload: dict[str, Any] | None = None
    if focus is not None:
        sender = focus.from_addr
        # Eğer gönderen sistem adresi (noreply vb) ise gövdeden gerçek kişiyi bulmaya çalış
        if any(x in (sender or "").lower() for x in ["noreply", "feedback", "info", "doviz.com"]):
            body_email = _extract_sender_email_from_any_body(msgs)
            if body_email:
                sender = body_email

        focus_payload = {
            "gmail_message_id": focus.gmail_message_id,
            "from": sender,
            "subject": (focus.subject or t.subject or "").strip(),
        }
    return {"templates": templates, "provider": used, "focus": focus_payload}


@router.post("/threads/{thread_id}/send")
@limiter.limit("15/minute")
async def inbox_thread_send(request: Request, thread_id: int, db: Session = Depends(get_db)):
    # Manual parse to avoid Pydantic forward ref issues
    to_email: str = ""
    _require_inbox_action_auth(request)
    subject: str = ""
    body_text: str = ""
    reply_to_gmail_message_id: str | None = None
    try:
        raw = await request.body()
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                to_email = str(parsed.get("to") or "").strip()
                subject = str(parsed.get("subject") or "").strip()
                body_text = str(parsed.get("text") or parsed.get("body") or "").strip()
                rgmid = parsed.get("reply_to_gmail_message_id")
                if rgmid and isinstance(rgmid, str):
                    reply_to_gmail_message_id = rgmid.strip() or None
    except Exception:  # noqa: BLE001
        pass

    t = _thread_or_404(db, thread_id)
    if not to_email or "@" not in to_email:
        raise HTTPException(status_code=400, detail="Alıcı e-posta adresi geçersiz veya boş.")
    if not body_text:
        raise HTTPException(
            status_code=400,
            detail="Gövde (metin) boş. Göndermeden önce ileti metnini doldurun.",
        )
    subj = subject or (f"Re: {t.subject}" if t.subject else "Re:")
    try:
        mid = inbox_sync.send_reply_plain(
            db,
            gmail_thread_id=t.gmail_thread_id,
            to_email=to_email,
            subject=subj,
            body=body_text,
            reply_to_gmail_message_id=reply_to_gmail_message_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("inbox send failed")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    t.answered_flag = True
    db.commit()
    return {"ok": True, "gmail_message_id": mid}
