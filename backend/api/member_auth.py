"""Google üyelik girişi ve üye yönetimi API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import AppMember
from backend.services import app_member_auth as ama

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["member-auth"])


def _safe_next_path(raw: str) -> str:
    p = str(raw or "/").strip()
    if not p.startswith("/") or p.startswith("//"):
        return "/"
    return p


def _record_member_access_event(
    db: Session,
    request: Request,
    *,
    event_type: str,
    actor_email: str = "",
) -> None:
    from backend.services import admin_access_log as aal

    try:
        aal.record_access_event(
            db,
            event_type=event_type,
            ip=aal.client_ip_from_request(request),
            user_agent=(request.headers.get("user-agent") or "")[:512],
            referer=(request.headers.get("referer") or "")[:512],
            accept_language=(request.headers.get("accept-language") or "")[:120],
            actor_email=actor_email,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Üye giriş kaydı / uyarı e-postası başarısız (%s): %s", event_type, exc)


@router.get("/auth/google/start")
def google_member_oauth_start(request: Request, next: str = "/"):
    if not ama.member_oauth_configured():
        return HTMLResponse(
            "Google OAuth yapılandırılmamış (GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET).",
            status_code=503,
        )
    safe_next = _safe_next_path(next)
    if ama.is_member_authenticated(request):
        return RedirectResponse(url=safe_next, status_code=303)
    state = ama.encode_oauth_state(safe_next, request=request)
    flow = ama.build_member_oauth_flow(state=state, request=request)
    redirect_uri = ama.get_member_oauth_redirect_uri(request=request)
    LOGGER.info("member oauth start redirect_uri=%s", redirect_uri)
    auth_kwargs: dict[str, str] = {
        "access_type": "online",
        "include_granted_scopes": "false",
    }
    auth_kwargs.update(ama.member_oauth_authorization_extra_params(request))
    auth_url, _ = flow.authorization_url(**auth_kwargs)
    return RedirectResponse(auth_url, status_code=302)


@router.get("/auth/google/callback")
def google_member_oauth_callback(request: Request, db: Session = Depends(get_db)):
    err = request.query_params.get("error")
    if err:
        from urllib.parse import quote

        _record_member_access_event(db, request, event_type="member_login_fail")
        msg = ama.format_member_oauth_login_error(err, request=request)
        return RedirectResponse(url=f"/admin/login?oauth_error={quote(msg)}", status_code=302)
    state = request.query_params.get("state")
    code = request.query_params.get("code")
    if not state or not code:
        return HTMLResponse("OAuth state veya kod eksik.", status_code=400)
    attempted_email = ""
    try:
        payload = ama.decode_oauth_state(state, request=request)
        flow = ama.build_member_oauth_flow(state=state, request=request)
        flow.fetch_token(authorization_response=ama.oauth_callback_authorization_response(request))
        creds = flow.credentials
        info = ama.fetch_google_userinfo(creds.token)
        email = str(info.get("email") or "").strip()
        attempted_email = email
        if not email:
            raise RuntimeError("Google hesabından e-posta alınamadı")
        if not ama.is_email_eligible_for_membership(email):
            from urllib.parse import quote

            _record_member_access_event(
                db,
                request,
                event_type="member_login_fail",
                actor_email=email,
            )
            return RedirectResponse(
                url=f"/admin/login?oauth_error={quote(ama.membership_rejection_message(email))}",
                status_code=302,
            )
        member = ama.upsert_member_from_google(
            db,
            email=email,
            google_sub=str(info.get("id") or info.get("sub") or ""),
            display_name=str(info.get("name") or ""),
            picture_url=str(info.get("picture") or ""),
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("member oauth callback failed")
        _record_member_access_event(
            db,
            request,
            event_type="member_login_fail",
            actor_email=attempted_email,
        )
        from urllib.parse import quote

        return RedirectResponse(
            url=f"/admin/login?oauth_error={quote(str(exc)[:160])}",
            status_code=302,
        )
    dest = _safe_next_path(str(payload.get("return_path") or "/"))
    resp = RedirectResponse(url=dest, status_code=303)
    ama.set_member_session_cookie(resp, request, member)
    _record_member_access_event(
        db,
        request,
        event_type="member_login_ok",
        actor_email=member.email,
    )
    return resp


@router.post("/auth/logout")
def member_logout_post(request: Request):
    resp = RedirectResponse(url="/admin/login", status_code=303)
    ama.clear_member_session_cookie(resp)
    return resp


@router.get("/auth/logout")
def member_logout_get(request: Request):
    resp = RedirectResponse(url="/admin/login", status_code=303)
    ama.clear_member_session_cookie(resp)
    return resp


def _require_membership_admin(request: Request) -> bool:
    import backend.main as main_mod

    return main_mod._is_membership_admin(request)


@router.get("/api/members")
def api_list_members(request: Request, db: Session = Depends(get_db)) -> JSONResponse:
    if not _require_membership_admin(request):
        return JSONResponse(status_code=403, content={"detail": "Yalnızca üyelik yöneticileri."})
    return JSONResponse({"members": ama.member_list_payload(db)})


@router.patch("/api/members/{member_id}")
async def api_patch_member(member_id: int, request: Request, db: Session = Depends(get_db)) -> JSONResponse:
    if not _require_membership_admin(request):
        return JSONResponse(status_code=403, content={"detail": "Yalnızca üyelik yöneticileri."})
    try:
        body: dict[str, Any] = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"detail": "Geçersiz JSON"})
    row = db.query(AppMember).filter(AppMember.id == member_id).first()
    if not row:
        return JSONResponse(status_code=404, content={"detail": "Üye bulunamadı"})
    if "role" in body:
        role = str(body.get("role") or "").strip().lower()
        if role not in ("admin", "member"):
            return JSONResponse(status_code=400, content={"detail": "role: admin veya member"})
        if ama.is_protected_admin_email(row.email):
            row.role = "admin"
        else:
            row.role = role
    if "is_active" in body:
        row.is_active = bool(body.get("is_active"))
    if "screen_permissions_json" in body:
        row.screen_permissions_json = str(body.get("screen_permissions_json") or ama.default_screen_permissions())
    db.commit()
    db.refresh(row)
    return JSONResponse(
        {
            "ok": True,
            "member": {
                "id": row.id,
                "email": row.email,
                "role": row.role,
                "is_active": row.is_active,
                "screen_permissions_json": row.screen_permissions_json,
            },
        }
    )
