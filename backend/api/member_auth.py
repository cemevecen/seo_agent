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
        member = ama.member_from_request(request)
        if member and ama.is_tmdb_only_member_email(member.email):
            return RedirectResponse(url=ama.tmdb_only_home_path(), status_code=303)
        if member and ama.is_sheet_only_member_email(member.email):
            return RedirectResponse(url=ama.sheet_only_home_path(), status_code=303)
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
        is_new_member = not ama.member_exists_by_email(db, email)
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
    if ama.is_tmdb_only_member_email(member.email):
        dest = ama.tmdb_only_home_path()
    elif ama.is_sheet_only_member_email(member.email):
        dest = ama.sheet_only_home_path()
    resp = RedirectResponse(url=dest, status_code=303)
    token = ama.set_member_session_cookie(resp, request, member)
    _record_member_access_event(
        db,
        request,
        event_type="member_register_ok" if is_new_member else "member_login_ok",
        actor_email=member.email,
    )
    try:
        from backend.services import admin_access_log as aal
        from backend.services import panel_visit_log as pvl
        from backend.services import panel_visitor_alerts as pva
        import backend.main as main_mod

        ip = aal.client_ip_from_request(request)
        ua = (request.headers.get("user-agent") or "")[:512]
        sk = pvl.member_session_key_from_token(token)
        pvl.open_auth_visit(
            session_key=sk,
            email=member.email,
            display_name=(member.display_name or member.email or "").strip(),
            session_kind="member",
            ip=ip,
            device=aal.parse_device_label(ua),
            path=dest,
        )
        # Owner toast / mail: yalnızca gerçek Google girişi
        if not pva.is_owner_email(member.email, ama.ADMIN_MEMBER_EMAILS):
            sess = {
                "email": member.email,
                "label": (member.display_name or member.email or "").strip(),
                "ip": ip,
                "device": aal.parse_device_label(ua),
                "user_agent": ua,
                "first_seen": __import__("datetime").datetime.utcnow(),
            }
            pva.maybe_alert_visitor_joined(
                getattr(main_mod, "_active_sessions", {}),
                email=member.email,
                session=sess,
                owner_emails=ama.ADMIN_MEMBER_EMAILS,
            )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Auth visit open failed: %s", exc)
    return resp


@router.get("/api/panel/online-users")
def api_panel_online_users(request: Request) -> JSONResponse:
    if not ama.can_view_online_presence(request):
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    import backend.main as main_mod

    payload = main_mod.get_online_presence_api_payload(request)
    return JSONResponse(payload)


@router.post("/api/panel/activity")
async def api_panel_activity(request: Request) -> JSONResponse:
    """Menü / özellik kullanımı — açık auth ziyaretine eklenir."""
    import backend.main as main_mod

    if not main_mod._is_app_panel_authenticated(request):
        return JSONResponse(status_code=401, content={"ok": False})
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    feature = str((body or {}).get("feature") or "").strip()
    label = str((body or {}).get("label") or feature).strip()
    path = str((body or {}).get("path") or "").strip()
    if not feature and not label:
        return JSONResponse({"ok": False, "detail": "feature required"}, status_code=400)
    try:
        from backend.services import panel_visit_log as pvl

        key = main_mod._current_panel_session_key(request)
        ok = pvl.record_feature_activity(
            session_key=key,
            feature=feature or label,
            label=label or feature,
            path=path or f"feature:{feature or label}",
        )
        return JSONResponse({"ok": bool(ok)})
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("panel activity: %s", exc)
        return JSONResponse({"ok": False})


def _close_member_visit(request: Request) -> None:
    try:
        import backend.main as main_mod
        from backend.services import panel_visit_log as pvl

        pvl.close_visit(main_mod._current_panel_session_key(request), reason="logout")
    except Exception:  # noqa: BLE001
        pass


def _record_member_logout(request: Request) -> None:
    member = ama.member_from_request(request)
    email = (member.email if member else "") or ""
    _close_member_visit(request)
    if not email:
        return
    try:
        from backend.database import SessionLocal
        from backend.services import admin_access_log as aal

        with SessionLocal() as db:
            aal.record_access_event(
                db,
                event_type="member_logout_ok",
                ip=aal.client_ip_from_request(request),
                user_agent=(request.headers.get("user-agent") or "")[:512],
                referer=(request.headers.get("referer") or "")[:512],
                accept_language=(request.headers.get("accept-language") or "")[:120],
                actor_email=email,
            )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Üye çıkış kaydı başarısız: %s", exc)


@router.post("/auth/logout")
def member_logout_post(request: Request):
    _send_logout_usage_summary(request)
    _record_member_logout(request)
    resp = RedirectResponse(url="/admin/login", status_code=303)
    ama.clear_member_session_cookie(resp)
    return resp


@router.get("/auth/logout")
def member_logout_get(request: Request):
    _send_logout_usage_summary(request)
    _record_member_logout(request)
    resp = RedirectResponse(url="/admin/login", status_code=303)
    ama.clear_member_session_cookie(resp)
    return resp


def _send_logout_usage_summary(request: Request) -> None:
    """Owner dışı üye çıkışında kullanım özeti maili."""
    try:
        import hashlib

        import backend.main as main_mod
        from backend.services import panel_visitor_alerts as pva

        member = ama.member_from_request(request)
        if member is None:
            return
        email = (member.email or "").strip()
        if not email or pva.is_owner_email(email, ama.ADMIN_MEMBER_EMAILS):
            return
        tok = str(request.cookies.get(ama.APP_MEMBER_COOKIE) or "")
        key = "m:" + hashlib.sha256(tok.encode()).hexdigest()[:16] if tok else ""
        sess = None
        if key and key in getattr(main_mod, "_active_sessions", {}):
            sess = dict(main_mod._active_sessions.get(key) or {})
            try:
                del main_mod._active_sessions[key]
            except Exception:  # noqa: BLE001
                pass
        if sess is None:
            from datetime import datetime

            from backend.services import admin_access_log as aal

            ip = aal.client_ip_from_request(request)
            ua = (request.headers.get("user-agent") or "")[:512]
            sess = {
                "email": email,
                "label": (member.display_name or email).strip(),
                "ip": ip,
                "device": aal.parse_device_label(ua),
                "user_agent": ua,
                "first_seen": None,
                "last_seen": datetime.utcnow(),
                "paths": [],
            }
        from datetime import datetime

        sess["last_seen"] = datetime.utcnow()
        pva.send_usage_summary_email(
            email=email,
            display_name=(member.display_name or "").strip(),
            session=sess,
            paths=list(sess.get("paths") or []),
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Logout usage summary failed: %s", exc)


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
