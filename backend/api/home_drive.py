"""Ana sayfa Google Drive yükleme API (OAuth + upload/delete/content)."""

import logging
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.rate_limiter import limiter
from backend.services import home_drive, home_drive_auth

LOGGER = logging.getLogger(__name__)
router = APIRouter(prefix="/home/drive", tags=["home-drive"])


@router.get("/status")
@limiter.limit("120/minute")
def home_drive_status(request: Request, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = home_drive_auth.get_home_drive_credential_row(db)
    return {
        "oauth_client_configured": home_drive_auth.home_drive_oauth_is_configured(),
        "connected": row is not None,
        "account_email": row.account_email if row else "",
        "redirect_uri": home_drive_auth.get_home_drive_oauth_redirect_uri(request=request),
        "folder_id": home_drive_auth.home_drive_folder_id(),
    }


@router.get("/oauth/start")
def home_drive_oauth_start(request: Request, next: str = "/"):
    if not home_drive_auth.home_drive_oauth_is_configured():
        return HTMLResponse(
            "Google OAuth istemcisi eksik (GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET).",
            status_code=400,
        )
    try:
        safe_next = next if next.startswith("/") else "/"
        state = home_drive_auth.encode_home_drive_oauth_state(safe_next, request=request)
        flow = home_drive_auth.build_home_drive_oauth_flow(state=state, request=request)
        authorization_url, _ = flow.authorization_url(
            access_type="offline",
            prompt="consent",
            include_granted_scopes="false",
        )
        return RedirectResponse(authorization_url, status_code=302)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("home drive oauth start failed")
        return RedirectResponse(
            url=f"/?oauth_drive_error={quote(str(exc)[:160])}",
            status_code=302,
        )


@router.get("/oauth/callback")
def home_drive_oauth_callback(request: Request, db: Session = Depends(get_db)):
    err = request.query_params.get("error")
    if err:
        return RedirectResponse(
            url=f"/?oauth_drive_error={quote(err)}",
            status_code=302,
        )
    state = request.query_params.get("state")
    code = request.query_params.get("code")
    if not state or not code:
        return HTMLResponse("OAuth state veya kod eksik.", status_code=400)
    try:
        payload = home_drive_auth.decode_home_drive_oauth_state(state, request=request)
        creds = home_drive_auth.exchange_home_drive_authorization_code(code, request=request)
        email = ""
        try:
            info = httpx.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {creds.token}"},
                timeout=15.0,
            )
            if info.status_code == 200:
                email = str(info.json().get("email") or "").strip()
        except Exception:  # noqa: BLE001
            LOGGER.warning("home drive userinfo failed", exc_info=True)
        home_drive_auth.save_home_drive_credentials(db, creds, email)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("home drive oauth callback failed")
        return RedirectResponse(
            url=f"/?oauth_drive_error={quote(str(exc)[:160])}",
            status_code=302,
        )
    return_path = str(payload.get("return_path") or "/")
    sep = "&" if "?" in return_path else "?"
    return RedirectResponse(f"{return_path}{sep}drive_connected=1", status_code=302)


@router.post("/oauth/disconnect")
@limiter.limit("20/minute")
def home_drive_oauth_disconnect(request: Request, db: Session = Depends(get_db)):
    home_drive_auth.delete_home_drive_credentials(db)
    return JSONResponse({"ok": True})


@router.post("/upload", response_model=None)
@limiter.limit("30/minute")
async def home_drive_upload(
    request: Request,
    db: Session = Depends(get_db),
    file: UploadFile = File(...),
):
    if not home_drive_auth.get_home_drive_credential_row(db):
        return JSONResponse({"ok": False, "error": "Drive bağlı değil."}, status_code=401)
    raw = await file.read()
    try:
        item = home_drive.upload_image(
            db,
            filename=file.filename or "image",
            content=raw,
            content_type=file.content_type,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("home drive upload failed")
        return JSONResponse({"ok": False, "error": str(exc)[:200]}, status_code=400)
    return JSONResponse({"ok": True, "file": item})


@router.delete("/files/{file_id}", response_model=None)
@limiter.limit("60/minute")
def home_drive_delete_file(file_id: str, request: Request, db: Session = Depends(get_db)):
    if not home_drive_auth.get_home_drive_credential_row(db):
        return JSONResponse({"ok": False, "error": "Drive bağlı değil."}, status_code=401)
    try:
        home_drive.delete_file(db, file_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("home drive delete failed")
        return JSONResponse({"ok": False, "error": str(exc)[:200]}, status_code=400)
    return JSONResponse({"ok": True, "deleted": [file_id]})


@router.post("/files/delete-batch", response_model=None)
@limiter.limit("20/minute")
async def home_drive_delete_batch(request: Request, db: Session = Depends(get_db)):
    if not home_drive_auth.get_home_drive_credential_row(db):
        return JSONResponse({"ok": False, "error": "Drive bağlı değil."}, status_code=401)
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    ids = payload.get("ids") if isinstance(payload, dict) else None
    if not isinstance(ids, list) or not ids:
        return JSONResponse({"ok": False, "error": "ids listesi gerekli."}, status_code=400)
    if len(ids) > 100:
        return JSONResponse({"ok": False, "error": "En fazla 100 dosya silinebilir."}, status_code=400)
    result = home_drive.delete_files(db, [str(x) for x in ids])
    ok = result["deleted_count"] > 0 and result["failed_count"] == 0
    partial = result["deleted_count"] > 0 and result["failed_count"] > 0
    return JSONResponse(
        {
            "ok": ok or partial,
            "partial": partial,
            **result,
            "error": (
                None
                if ok
                else (
                    result["failed"][0]["error"]
                    if result["failed"]
                    else "Silinemedi"
                )
            ),
        },
        status_code=200 if (ok or partial) else 400,
    )


@router.get("/files/{file_id}/content", response_model=None)
@limiter.limit("120/minute")
def home_drive_file_content(file_id: str, request: Request, db: Session = Depends(get_db)):
    try:
        data, mime, name = home_drive.download_file_bytes(db, file_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("home drive content failed: %s", exc)
        return Response(status_code=404)
    headers = {
        "Cache-Control": "private, max-age=300",
        "Content-Disposition": f'inline; filename="{name}"',
    }
    return Response(content=data, media_type=mime, headers=headers)
