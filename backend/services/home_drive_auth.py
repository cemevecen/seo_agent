"""Ana sayfa Google Drive OAuth — klasöre görsel yükleme/listeleme/silme."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx
from fastapi import Request
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from sqlalchemy.orm import Session

from backend.config import is_railway_runtime, settings
from backend.models import HomeDriveCredential
from backend.services.crypto import decrypt_text, encrypt_text

# Klasördeki tüm görselleri listelemek / silmek için Drive scope (kullanıcı onaylar).
HOME_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

LOGGER = logging.getLogger(__name__)
_GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"


def home_drive_oauth_is_configured() -> bool:
    return bool(settings.google_client_id.strip() and settings.google_client_secret.strip())


def home_drive_folder_id() -> str:
    return (settings.home_drive_folder_id or "").strip()


def _request_public_origin(request: Request | None) -> str | None:
    """Railway proxy arkasında https için X-Forwarded-* kullan (base_url çoğu zaman http)."""
    if request is None:
        return None
    try:
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
        host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",")[0].strip()
        if not host:
            return str(request.base_url).rstrip("/") or None
        return f"{proto}://{host}"
    except Exception:  # noqa: BLE001
        return None


def _configured_redirect_is_localhost(configured: str) -> bool:
    c = (configured or "").strip().lower()
    return not c or c.startswith("http://127.0.0.1") or c.startswith("http://localhost")


def get_home_drive_oauth_redirect_uri(*, request: Request | None = None) -> str:
    configured = (settings.home_drive_oauth_redirect_uri or "").strip()
    origin = _request_public_origin(request)
    if origin and is_railway_runtime():
        from_request = f"{origin}/api/home/drive/oauth/callback"
        if _configured_redirect_is_localhost(configured):
            return from_request
        if configured:
            cfg_host = urlparse(configured).netloc
            req_host = urlparse(origin).netloc
            if cfg_host and req_host and cfg_host != req_host:
                LOGGER.warning(
                    "HOME_DRIVE_OAUTH_REDIRECT_URI host (%s) istek host (%s) ile uyuşmuyor; istek kullanılıyor.",
                    cfg_host,
                    req_host,
                )
                return from_request
            return configured
        return from_request
    if configured:
        return configured
    if origin:
        return f"{origin}/api/home/drive/oauth/callback"
    return "http://127.0.0.1:8012/api/home/drive/oauth/callback"


def build_home_drive_oauth_flow(state: str | None = None, *, request: Request | None = None) -> Flow:
    redirect = get_home_drive_oauth_redirect_uri(request=request)
    client_config = {
        "web": {
            "client_id": settings.google_client_id.strip(),
            "client_secret": settings.google_client_secret.strip(),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect],
        }
    }
    return Flow.from_client_config(
        client_config,
        scopes=HOME_DRIVE_SCOPES,
        redirect_uri=redirect,
        state=state,
        autogenerate_code_verifier=False,
    )


def exchange_home_drive_authorization_code(code: str, *, request: Request | None = None) -> Credentials:
    redirect = get_home_drive_oauth_redirect_uri(request=request)
    body = {
        "code": code.strip(),
        "client_id": settings.google_client_id.strip(),
        "client_secret": settings.google_client_secret.strip(),
        "redirect_uri": redirect,
        "grant_type": "authorization_code",
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(_GOOGLE_TOKEN_URI, data=body)
    try:
        data = resp.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError("Google token yanıtı okunamadı.") from exc
    if resp.status_code != 200 or "error" in data:
        err = str(data.get("error") or "token_error")
        desc = str(data.get("error_description") or resp.text[:400])
        raise RuntimeError(f"{err}: {desc}".strip())
    access = data.get("access_token")
    if not access:
        raise RuntimeError("Google token yanıtında access_token yok.")
    expires_in = int(data.get("expires_in") or 3600)
    expiry = datetime.utcnow() + timedelta(seconds=max(60, expires_in))
    return Credentials(
        token=access,
        refresh_token=data.get("refresh_token"),
        token_uri=_GOOGLE_TOKEN_URI,
        client_id=settings.google_client_id.strip(),
        client_secret=settings.google_client_secret.strip(),
        scopes=list(HOME_DRIVE_SCOPES),
        expiry=expiry,
    )


def encode_home_drive_oauth_state(return_path: str = "/", *, request: Request | None = None) -> str:
    safe = return_path if return_path.startswith("/") else "/"
    redirect = get_home_drive_oauth_redirect_uri(request=request)
    payload = {
        "kind": "home_drive",
        "issued_at": datetime.utcnow().isoformat(),
        "redirect_host": urlparse(redirect).netloc,
        "return_path": safe,
    }
    return encrypt_text(json.dumps(payload, ensure_ascii=False))


def decode_home_drive_oauth_state(state: str, *, request: Request | None = None) -> dict:
    payload = json.loads(decrypt_text(state))
    if payload.get("kind") != "home_drive":
        raise ValueError("OAuth state Drive yükleme için değil.")
    issued_at = datetime.fromisoformat(payload["issued_at"])
    if issued_at < datetime.utcnow() - timedelta(minutes=20):
        raise ValueError("OAuth state zaman aşımına uğradı.")
    if payload.get("redirect_host") != urlparse(get_home_drive_oauth_redirect_uri(request=request)).netloc:
        raise ValueError("OAuth state geçersiz host içeriyor.")
    rp = str(payload.get("return_path") or "/")
    payload["return_path"] = rp if rp.startswith("/") else "/"
    return payload


def serialize_oauth_credentials(credentials: Credentials) -> dict[str, object]:
    return {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": list(credentials.scopes or []),
        "expiry": credentials.expiry.isoformat() if credentials.expiry else None,
        "saved_at": datetime.utcnow().isoformat(),
    }


def credentials_from_payload(payload: dict) -> Credentials:
    exp = payload.get("expiry")
    expiry = datetime.fromisoformat(exp) if isinstance(exp, str) and exp else None
    return Credentials(
        token=payload.get("token"),
        refresh_token=payload.get("refresh_token"),
        token_uri=payload.get("token_uri") or _GOOGLE_TOKEN_URI,
        client_id=payload.get("client_id") or settings.google_client_id.strip(),
        client_secret=payload.get("client_secret") or settings.google_client_secret.strip(),
        scopes=payload.get("scopes") or HOME_DRIVE_SCOPES,
        expiry=expiry,
    )


def get_home_drive_credential_row(db: Session) -> HomeDriveCredential | None:
    return db.query(HomeDriveCredential).order_by(HomeDriveCredential.id.asc()).first()


def save_home_drive_credentials(
    db: Session, credentials: Credentials, account_email: str
) -> HomeDriveCredential:
    encrypted = encrypt_text(json.dumps(serialize_oauth_credentials(credentials), ensure_ascii=False))
    row = get_home_drive_credential_row(db)
    if row is None:
        row = HomeDriveCredential(
            account_email=(account_email or "").strip(), encrypted_data=encrypted
        )
        db.add(row)
    else:
        row.account_email = (account_email or "").strip() or row.account_email
        row.encrypted_data = encrypted
        row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


def delete_home_drive_credentials(db: Session) -> bool:
    row = get_home_drive_credential_row(db)
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def load_home_drive_credentials(db: Session) -> Credentials | None:
    row = get_home_drive_credential_row(db)
    if row is None:
        return None
    payload = json.loads(decrypt_text(row.encrypted_data))
    return credentials_from_payload(payload)


def persist_credentials_if_refreshed(
    db: Session, creds: Credentials, row: HomeDriveCredential | None
) -> None:
    if row is None:
        return
    encrypted = encrypt_text(json.dumps(serialize_oauth_credentials(creds), ensure_ascii=False))
    row.encrypted_data = encrypted
    row.updated_at = datetime.utcnow()
    db.commit()


def ensure_fresh_credentials(db: Session) -> Credentials | None:
    row = get_home_drive_credential_row(db)
    if row is None:
        return None
    creds = load_home_drive_credentials(db)
    if creds is None:
        return None
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(GoogleAuthRequest())
            persist_credentials_if_refreshed(db, creds, row)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Home Drive token refresh failed")
            return None
    return creds
