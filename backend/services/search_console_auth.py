"""Search Console OAuth credential yardımcıları."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import SiteCredential
from backend.services.crypto import decrypt_text, encrypt_text

SEARCH_CONSOLE_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
OAUTH_CREDENTIAL_TYPE = "search_console_oauth"
SERVICE_ACCOUNT_CREDENTIAL_TYPE = "search_console"


def oauth_is_configured() -> bool:
    return bool(settings.google_client_id.strip() and settings.google_client_secret.strip())


def get_oauth_redirect_uri() -> str:
    return settings.google_oauth_redirect_uri.strip() or "http://127.0.0.1:8012/api/search-console/oauth/callback"


def build_oauth_flow(state: str | None = None) -> Flow:
    client_config = {
        "web": {
            "client_id": settings.google_client_id.strip(),
            "client_secret": settings.google_client_secret.strip(),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [get_oauth_redirect_uri()],
        }
    }
    return Flow.from_client_config(
        client_config,
        scopes=SEARCH_CONSOLE_SCOPES,
        redirect_uri=get_oauth_redirect_uri(),
        state=state,
    )


def encode_oauth_state(site_id: int, return_path: str = "/settings") -> str:
    safe_return_path = return_path if return_path.startswith("/") else "/settings"
    payload = {
        "site_id": site_id,
        "issued_at": datetime.utcnow().isoformat(),
        "redirect_host": urlparse(get_oauth_redirect_uri()).netloc,
        "return_path": safe_return_path,
    }
    return encrypt_text(json.dumps(payload))


def decode_oauth_state(state: str) -> dict:
    payload = json.loads(decrypt_text(state))
    issued_at = datetime.fromisoformat(payload["issued_at"])
    if issued_at < datetime.utcnow() - timedelta(minutes=15):
        raise ValueError("OAuth state zaman asimina ugradi.")
    if payload.get("redirect_host") != urlparse(get_oauth_redirect_uri()).netloc:
        raise ValueError("OAuth state gecersiz host iceriyor.")
    return_path = str(payload.get("return_path") or "/settings")
    payload["return_path"] = return_path if return_path.startswith("/") else "/settings"
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
    }


def save_oauth_credentials(db: Session, site_id: int, credentials: Credentials) -> SiteCredential:
    encrypted = encrypt_text(json.dumps(serialize_oauth_credentials(credentials), ensure_ascii=False))
    record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == OAUTH_CREDENTIAL_TYPE)
        .first()
    )
    if record is None:
        record = SiteCredential(site_id=site_id, credential_type=OAUTH_CREDENTIAL_TYPE, encrypted_data=encrypted)
        db.add(record)
    else:
        record.encrypted_data = encrypted
    db.commit()
    db.refresh(record)
    return record


def get_search_console_credentials_record(db: Session, site_id: int) -> SiteCredential | None:
    return (
        db.query(SiteCredential)
        .filter(
            SiteCredential.site_id == site_id,
            SiteCredential.credential_type.in_([OAUTH_CREDENTIAL_TYPE, SERVICE_ACCOUNT_CREDENTIAL_TYPE]),
        )
        .order_by(SiteCredential.id.desc())
        .first()
    )


def load_google_credentials(credential: SiteCredential) -> Credentials | dict:
    payload = json.loads(decrypt_text(credential.encrypted_data))
    if credential.credential_type == OAUTH_CREDENTIAL_TYPE:
        return Credentials.from_authorized_user_info(payload, payload.get("scopes") or SEARCH_CONSOLE_SCOPES)
    return payload


def delete_oauth_credentials(db: Session, site_id: int) -> bool:
    record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == OAUTH_CREDENTIAL_TYPE)
        .first()
    )
    if record is None:
        return False
    db.delete(record)
    db.commit()
    return True


def get_search_console_connection_status(db: Session, site_id: int) -> dict[str, str | bool]:
    oauth_record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == OAUTH_CREDENTIAL_TYPE)
        .first()
    )
    if oauth_record is not None:
        return {"connected": True, "method": "oauth", "label": "OAuth bagli"}

    service_record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == SERVICE_ACCOUNT_CREDENTIAL_TYPE)
        .first()
    )
    if service_record is not None:
        return {"connected": True, "method": "service_account", "label": "Service account bagli"}

    return {"connected": False, "method": "none", "label": "Baglanti yok"}
