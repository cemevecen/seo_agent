"""Search Console OAuth credential yardımcıları."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import CollectorRun, SiteCredential
from backend.services.crypto import decrypt_text, encrypt_text

SEARCH_CONSOLE_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
OAUTH_CREDENTIAL_TYPE = "search_console_oauth"
SERVICE_ACCOUNT_CREDENTIAL_TYPE = "search_console"
_OAUTH_REVOKED_MARKERS = (
    "invalid_grant",
    "token has been expired or revoked",
)


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
        raise ValueError("OAuth state zaman aşımına uğradı.")
    if payload.get("redirect_host") != urlparse(get_oauth_redirect_uri()).netloc:
        raise ValueError("OAuth state geçersiz host içeriyor.")
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
        "saved_at": datetime.utcnow().isoformat(),
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


def _is_oauth_revoked_error(message: str | None) -> bool:
    text = str(message or "").lower()
    return any(marker in text for marker in _OAUTH_REVOKED_MARKERS)


def _oauth_saved_at(record: SiteCredential | None) -> datetime | None:
    if record is None:
        return None
    try:
        payload = json.loads(decrypt_text(record.encrypted_data))
    except Exception:
        return None
    raw = str(payload.get("saved_at") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _site_requires_oauth_reconnect(db: Session, site_id: int, oauth_record: SiteCredential | None = None) -> bool:
    latest_run = (
        db.query(CollectorRun.status, CollectorRun.error_message, CollectorRun.requested_at)
        .filter(
            CollectorRun.site_id == site_id,
            CollectorRun.provider == "search_console",
            CollectorRun.strategy == "all",
        )
        .order_by(CollectorRun.id.desc())
        .first()
    )
    if not latest_run:
        return False
    failed_reauth = (latest_run.status or "").lower() == "failed" and _is_oauth_revoked_error(latest_run.error_message)
    if not failed_reauth:
        return False
    saved_at = _oauth_saved_at(oauth_record)
    if saved_at is None:
        return False
    run_at = latest_run.requested_at
    if not run_at:
        return True
    return run_at >= saved_at


def get_search_console_connection_status(db: Session, site_id: int) -> dict[str, str | bool]:
    oauth_record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == OAUTH_CREDENTIAL_TYPE)
        .first()
    )
    if oauth_record is not None:
        requires_reauth = _site_requires_oauth_reconnect(db, site_id, oauth_record)
        return {
            "connected": True,
            "method": "oauth",
            "label": "OAuth yeniden bağlanmalı" if requires_reauth else "OAuth bağlı",
            "requires_reauth": requires_reauth,
        }

    service_record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == SERVICE_ACCOUNT_CREDENTIAL_TYPE)
        .first()
    )
    if service_record is not None:
        return {"connected": True, "method": "service_account", "label": "Service account bağlı", "requires_reauth": False}

    return {"connected": False, "method": "none", "label": "Bağlantı yok", "requires_reauth": False}


def get_sc_connections_batch(db: "Session", site_ids: list[int]) -> "dict[int, dict]":
    """Multiple sites için SC bağlantı durumunu tek sorguda döndürür."""
    if not site_ids:
        return {}
    creds = (
        db.query(SiteCredential.site_id, SiteCredential.credential_type, SiteCredential.encrypted_data)
        .filter(
            SiteCredential.site_id.in_(site_ids),
            SiteCredential.credential_type.in_([OAUTH_CREDENTIAL_TYPE, SERVICE_ACCOUNT_CREDENTIAL_TYPE]),
        )
        .all()
    )
    cred_by_site: dict[int, set] = {sid: set() for sid in site_ids}
    oauth_saved_at_by_site: dict[int, datetime] = {}
    for row in creds:
        cred_by_site[row.site_id].add(row.credential_type)
        if row.credential_type == OAUTH_CREDENTIAL_TYPE:
            try:
                payload = json.loads(decrypt_text(row.encrypted_data))
                raw = str(payload.get("saved_at") or "").strip()
                if raw:
                    parsed = datetime.fromisoformat(raw)
                    prev = oauth_saved_at_by_site.get(row.site_id)
                    if prev is None or parsed > prev:
                        oauth_saved_at_by_site[row.site_id] = parsed
            except Exception:
                pass

    latest_run_ids = {
        sid: rid
        for sid, rid in (
            db.query(CollectorRun.site_id, func.max(CollectorRun.id))
            .filter(
                CollectorRun.site_id.in_(site_ids),
                CollectorRun.provider == "search_console",
                CollectorRun.strategy == "all",
            )
            .group_by(CollectorRun.site_id)
            .all()
        )
    }
    run_errors: dict[int, tuple[str, str, datetime | None]] = {}
    if latest_run_ids:
        for rid, sid, status, err, requested_at in (
            db.query(CollectorRun.id, CollectorRun.site_id, CollectorRun.status, CollectorRun.error_message, CollectorRun.requested_at)
            .filter(CollectorRun.id.in_(list(latest_run_ids.values())))
            .all()
        ):
            run_errors[sid] = (str(status or ""), str(err or ""), requested_at)

    result: dict[int, dict] = {}
    for sid in site_ids:
        types = cred_by_site[sid]
        status_val, err_val, run_at = run_errors.get(sid, ("", "", None))
        failed_reauth = status_val.lower() == "failed" and _is_oauth_revoked_error(err_val)
        saved_at = oauth_saved_at_by_site.get(sid)
        requires_reauth = bool(saved_at) and failed_reauth and (run_at is None or run_at >= saved_at)
        if OAUTH_CREDENTIAL_TYPE in types:
            result[sid] = {
                "connected": True,
                "method": "oauth",
                "label": "OAuth yeniden bağlanmalı" if requires_reauth else "OAuth bağlı",
                "requires_reauth": requires_reauth,
            }
        elif SERVICE_ACCOUNT_CREDENTIAL_TYPE in types:
            result[sid] = {
                "connected": True,
                "method": "service_account",
                "label": "Service account bağlı",
                "requires_reauth": False,
            }
        else:
            result[sid] = {"connected": False, "method": "none", "label": "Bağlantı yok", "requires_reauth": False}
    return result
