"""Gmail API OAuth (gelen kutusu) — Search Console OAuth’undan ayrı redirect URI ve scope."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import InboxGmailCredential
from backend.services.crypto import decrypt_text, encrypt_text

GMAIL_INBOX_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

LOGGER = logging.getLogger(__name__)
_GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"


def inbox_oauth_is_configured() -> bool:
    return bool(settings.google_client_id.strip() and settings.google_client_secret.strip())


def get_inbox_oauth_redirect_uri() -> str:
    return (
        settings.gmail_inbox_oauth_redirect_uri.strip()
        or "http://127.0.0.1:8012/api/inbox/oauth/callback"
    )


def build_inbox_oauth_flow(state: str | None = None) -> Flow:
    redirect = get_inbox_oauth_redirect_uri()
    client_config = {
        "web": {
            "client_id": settings.google_client_id.strip(),
            "client_secret": settings.google_client_secret.strip(),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect],
        }
    }
    # Gizli web istemcisi: PKCE kullanmıyoruz. Aksi halde /oauth/start ile /oauth/callback
    # farklı Flow örnekleri oluşturulduğunda her biri yeni code_verifier üretir; token
    # değişiminde Google (invalid_grant) reddeder.
    return Flow.from_client_config(
        client_config,
        scopes=GMAIL_INBOX_SCOPES,
        redirect_uri=redirect,
        state=state,
        autogenerate_code_verifier=False,
    )


def exchange_inbox_authorization_code(code: str) -> Credentials:
    """Yetkilendirme kodunu doğrudan Google token uç noktasında değiştirir (oauthlib yok).

    Aynı Client ID ile Search Console izni varken yanıtta ek ``webmasters.readonly`` scope
    gelebilir; oauthlib ``Scope has changed`` ile düşer. Burada yanıt ayrıştırılır, Gmail
    için Credentials üretilir.
    """
    redirect = get_inbox_oauth_redirect_uri()
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
        LOGGER.warning("inbox token response not json status=%s body=%s", resp.status_code, resp.text[:500])
        raise RuntimeError("Google token yanıtı okunamadı.") from exc

    if resp.status_code != 200 or "error" in data:
        err = str(data.get("error") or "token_error")
        desc = str(data.get("error_description") or data.get("error_uri") or resp.text[:400])
        raise RuntimeError(f"{err}: {desc}".strip())

    access = data.get("access_token")
    if not access:
        raise RuntimeError("Google token yanıtında access_token yok.")

    expires_in = int(data.get("expires_in") or 3600)
    expiry = datetime.utcnow() + timedelta(seconds=max(60, expires_in))

    # Google aynı Client ID ile önceden verilmiş scope'ları (ör. webmasters.readonly)
    # token yanıtına ekleyebilir. granted_scopes parametresi bu listeyi Credentials'a
    # yazarsa google-api-python-client scope uyuşmazlığı tespit edip hata verir.
    # Sadece GMAIL_INBOX_SCOPES'u scopes olarak set edip granted_scopes'u vermiyoruz.
    LOGGER.info(
        "inbox token exchange ok; google returned scopes=%s (using fixed=%s)",
        data.get("scope"),
        GMAIL_INBOX_SCOPES,
    )
    return Credentials(
        token=access,
        refresh_token=data.get("refresh_token"),
        token_uri=_GOOGLE_TOKEN_URI,
        client_id=settings.google_client_id.strip(),
        client_secret=settings.google_client_secret.strip(),
        scopes=list(GMAIL_INBOX_SCOPES),
        expiry=expiry,
        # granted_scopes verilmiyor: Google'ın ekstra scope döndürmesi hata vermez.
    )


def encode_inbox_oauth_state(return_path: str = "/inbox") -> str:
    safe = return_path if return_path.startswith("/") else "/inbox"
    payload = {
        "kind": "inbox",
        "issued_at": datetime.utcnow().isoformat(),
        "redirect_host": urlparse(get_inbox_oauth_redirect_uri()).netloc,
        "return_path": safe,
    }
    return encrypt_text(json.dumps(payload, ensure_ascii=False))


def decode_inbox_oauth_state(state: str) -> dict:
    payload = json.loads(decrypt_text(state))
    if payload.get("kind") != "inbox":
        raise ValueError("OAuth state gelen kutusu için değil.")
    issued_at = datetime.fromisoformat(payload["issued_at"])
    if issued_at < datetime.utcnow() - timedelta(minutes=20):
        raise ValueError("OAuth state zaman aşımına uğradı.")
    if payload.get("redirect_host") != urlparse(get_inbox_oauth_redirect_uri()).netloc:
        raise ValueError("OAuth state geçersiz host içeriyor.")
    rp = str(payload.get("return_path") or "/inbox")
    payload["return_path"] = rp if rp.startswith("/") else "/inbox"
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
        token_uri=payload.get("token_uri") or "https://oauth2.googleapis.com/token",
        client_id=payload.get("client_id") or settings.google_client_id.strip(),
        client_secret=payload.get("client_secret") or settings.google_client_secret.strip(),
        scopes=payload.get("scopes") or GMAIL_INBOX_SCOPES,
        expiry=expiry,
    )


def get_inbox_credential_row(db: Session) -> InboxGmailCredential | None:
    return db.query(InboxGmailCredential).order_by(InboxGmailCredential.id.asc()).first()


def save_inbox_credentials(db: Session, credentials: Credentials, account_email: str) -> InboxGmailCredential:
    encrypted = encrypt_text(json.dumps(serialize_oauth_credentials(credentials), ensure_ascii=False))
    row = get_inbox_credential_row(db)
    if row is None:
        row = InboxGmailCredential(account_email=(account_email or "").strip(), encrypted_data=encrypted)
        db.add(row)
    else:
        row.account_email = (account_email or "").strip() or row.account_email
        row.encrypted_data = encrypted
        row.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(row)
    return row


def delete_inbox_credentials(db: Session) -> bool:
    row = get_inbox_credential_row(db)
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def load_inbox_credentials(db: Session) -> Credentials | None:
    row = get_inbox_credential_row(db)
    if row is None:
        return None
    payload = json.loads(decrypt_text(row.encrypted_data))
    return credentials_from_payload(payload)


def persist_credentials_if_refreshed(db: Session, creds: Credentials, row: InboxGmailCredential | None) -> None:
    if row is None:
        return
    encrypted = encrypt_text(
        json.dumps(serialize_oauth_credentials(creds), ensure_ascii=False),
    )
    row.encrypted_data = encrypted
    row.updated_at = datetime.utcnow()
    db.commit()
