"""Search Console OAuth credential yardımcıları."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse

from fastapi import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from sqlalchemy import func
from sqlalchemy.orm import Session

from backend.config import is_railway_runtime, settings

LOGGER = logging.getLogger(__name__)
from backend.models import CollectorRun, SiteCredential
from backend.services.crypto import decrypt_text, encrypt_text

SEARCH_CONSOLE_SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
OAUTH_CREDENTIAL_TYPE = "search_console_oauth"
SERVICE_ACCOUNT_CREDENTIAL_TYPE = "search_console"
_OAUTH_REVOKED_MARKERS = (
    "invalid_grant",
    "token has been expired or revoked",
)


class SearchConsoleOAuthError(Exception):
    """Search Console OAuth yenilemesi başarısız; Ayarlar'dan yeniden bağlanılmalı."""


def refresh_search_console_oauth_if_needed(credentials: Credentials) -> None:
    """OAuth access token süresi dolmuşsa refresh dener; invalid_grant → SearchConsoleOAuthError."""
    if not credentials.refresh_token:
        return
    if not (credentials.expired or not credentials.valid):
        return
    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest
    except ImportError as exc:
        raise SearchConsoleOAuthError("Google auth kütüphanesi yüklü değil.") from exc
    try:
        credentials.refresh(GoogleAuthRequest())
    except Exception as exc:  # noqa: BLE001
        msg = format_search_console_error_for_ui(str(exc))
        if _is_oauth_revoked_error(str(exc)):
            raise SearchConsoleOAuthError(msg) from exc
        raise SearchConsoleOAuthError(msg or str(exc)) from exc


def prepare_search_console_oauth_credentials(credential: SiteCredential) -> Credentials:
    if credential.credential_type != OAUTH_CREDENTIAL_TYPE:
        raise ValueError("OAuth credential bekleniyor.")
    credentials = load_google_credentials(credential)
    if not isinstance(credentials, Credentials):
        raise ValueError("Geçersiz OAuth credential.")
    refresh_search_console_oauth_if_needed(credentials)
    return credentials


def record_search_console_oauth_revoked(db: Session, site_id: int, error_message: str) -> None:
    """UI'da «yeniden bağlan» göstermek için başarısız collector kaydı (üye girişi token'ı değiştirmez)."""
    from backend.services.warehouse import finish_collector_run, start_collector_run

    msg = format_search_console_error_for_ui(error_message) or str(error_message or "")[:500]
    if not _is_oauth_revoked_error(msg):
        return
    try:
        run = start_collector_run(
            db,
            site_id=site_id,
            provider="search_console",
            strategy="all",
            trigger_source="oauth_refresh",
        )
        finish_collector_run(
            db,
            run,
            status="failed",
            error_message=msg[:2000],
            summary={"source": "oauth_refresh_failed"},
            row_count=0,
        )
        db.commit()
    except Exception:
        LOGGER.exception("SC oauth revoked kaydı yazılamadı site_id=%s", site_id)


def oauth_is_configured() -> bool:
    return bool(settings.google_client_id.strip() and settings.google_client_secret.strip())


def _request_public_origin(request: Request | None) -> str | None:
    if request is None:
        return None
    try:
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
        host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",")[0].strip()
        if not host:
            return None
        return f"{proto}://{host}"
    except Exception:
        return None


def _configured_sc_redirect_is_localhost(configured: str) -> bool:
    c = (configured or "").strip().lower()
    return not c or c.startswith("http://127.0.0.1") or c.startswith("http://localhost")


def get_search_console_oauth_redirect_uri(*, request: Request | None = None) -> str:
    """OAuth redirect URI — Railway'de istek host'undan türetilir (env localhost/uyumsuz ise)."""
    configured = (settings.google_oauth_redirect_uri or "").strip()
    origin = _request_public_origin(request)
    if origin and is_railway_runtime():
        from_request = f"{origin}/api/search-console/oauth/callback"
        if _configured_sc_redirect_is_localhost(configured):
            return from_request
        if configured:
            cfg_host = urlparse(configured).netloc
            req_host = urlparse(origin).netloc
            if cfg_host and req_host and cfg_host != req_host:
                LOGGER.warning(
                    "GOOGLE_OAUTH_REDIRECT_URI host (%s) istek host (%s) ile uyuşmuyor; istek kullanılıyor.",
                    cfg_host,
                    req_host,
                )
                return from_request
            return configured
        return from_request
    if configured:
        return configured
    if origin:
        return f"{origin}/api/search-console/oauth/callback"
    return "http://127.0.0.1:8012/api/search-console/oauth/callback"


def get_oauth_redirect_uri() -> str:
    return get_search_console_oauth_redirect_uri()


def build_oauth_flow(state: str | None = None, *, request: Request | None = None) -> Flow:
    redirect = get_search_console_oauth_redirect_uri(request=request)
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
        scopes=SEARCH_CONSOLE_SCOPES,
        redirect_uri=redirect,
        state=state,
    )


def encode_oauth_state(
    site_id: int,
    return_path: str = "/settings",
    *,
    request: Request | None = None,
) -> str:
    safe_return_path = return_path if return_path.startswith("/") else "/settings"
    payload = {
        "site_id": site_id,
        "issued_at": datetime.utcnow().isoformat(),
        "redirect_host": urlparse(get_search_console_oauth_redirect_uri(request=request)).netloc,
        "return_path": safe_return_path,
    }
    return encrypt_text(json.dumps(payload))


def decode_oauth_state(state: str, *, request: Request | None = None) -> dict:
    payload = json.loads(decrypt_text(state))
    issued_at = datetime.fromisoformat(payload["issued_at"])
    if issued_at < datetime.utcnow() - timedelta(minutes=15):
        raise ValueError("OAuth state zaman aşımına uğradı.")
    if payload.get("redirect_host") != urlparse(get_search_console_oauth_redirect_uri(request=request)).netloc:
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
    from backend.services.connection_alerts import clear_oauth_connection_alert

    clear_oauth_connection_alert(db, f"search_console:site:{site_id}")
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


def _dt_naive_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def format_search_console_error_for_ui(message: str | None) -> str:
    """Kullanıcıya gösterilecek SC hata metni (OAuth invalid_grant vb.)."""
    raw = str(message or "").strip()
    if not raw:
        return ""
    if _is_oauth_revoked_error(raw):
        return (
            "Google OAuth oturumu sona ermiş veya iptal edilmiş (invalid_grant). "
            "«Bağlantıyı Kaldır» → «Google ile Bağlan» ile Search Console erişimi olan hesapla yeniden yetkilendirin. "
            "Google Cloud OAuth istemcisinde redirect URI: "
            f"{get_oauth_redirect_uri()}"
        )
    return raw


def search_console_last_run_error_for_ui(
    *,
    error_message: str | None,
    requires_reauth: bool,
    oauth_saved_at: datetime | None,
    run_requested_at: datetime | None,
) -> str:
    """Son collector hatası — OAuth yeniden bağlandıysa eski invalid_grant gösterme."""
    raw = str(error_message or "").strip()
    if not raw:
        return ""
    if _is_oauth_revoked_error(raw) and not requires_reauth:
        saved = _dt_naive_utc(oauth_saved_at)
        run_at = _dt_naive_utc(run_requested_at)
        if saved and (run_at is None or run_at < saved):
            return ""
    return format_search_console_error_for_ui(raw)


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
        # Eski SiteCredential JSON'unda saved_at yok; invalid_grant yine de yeniden bağlanma gerektirir.
        return True
    run_at = _dt_naive_utc(latest_run.requested_at)
    saved = _dt_naive_utc(saved_at)
    if not run_at:
        return True
    if not saved:
        return True
    return run_at >= saved


def oauth_saved_at_for_site(db: Session, site_id: int) -> datetime | None:
    record = (
        db.query(SiteCredential)
        .filter(SiteCredential.site_id == site_id, SiteCredential.credential_type == OAUTH_CREDENTIAL_TYPE)
        .first()
    )
    return _oauth_saved_at(record)


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
        if not failed_reauth:
            requires_reauth = False
        elif not saved_at:
            requires_reauth = True
        else:
            run_naive = _dt_naive_utc(run_at)
            saved_naive = _dt_naive_utc(saved_at)
            requires_reauth = run_naive is None or (saved_naive is not None and run_naive >= saved_naive)
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
