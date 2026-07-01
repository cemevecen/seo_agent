"""Google ile uygulama üyeliği (giriş / oturum).

Bu modül yalnızca uygulama kapısıdır (kim panele girebilir). Site bazlı Search Console /
GA4 / Inbox Gmail OAuth token'ları veritabanında ayrı saklanır; üye girişi bu bağlantıları
değiştirmez veya sıfırlamaz.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import httpx
from google_auth_oauthlib.flow import Flow
from sqlalchemy.orm import Session

from backend.config import is_railway_runtime, settings
from backend.models import AppMember

if TYPE_CHECKING:
    from starlette.requests import Request

LOGGER = logging.getLogger(__name__)

APP_MEMBER_COOKIE = "seo_app_member"
PANEL_MEMBER_SEEN_COOKIE = "seo_panel_member_seen"
MEMBER_SESSION_DAYS = 30
PANEL_MEMBER_SEEN_DAYS = 400
MEMBER_OAUTH_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

ADMIN_MEMBER_EMAILS = frozenset(
    {
        "cemevecen@nokta.com",
        "cemevecen@gmail.com",
    }
)

# Header çevrimiçi göstergesi yalnızca bu hesaplara görünür.
ONLINE_PRESENCE_VIEWER_EMAILS = ADMIN_MEMBER_EMAILS

# Üyelik: @nokta.com dışında yalnızca bu adresler (küçük harf).
MEMBER_EMAIL_ALLOWLIST_EXCEPTIONS = frozenset(
    {
        "cemevecen@gmail.com",
    }
)

# Google üyeliği — yalnızca /tmdb-upcoming (üst menü gizli, diğer sayfalar 403/yönlendirme).
TMDB_ONLY_MEMBER_EMAILS = frozenset(
    {
        "gozdeunaldi@nokta.com",
    }
)

# Listede / yeşil noktada gösterilecek üyeler (gizlilik: diğer @nokta üyeleri sayılmaz).
ONLINE_PRESENCE_TRACKED_MEMBER_EMAILS = frozenset(
    set(ADMIN_MEMBER_EMAILS) | set(TMDB_ONLY_MEMBER_EMAILS)
)


def member_oauth_configured() -> bool:
    cid, secret = _member_oauth_client_credentials()
    return bool(cid and secret)


def _member_oauth_client_credentials() -> tuple[str, str]:
    mid = (getattr(settings, "google_member_client_id", "") or "").strip()
    msec = (getattr(settings, "google_member_client_secret", "") or "").strip()
    if mid and msec:
        return mid, msec
    return settings.google_client_id.strip(), settings.google_client_secret.strip()


def _secret_bytes() -> bytes:
    return str(getattr(settings, "secret_key", "") or "").encode("utf-8")


def _request_public_origin(request: Request | None) -> str | None:
    if request is None:
        return None
    try:
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip()
        host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",")[0].strip()
        if not host:
            return str(request.base_url).rstrip("/")
        return f"{proto}://{host}"
    except Exception:  # noqa: BLE001
        return None


def _configured_redirect_is_localhost(configured: str) -> bool:
    c = (configured or "").strip().lower()
    return not c or c.startswith("http://127.0.0.1") or c.startswith("http://localhost")


def format_member_oauth_login_error(error_code: str, *, request: Request | None = None) -> str:
    """Google OAuth hata kodlarını giriş sayfası için Türkçe metne çevirir."""
    code = str(error_code or "").strip().lower()
    redirect = get_member_oauth_redirect_uri(request=request)
    if code == "redirect_uri_mismatch":
        uses_member_client = bool((getattr(settings, "google_member_client_id", "") or "").strip())
        client_hint = "GOOGLE_MEMBER_CLIENT_ID" if uses_member_client else "GOOGLE_CLIENT_ID"
        return (
            "Google OAuth redirect URI eşleşmiyor (redirect_uri_mismatch). "
            f"Google Cloud Console → Credentials → OAuth 2.0 Client ({client_hint}) → "
            f"Authorized redirect URIs listesine birebir ekleyin: {redirect}"
        )
    if code == "access_denied":
        return "Google girişi iptal edildi veya erişim reddedildi."
    return str(error_code or "").strip()[:240]


def is_tmdb_only_member_email(email: str) -> bool:
    return _normalize_email(email) in TMDB_ONLY_MEMBER_EMAILS


def tmdb_only_home_path() -> str:
    from backend.services.tmdb_guest_auth import TMDB_GUEST_PATH

    return TMDB_GUEST_PATH


def is_email_eligible_for_membership(email: str) -> bool:
    em = _normalize_email(email)
    if not em or "@" not in em:
        return False
    if em in MEMBER_EMAIL_ALLOWLIST_EXCEPTIONS:
        return True
    return em.endswith("@nokta.com")


def membership_rejection_message(email: str) -> str:
    em = _normalize_email(email)
    return (
        "Bu panel yalnızca @nokta.com e-posta adresleri içindir. "
        f"Giriş yapmaya çalıştığınız hesap: {em or '—'}"
    )


def get_member_oauth_redirect_uri(*, request: Request | None = None) -> str:
    configured = (getattr(settings, "app_member_oauth_redirect_uri", "") or "").strip()
    # Railway: env'de canlı URI varsa proxy/host sapmasına karşı önce onu kullan.
    if is_railway_runtime() and configured and not _configured_redirect_is_localhost(configured):
        return configured.rstrip("/") if configured.endswith("/auth/google/callback/") else configured
    origin = _request_public_origin(request)
    if origin and is_railway_runtime():
        from_request = f"{origin.rstrip('/')}/auth/google/callback"
        if _configured_redirect_is_localhost(configured):
            return from_request
        if configured:
            cfg_host = urlparse(configured).netloc
            req_host = urlparse(origin).netloc
            if cfg_host and req_host and cfg_host != req_host:
                return from_request
            return configured.rstrip("/")
        return from_request
    if configured and not _configured_redirect_is_localhost(configured):
        return configured.rstrip("/")
    if configured:
        return configured
    if origin:
        return f"{origin.rstrip('/')}/auth/google/callback"
    return "http://127.0.0.1:8012/auth/google/callback"


def oauth_callback_authorization_response(request: Request) -> str:
    """Proxy arkasında scheme/host düzeltmesi — token exchange redirect_uri ile uyumlu."""
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

    redirect = get_member_oauth_redirect_uri(request=request)
    red = urlparse(redirect)
    incoming = urlparse(str(request.url))
    query = urlencode(parse_qsl(incoming.query, keep_blank_values=True))
    path = incoming.path or "/auth/google/callback"
    return urlunparse((red.scheme, red.netloc, path, "", query, ""))


def build_member_oauth_flow(state: str | None = None, *, request: Request | None = None) -> Flow:
    redirect = get_member_oauth_redirect_uri(request=request)
    LOGGER.info("Member OAuth start redirect_uri=%s", redirect)
    client_id, client_secret = _member_oauth_client_credentials()
    client_config = {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [redirect],
        }
    }
    return Flow.from_client_config(
        client_config,
        scopes=MEMBER_OAUTH_SCOPES,
        state=state,
        redirect_uri=redirect,
    )


def encode_oauth_state(return_path: str, *, request: Request) -> str:
    exp = int(time.time()) + 600
    nonce = secrets.token_urlsafe(16)
    payload = json.dumps({"next": return_path, "exp": exp, "n": nonce}, separators=(",", ":"))
    sig = hmac.new(_secret_bytes(), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def decode_oauth_state(state: str, *, request: Request) -> dict[str, Any]:
    raw = str(state or "").strip()
    if "." not in raw:
        raise ValueError("Geçersiz OAuth state")
    payload, sig = raw.rsplit(".", 1)
    expected = hmac.new(_secret_bytes(), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise ValueError("OAuth state imzası geçersiz")
    data = json.loads(payload)
    if int(data.get("exp") or 0) < int(time.time()):
        raise ValueError("OAuth state süresi doldu")
    nxt = str(data.get("next") or "/")
    if not nxt.startswith("/"):
        nxt = "/"
    return {"return_path": nxt}


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def default_screen_permissions() -> str:
    return json.dumps({"screens": "*"}, separators=(",", ":"))


def role_for_email(email: str) -> str:
    return "admin" if _normalize_email(email) in ADMIN_MEMBER_EMAILS else "member"


def is_protected_admin_email(email: str) -> bool:
    return _normalize_email(email) in ADMIN_MEMBER_EMAILS


def member_exists_by_email(db: Session, email: str) -> bool:
    em = _normalize_email(email)
    if not em:
        return False
    return db.query(AppMember).filter(AppMember.email == em).first() is not None


def upsert_member_from_google(
    db: Session,
    *,
    email: str,
    google_sub: str = "",
    display_name: str = "",
    picture_url: str = "",
) -> AppMember:
    em = _normalize_email(email)
    if not em or "@" not in em:
        raise ValueError("Geçersiz e-posta")
    row = db.query(AppMember).filter(AppMember.email == em).first()
    now = datetime.utcnow()
    desired_role = role_for_email(em)
    if not row:
        row = AppMember(
            email=em,
            google_sub=(google_sub or "")[:128],
            display_name=(display_name or "")[:255],
            picture_url=(picture_url or "")[:1024],
            role=desired_role,
            is_active=True,
            screen_permissions_json=default_screen_permissions(),
            created_at=now,
            last_login_at=now,
        )
        db.add(row)
    else:
        if google_sub:
            row.google_sub = google_sub[:128]
        if display_name:
            row.display_name = display_name[:255]
        if picture_url:
            row.picture_url = picture_url[:1024]
        row.role = desired_role
        row.is_active = True
        row.last_login_at = now
    db.commit()
    db.refresh(row)
    return row


def build_member_session_token(member_id: int, email: str) -> str:
    exp = int(time.time()) + MEMBER_SESSION_DAYS * 86400
    em = _normalize_email(email)
    body = f"{member_id}:{exp}:{em}"
    sig = hmac.new(_secret_bytes(), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{member_id}.{exp}.{sig}"


def parse_member_session_token(token: str) -> tuple[int, int] | None:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        return None
    try:
        member_id = int(parts[0])
        exp = int(parts[1])
    except ValueError:
        return None
    if exp < int(time.time()):
        return None
    body = f"{member_id}:{exp}:"
    # email not in token verify — re-load from DB
    sig = parts[2]
    return member_id, exp


def verify_member_session_token(db: Session, token: str) -> AppMember | None:
    parsed = parse_member_session_token(token)
    if not parsed:
        return None
    member_id, exp = parsed
    row = db.query(AppMember).filter(AppMember.id == member_id).first()
    if not row or not row.is_active:
        return None
    em = _normalize_email(row.email)
    body = f"{member_id}:{exp}:{em}"
    expected = hmac.new(_secret_bytes(), body.encode("utf-8"), hashlib.sha256).hexdigest()
    parts = str(token or "").split(".")
    if len(parts) != 3 or not hmac.compare_digest(parts[2], expected):
        return None
    return row


def member_from_request(request: Request) -> AppMember | None:
    token = str(request.cookies.get(APP_MEMBER_COOKIE) or "")
    if not token:
        return None
    from backend.database import SessionLocal

    with SessionLocal() as db:
        return verify_member_session_token(db, token)


def is_member_authenticated(request: Request) -> bool:
    return member_from_request(request) is not None


def can_view_online_presence(request: Request | None) -> bool:
    if request is None:
        return False
    member = member_from_request(request)
    if member is None:
        return False
    return _normalize_email(member.email) in ONLINE_PRESENCE_VIEWER_EMAILS


def is_membership_admin(request: Request) -> bool:
    m = member_from_request(request)
    if m and m.role == "admin":
        return True
    return False


def panel_member_seen_on_request(request: Request) -> bool:
    return (request.cookies.get(PANEL_MEMBER_SEEN_COOKIE) or "").strip() == "1"


def member_oauth_authorization_extra_params(request: Request) -> dict[str, str]:
    """İlk Google girişinde hesap seçimi zorunlu; daha önce panel oturumu açılmışsa tekrar sorma."""
    if panel_member_seen_on_request(request):
        return {}
    return {"prompt": "select_account"}


def set_panel_member_seen_cookie(response, request: Request) -> None:
    response.set_cookie(
        key=PANEL_MEMBER_SEEN_COOKIE,
        value="1",
        httponly=True,
        secure=member_cookie_secure(request),
        samesite="lax",
        max_age=PANEL_MEMBER_SEEN_DAYS * 86400,
        path="/",
    )


def clear_panel_member_seen_cookie(response) -> None:
    response.delete_cookie(PANEL_MEMBER_SEEN_COOKIE, path="/")


def member_cookie_secure(request: Request) -> bool:
    return request.url.scheme == "https"


def set_member_session_cookie(response, request: Request, member: AppMember) -> None:
    token = build_member_session_token(member.id, member.email)
    response.set_cookie(
        key=APP_MEMBER_COOKIE,
        value=token,
        httponly=True,
        secure=member_cookie_secure(request),
        samesite="lax",
        max_age=MEMBER_SESSION_DAYS * 86400,
        path="/",
    )
    set_panel_member_seen_cookie(response, request)


def clear_member_session_cookie(response) -> None:
    response.delete_cookie(APP_MEMBER_COOKIE, path="/")
    clear_panel_member_seen_cookie(response)


def fetch_google_userinfo(access_token: str) -> dict[str, Any]:
    resp = httpx.get(
        "https://www.googleapis.com/oauth2/v2/userinfo",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15.0,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Google userinfo alınamadı: {resp.status_code}")
    return resp.json()


def member_list_payload(db: Session) -> list[dict[str, Any]]:
    from backend.services.timezone_utils import format_local_datetime

    rows = db.query(AppMember).order_by(AppMember.created_at.desc()).all()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        em = _normalize_email(r.email)
        seen.add(em)
        last_raw = r.last_login_at
        out.append(
            {
                "id": r.id,
                "email": r.email,
                "display_name": r.display_name or "",
                "role": r.role,
                "is_active": bool(r.is_active),
                "screen_permissions_json": r.screen_permissions_json or default_screen_permissions(),
                "created_at": r.created_at.isoformat() if r.created_at else "",
                "last_login_at": last_raw.isoformat() if last_raw else "",
                "last_login_at_tr": format_local_datetime(last_raw, fallback="—"),
                "pending_first_login": False,
                "access_note": "",
            }
        )
    for em in sorted(set(TMDB_ONLY_MEMBER_EMAILS) | set(MEMBER_EMAIL_ALLOWLIST_EXCEPTIONS)):
        if em in seen:
            continue
        note = "tmdb-only" if em in TMDB_ONLY_MEMBER_EMAILS else "allowlist"
        out.append(
            {
                "id": None,
                "email": em,
                "display_name": "",
                "role": "member",
                "is_active": None,
                "screen_permissions_json": default_screen_permissions(),
                "created_at": "",
                "last_login_at": "",
                "last_login_at_tr": "—",
                "pending_first_login": True,
                "access_note": note,
            }
        )
    out.sort(
        key=lambda row: (
            1 if row.get("pending_first_login") else 0,
            str(row.get("email") or "").lower(),
        )
    )
    return out
