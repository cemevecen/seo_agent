"""Gmail thread senkronu ve MIME yardımcıları."""

from __future__ import annotations

import base64
import json
import logging
import re
from datetime import datetime
from email.mime.text import MIMEText
from typing import Any

from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import SupportInboxMessage, SupportInboxThread
from backend.services import inbox_gmail_auth

LOGGER = logging.getLogger(__name__)

_INFO_RE = re.compile(r"info@doviz\.com", re.I)
_FB_RE = re.compile(r"feedback@doviz\.com", re.I)


def _header_map(msg: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for h in msg.get("payload", {}).get("headers") or []:
        name = (h.get("name") or "").strip()
        if name:
            out[name.lower()] = (h.get("value") or "").strip()
    return out


def _decode_b64url(data: str) -> str:
    raw = data.replace("-", "+").replace("_", "/")
    pad = len(raw) % 4
    if pad:
        raw += "=" * (4 - pad)
    try:
        return base64.b64decode(raw).decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return ""


def _attachment_text(service: Any, *, user_id: str, gmail_message_id: str, attachment_id: str) -> str:
    try:
        att = (
            service.users()
            .messages()
            .attachments()
            .get(userId=user_id, messageId=gmail_message_id, id=attachment_id)
            .execute()
        )
        raw = att.get("data")
        return _decode_b64url(raw) if raw else ""
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("gmail attachment fetch failed mid=%s aid=%s: %s", gmail_message_id, attachment_id, exc)
        return ""


def _extract_body_text(
    payload: dict[str, Any],
    *,
    service: Any | None = None,
    gmail_message_id: str | None = None,
    user_id: str = "me",
) -> str:
    """text/plain tercih; yoksa HTML. ``attachmentId`` ile gelen parçalar için Gmail attachments API kullanılır."""
    plain: list[str] = []
    html: list[str] = []

    def walk(p: dict[str, Any]) -> None:
        mime = (p.get("mimeType") or "").lower()
        body = p.get("body") or {}
        data = body.get("data")
        aid = body.get("attachmentId")
        chunk = ""
        if data:
            chunk = _decode_b64url(data)
        elif aid and service and gmail_message_id and mime in ("text/plain", "text/html"):
            chunk = _attachment_text(service, user_id=user_id, gmail_message_id=gmail_message_id, attachment_id=aid)
        if chunk:
            if mime == "text/plain":
                plain.append(chunk)
            elif mime == "text/html":
                html.append(chunk)
        for child in p.get("parts") or []:
            walk(child)

    walk(payload)
    if plain:
        return "\n\n".join(plain).strip()
    if html:
        text = re.sub(r"(?is)<script.*?>.*?</script>", " ", "\n\n".join(html))
        text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        return re.sub(r"\s+", " ", text).strip()
    return ""


def _route_tag_from_addrs(text: str) -> str:
    t = text or ""
    has_i = bool(_INFO_RE.search(t))
    has_f = bool(_FB_RE.search(t))
    if has_i and has_f:
        return "mixed"
    if has_i:
        return "info"
    if has_f:
        return "feedback"
    return "mixed"


def _thread_has_unread(full: dict[str, Any]) -> bool:
    for m in full.get("messages") or []:
        if "UNREAD" in (m.get("labelIds") or []):
            return True
    return False


def _is_outbound(msg: dict[str, Any], account_lower: str) -> bool:
    labs = {x.upper() for x in (msg.get("labelIds") or [])}
    if "SENT" in labs:
        return True
    h = _header_map(msg)
    from_ = (h.get("from") or "").lower()
    return account_lower in from_


def _gmail_service(creds):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _gmail_http_error_message(exc: HttpError) -> str:
    try:
        raw = exc.content.decode("utf-8", errors="replace") if exc.content else ""
        payload = json.loads(raw) if raw else {}
        err = payload.get("error") or {}
        if isinstance(err, dict):
            return str(err.get("message") or err.get("status") or raw[:240])
        return str(err)[:300]
    except Exception:  # noqa: BLE001
        return str(exc)[:300]


def _ensure_fresh_creds(db: Session, creds):
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        inbox_gmail_auth.persist_credentials_if_refreshed(db, creds, row)
    return creds


def sync_inbox_threads(db: Session, *, max_threads: int = 30) -> dict[str, Any]:
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    account_lower = (row.account_email if row else "").strip().lower()

    service = _gmail_service(creds)
    q = (settings.inbox_gmail_query or "").strip() or "(to:info@doviz.com OR to:feedback@doviz.com)"
    try:
        lst = (
            service.users()
            .threads()
            .list(userId="me", q=q, maxResults=max_threads)
            .execute()
        )
    except HttpError as exc:
        msg = _gmail_http_error_message(exc)
        st_raw = getattr(getattr(exc, "resp", None), "status", None)
        try:
            st = int(st_raw) if st_raw is not None else 0
        except (TypeError, ValueError):
            st = 0
        if st == 401:
            raise RuntimeError(
                "Gmail oturumu geçersiz veya süresi doldu. «Bağlantıyı kes» deyip yeniden Google ile bağlanın."
            ) from exc
        if st == 403:
            raise RuntimeError(f"Gmail erişim reddedildi: {msg}") from exc
        if st == 429:
            raise RuntimeError(
                f"Gmail API hız sınırı; birkaç dakika sonra tekrar deneyin. ({msg})"
            ) from exc
        raise RuntimeError(f"Gmail ileti listesi alınamadı (HTTP {st}): {msg}") from exc
    thread_list = lst.get("threads") or []
    synced = 0
    for tref in thread_list:
        tid = tref.get("id")
        if not tid:
            continue
        try:
            full = service.users().threads().get(userId="me", id=tid, format="full").execute()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("gmail thread get failed %s: %s", tid, exc)
            continue
        _upsert_thread_from_gmail(db, full, account_lower, service)
        synced += 1
    db.commit()
    return {"synced_threads": synced, "query": q}


def _upsert_thread_from_gmail(
    db: Session, full: dict[str, Any], account_lower: str, service: Any
) -> SupportInboxThread:
    tid = str(full.get("id") or "")
    gmail_unread = _thread_has_unread(full)
    msgs_raw = full.get("messages") or []
    last_ms = 0
    route_src = ""
    subject0 = ""
    snippet = str(full.get("snippet") or "")

    for m in msgs_raw:
        try:
            ms = int(m.get("internalDate") or 0)
        except (TypeError, ValueError):
            ms = 0
        if ms > last_ms:
            last_ms = ms
        h = _header_map(m)
        route_src += " " + (h.get("delivered-to") or h.get("to") or h.get("cc") or "")
        if not subject0:
            subject0 = h.get("subject") or ""

    route_tag = _route_tag_from_addrs(route_src)

    row = db.query(SupportInboxThread).filter(SupportInboxThread.gmail_thread_id == tid).first()
    now = datetime.utcnow()
    if row is None:
        row = SupportInboxThread(
            gmail_thread_id=tid,
            subject=subject0[:998] or "(konu yok)",
            snippet=snippet,
            route_tag=route_tag,
            gmail_unread=gmail_unread,
            last_internal_ms=last_ms,
            last_synced_at=now,
        )
        db.add(row)
        db.flush()
    else:
        row.subject = subject0[:998] or row.subject
        row.snippet = snippet or row.snippet
        row.route_tag = route_tag
        row.gmail_unread = gmail_unread
        row.last_internal_ms = last_ms
        row.last_synced_at = now

    db.query(SupportInboxMessage).filter(SupportInboxMessage.thread_id == row.id).delete()
    for m in sorted(msgs_raw, key=lambda x: int(x.get("internalDate") or 0)):
        mid = str(m.get("id") or "")
        if not mid:
            continue
        h = _header_map(m)
        body = _extract_body_text(
            m.get("payload") or {},
            service=service,
            gmail_message_id=mid,
            user_id="me",
        )
        try:
            ims = int(m.get("internalDate") or 0)
        except (TypeError, ValueError):
            ims = 0
        out = _is_outbound(m, account_lower)
        db.add(
            SupportInboxMessage(
                thread_id=row.id,
                gmail_message_id=mid,
                from_addr=(h.get("from") or "")[:512],
                to_addr=(h.get("to") or "")[:512],
                subject=(h.get("subject") or "")[:998],
                body_text=body,
                internal_ms=ims,
                is_outbound=out,
            )
        )
    return row


def refresh_thread_bodies_from_gmail(db: Session, *, thread_id: int) -> dict[str, Any]:
    """Veritabanındaki iletilerin gövdesini Gmail ``messages.get`` ile yeniden çeker (büyük/HTML gövdeler)."""
    row = db.query(SupportInboxThread).filter(SupportInboxThread.id == thread_id).first()
    if row is None:
        raise RuntimeError("Konuşma bulunamadı.")
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)
    msgs = (
        db.query(SupportInboxMessage)
        .filter(SupportInboxMessage.thread_id == row.id)
        .order_by(SupportInboxMessage.internal_ms.asc())
        .all()
    )
    updated = 0
    for mrow in msgs:
        gid = (mrow.gmail_message_id or "").strip()
        if not gid:
            continue
        try:
            full = service.users().messages().get(userId="me", id=gid, format="full").execute()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("gmail message get failed %s: %s", gid, exc)
            continue
        body = _extract_body_text(
            full.get("payload") or {},
            service=service,
            gmail_message_id=gid,
            user_id="me",
        )
        prev = (mrow.body_text or "").strip()
        new = body.strip()
        if new and new != prev:
            mrow.body_text = body
            updated += 1
    row.last_synced_at = datetime.utcnow()
    db.commit()
    return {"refreshed_messages": updated, "gmail_thread_id": row.gmail_thread_id}


def _rfc_message_id_header(service: Any, *, gmail_message_id: str) -> str | None:
    try:
        fullm = service.users().messages().get(userId="me", id=gmail_message_id, format="full").execute()
        h = _header_map(fullm)
        mid = (h.get("message-id") or "").strip()
        return mid or None
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("message-id fetch failed %s: %s", gmail_message_id, exc)
        return None


def set_thread_gmail_read(db: Session, *, gmail_thread_id: str, read: bool) -> None:
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)
    body: dict[str, Any] = {}
    if read:
        body["removeLabelIds"] = ["UNREAD"]
    else:
        body["addLabelIds"] = ["UNREAD"]
    service.users().threads().modify(userId="me", id=gmail_thread_id, body=body).execute()
    row = db.query(SupportInboxThread).filter(SupportInboxThread.gmail_thread_id == gmail_thread_id).first()
    if row:
        row.gmail_unread = not read
        row.last_synced_at = datetime.utcnow()
        db.commit()


def send_reply_plain(
    db: Session,
    *,
    gmail_thread_id: str,
    to_email: str,
    subject: str,
    body: str,
    reply_to_gmail_message_id: str | None = None,
) -> str:
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)

    msg = MIMEText(body, "plain", "utf-8")
    msg["to"] = to_email.strip()
    msg["subject"] = subject.strip() or "Re:"
    if reply_to_gmail_message_id:
        rfc_mid = _rfc_message_id_header(service, gmail_message_id=reply_to_gmail_message_id.strip())
        if rfc_mid:
            # Başlık değeri çoğu zaman <...@...> biçiminde gelir
            clean = rfc_mid.strip()
            if not clean.startswith("<"):
                clean = f"<{clean}>" if "@" in clean else clean
            msg["In-Reply-To"] = clean
            msg["References"] = clean
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    send_body = {"raw": raw, "threadId": gmail_thread_id}
    sent = service.users().messages().send(userId="me", body=send_body).execute()
    sync_inbox_threads(db, max_threads=5)
    return str(sent.get("id") or "")
