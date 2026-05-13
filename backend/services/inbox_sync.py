"""Gmail thread senkronu ve MIME yardımcıları."""

from __future__ import annotations

import base64
import logging
import re
from datetime import datetime
from email.mime.text import MIMEText
from typing import Any

from google.auth.transport.requests import Request
from googleapiclient.discovery import build
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


def _extract_body_text(payload: dict[str, Any]) -> str:
    """text/plain tercih; yoksa HTML’den kaba düşüm."""
    plain: list[str] = []
    html: list[str] = []

    def walk(p: dict[str, Any]) -> None:
        mime = (p.get("mimeType") or "").lower()
        body = p.get("body") or {}
        data = body.get("data")
        if data and mime == "text/plain":
            plain.append(_decode_b64url(data))
        elif data and mime == "text/html":
            html.append(_decode_b64url(data))
        for child in p.get("parts") or []:
            walk(child)

    walk(payload)
    if plain:
        return "\n\n".join(plain).strip()
    if html:
        text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html[0])
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
    lst = (
        service.users()
        .threads()
        .list(userId="me", q=q, maxResults=max_threads)
        .execute()
    )
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
        _upsert_thread_from_gmail(db, full, account_lower)
        synced += 1
    db.commit()
    return {"synced_threads": synced, "query": q}


def _upsert_thread_from_gmail(db: Session, full: dict[str, Any], account_lower: str) -> SupportInboxThread:
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
        body = _extract_body_text(m.get("payload") or {})
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
) -> str:
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)

    msg = MIMEText(body, "plain", "utf-8")
    msg["to"] = to_email.strip()
    msg["subject"] = subject.strip() or "Re:"
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    send_body = {"raw": raw, "threadId": gmail_thread_id}
    sent = service.users().messages().send(userId="me", body=send_body).execute()
    sync_inbox_threads(db, max_threads=5)
    return str(sent.get("id") or "")
