"""Gmail thread senkronu ve MIME yardımcıları."""

from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import Iterator
from datetime import datetime, timezone
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

INBOX_SYNC_MAX_THREADS = 50
INBOX_LIST_LIMIT = 50

INBOX_ROUTE_GMAIL_QUERIES: dict[str, str] = {
    "firebase": "from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com",
    "ziyaret": "from:noreply@doviz.com",
    "info": "to:info@doviz.com OR deliveredto:info@doviz.com",
    "feedback": (
        "to:feedback@doviz.com OR deliveredto:feedback@doviz.com OR "
        "to:feedback@sinemalar.com OR deliveredto:feedback@sinemalar.com"
    ),
    "sinemalar": "to:info@sinemalar.com OR deliveredto:info@sinemalar.com",
    "tome": (
        "to:me -to:info@doviz.com -to:feedback@doviz.com -to:info@sinemalar.com "
        "-to:feedback@sinemalar.com -from:firebase-noreply@google.com "
        "-from:firebase-noreply.googleapis.com -from:noreply@doviz.com"
    ),
}

# Gmail’de «cevaplandı» için kullanılan özel etiket (threads.modify ile eklenir/kaldırılır).
ANSWERED_LABEL_NAME = "SEO-Agent · Cevaplandı"
_answered_label_id_cache: str | None = None

_INFO_DOVIZ_RE = re.compile(r"info@doviz\.com", re.I)
_INFO_SINEMALAR_RE = re.compile(r"info@sinemalar\.com", re.I)
_FB_RE = re.compile(r"feedback@doviz\.com", re.I)
_FIREBASE_FROM_RE = re.compile(r"firebase-noreply@(google\.com|googleapis\.com)", re.I)
_ZIYARET_FROM_RE = re.compile(r"noreply@doviz\.com", re.I)
_SUPPORT_ADDR_MARKERS = (
    "info@doviz.com",
    "feedback@doviz.com",
    "info@sinemalar.com",
    "feedback@sinemalar.com",
)


def _is_firebase_sender(text: str) -> bool:
    return bool(_FIREBASE_FROM_RE.search(text or ""))


def _is_ziyaret_sender(text: str) -> bool:
    return bool(_ZIYARET_FROM_RE.search(text or ""))


def _default_inbox_gmail_query() -> str:
    """Destek adresleri + Firebase Crashlytics uyarı mailleri (from: firebase-noreply)."""
    return (
        "("
        "to:info@doviz.com OR to:feedback@doviz.com OR to:info@sinemalar.com OR to:feedback@sinemalar.com "
        "OR deliveredto:info@doviz.com OR deliveredto:feedback@doviz.com "
        "OR deliveredto:info@sinemalar.com OR deliveredto:feedback@sinemalar.com "
        "OR from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com "
        "OR from:noreply@doviz.com OR to:me"
        ")"
    )


def _firebase_only_gmail_query() -> str:
    return "from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com"


SCHEDULED_INBOX_SYNC_LOOKBACK_DAYS = 3


def _append_or_clause_to_query(q: str, clause: str) -> str:
    q = (q or "").strip()
    if q.startswith("(") and q.endswith(")"):
        return q[:-1] + f" OR {clause})"
    if q:
        return f"({q}) OR ({clause})"
    return clause


def _ensure_inbox_query_clauses(q: str) -> str:
    q = (q or "").strip()
    if "firebase-noreply" not in q.lower():
        q = _append_or_clause_to_query(
            q, "from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com"
        )
    if "noreply@doviz.com" not in q.lower():
        q = _append_or_clause_to_query(q, "from:noreply@doviz.com")
    if "to:me" not in q.lower():
        q = _append_or_clause_to_query(q, "to:me")
    return q


def _normalize_inbox_gmail_query(
    raw: str,
    *,
    lookback_days: int | None = 60,
    after_unix: int | None = None,
    merge_global_clauses: bool = True,
) -> str:
    q = (raw or "").strip()
    if not q:
        q = _default_inbox_gmail_query()
        merge_global_clauses = True
    if merge_global_clauses:
        q = _ensure_inbox_query_clauses(q)
    q = re.sub(r"\bis:unread\b", "", q, flags=re.IGNORECASE).strip()
    q = re.sub(r"\s{2,}", " ", q).strip()
    if after_unix is not None and after_unix > 0:
        q = f"after:{after_unix} {q}"
    has_newer_than = bool(re.search(r"\bnewer_than:\d", q, re.IGNORECASE))
    has_after = bool(re.search(r"\bafter:", q, re.IGNORECASE))
    if lookback_days is not None and not has_newer_than:
        q = f"newer_than:{lookback_days}d {q}"
    elif lookback_days is None and not has_newer_than and not has_after:
        q = f"newer_than:60d {q}"
    return q


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


def _extract_body_parts(
    payload: dict[str, Any],
    *,
    service: Any | None = None,
    gmail_message_id: str | None = None,
    user_id: str = "me",
) -> tuple[str, str]:
    """(plain_text, html) — text/plain tercih; HTML ayrı saklanır."""
    plain: list[str] = []
    html_parts: list[str] = []

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
                html_parts.append(chunk)
        for child in p.get("parts") or []:
            walk(child)

    walk(payload)
    plain_text = "\n\n".join(plain).strip()
    html_body = "\n".join(html_parts).strip()
    if not plain_text and html_body:
        text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_body)
        text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        plain_text = re.sub(r"\s+", " ", text).strip()
    return plain_text, html_body


def _extract_body_text(
    payload: dict[str, Any],
    *,
    service: Any | None = None,
    gmail_message_id: str | None = None,
    user_id: str = "me",
) -> str:
    """text/plain tercih; yoksa HTML'den düz metin."""
    plain, _ = _extract_body_parts(
        payload, service=service, gmail_message_id=gmail_message_id, user_id=user_id
    )
    return plain


def _route_tag_from_addrs(text: str) -> str | None:
    t = (text or "").lower()

    is_info = "info@doviz.com" in t or "info@sinemalar.com" in t
    is_feedback = "feedback@doviz.com" in t or "feedback@sinemalar.com" in t
    is_sinemalar = "sinemalar.com" in t and "info" in t

    found: list[str] = []
    if is_info:
        found.append("info")
    if is_feedback:
        found.append("feedback")
    if is_sinemalar and "info" not in found:
        found.append("sinemalar")

    if len(found) > 1:
        for pref in ("info", "feedback", "sinemalar"):
            if pref in found:
                return pref
        return found[0]
    if len(found) == 1:
        return found[0]
    return None


def _is_direct_to_account(
    msgs_raw: list[dict[str, Any]], route_src: str, account_lower: str
) -> bool:
    if not account_lower:
        return False
    t = (route_src or "").lower()
    if any(marker in t for marker in _SUPPORT_ADDR_MARKERS):
        return False
    for m in msgs_raw:
        h = _header_map(m)
        from_ = (h.get("from") or "").lower()
        if account_lower in from_:
            continue
        for key in ("to", "delivered-to", "x-original-to", "envelope-to", "cc"):
            hdr = (h.get(key) or "").lower()
            if account_lower in hdr:
                return True
    return account_lower in t


def _route_tag_from_thread(
    msgs_raw: list[dict[str, Any]], route_src: str, account_lower: str = ""
) -> str:
    for m in msgs_raw:
        h = _header_map(m)
        if _is_firebase_sender(h.get("from") or ""):
            return "firebase"
    for m in msgs_raw:
        h = _header_map(m)
        if _is_ziyaret_sender(h.get("from") or ""):
            return "ziyaret"
    tag = _route_tag_from_addrs(route_src)
    if tag:
        return tag
    if _is_direct_to_account(msgs_raw, route_src, account_lower):
        return "tome"
    return "tome"


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


def _ensure_answered_label_id(service: Any) -> str:
    """Gmail’de «cevaplandı» etiketinin id’si; yoksa oluşturur."""
    global _answered_label_id_cache
    if _answered_label_id_cache:
        return _answered_label_id_cache
    try:
        resp = service.users().labels().list(userId="me").execute()
    except HttpError as exc:
        detail = _gmail_http_error_message(exc)
        st = getattr(getattr(exc, "resp", None), "status", "") or ""
        raise RuntimeError(f"Gmail etiket listesi alınamadı (HTTP {st}): {detail}") from exc
    for lab in resp.get("labels") or []:
        if (lab.get("name") or "") == ANSWERED_LABEL_NAME:
            _answered_label_id_cache = str(lab.get("id") or "").strip()
            if _answered_label_id_cache:
                return _answered_label_id_cache
    try:
        created = service.users().labels().create(
            userId="me",
            body={
                "name": ANSWERED_LABEL_NAME,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show",
            },
        ).execute()
    except HttpError as exc:
        detail = _gmail_http_error_message(exc)
        st = getattr(getattr(exc, "resp", None), "status", "") or ""
        raise RuntimeError(f"Gmail etiketi oluşturulamadı (HTTP {st}): {detail}") from exc
    _answered_label_id_cache = str(created.get("id") or "").strip()
    if not _answered_label_id_cache:
        raise RuntimeError("Gmail etiketi oluşturuldu ancak kimlik dönmedi.")
    return _answered_label_id_cache


def _thread_subject_snippet_preview(full: dict[str, Any]) -> tuple[str, str]:
    snippet = (str(full.get("snippet") or ""))[:200]
    subject0 = ""
    for m in full.get("messages") or []:
        h = _header_map(m)
        subj = (h.get("subject") or "").strip()
        if subj:
            subject0 = subj[:998]
            break
    return (subject0 or "(konu yok)", snippet)


def _sync_pct_saved(i_saved: int, n_total: int) -> int:
    if n_total <= 0:
        return 100
    return min(99, int(12 + (i_saved / n_total) * 88))


def iter_sync_inbox_threads(
    db: Session,
    *,
    max_threads: int = 30,
    gmail_query: str | None = None,
    lookback_days: int | None = 60,
    after_unix: int | None = None,
) -> Iterator[dict[str, Any]]:
    yield {
        "type": "phase",
        "phase": "auth",
        "message": "Gmail oturumu doğrulanıyor…",
        "pct": 2,
    }
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    account_lower = (row.account_email if row else "").strip().lower()

    yield {
        "type": "phase",
        "phase": "connect",
        "message": "Gmail API bağlantısı kuruluyor…",
        "pct": 5,
    }
    service = _gmail_service(creds)
    # Forwarding setup'ları için to: ve deliveredto: birlikte kullanılıyor:
    # - to:   → mailin orijinal To header'ına bakar (doğrudan gelenler)
    # - deliveredto: → forward sonrası Delivered-To header'ına bakar
    # Bu sayede info@doviz.com → cemevecen@nokta.com forward zincirleri de yakalanır.
    # from:firebase-noreply → Firebase Console crash/ANR e-posta uyarıları.
    q = _normalize_inbox_gmail_query(
        gmail_query if gmail_query is not None else (settings.inbox_gmail_query or ""),
        lookback_days=lookback_days,
        after_unix=after_unix,
    )
    yield {
        "type": "phase",
        "phase": "listing",
        "message": "Konuşma listesi Gmail’den isteniyor…",
        "query": q,
        "pct": 8,
    }
    try:
        lst = (
            service.users()
            .threads()
            .list(userId="me", q=q, maxResults=max_threads)
            .execute()
        )
        LOGGER.info("Inbox Sync: Gmail listeleme başarılı (query=%s, max=%d)", q, max_threads)
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
        LOGGER.error("Inbox Sync: Gmail listeleme hatası (HTTP %s): %s (query=%s)", st, msg, q)
        raise RuntimeError(f"Gmail ileti listesi alınamadı (HTTP {st}): {msg}") from exc
    thread_list = lst.get("threads") or []
    n = len(thread_list)
    yield {
        "type": "listed",
        "total": n,
        "query": q,
        "message": f"{n} konuşma bulundu; tek tek çekiliyor…",
        "pct": 12,
    }
    synced = 0
    for i, tref in enumerate(thread_list):
        tid = tref.get("id")
        if not tid:
            continue
        yield {
            "type": "thread",
            "step": "fetch",
            "current": i + 1,
            "total": n,
            "gmail_thread_id": tid,
            "message": f"{i + 1}/{n} — konuşma Gmail’den indiriliyor…",
            "pct": max(12, _sync_pct_saved(synced, n)),
        }
        try:
            full = service.users().threads().get(userId="me", id=tid, format="full").execute()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("gmail thread get failed %s: %s", tid, exc)
            yield {
                "type": "thread",
                "step": "skip",
                "current": i + 1,
                "total": n,
                "gmail_thread_id": tid,
                "message": f"{i + 1}/{n} — atlandı (Gmail hatası)",
                "error": str(exc)[:200],
                "pct": max(12, _sync_pct_saved(synced, n)),
            }
            continue
        nmsg = sum(1 for m in (full.get("messages") or []) if m.get("id"))
        subj, snip = _thread_subject_snippet_preview(full)
        _upsert_thread_from_gmail(db, full, account_lower, service)
        synced += 1
        yield {
            "type": "thread",
            "step": "save",
            "current": i + 1,
            "total": n,
            "gmail_thread_id": tid,
            "subject": subj,
            "snippet": snip,
            "messages_written": nmsg,
            "synced_so_far": synced,
            "message": f"{i + 1}/{n} kaydedildi: {subj[:80]}{'…' if len(subj) > 80 else ''}",
            "pct": _sync_pct_saved(synced, n),
        }
    db.commit()
    yield {
        "type": "complete",
        "synced_threads": synced,
        "query": q,
        "message": f"Tamamlandı — {synced} konuşma veritabanına yazıldı.",
        "pct": 100,
    }


def iter_sync_inbox_all_routes(
    db: Session,
    *,
    max_threads_per_route: int = INBOX_SYNC_MAX_THREADS,
    lookback_days: int | None = 60,
    after_unix: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Her sekme için ayrı Gmail sorgusu — sekme başına en fazla max_threads_per_route konuşma."""
    yield {
        "type": "phase",
        "phase": "auth",
        "message": "Gmail oturumu doğrulanıyor…",
        "pct": 2,
    }
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    account_lower = (row.account_email if row else "").strip().lower()
    service = _gmail_service(creds)

    thread_refs: list[tuple[str, dict[str, Any]]] = []
    routes = list(INBOX_ROUTE_GMAIL_QUERIES.items())
    route_counts: dict[str, int] = {}
    for ri, (route, base_q) in enumerate(routes):
        q = _normalize_inbox_gmail_query(
            base_q,
            lookback_days=lookback_days,
            after_unix=after_unix,
            merge_global_clauses=False,
        )
        yield {
            "type": "phase",
            "phase": "listing",
            "message": f"{route} sekmesi taranıyor…",
            "query": q,
            "route": route,
            "pct": 5 + int((ri / max(len(routes), 1)) * 7),
        }
        try:
            lst = (
                service.users()
                .threads()
                .list(userId="me", q=q, maxResults=max_threads_per_route)
                .execute()
            )
        except HttpError as exc:
            msg = _gmail_http_error_message(exc)
            LOGGER.warning("Inbox sync route=%s list failed: %s", route, msg)
            route_counts[route] = 0
            continue
        n_route = 0
        for tref in lst.get("threads") or []:
            tid = str(tref.get("id") or "")
            if tid:
                thread_refs.append((route, tref))
                n_route += 1
        route_counts[route] = n_route
        LOGGER.info("Inbox sync route=%s listed %d threads (query=%s)", route, n_route, q)

    unique_refs: list[tuple[str, dict[str, Any]]] = []
    seen_ids: set[str] = set()
    for route, tref in thread_refs:
        tid = str(tref.get("id") or "")
        if tid and tid not in seen_ids:
            seen_ids.add(tid)
            unique_refs.append((route, tref))

    n = len(unique_refs)
    yield {
        "type": "listed",
        "total": n,
        "route_counts": route_counts,
        "message": f"{n} konuşma bulundu ({len(routes)} sekme); indiriliyor…",
        "pct": 12,
    }
    synced = 0
    for i, (route, tref) in enumerate(unique_refs):
        tid = str(tref.get("id") or "")
        if not tid:
            continue
        yield {
            "type": "thread",
            "step": "fetch",
            "current": i + 1,
            "total": n,
            "route": route,
            "gmail_thread_id": tid,
            "message": f"{i + 1}/{n} [{route}] — konuşma indiriliyor…",
            "pct": max(12, _sync_pct_saved(synced, n)),
        }
        try:
            full = service.users().threads().get(userId="me", id=tid, format="full").execute()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("gmail thread get failed %s: %s", tid, exc)
            continue
        nmsg = sum(1 for m in (full.get("messages") or []) if m.get("id"))
        subj, snip = _thread_subject_snippet_preview(full)
        _upsert_thread_from_gmail(db, full, account_lower, service)
        synced += 1
        yield {
            "type": "thread",
            "step": "save",
            "current": i + 1,
            "total": n,
            "route": route,
            "gmail_thread_id": tid,
            "subject": subj,
            "snippet": snip,
            "messages_written": nmsg,
            "synced_so_far": synced,
            "message": f"{i + 1}/{n} [{route}] kaydedildi: {subj[:70]}{'…' if len(subj) > 70 else ''}",
            "pct": _sync_pct_saved(synced, n),
        }
    db.commit()
    yield {
        "type": "complete",
        "synced_threads": synced,
        "routes": len(routes),
        "message": f"Tamamlandı — {synced} konuşma ({len(routes)} sekme) veritabanına yazıldı.",
        "pct": 100,
    }


def sync_inbox_threads(
    db: Session,
    *,
    max_threads: int = INBOX_SYNC_MAX_THREADS,
    gmail_query: str | None = None,
    lookback_days: int | None = 60,
    after_unix: int | None = None,
) -> dict[str, Any]:
    """Tek JSON yanıtı; gmail_query yoksa tüm sekmeler ayrı ayrı taranır."""
    out: dict[str, Any] | None = None
    if gmail_query is None:
        iterator = iter_sync_inbox_all_routes(
            db,
            max_threads_per_route=max_threads,
            lookback_days=lookback_days,
            after_unix=after_unix,
        )
    else:
        iterator = iter_sync_inbox_threads(
            db,
            max_threads=max_threads,
            gmail_query=gmail_query,
            lookback_days=lookback_days,
            after_unix=after_unix,
        )
    for evt in iterator:
        if evt.get("type") == "complete":
            out = {
                "synced_threads": evt.get("synced_threads", 0),
                "query": evt.get("query", "all-routes"),
                "routes": evt.get("routes"),
            }
    if out is None:
        raise RuntimeError("Senkron tamamlanamadı.")
    return out


def sync_firebase_inbox_threads(db: Session, *, max_threads: int = 25) -> dict[str, Any]:
    """Yalnızca firebase-noreply@google.com uyarılarını hızlı çeker."""
    return sync_inbox_threads(
        db,
        max_threads=max_threads,
        gmail_query=_firebase_only_gmail_query(),
        lookback_days=SCHEDULED_INBOX_SYNC_LOOKBACK_DAYS,
    )


def sync_scheduled_inbox_threads(db: Session, *, max_threads: int = INBOX_SYNC_MAX_THREADS) -> dict[str, Any]:
    """Zamanlanmış inbox senkronu: ilk çalışmada son 3 gün; sonraki çalışmalarda yalnızca son başarılı kontrolden sonraki mailler."""
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    if row is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")

    last_success = row.scheduled_sync_last_success_at
    after_unix: int | None = None
    if last_success is not None:
        after_unix = int(last_success.replace(tzinfo=timezone.utc).timestamp())

    mode = "incremental" if after_unix else "initial"
    LOGGER.info(
        "Scheduled inbox sync (%s): lookback=%dd after=%s",
        mode,
        SCHEDULED_INBOX_SYNC_LOOKBACK_DAYS,
        after_unix,
    )
    try:
        out = sync_inbox_threads(
            db,
            max_threads=max_threads,
            lookback_days=SCHEDULED_INBOX_SYNC_LOOKBACK_DAYS,
            after_unix=after_unix,
        )
        row.scheduled_sync_last_success_at = datetime.utcnow()
        db.commit()
        return {**out, "sync_mode": mode, "after_unix": after_unix}
    except Exception:
        db.rollback()
        raise


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
        route_src += " " + (h.get("from") or "")
        if not subject0:
            subject0 = h.get("subject") or ""

    route_tag = _route_tag_from_thread(msgs_raw, route_src, account_lower)

    row = db.query(SupportInboxThread).filter(SupportInboxThread.gmail_thread_id == tid).first()
    now = datetime.utcnow()
    answered_from_gmail = False
    try:
        answered_lid = _ensure_answered_label_id(service)
        answered_from_gmail = any(
            answered_lid in (m.get("labelIds") or []) for m in msgs_raw
        )
    except (HttpError, RuntimeError) as exc:
        LOGGER.warning("gmail cevaplandı etiketi okunamadı: %s", exc)
        answered_from_gmail = bool(row.answered_flag) if row is not None else False

    if row is None:
        row = SupportInboxThread(
            gmail_thread_id=tid,
            subject=subject0[:998] or "(konu yok)",
            snippet=snippet,
            route_tag=route_tag,
            gmail_unread=gmail_unread,
            answered_flag=answered_from_gmail,
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
        row.answered_flag = answered_from_gmail
        row.last_internal_ms = last_ms
        row.last_synced_at = now

    db.query(SupportInboxMessage).filter(SupportInboxMessage.thread_id == row.id).delete()
    for m in sorted(msgs_raw, key=lambda x: int(x.get("internalDate") or 0)):
        mid = str(m.get("id") or "")
        if not mid:
            continue
        h = _header_map(m)
        body, body_html = _extract_body_parts(
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
                body_html=body_html,
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
        body, body_html = _extract_body_parts(
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
        if body_html and body_html != (mrow.body_html or ""):
            mrow.body_html = body_html
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


def set_thread_gmail_answered(db: Session, *, gmail_thread_id: str, answered: bool) -> None:
    """Gmail thread’e «cevaplandı» özel etiketini ekler veya kaldırır; yerel ``answered_flag`` güncellenir."""
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)
    label_id = _ensure_answered_label_id(service)
    body: dict[str, Any] = {}
    if answered:
        body["addLabelIds"] = [label_id]
    else:
        body["removeLabelIds"] = [label_id]
    try:
        service.users().threads().modify(userId="me", id=gmail_thread_id, body=body).execute()
    except HttpError as exc:
        detail = _gmail_http_error_message(exc)
        st = getattr(getattr(exc, "resp", None), "status", "") or ""
        raise RuntimeError(f"Gmail cevaplandı güncellenemedi (HTTP {st}): {detail}") from exc
    row = db.query(SupportInboxThread).filter(SupportInboxThread.gmail_thread_id == gmail_thread_id).first()
    if row:
        row.answered_flag = answered
        row.last_synced_at = datetime.utcnow()
        db.commit()


def trash_thread_gmail_and_delete_local(db: Session, *, thread_id: int) -> None:
    """Gmail’de thread’i çöpe taşır; yerel konuşmayı ve iletileri siler."""
    row = db.query(SupportInboxThread).filter(SupportInboxThread.id == thread_id).first()
    if row is None:
        raise RuntimeError("Konuşma bulunamadı.")
    gid = (row.gmail_thread_id or "").strip()
    if not gid:
        raise RuntimeError("Geçersiz Gmail thread kimliği.")
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)
    try:
        service.users().threads().trash(userId="me", id=gid).execute()
    except HttpError as exc:
        st_raw = getattr(getattr(exc, "resp", None), "status", None)
        try:
            st = int(st_raw) if st_raw is not None else 0
        except (TypeError, ValueError):
            st = 0
        if st != 404:
            detail = _gmail_http_error_message(exc)
            raise RuntimeError(f"Gmail çöpe taşınamadı (HTTP {st}): {detail}") from exc
    db.delete(row)
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
    cred_row = inbox_gmail_auth.get_inbox_credential_row(db)
    from_account = (cred_row.account_email if cred_row else "").strip()

    msg = MIMEText(body, "plain", "utf-8")
    msg["to"] = to_email.strip()
    msg["subject"] = subject.strip() or "Re:"
    if from_account:
        msg["From"] = from_account
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
    try:
        sent = service.users().messages().send(userId="me", body=send_body).execute()
    except HttpError as exc:
        detail = _gmail_http_error_message(exc)
        st = getattr(getattr(exc, "resp", None), "status", "") or ""
        raise RuntimeError(f"Gmail gönderilemedi (HTTP {st}): {detail}") from exc
    try:
        sync_inbox_threads(db, max_threads=5)
    except Exception as sync_exc:  # noqa: BLE001
        LOGGER.warning("Gmail gönderimi başarılı; gelen kutu senkronu atlandı: %s", sync_exc)
    return str(sent.get("id") or "")
