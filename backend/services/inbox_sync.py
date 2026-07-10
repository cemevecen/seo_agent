"""Gmail thread senkronu ve MIME yardımcıları."""

from __future__ import annotations

import base64
import json
import logging
import re
from collections.abc import Iterator
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any

from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.config import settings
from backend.models import SupportInboxMessage, SupportInboxThread
from backend.services import inbox_gmail_auth
from backend.services.inbox_visit_report import is_ziyaret_report_subject

LOGGER = logging.getLogger(__name__)

INBOX_SYNC_MAX_THREADS = 50
INBOX_LIST_LIMIT = 50
INBOX_DEFAULT_TAB = "doviz"

# UI sekmeleri — soldan sağa sıra
INBOX_TAB_ORDER: tuple[str, ...] = ("doviz", "sinemalar", "medya", "nstat", "firebase", "reklam", "all")

# Canonical route_tag değerleri (UI sekmeleriyle birebir)
INBOX_ROUTE_FIREBASE = "firebase"
INBOX_ROUTE_DOVIZ = "doviz"
INBOX_ROUTE_SINEMALAR = "sinemalar"
INBOX_ROUTE_REKLAM = "reklam"
INBOX_ROUTE_MEDYA = "medya"
INBOX_ROUTE_NSTAT = "nstat"
INBOX_ROUTE_ALL = "all"
# Sanal sekme: cevaplanan tüm konuşmalar (tek bir route_tag değil; answered_flag'e göre).
INBOX_ROUTE_ANSWERED = "answered"

# Aynı Gmail thread birden fazla sekme sorgusunda görünürse en spesifik sekme kazanır.
_INBOX_ROUTE_RANK: dict[str, int] = {
    INBOX_ROUTE_FIREBASE: 0,
    INBOX_ROUTE_NSTAT: 1,
    INBOX_ROUTE_REKLAM: 2,
    INBOX_ROUTE_MEDYA: 3,
    INBOX_ROUTE_SINEMALAR: 4,
    INBOX_ROUTE_DOVIZ: 5,
    INBOX_ROUTE_ALL: 6,
}

# all sekmesinde gösterilen paylaşılan destek adresleri
INBOX_ALL_SHARED_ADDRESSES: tuple[str, ...] = (
    "info@blogcu.com",
    "info@izlesene.com",
)

INBOX_ROUTE_LEGACY_MAP: dict[str, str] = {
    "info": INBOX_ROUTE_DOVIZ,
    "feedback": INBOX_ROUTE_DOVIZ,
    "ziyaret": INBOX_ROUTE_NSTAT,
    "tome": INBOX_ROUTE_ALL,
    "mixed": INBOX_ROUTE_ALL,
}

INBOX_ALERT_ROUTES = frozenset({INBOX_ROUTE_FIREBASE, INBOX_ROUTE_NSTAT})

def _gmail_addr_clauses(addr: str) -> str:
    a = addr.strip().lower()
    return f"to:{a} OR deliveredto:{a}"


def _inbox_all_gmail_query() -> str:
    shared = " OR ".join(_gmail_addr_clauses(a) for a in INBOX_ALL_SHARED_ADDRESSES)
    return (
        f"(to:me OR {shared}) "
        "-to:info@doviz.com -to:feedback@doviz.com -to:info@sinemalar.com "
        "-to:feedback@sinemalar.com -to:reklam@nokta.com -to:medya@nokta.com "
        "-from:firebase-noreply@google.com -from:firebase-noreply.googleapis.com "
        "-from:noreply@doviz.com"
    )


INBOX_ROUTE_GMAIL_QUERIES: dict[str, str] = {
    "all": _inbox_all_gmail_query(),
    "doviz": (
        "to:info@doviz.com OR deliveredto:info@doviz.com OR "
        "to:feedback@doviz.com OR deliveredto:feedback@doviz.com"
    ),
    "sinemalar": (
        "to:info@sinemalar.com OR deliveredto:info@sinemalar.com OR "
        "to:feedback@sinemalar.com OR deliveredto:feedback@sinemalar.com"
    ),
    "reklam": _gmail_addr_clauses("reklam@nokta.com"),
    "medya": _gmail_addr_clauses("medya@nokta.com"),
    "nstat": 'from:noreply@doviz.com subject:"ziyaret edilen sayfalar"',
    "firebase": "from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com",
}

# UI sekmesi → veritabanı route_tag (canonical + eski kayıtlar)
INBOX_TAB_ROUTE_TAGS: dict[str, tuple[str, ...]] = {
    "all": ("all", "tome", "mixed"),
    "doviz": ("doviz", "info", "feedback"),
    "sinemalar": ("sinemalar",),
    "reklam": ("reklam",),
    "medya": ("medya",),
    "nstat": ("nstat", "ziyaret"),
    "firebase": ("firebase",),
    # Sanal sekme: route_tag filtresi yerine answered_flag'e göre süzülür (endpoint'te özel ele alınır).
    "answered": (),
}

# Gmail’de «cevaplandı» için kullanılan özel etiket (threads.modify ile eklenir/kaldırılır).
ANSWERED_LABEL_NAME = "SEO-Agent · Cevaplandı"
_answered_label_id_cache: str | None = None

_INFO_DOVIZ_RE = re.compile(r"info@doviz\.com", re.I)
_INFO_SINEMALAR_RE = re.compile(r"info@sinemalar\.com", re.I)
_FIREBASE_FROM_RE = re.compile(r"firebase-noreply@(google\.com|googleapis\.com)", re.I)
_ZIYARET_FROM_RE = re.compile(r"noreply@doviz\.com", re.I)
_SUPPORT_ADDR_MARKERS = (
    "info@doviz.com",
    "feedback@doviz.com",
    "info@sinemalar.com",
    "feedback@sinemalar.com",
    "reklam@nokta.com",
    "medya@nokta.com",
    *INBOX_ALL_SHARED_ADDRESSES,
)

# Instagram / Meta sosyal özetleri (forward ile sinemalar vb. kutusuna düşenler) — inbox’a alınmaz.
INBOX_SOCIAL_DIGEST_EXCLUDE_MARKERS: tuple[str, ...] = (
    "see what's been happening on instagram",
    "others recently added to their stories",
    "others started following you",
    "unread messages",
    "and more in your feed",
    "see what\u2019s been happening on instagram",
)


def _normalize_inbox_exclusion_haystack(*parts: str) -> str:
    text = " ".join(p for p in parts if (p or "").strip())
    text = text.lower().replace("\u2019", "'").replace("\u2018", "'")
    return re.sub(r"\s+", " ", text).strip()


def inbox_thread_is_excluded(
    *,
    subject: str = "",
    snippet: str = "",
    from_addrs: str = "",
) -> bool:
    """Instagram digest ve benzeri otomatik bildirimler."""
    hay = _normalize_inbox_exclusion_haystack(subject, snippet, from_addrs)
    if not hay:
        return False
    for marker in INBOX_SOCIAL_DIGEST_EXCLUDE_MARKERS:
        if marker in hay:
            return True
    if "sinemalarcom" in hay and (
        "instagram" in hay
        or "see what" in hay
        or "stories" in hay
        or "following you" in hay
        or "your feed" in hay
    ):
        return True
    if hay.startswith("sinemalarcom,") or hay.startswith("sinemalarcom "):
        return True
    from_l = (from_addrs or "").lower()
    if ("instagram.com" in from_l or "facebookmail.com" in from_l) and any(
        m in hay for m in INBOX_SOCIAL_DIGEST_EXCLUDE_MARKERS
    ):
        return True
    return False


def _thread_from_addrs_from_gmail(full: dict[str, Any]) -> str:
    parts: list[str] = []
    for m in full.get("messages") or []:
        h = _header_map(m)
        frm = (h.get("from") or "").strip()
        if frm:
            parts.append(frm)
    return " ".join(parts)


def _pick_unique_thread_refs(
    thread_refs: list[tuple[str, dict[str, Any]]],
) -> list[tuple[str, dict[str, Any]]]:
    """Sekme taramalarından gelen thread listesini gmail_thread_id ile tekilleştirir."""
    seen_ids: dict[str, str] = {}
    for route, tref in thread_refs:
        tid = str(tref.get("id") or "")
        if not tid:
            continue
        prev = seen_ids.get(tid)
        if prev is None or _INBOX_ROUTE_RANK.get(route, 99) < _INBOX_ROUTE_RANK.get(prev, 99):
            seen_ids[tid] = route

    unique_refs: list[tuple[str, dict[str, Any]]] = []
    seen_final: set[str] = set()
    for route, tref in thread_refs:
        tid = str(tref.get("id") or "")
        if not tid or tid in seen_final:
            continue
        if seen_ids.get(tid) == route:
            seen_final.add(tid)
            unique_refs.append((route, tref))
    return unique_refs


def _update_thread_row_from_gmail(
    row: SupportInboxThread,
    *,
    subject0: str,
    snippet: str,
    route_tag: str,
    gmail_unread: bool,
    answered_from_gmail: bool,
    last_ms: int,
    now: datetime,
) -> None:
    row.subject = subject0[:998] or row.subject or "(konu yok)"
    row.snippet = snippet or row.snippet
    row.route_tag = route_tag
    row.gmail_unread = gmail_unread
    row.answered_flag = answered_from_gmail
    row.last_internal_ms = last_ms
    row.last_synced_at = now


    if not gmail_thread_id:
        return False
    row = (
        db.query(SupportInboxThread)
        .filter(SupportInboxThread.gmail_thread_id == gmail_thread_id)
        .first()
    )
    if row is None:
        return False
    db.query(SupportInboxMessage).filter(SupportInboxMessage.thread_id == row.id).delete()
    db.delete(row)
    return True


def purge_excluded_inbox_threads(db: Session) -> dict[str, int]:
    """Veritabanındaki daha önce kaydedilmiş sosyal özet konuşmalarını siler."""
    deleted = 0
    for row in db.query(SupportInboxThread).all():
        if inbox_thread_is_excluded(subject=row.subject or "", snippet=row.snippet or ""):
            db.query(SupportInboxMessage).filter(SupportInboxMessage.thread_id == row.id).delete()
            db.delete(row)
            deleted += 1
    if deleted:
        db.commit()
        LOGGER.info("Inbox excluded-thread purge: %d silindi", deleted)
    return {"deleted": deleted}


def _sync_gmail_thread_or_exclude(
    db: Session,
    full: dict[str, Any],
    account_lower: str,
    service: Any,
    *,
    sync_route_hint: str | None = None,
) -> str:
    """Kaydet veya hariç tut. Dönüş: ``saved`` | ``excluded``."""
    tid = str(full.get("id") or "")
    subj, snip = _thread_subject_snippet_preview(full)
    from_text = _thread_from_addrs_from_gmail(full)
    if inbox_thread_is_excluded(subject=subj, snippet=snip, from_addrs=from_text):
        _delete_inbox_thread_by_gmail_id(db, tid)
        return "excluded"
    _upsert_thread_from_gmail(
        db, full, account_lower, service, sync_route_hint=sync_route_hint
    )
    return "saved"


def _is_firebase_sender(text: str) -> bool:
    return bool(_FIREBASE_FROM_RE.search(text or ""))


def _is_ziyaret_sender(text: str) -> bool:
    return bool(_ZIYARET_FROM_RE.search(text or ""))


def _default_inbox_gmail_query() -> str:
    """Destek adresleri + Firebase Crashlytics uyarı mailleri (from: firebase-noreply)."""
    shared = " OR ".join(_gmail_addr_clauses(a) for a in INBOX_ALL_SHARED_ADDRESSES)
    return (
        "("
        "to:info@doviz.com OR to:feedback@doviz.com OR to:info@sinemalar.com OR to:feedback@sinemalar.com "
        "OR deliveredto:info@doviz.com OR deliveredto:feedback@doviz.com "
        "OR deliveredto:info@sinemalar.com OR deliveredto:feedback@sinemalar.com "
        f"OR {shared} OR {_gmail_addr_clauses('reklam@nokta.com')} "
        f"OR {_gmail_addr_clauses('medya@nokta.com')} "
        "OR from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com "
        "OR from:noreply@doviz.com OR to:me"
        ")"
    )


def _firebase_only_gmail_query() -> str:
    return "from:firebase-noreply@google.com OR from:firebase-noreply.googleapis.com"


SCHEDULED_INBOX_SYNC_LOOKBACK_DAYS = 3


def scheduled_sync_after_unix(last_success: datetime | None) -> int | None:
    """Naive UTC veya aware datetime → Gmail ``after:`` unix saniyesi."""
    if last_success is None:
        return None
    if last_success.tzinfo is None:
        aware = last_success.replace(tzinfo=timezone.utc)
    else:
        aware = last_success.astimezone(timezone.utc)
    return int(aware.timestamp())


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


def _part_charset(part: dict[str, Any]) -> str | None:
    for h in part.get("headers") or []:
        if (h.get("name") or "").lower() != "content-type":
            continue
        m = re.search(r"charset\s*=\s*['\"]?([a-zA-Z0-9_\-]+)", h.get("value") or "", re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _decode_bytes(data: bytes, charset: str | None) -> str:
    from backend.services.inbox_email_render import _inbox_text_quality, repair_utf8_mojibake

    encodings: list[str] = []
    if charset:
        encodings.append(charset.strip().lower())
    encodings.extend(["utf-8", "utf-8-sig", "cp1254", "iso-8859-9", "latin-1", "cp1252"])
    seen: set[str] = set()
    candidates: list[str] = []
    for enc in encodings:
        key = enc.replace("_", "-")
        if key in seen:
            continue
        seen.add(key)
        try:
            candidates.append(data.decode(enc))
        except (LookupError, UnicodeDecodeError):
            continue
    if not candidates:
        return repair_utf8_mojibake(data.decode("utf-8", errors="replace"))

    best = candidates[0]
    best_score = -10_000
    for raw in candidates:
        for variant in (raw, repair_utf8_mojibake(raw)):
            sc = _inbox_text_quality(variant)
            if sc > best_score:
                best_score = sc
                best = variant
    return best


def _decode_b64url(data: str, *, charset: str | None = None) -> str:
    raw = data.replace("-", "+").replace("_", "/")
    pad = len(raw) % 4
    if pad:
        raw += "=" * (4 - pad)
    try:
        return _decode_bytes(base64.b64decode(raw), charset)
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


_INLINE_IMAGE_MIME_TYPES = {
    "image/png",
    "image/jpeg",
    "image/jpg",
    "image/gif",
    "image/webp",
    "image/bmp",
}


def _part_header_map(part: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for h in part.get("headers") or []:
        name = (h.get("name") or "").strip().lower()
        if name:
            out[name] = (h.get("value") or "").strip()
    return out


def _normalize_b64url_for_data_uri(raw: str | None) -> str:
    data = (raw or "").strip()
    if not data:
        return ""
    data = data.replace("-", "+").replace("_", "/")
    pad = len(data) % 4
    if pad:
        data += "=" * (4 - pad)
    return data


def _attachment_data_b64(
    service: Any,
    *,
    user_id: str,
    gmail_message_id: str,
    attachment_id: str,
) -> str:
    try:
        att = (
            service.users()
            .messages()
            .attachments()
            .get(userId=user_id, messageId=gmail_message_id, id=attachment_id)
            .execute()
        )
        return _normalize_b64url_for_data_uri(att.get("data"))
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("gmail image attachment fetch failed mid=%s aid=%s: %s", gmail_message_id, attachment_id, exc)
        return ""


def _extract_image_attachments_from_payload(
    payload: dict[str, Any],
    *,
    service: Any,
    gmail_message_id: str,
    user_id: str = "me",
) -> list[dict[str, Any]]:
    """Gmail MIME ağacındaki image/* parçalarını data URI olarak çıkarır."""
    images: list[dict[str, Any]] = []

    def walk(part: dict[str, Any]) -> None:
        mime = (part.get("mimeType") or "").lower().strip()
        body = part.get("body") or {}
        headers = _part_header_map(part)
        if mime in _INLINE_IMAGE_MIME_TYPES:
            raw_b64 = _normalize_b64url_for_data_uri(body.get("data"))
            if not raw_b64 and body.get("attachmentId"):
                raw_b64 = _attachment_data_b64(
                    service,
                    user_id=user_id,
                    gmail_message_id=gmail_message_id,
                    attachment_id=str(body.get("attachmentId") or ""),
                )
            if raw_b64:
                cid = (headers.get("content-id") or "").strip().strip("<>")
                disposition = (headers.get("content-disposition") or "").strip()
                filename = str(part.get("filename") or "").strip()
                images.append(
                    {
                        "filename": filename or "image",
                        "mime_type": mime,
                        "content_id": cid,
                        "content_disposition": disposition,
                        "size": int(body.get("size") or 0),
                        "data_uri": f"data:{mime};base64,{raw_b64}",
                    }
                )
        for child in part.get("parts") or []:
            walk(child)

    walk(payload or {})
    return images


def fetch_image_attachments_for_messages(
    db: Session,
    gmail_message_ids: list[str],
    *,
    user_id: str = "me",
) -> dict[str, list[dict[str, Any]]]:
    """Seçili thread detayında gösterilecek imaj eklerini Gmail'den çeker."""
    ids = [str(x or "").strip() for x in gmail_message_ids if str(x or "").strip()]
    if not ids:
        return {}
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        return {}
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)
    out: dict[str, list[dict[str, Any]]] = {}
    for gid in ids:
        try:
            full = service.users().messages().get(userId=user_id, id=gid, format="full").execute()
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("gmail message get for image attachments failed %s: %s", gid, exc)
            continue
        imgs = _extract_image_attachments_from_payload(
            full.get("payload") or {},
            service=service,
            gmail_message_id=gid,
            user_id=user_id,
        )
        if imgs:
            out[gid] = imgs
    return out


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
        charset = _part_charset(p)
        body = p.get("body") or {}
        data = body.get("data")
        aid = body.get("attachmentId")
        chunk = ""
        if data:
            chunk = _decode_b64url(data, charset=charset)
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
    html_body = "\n".join(html_parts).strip()
    from backend.services.inbox_email_render import effective_plain_text

    plain_text = effective_plain_text("\n\n".join(plain).strip(), html_body)
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


_ROUTE_HEADER_KEYS = (
    "delivered-to",
    "to",
    "cc",
    "x-original-to",
    "envelope-to",
    "x-forwarded-to",
    "x-envelope-to",
)


def _route_text_from_headers(h: dict[str, str]) -> str:
    parts: list[str] = []
    for key in _ROUTE_HEADER_KEYS:
        val = (h.get(key) or "").strip()
        if val:
            parts.append(val)
    from_ = (h.get("from") or "").strip()
    if from_:
        parts.append(from_)
    return " ".join(parts)


def normalize_inbox_route_tag(route_tag: str | None) -> str:
    """Eski route_tag değerlerini yeni sekmelere map eder."""
    tag = (route_tag or "").strip().lower()
    if not tag:
        return INBOX_ROUTE_ALL
    return INBOX_ROUTE_LEGACY_MAP.get(tag, tag)


def migrate_legacy_inbox_route_tags(db: Session) -> int:
    """DB'deki eski etiketleri (info/ziyaret/tome vb.) yeni isimlere taşır."""
    changed = 0
    for row in db.query(SupportInboxThread).all():
        canonical = normalize_inbox_route_tag(row.route_tag)
        if row.route_tag != canonical:
            row.route_tag = canonical
            changed += 1
    if changed:
        db.commit()
        LOGGER.info("Inbox legacy route migration: %d thread güncellendi", changed)
    return changed


def _route_tag_from_addrs(text: str) -> str | None:
    t = (text or "").lower()

    if "reklam@nokta.com" in t:
        return INBOX_ROUTE_REKLAM
    if "medya@nokta.com" in t:
        return INBOX_ROUTE_MEDYA

    has_info_doviz = "info@doviz.com" in t or "feedback@doviz.com" in t
    has_info_sinemalar = "info@sinemalar.com" in t or "feedback@sinemalar.com" in t

    found: list[str] = []
    if has_info_doviz:
        found.append(INBOX_ROUTE_DOVIZ)
    if has_info_sinemalar:
        found.append(INBOX_ROUTE_SINEMALAR)

    if len(found) > 1:
        for pref in (INBOX_ROUTE_SINEMALAR, INBOX_ROUTE_DOVIZ):
            if pref in found:
                return pref
        return found[0]
    if len(found) == 1:
        return found[0]
    return None


def _finalize_route_tag(
    computed: str,
    route_src: str,
    sync_route_hint: str | None,
    *,
    subject: str = "",
) -> str:
    """Header'dan net rota varsa onu kullan; yoksa Gmail sekme sorgusunu ipucu al."""
    header_tag = _route_tag_from_addrs(route_src)
    if header_tag:
        return header_tag
    hint = normalize_inbox_route_tag(sync_route_hint)
    if hint == INBOX_ROUTE_NSTAT and computed in (INBOX_ROUTE_ALL, hint):
        if is_ziyaret_report_subject(subject):
            return hint
        return normalize_inbox_route_tag(computed)
    if hint == INBOX_ROUTE_FIREBASE and computed in (INBOX_ROUTE_ALL, hint):
        return hint
    if hint == INBOX_ROUTE_REKLAM and computed in (INBOX_ROUTE_ALL, hint):
        return hint
    if hint == INBOX_ROUTE_MEDYA and computed in (INBOX_ROUTE_ALL, hint):
        return hint
    if hint in (
        INBOX_ROUTE_DOVIZ,
        INBOX_ROUTE_SINEMALAR,
        INBOX_ROUTE_REKLAM,
        INBOX_ROUTE_MEDYA,
    ) and computed == INBOX_ROUTE_ALL:
        return hint
    return normalize_inbox_route_tag(computed)


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
        for key in _ROUTE_HEADER_KEYS + ("from",):
            hdr = (h.get(key) or "").lower()
            if account_lower in hdr:
                return True
    return account_lower in t


def _thread_subject_from_msgs(msgs_raw: list[dict[str, Any]]) -> str:
    for m in msgs_raw:
        h = _header_map(m)
        subj = (h.get("subject") or "").strip()
        if subj:
            return subj
    return ""


def _route_tag_from_thread(
    msgs_raw: list[dict[str, Any]], route_src: str, account_lower: str = ""
) -> str:
    subject = _thread_subject_from_msgs(msgs_raw)
    for m in msgs_raw:
        h = _header_map(m)
        if _is_firebase_sender(h.get("from") or ""):
            return "firebase"
    for m in msgs_raw:
        h = _header_map(m)
        if _is_ziyaret_sender(h.get("from") or ""):
            subj = (h.get("subject") or subject or "").strip()
            if is_ziyaret_report_subject(subj):
                return INBOX_ROUTE_NSTAT
            return INBOX_ROUTE_ALL
    tag = _route_tag_from_addrs(route_src)
    if tag:
        return tag
    if _is_direct_to_account(msgs_raw, route_src, account_lower):
        return INBOX_ROUTE_ALL
    return INBOX_ROUTE_ALL


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
        outcome = _sync_gmail_thread_or_exclude(db, full, account_lower, service)
        if outcome == "excluded":
            yield {
                "type": "thread",
                "step": "skip",
                "current": i + 1,
                "total": n,
                "gmail_thread_id": tid,
                "subject": subj,
                "message": f"{i + 1}/{n} — sosyal özet (Instagram); inbox dışı",
                "pct": max(12, _sync_pct_saved(synced, n)),
            }
            continue
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
    repair = repair_misrouted_inbox_threads(db)
    purged = purge_excluded_inbox_threads(db)
    yield {
        "type": "complete",
        "synced_threads": synced,
        "query": q,
        "repaired_route_tags": repair.get("repaired", 0),
        "purged_excluded_threads": purged.get("deleted", 0),
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
    yield {
        "type": "phase",
        "phase": "auth_refresh",
        "message": "Gmail token yenileniyor…",
        "pct": 3,
    }
    creds = _ensure_fresh_creds(db, creds)
    yield {
        "type": "phase",
        "phase": "auth_ready",
        "message": "Gmail bağlantısı hazır…",
        "pct": 4,
    }
    row = inbox_gmail_auth.get_inbox_credential_row(db)
    account_lower = (row.account_email if row else "").strip().lower()
    service = _gmail_service(creds)

    thread_refs: list[tuple[str, dict[str, Any]]] = []
    routes = [(route, INBOX_ROUTE_GMAIL_QUERIES[route]) for route in INBOX_TAB_ORDER]
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

    unique_refs = _pick_unique_thread_refs(thread_refs)

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
            yield {
                "type": "thread",
                "step": "skip",
                "current": i + 1,
                "total": n,
                "route": route,
                "gmail_thread_id": tid,
                "message": f"{i + 1}/{n} [{route}] — atlandı (Gmail hatası)",
                "error": str(exc)[:200],
                "pct": max(12, _sync_pct_saved(synced, n)),
            }
            continue
        nmsg = sum(1 for m in (full.get("messages") or []) if m.get("id"))
        subj, snip = _thread_subject_snippet_preview(full)
        outcome = _sync_gmail_thread_or_exclude(
            db, full, account_lower, service, sync_route_hint=route
        )
        if outcome == "excluded":
            yield {
                "type": "thread",
                "step": "skip",
                "current": i + 1,
                "total": n,
                "route": route,
                "gmail_thread_id": tid,
                "subject": subj,
                "message": f"{i + 1}/{n} [{route}] — sosyal özet; inbox dışı",
                "pct": max(12, _sync_pct_saved(synced, n)),
            }
            continue
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
        "type": "phase",
        "phase": "finalize",
        "message": "Kayıtlar düzeltiliyor…",
        "pct": 98,
    }
    repair = repair_misrouted_inbox_threads(db)
    purged = purge_excluded_inbox_threads(db)
    yield {
        "type": "complete",
        "synced_threads": synced,
        "routes": len(routes),
        "route_counts": route_counts,
        "repaired_route_tags": repair.get("repaired", 0),
        "purged_excluded_threads": purged.get("deleted", 0),
        "message": f"Tamamlandı — {synced} konuşma ({len(routes)} sekme) veritabanına yazıldı.",
        "pct": 100,
    }


def repair_misrouted_inbox_threads(db: Session) -> dict[str, Any]:
    """Kayıtlı To/Delivered-To alanlarından route_tag yeniden hesaplar."""
    migrate_legacy_inbox_route_tags(db)
    changed = 0
    rows = db.query(SupportInboxThread).all()
    for row in rows:
        msgs = (
            db.query(SupportInboxMessage)
            .filter(SupportInboxMessage.thread_id == row.id)
            .order_by(SupportInboxMessage.internal_ms.asc())
            .all()
        )
        subject = (row.subject or "").strip()
        if not subject and msgs:
            subject = (msgs[0].subject or "").strip()

        if row.route_tag in (INBOX_ROUTE_NSTAT, "ziyaret"):
            if not is_ziyaret_report_subject(subject):
                row.route_tag = INBOX_ROUTE_ALL
                changed += 1
            continue

        if row.route_tag == INBOX_ROUTE_FIREBASE:
            continue

        route_src = " ".join(f"{m.to_addr} {m.from_addr}" for m in msgs)
        rerouted = False
        for m in msgs:
            if _is_firebase_sender(m.from_addr or ""):
                if row.route_tag != INBOX_ROUTE_FIREBASE:
                    row.route_tag = INBOX_ROUTE_FIREBASE
                    changed += 1
                rerouted = True
                break
            if _is_ziyaret_sender(m.from_addr or "") and is_ziyaret_report_subject(
                (m.subject or subject or "").strip()
            ):
                if row.route_tag != INBOX_ROUTE_NSTAT:
                    row.route_tag = INBOX_ROUTE_NSTAT
                    changed += 1
                rerouted = True
                break
        if rerouted:
            continue
        header_tag = _route_tag_from_addrs(route_src)
        if header_tag:
            header_tag = normalize_inbox_route_tag(header_tag)
        if header_tag and header_tag != row.route_tag:
            row.route_tag = header_tag
            changed += 1
    if changed:
        db.commit()
        LOGGER.info("Inbox route repair: %d thread güncellendi", changed)
    return {"repaired": changed}


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
    after_unix = scheduled_sync_after_unix(last_success)
    mode = "incremental" if after_unix else "initial"
    lookback_days = SCHEDULED_INBOX_SYNC_LOOKBACK_DAYS if after_unix is None else None
    LOGGER.info(
        "Scheduled inbox sync (%s): lookback=%s after=%s",
        mode,
        f"{lookback_days}d" if lookback_days else "incremental-only",
        after_unix,
    )
    try:
        out = sync_inbox_threads(
            db,
            max_threads=max_threads,
            lookback_days=lookback_days,
            after_unix=after_unix,
        )
        row.scheduled_sync_last_success_at = datetime.utcnow()
        db.commit()
        return {**out, "sync_mode": mode, "after_unix": after_unix}
    except Exception:
        db.rollback()
        raise


def _upsert_thread_from_gmail(
    db: Session,
    full: dict[str, Any],
    account_lower: str,
    service: Any,
    *,
    sync_route_hint: str | None = None,
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
        route_src += " " + _route_text_from_headers(h)
        if not subject0:
            subject0 = h.get("subject") or ""

    from backend.services.inbox_email_render import normalize_inbox_text

    subject0 = normalize_inbox_text(subject0)
    snippet = normalize_inbox_text(snippet)

    computed_tag = _route_tag_from_thread(msgs_raw, route_src, account_lower)
    route_tag = normalize_inbox_route_tag(
        _finalize_route_tag(computed_tag, route_src, sync_route_hint, subject=subject0)
    )

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
        try:
            with db.begin_nested():
                db.flush()
        except IntegrityError:
            db.expunge(row)
            row = (
                db.query(SupportInboxThread)
                .filter(SupportInboxThread.gmail_thread_id == tid)
                .first()
            )
            if row is None:
                raise
            LOGGER.info(
                "Inbox thread upsert race resolved for gmail_thread_id=%s (concurrent insert)",
                tid,
            )
            _update_thread_row_from_gmail(
                row,
                subject0=subject0,
                snippet=snippet,
                route_tag=route_tag,
                gmail_unread=gmail_unread,
                answered_from_gmail=answered_from_gmail,
                last_ms=last_ms,
                now=now,
            )
    else:
        _update_thread_row_from_gmail(
            row,
            subject0=subject0,
            snippet=snippet,
            route_tag=route_tag,
            gmail_unread=gmail_unread,
            answered_from_gmail=answered_from_gmail,
            last_ms=last_ms,
            now=now,
        )

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
        to_parts = [h.get("to"), h.get("delivered-to"), h.get("x-original-to"), h.get("envelope-to")]
        to_combined = " ".join(p for p in to_parts if p).strip()[:512]
        db.add(
            SupportInboxMessage(
                thread_id=row.id,
                gmail_message_id=mid,
                from_addr=(h.get("from") or "")[:512],
                to_addr=to_combined or (h.get("to") or "")[:512],
                subject=normalize_inbox_text(h.get("subject") or "")[:998],
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


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
INBOX_SEND_AS_CHOICES: tuple[str, ...] = (
    "cemevecen@nokta.com",
    "info@doviz.com",
    "info@sinemalar.com",
)


def _emails_in(text: str) -> list[str]:
    """Metindeki e-posta adreslerini (küçük harf, sırayı koruyarak, tekilleştirerek) döndürür."""
    out: list[str] = []
    for m in _EMAIL_RE.findall(text or ""):
        e = m.lower()
        if e not in out:
            out.append(e)
    return out


def normalize_requested_send_as(value: str | None) -> str:
    """UI'dan gelen From seçimini güvenli allow-list'e indirger."""
    raw = (value or "").strip()
    if not raw:
        return ""
    emails = _emails_in(raw)
    email = (emails[0] if emails else raw).strip().lower()
    if email not in INBOX_SEND_AS_CHOICES:
        allowed = ", ".join(INBOX_SEND_AS_CHOICES)
        raise RuntimeError(f"Gönderen adresi izinli değil: {email}. İzinli adresler: {allowed}")
    return email


def _list_send_as_aliases(service) -> list[dict]:
    """Hesapta TANIMLI ve doğrulanmış 'send-as' alias'larını döndürür."""
    try:
        resp = service.users().settings().sendAs().list(userId="me").execute()
        return [
            a for a in (resp.get("sendAs") or [])
            # Sadece doğrulanmış (gönderime uygun) alias'lar; default/primary de dahil.
            if (a.get("verificationStatus") in (None, "", "accepted") or a.get("isPrimary"))
        ]
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Gmail sendAs alias listesi alınamadı: %s", exc)
        return []


def _resolve_reply_from(db: Session, service, *, gmail_thread_id: str, account_email: str) -> str:
    """Cevabın 'From' adresini belirler.

    Mesaj hangi adrese geldiyse (örn. feedback@nokta.com) cevap o adresten gider —
    yeter ki o adres hesapta doğrulanmış bir 'send-as' alias olsun. Eşleşme yoksa
    bağlı hesabın ana adresine düşülür.
    """
    thread = (
        db.query(SupportInboxThread)
        .filter(SupportInboxThread.gmail_thread_id == gmail_thread_id)
        .first()
    )
    recipient_emails: list[str] = []
    if thread is not None:
        inbound = (
            db.query(SupportInboxMessage)
            .filter(
                SupportInboxMessage.thread_id == thread.id,
                SupportInboxMessage.is_outbound.is_(False),
            )
            .order_by(SupportInboxMessage.internal_ms.desc())
            .all()
        )
        for m in inbound:
            for e in _emails_in(m.to_addr):
                if e not in recipient_emails:
                    recipient_emails.append(e)

    aliases = _list_send_as_aliases(service)
    alias_map = {(a.get("sendAsEmail") or "").strip().lower(): a for a in aliases}

    # Gelen mesajın ulaştığı adreslerden, hesapta send-as olarak tanımlı OLAN ilkini seç.
    for e in recipient_emails:
        alias = alias_map.get(e)
        if alias:
            email = (alias.get("sendAsEmail") or "").strip()
            disp = (alias.get("displayName") or "").strip()
            return formataddr((disp, email)) if disp else email

    # Yedek: bağlı hesabın ana adresi (varsa display adıyla)
    primary = next((a for a in aliases if a.get("isPrimary")), None)
    if primary:
        email = (primary.get("sendAsEmail") or "").strip() or account_email
        disp = (primary.get("displayName") or "").strip()
        return formataddr((disp, email)) if disp else email
    return account_email


def _resolve_requested_send_as(service, requested_from_email: str, *, account_email: str) -> str:
    """Seçilen From adresini Gmail send-as alias listesinde doğrular."""
    requested = normalize_requested_send_as(requested_from_email)
    if not requested:
        return ""
    aliases = _list_send_as_aliases(service)
    alias_map = {(a.get("sendAsEmail") or "").strip().lower(): a for a in aliases}
    alias = alias_map.get(requested)
    if not alias:
        if requested == (account_email or "").strip().lower():
            return account_email
        raise RuntimeError(
            f"Gmail hesabında '{requested}' gönderici alias olarak tanımlı/doğrulanmış değil. "
            "Gmail Settings > Accounts > Send mail as alanından eklenmeli."
        )
    email = (alias.get("sendAsEmail") or "").strip()
    disp = (alias.get("displayName") or "").strip()
    return formataddr((disp, email)) if disp else email


def send_reply_plain(
    db: Session,
    *,
    gmail_thread_id: str,
    to_email: str,
    subject: str,
    body: str,
    reply_to_gmail_message_id: str | None = None,
    from_email: str | None = None,
) -> str:
    creds = inbox_gmail_auth.load_inbox_credentials(db)
    if creds is None:
        raise RuntimeError("Gmail gelen kutusu bağlı değil.")
    creds = _ensure_fresh_creds(db, creds)
    service = _gmail_service(creds)
    cred_row = inbox_gmail_auth.get_inbox_credential_row(db)
    from_account = (cred_row.account_email if cred_row else "").strip()

    if from_email:
        from_value = _resolve_requested_send_as(service, from_email, account_email=from_account)
    else:
        # Mesaj hangi adrese geldiyse cevap o adresten gitsin (doğrulanmış send-as alias ise).
        from_value = _resolve_reply_from(
            db, service, gmail_thread_id=gmail_thread_id, account_email=from_account
        )

    msg = MIMEText(body, "plain", "utf-8")
    msg["to"] = to_email.strip()
    msg["subject"] = subject.strip() or "Re:"
    if from_value:
        msg["From"] = from_value
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
