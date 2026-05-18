"""Ad Manager Policy Center CSV import + URL sayfa başlığı çekme.

Workflow:
  1. Kullanıcı Ad Manager → Policy Center'dan CSV indirir.
  2. /policy sayfasından CSV'yi yükler.
  3. Bu modül CSV'yi parse edip AdPolicyViolation tablosuna UPSERT eder
     (url + issue_type bileşik anahtarı).
  4. Arka planda her satırın URL'sine HTTP isteği atıp <title>'ı çeker.
  5. Excel (.xlsx) export desteği.
"""
from __future__ import annotations

import csv
import io
import json
import logging
import re
import threading
from datetime import date, datetime
from html import unescape
from typing import Any, Iterable
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# ── CSV parse ─────────────────────────────────────────────────────────────────

# Olası header isimleri → standart alan adı.
# Google Ad Manager Policy Center CSV'sinde sütun isimleri dilime/sürüme göre
# farklı olabilir — bu yüzden esnek eşleme yapıyoruz.
_HEADER_ALIASES: dict[str, list[str]] = {
    "url": [
        "url", "page url", "page", "sayfa", "site url", "site", "destination url",
        "landing page", "domain", "page_url",
        # Ad Manager TR export
        "sorunun konumu",
    ],
    "issue_type": [
        "violation type", "violation", "ihlal türü", "ihlal", "issue", "issue type",
        "policy", "policy violation", "policy issue", "reason", "neden",
        # Ad Manager TR export: "Sorunlar" = detaylı ihlal açıklaması
        "sorunlar", "sorun",
    ],
    "policy_topic": [
        # Ad Manager TR: "Sorun türü" = üst-kategori (Politika sorunu / Reklamveren tercihi / Yayıncı politikası)
        "sorun türü", "policy topic", "policy_topic",
    ],
    "enforcement": [
        "enforcement", "enforcement status", "status", "yaptırım", "uygulama",
        "action taken", "enforcement_status",
        # Ad Manager TR: "Durum" = "Kısıtlanmış reklam sunumu" vb.
        "durum",
    ],
    "ad_requests_7d": [
        "ad requests", "ad requests (7 days)", "ad_requests_7d", "ad requests 7d",
        "reklam istekleri", "reklam isteği", "weekly ad requests",
        "weekly_ad_request_count", "weeklyadrequestcount", "ad request count",
        "ad requests (last 7 days)", "ad requests (7d)", "requests",
        # Ad Manager TR export
        "reklam istekleri: son 7 gün", "reklam istekleri son 7 gün",
    ],
    "first_reported": [
        "first detected", "first reported", "first seen", "ilk tespit",
        "ilk bildirim", "first_detected_date", "detected on", "ilk_görülme",
        "ilk görülme",
        # Ad Manager TR: "Bildirim tarihi" = ilk tespit
        "bildirim tarihi",
    ],
    "last_reported": [
        "last detected", "last reported", "last seen", "son tespit",
        "son bildirim", "last_detected_date", "last updated", "son güncelleme",
        "son_görülme", "son görülme",
        # Ad Manager TR
        "son bulunma tarihi",
    ],
    "asset_type": [
        # Ad Manager TR: "Varlık" = Sayfa / Uygulama
        "varlık", "asset", "asset type",
    ],
    "property_codes": [
        # Ad Manager TR: "Mülk kodları" = ca-pub-XXX;ca-video-pub-XXX
        "mülk kodları", "property codes", "ad unit codes",
    ],
}


def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("﻿", "")


def _build_header_map(headers: list[str]) -> dict[str, int]:
    """{standart_alan: csv_kolon_index} eşlemesi üret."""
    norm_headers = [_norm(h) for h in headers]
    out: dict[str, int] = {}
    for std_key, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            alias_norm = _norm(alias)
            for i, h in enumerate(norm_headers):
                if h == alias_norm:
                    out[std_key] = i
                    break
            if std_key in out:
                break
    return out


def _parse_int(v: Any) -> int:
    if v is None:
        return 0
    s = str(v).strip().replace(",", "").replace(".", "").replace(" ", "")
    if not s:
        return 0
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return 0


_DATE_PATTERNS = [
    "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y",
    "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
    "%b %d, %Y", "%d %b %Y",
]


def _parse_date(v: Any) -> date | None:
    if not v:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("-", "n/a", "na", "null"):
        return None
    for fmt in _DATE_PATTERNS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # ISO benzeri ek try
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    return None


def _categorize(issue_type: str) -> str:
    t = (issue_type or "").lower()
    if any(k in t for k in ("sexual", "adult", "cinsel", "yetişkin", "porn", "çıplak")):
        return "Yetişkinlere özel"
    if any(k in t for k in ("shocking", "şok", "violence", "şiddet", "graphic", "kanlı")):
        return "Şok edici içerik"
    if any(k in t for k in ("malware", "phishing", "güvenlik", "security", "harmful", "kötü amaçlı")):
        return "Güvenlik"
    if any(k in t for k in ("copyright", "telif", "trademark", "marka")):
        return "Telif/Marka"
    if any(k in t for k in ("yayıncı içeriği olmayan", "no content", "low value", "düşük değer")):
        return "Yayıncı içeriği yok"
    if any(k in t for k in ("dangerous", "tehlikeli", "weapons", "silah", "uyuşturucu", "drug")):
        return "Tehlikeli içerik"
    if any(k in t for k in ("misleading", "yanıltıcı", "deceptive", "aldatıcı")):
        return "Yanıltıcı içerik"
    if any(k in t for k in ("nefret", "hate", "ırk", "ayrımcılık", "discrimination")):
        return "Nefret/Ayrımcılık"
    if any(k in t for k in ("siyasi", "political", "seçim", "election")):
        return "Siyasi içerik"
    if any(k in t for k in ("hassas", "sensitive", "trajedi", "tragedy")):
        return "Hassas içerik"
    return "Politika sorunu"


def parse_csv(content: bytes) -> tuple[list[dict], list[str], str | None]:
    """CSV'yi parse et.

    Döner: (rows, headers, error_message)
    """
    # Encoding dene
    text = None
    for enc in ("utf-8-sig", "utf-8", "cp1254", "iso-8859-9", "latin-1"):
        try:
            text = content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return [], [], "CSV dosyası okunamadı (encoding sorunu)."

    # Delimiter sniff
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        class _D:
            delimiter = ","
            quotechar = '"'
        dialect = _D  # type: ignore

    reader = csv.reader(io.StringIO(text), dialect)
    rows_raw = list(reader)
    if not rows_raw:
        return [], [], "CSV boş."

    headers = rows_raw[0]
    header_map = _build_header_map(headers)

    if "url" not in header_map:
        return [], headers, (
            f"CSV'de URL kolonu bulunamadı. Mevcut başlıklar: {headers}. "
            "URL içeren bir kolon olmalı (örn. 'URL', 'Page URL', 'Sayfa')."
        )
    if "issue_type" not in header_map:
        return [], headers, (
            f"CSV'de ihlal türü kolonu bulunamadı. Mevcut başlıklar: {headers}. "
            "İhlal/violation içeren bir kolon olmalı."
        )

    rows: list[dict] = []
    skipped_columns = set(range(len(headers))) - set(header_map.values())

    for raw in rows_raw[1:]:
        if not raw or not any(raw):
            continue

        def get(key: str) -> str:
            i = header_map.get(key)
            if i is None or i >= len(raw):
                return ""
            return (raw[i] or "").strip()

        url = get("url")
        issue_type = get("issue_type")
        if not url or not issue_type:
            continue

        # URL eğer http(s) yoksa ekle — Ad Manager "m.sinemalar.com/..." şeklinde verir
        if not url.startswith(("http://", "https://")):
            url = "https://" + url.lstrip("/")

        # Bilinmeyen + bilinen ek bilgiler extras'a
        extras: dict[str, str] = {}
        for i in skipped_columns:
            if i < len(raw) and raw[i]:
                col_name = headers[i] if i < len(headers) else f"col_{i}"
                extras[col_name] = raw[i].strip()
        # Bilinen ama ana alanlarda yer almayan bilgiler
        for key in ("policy_topic", "asset_type", "property_codes"):
            v = get(key)
            if v:
                extras[key] = v

        # Category her zaman issue_type'tan otomatik üretilir (chip renkleri buna bağlı)
        category = _categorize(issue_type)

        rows.append({
            "url": url,
            "issue_type": issue_type,
            "category": category,
            "enforcement": get("enforcement"),
            "ad_requests_7d": _parse_int(get("ad_requests_7d")),
            "first_reported": _parse_date(get("first_reported")),
            "last_reported": _parse_date(get("last_reported")),
            "extra_json": json.dumps(extras, ensure_ascii=False) if extras else "",
        })

    return rows, headers, None


# ── DB import (UPSERT, duplicate koruması) ────────────────────────────────────

def import_rows(db, rows: list[dict]) -> tuple[int, int]:
    """Satırları DB'ye UPSERT et.

    Aynı (url, issue_type) çiftine sahip satır varsa günceller —
    our_status, our_notes, page_title, page_title_fetched_at korunur.

    Döner: (yeni_eklenen, güncellenen)
    """
    from backend.models import AdPolicyViolation

    if not rows:
        return 0, 0

    now = datetime.utcnow()
    new_count = 0
    upd_count = 0

    for r in rows:
        existing = (
            db.query(AdPolicyViolation)
            .filter(
                AdPolicyViolation.url == r["url"],
                AdPolicyViolation.issue_type == r["issue_type"],
            )
            .first()
        )
        if existing:
            existing.category = r["category"] or existing.category
            existing.enforcement = r["enforcement"] or existing.enforcement
            existing.ad_requests_7d = r["ad_requests_7d"]
            if r["first_reported"]:
                if not existing.first_reported or r["first_reported"] < existing.first_reported:
                    existing.first_reported = r["first_reported"]
            existing.last_reported = r["last_reported"] or date.today()
            existing.extra_json = r["extra_json"] or existing.extra_json
            existing.fetched_at = now
            # our_status, our_notes, page_title, page_title_fetched_at KORUNUR
            upd_count += 1
        else:
            db.add(AdPolicyViolation(
                url=r["url"],
                issue_type=r["issue_type"],
                category=r["category"],
                enforcement=r["enforcement"],
                ad_requests_7d=r["ad_requests_7d"],
                first_reported=r["first_reported"] or date.today(),
                last_reported=r["last_reported"] or date.today(),
                page_title="",
                page_title_fetched_at=None,
                extra_json=r["extra_json"],
                our_status="new",
                our_notes="",
                fetched_at=now,
                updated_at=now,
            ))
            new_count += 1

    db.commit()
    return new_count, upd_count


def save_csv_blob(db, filename: str, content: bytes, row_count: int,
                  new_count: int, updated_count: int) -> int:
    """Yüklenen CSV'yi DB'de sakla (sadece son 5 tanesini tut)."""
    from backend.models import PolicyCSVUpload

    upload = PolicyCSVUpload(
        filename=filename,
        row_count=row_count,
        new_count=new_count,
        updated_count=updated_count,
        content=content,
        uploaded_at=datetime.utcnow(),
    )
    db.add(upload)
    db.flush()
    upload_id = upload.id

    # Eski yüklemeleri temizle (son 5'i tut)
    old = (
        db.query(PolicyCSVUpload)
        .order_by(PolicyCSVUpload.uploaded_at.desc())
        .offset(5)
        .all()
    )
    for o in old:
        db.delete(o)
    db.commit()
    return upload_id


def get_latest_upload(db):
    from backend.models import PolicyCSVUpload
    return (
        db.query(PolicyCSVUpload)
        .order_by(PolicyCSVUpload.uploaded_at.desc())
        .first()
    )


# ── Sorgular ──────────────────────────────────────────────────────────────────

def get_violations(db, *, status: str | None = None, category: str | None = None,
                   order_by: str = "ad_requests", limit: int = 2000) -> list[dict]:
    from backend.models import AdPolicyViolation
    from sqlalchemy import desc

    q = db.query(AdPolicyViolation)
    if status and status != "all":
        q = q.filter(AdPolicyViolation.our_status == status)
    if category and category != "all":
        q = q.filter(AdPolicyViolation.category == category)

    if order_by == "date":
        q = q.order_by(desc(AdPolicyViolation.last_reported), desc(AdPolicyViolation.ad_requests_7d))
    elif order_by == "url":
        q = q.order_by(AdPolicyViolation.url)
    else:
        q = q.order_by(desc(AdPolicyViolation.ad_requests_7d))

    return [_violation_to_dict(r) for r in q.limit(limit).all()]


def get_stats(db) -> dict:
    from backend.models import AdPolicyViolation
    from sqlalchemy import func

    total = db.query(func.count(AdPolicyViolation.id)).scalar() or 0
    new_count = db.query(func.count(AdPolicyViolation.id)).filter(
        AdPolicyViolation.our_status == "new"
    ).scalar() or 0
    total_requests = db.query(func.sum(AdPolicyViolation.ad_requests_7d)).scalar() or 0
    last_fetch = db.query(func.max(AdPolicyViolation.fetched_at)).scalar()
    try:
        with_title = db.query(func.count(AdPolicyViolation.id)).filter(
            AdPolicyViolation.page_title != ""
        ).scalar() or 0
    except Exception:
        db.rollback()
        with_title = 0

    by_category: dict[str, int] = {}
    for row in db.query(AdPolicyViolation.category, func.count(AdPolicyViolation.id)).group_by(
        AdPolicyViolation.category
    ).order_by(func.count(AdPolicyViolation.id).desc()).all():
        by_category[row[0] or "Diğer"] = row[1]

    by_status: dict[str, int] = {}
    for row in db.query(AdPolicyViolation.our_status, func.count(AdPolicyViolation.id)).group_by(
        AdPolicyViolation.our_status
    ).all():
        by_status[row[0]] = row[1]

    return {
        "total": total,
        "new": new_count,
        "with_title": with_title,
        "without_title": total - with_title,
        "total_ad_requests_7d": int(total_requests),
        "last_fetch": last_fetch.isoformat() if last_fetch else None,
        "by_category": by_category,
        "by_status": by_status,
    }


def _admin_link(url: str) -> str | None:
    m = re.search(r"/(\d+)(?:/amp)?/?$", url)
    if not m:
        return None
    return f"https://www.sinemalar.com/admin/contents/{m.group(1)}/edit"


def _violation_to_dict(r) -> dict:
    extras = {}
    if r.extra_json:
        try:
            extras = json.loads(r.extra_json)
        except (json.JSONDecodeError, TypeError):
            extras = {}
    return {
        "id": r.id,
        "url": r.url,
        "page_title": r.page_title or "",
        "page_title_fetched_at": r.page_title_fetched_at.isoformat() if r.page_title_fetched_at else None,
        "issue_type": r.issue_type,
        "category": r.category,
        "ad_requests_7d": r.ad_requests_7d,
        "enforcement": r.enforcement,
        "first_reported": r.first_reported.isoformat() if r.first_reported else None,
        "last_reported": r.last_reported.isoformat() if r.last_reported else None,
        "our_status": r.our_status,
        "our_notes": r.our_notes,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        "fetched_at": r.fetched_at.isoformat() if r.fetched_at else None,
        "admin_link": _admin_link(r.url),
        "extras": extras,
    }


# ── Sayfa başlığı çekme ───────────────────────────────────────────────────────

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_OG_TITLE_RE = re.compile(
    r"""<meta\s+[^>]*property\s*=\s*['"]og:title['"]\s+[^>]*content\s*=\s*['"]([^'"]+)['"]""",
    re.IGNORECASE,
)
_TWITTER_TITLE_RE = re.compile(
    r"""<meta\s+[^>]*name\s*=\s*['"]twitter:title['"]\s+[^>]*content\s*=\s*['"]([^'"]+)['"]""",
    re.IGNORECASE,
)

_USER_AGENT = (
    "Mozilla/5.0 (compatible; SeoAgentPolicyBot/1.0; "
    "+https://www.sinemalar.com/admin)"
)


def _clean_title(t: str) -> str:
    t = unescape(t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:480]


def fetch_title(url: str, timeout: float = 10.0) -> str | None:
    """URL'den HTML <title> ya da og:title çek."""
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.5"},
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code >= 400:
            return f"[HTTP {resp.status_code}]"
        # İlk 100 KB yeter
        html = resp.text[:100_000]

        m = _TITLE_RE.search(html)
        if m and m.group(1).strip():
            return _clean_title(m.group(1))

        m = _OG_TITLE_RE.search(html)
        if m:
            return _clean_title(m.group(1))

        m = _TWITTER_TITLE_RE.search(html)
        if m:
            return _clean_title(m.group(1))

        return "[başlık yok]"
    except requests.Timeout:
        return "[timeout]"
    except requests.RequestException as exc:
        return f"[hata: {str(exc)[:80]}]"


# ── Batch title fetch job (in-process state) ──────────────────────────────────

_TITLE_JOB_STATE: dict[str, Any] = {
    "running": False,
    "total": 0,
    "done": 0,
    "started_at": None,
    "finished_at": None,
    "error": None,
}
_TITLE_JOB_LOCK = threading.Lock()


def get_title_job_state() -> dict:
    with _TITLE_JOB_LOCK:
        s = dict(_TITLE_JOB_STATE)
    if s["started_at"]:
        s["started_at"] = s["started_at"].isoformat() if isinstance(s["started_at"], datetime) else s["started_at"]
    if s["finished_at"]:
        s["finished_at"] = s["finished_at"].isoformat() if isinstance(s["finished_at"], datetime) else s["finished_at"]
    return s


def start_title_job(session_factory, *, only_missing: bool = True) -> bool:
    """Sayfa başlıklarını arka planda çek. True döner: başladı; False: zaten çalışıyor."""
    with _TITLE_JOB_LOCK:
        if _TITLE_JOB_STATE["running"]:
            return False
        _TITLE_JOB_STATE["running"] = True
        _TITLE_JOB_STATE["done"] = 0
        _TITLE_JOB_STATE["total"] = 0
        _TITLE_JOB_STATE["started_at"] = datetime.utcnow()
        _TITLE_JOB_STATE["finished_at"] = None
        _TITLE_JOB_STATE["error"] = None

    def _worker():
        from backend.models import AdPolicyViolation
        db = session_factory()
        try:
            q = db.query(AdPolicyViolation)
            if only_missing:
                q = q.filter(AdPolicyViolation.page_title == "")
            # Unique URL listesi (aynı URL birden fazla ihlalde olabilir)
            urls = [u for (u,) in q.with_entities(AdPolicyViolation.url).distinct().all()]
            with _TITLE_JOB_LOCK:
                _TITLE_JOB_STATE["total"] = len(urls)

            for url in urls:
                title = fetch_title(url) or "[başlık yok]"
                now = datetime.utcnow()
                # Aynı URL'ye sahip tüm satırları güncelle
                db.query(AdPolicyViolation).filter(
                    AdPolicyViolation.url == url
                ).update({
                    AdPolicyViolation.page_title: title,
                    AdPolicyViolation.page_title_fetched_at: now,
                }, synchronize_session=False)
                db.commit()
                with _TITLE_JOB_LOCK:
                    _TITLE_JOB_STATE["done"] += 1
        except Exception as exc:
            logger.exception("Title fetch job hatası")
            with _TITLE_JOB_LOCK:
                _TITLE_JOB_STATE["error"] = str(exc)[:300]
        finally:
            db.close()
            with _TITLE_JOB_LOCK:
                _TITLE_JOB_STATE["running"] = False
                _TITLE_JOB_STATE["finished_at"] = datetime.utcnow()

    threading.Thread(target=_worker, daemon=True, name="policy-title-fetch").start()
    return True


def refresh_single_title(db, vid: int) -> str | None:
    """Tek bir satırın sayfa başlığını yeniden çek."""
    from backend.models import AdPolicyViolation
    row = db.query(AdPolicyViolation).filter(AdPolicyViolation.id == vid).first()
    if not row:
        return None
    title = fetch_title(row.url) or "[başlık yok]"
    now = datetime.utcnow()
    # Aynı URL'ye sahip tüm satırları güncelle
    db.query(AdPolicyViolation).filter(
        AdPolicyViolation.url == row.url
    ).update({
        AdPolicyViolation.page_title: title,
        AdPolicyViolation.page_title_fetched_at: now,
    }, synchronize_session=False)
    db.commit()
    return title


# ── Excel export ──────────────────────────────────────────────────────────────

def build_xlsx(violations: list[dict]) -> bytes:
    """Violations listesini .xlsx olarak serialize et."""
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Policy İhlalleri"

    headers = [
        "URL", "Sayfa Başlığı", "İhlal Türü", "Kategori",
        "Yaptırım", "Reklam İsteği (7g)", "İlk Tespit", "Son Tespit",
        "Durum", "Notumuz", "Admin Link", "Güncellendi",
    ]
    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="1F2937")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    for col_i, _ in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_i)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="left", vertical="center")

    status_label = {
        "new": "Yeni", "reviewed": "İncelendi",
        "fixed": "Düzeltildi", "ignored": "Görmezden",
    }

    for v in violations:
        ws.append([
            v.get("url", ""),
            v.get("page_title", ""),
            v.get("issue_type", ""),
            v.get("category", ""),
            v.get("enforcement", ""),
            v.get("ad_requests_7d", 0),
            v.get("first_reported") or "",
            v.get("last_reported") or "",
            status_label.get(v.get("our_status", ""), v.get("our_status", "")),
            v.get("our_notes", ""),
            v.get("admin_link") or "",
            (v.get("updated_at") or "")[:19],
        ])

    widths = [55, 50, 40, 22, 18, 14, 12, 12, 14, 30, 60, 19]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()
