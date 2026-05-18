"""
SEO Meta Tag Monitoring servisi.
UrlAuditRecord'dan issue özetleri, duplicate tespiti, günlük snapshot ve regresyon diff'i.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from urllib.parse import urlparse

from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Sağlıklı aralıklar
TITLE_MIN, TITLE_MAX = 20, 65
DESC_MIN, DESC_MAX = 70, 170


def _issues_for(row) -> list[str]:
    issues = []
    if not row.has_title:
        issues.append("title_missing")
    elif row.title_length < TITLE_MIN:
        issues.append("title_short")
    elif row.title_length > TITLE_MAX:
        issues.append("title_long")

    if not row.has_meta_description:
        issues.append("desc_missing")
    elif row.meta_description_length < DESC_MIN:
        issues.append("desc_short")
    elif row.meta_description_length > DESC_MAX:
        issues.append("desc_long")

    if not row.has_canonical:
        issues.append("canonical_missing")
    elif not row.canonical_matches_final:
        issues.append("canonical_mismatch")

    if row.is_noindex:
        issues.append("noindex")

    if not row.has_og_title or not row.has_og_description:
        issues.append("og_missing")

    # H1 kontrolü
    if not row.has_h1:
        issues.append("h1_missing")
    elif row.h1_count > 1:
        issues.append("h1_multiple")

    # Schema (yapılandırılmış veri) kontrolü
    if not row.has_schema:
        issues.append("schema_missing")

    # H2 kontrolü (sütun henüz eklenmemiş DB'lerde getattr ile güvenli)
    if getattr(row, "h2_count", 0) == 0:
        issues.append("h2_missing")

    return issues


def _cnt(cond) -> Any:
    """Koşulu karşılayan satır sayısı — COUNT(CASE WHEN cond THEN 1 END)."""
    return func.count(case((cond, 1)))


def _base_summary_query(db, M, site_id, include_h2: bool):
    exprs = [
        func.count(M.id).label("total"),
        func.max(M.collected_at).label("last_crawled"),
        _cnt(M.seo_score == "good").label("good"),
        _cnt(M.seo_score == "needs_improvement").label("needs_improvement"),
        _cnt(M.seo_score == "poor").label("poor"),
        _cnt(M.has_title.is_(False)).label("missing_title"),
        _cnt(and_(M.has_title.is_(True), M.title_length < TITLE_MIN)).label("short_title"),
        _cnt(and_(M.has_title.is_(True), M.title_length > TITLE_MAX)).label("long_title"),
        _cnt(M.has_meta_description.is_(False)).label("missing_desc"),
        _cnt(and_(M.has_meta_description.is_(True), M.meta_description_length < DESC_MIN)).label("short_desc"),
        _cnt(and_(M.has_meta_description.is_(True), M.meta_description_length > DESC_MAX)).label("long_desc"),
        _cnt(M.has_canonical.is_(False)).label("missing_canonical"),
        _cnt(and_(M.has_canonical.is_(True), M.canonical_matches_final.is_(False))).label("broken_canonical"),
        _cnt(M.is_noindex.is_(True)).label("noindex"),
        _cnt(or_(M.has_og_title.is_(False), M.has_og_description.is_(False))).label("missing_og"),
        _cnt(M.has_h1.is_(False)).label("missing_h1"),
        _cnt(M.h1_count > 1).label("multiple_h1"),
        _cnt(M.has_schema.is_(False)).label("missing_schema"),
    ]
    if include_h2:
        exprs.append(_cnt(M.h2_count == 0).label("missing_h2"))
    else:
        exprs.append(func.count(None).label("missing_h2"))
    return db.query(*exprs).filter(M.site_id == site_id).first()


def get_audit_summary(db: Session, site_id: int) -> dict[str, Any]:
    """SQL aggregation ile site geneli SEO özeti — RAM'a tüm satırları yüklemez."""
    from backend.models import UrlAuditRecord as M

    try:
        row = _base_summary_query(db, M, site_id, include_h2=True)
    except Exception:
        db.rollback()
        try:
            row = _base_summary_query(db, M, site_id, include_h2=False)
        except Exception:
            db.rollback()
            return {"total_pages": 0, "score_counts": {}, "issue_counts": {}, "total_issues": 0,
                    "duplicate_title_groups": 0, "duplicate_desc_groups": 0, "last_crawled": None}

    if not row or not row.total:
        return {"total_pages": 0, "score_counts": {}, "issue_counts": {}, "total_issues": 0,
                "duplicate_title_groups": 0, "duplicate_desc_groups": 0, "last_crawled": None}

    # Duplicate grup sayısı — subdomain ayrımıyla Python'da hesapla
    _dup_rows = (
        db.query(M.url, M.title, M.meta_description, M.has_title, M.has_meta_description)
        .filter(M.site_id == site_id)
        .limit(5000)
        .all()
    )

    def _count_dup_groups(value_attr, has_attr):
        buckets: dict[tuple[str, str], set[str]] = {}
        for r in _dup_rows:
            val = getattr(r, value_attr) or ""
            has = getattr(r, has_attr)
            if not has or not val:
                continue
            host = _url_host(r.url)
            norm = _normalize_url(r.url)
            buckets.setdefault((host, val), set()).add(norm)
        return sum(1 for urls in buckets.values() if len(urls) > 1)

    title_dup_count = _count_dup_groups("title", "has_title")
    desc_dup_count = _count_dup_groups("meta_description", "has_meta_description")

    issue_counts = {
        "missing_title": row.missing_title or 0,
        "short_title": row.short_title or 0,
        "long_title": row.long_title or 0,
        "missing_desc": row.missing_desc or 0,
        "short_desc": row.short_desc or 0,
        "long_desc": row.long_desc or 0,
        "missing_canonical": row.missing_canonical or 0,
        "broken_canonical": row.broken_canonical or 0,
        "noindex": row.noindex or 0,
        "missing_og": row.missing_og or 0,
        "missing_h1": row.missing_h1 or 0,
        "multiple_h1": row.multiple_h1 or 0,
        "missing_schema": row.missing_schema or 0,
        "missing_h2": row.missing_h2 or 0,
    }

    return {
        "total_pages": row.total or 0,
        "score_counts": {
            "good": row.good or 0,
            "needs_improvement": row.needs_improvement or 0,
            "poor": row.poor or 0,
        },
        "issue_counts": issue_counts,
        "total_issues": sum(issue_counts.values()),
        "duplicate_title_groups": title_dup_count,
        "duplicate_desc_groups": desc_dup_count,
        "last_crawled": row.last_crawled.isoformat() if row.last_crawled else None,
    }


_FILTER_MAP = {
    "poor": lambda q, M: q.filter(M.seo_score == "poor"),
    "needs_improvement": lambda q, M: q.filter(M.seo_score == "needs_improvement"),
    "missing_title": lambda q, M: q.filter(M.has_title.is_(False)),
    "short_title": lambda q, M: q.filter(M.has_title.is_(True), M.title_length < TITLE_MIN),
    "long_title": lambda q, M: q.filter(M.has_title.is_(True), M.title_length > TITLE_MAX),
    "missing_desc": lambda q, M: q.filter(M.has_meta_description.is_(False)),
    "short_desc": lambda q, M: q.filter(M.has_meta_description.is_(True), M.meta_description_length < DESC_MIN),
    "long_desc": lambda q, M: q.filter(M.has_meta_description.is_(True), M.meta_description_length > DESC_MAX),
    "missing_canonical": lambda q, M: q.filter(M.has_canonical.is_(False)),
    "broken_canonical": lambda q, M: q.filter(M.has_canonical.is_(True), M.canonical_matches_final.is_(False)),
    "noindex": lambda q, M: q.filter(M.is_noindex.is_(True)),
    "missing_og": lambda q, M: q.filter(or_(M.has_og_title.is_(False), M.has_og_description.is_(False))),
    "missing_h1": lambda q, M: q.filter(M.has_h1.is_(False)),
    "multiple_h1": lambda q, M: q.filter(M.h1_count > 1),
    "missing_schema": lambda q, M: q.filter(M.has_schema.is_(False)),
    "missing_h2": lambda q, M: q.filter(M.h2_count == 0),
}


def get_audit_issues(
    db: Session,
    site_id: int,
    filter_key: str = "all",
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Filtrelenmiş URL audit listesi döner."""
    from backend.models import UrlAuditRecord

    q = db.query(UrlAuditRecord).filter(UrlAuditRecord.site_id == site_id)

    if filter_key in _FILTER_MAP:
        q = _FILTER_MAP[filter_key](q, UrlAuditRecord)

    try:
        rows = q.order_by(UrlAuditRecord.seo_score, UrlAuditRecord.url).offset(offset).limit(limit).all()
    except Exception:
        db.rollback()
        rows = []

    result = []
    for r in rows:
        result.append({
            "url": r.url,
            "title": r.title,
            "title_length": r.title_length,
            "has_title": r.has_title,
            "meta_description": r.meta_description,
            "meta_description_length": r.meta_description_length,
            "has_meta_description": r.has_meta_description,
            "canonical_url": r.canonical_url,
            "has_canonical": r.has_canonical,
            "canonical_matches_final": r.canonical_matches_final,
            "seo_score": r.seo_score,
            "is_noindex": r.is_noindex,
            "has_og_title": r.has_og_title,
            "has_og_description": r.has_og_description,
            "h1_count": getattr(r, "h1_count", 0),
            "h2_count": getattr(r, "h2_count", 0),
            "has_schema": r.has_schema,
            "meta_robots": r.meta_robots,
            "collected_at": r.collected_at.isoformat() if r.collected_at else "",
            "issues": _issues_for(r),
        })
    return result


def get_audit_issues_count(db: Session, site_id: int, filter_key: str = "all") -> int:
    from backend.models import UrlAuditRecord
    q = db.query(func.count(UrlAuditRecord.id)).filter(UrlAuditRecord.site_id == site_id)
    if filter_key in _FILTER_MAP:
        q = _FILTER_MAP[filter_key](q, UrlAuditRecord)
    try:
        return q.scalar() or 0
    except Exception:
        db.rollback()
        return 0


def _url_host(url: str) -> str:
    """URL'den host (subdomain dahil) döndür: m.doviz.com, haber.doviz.com vb."""
    try:
        return urlparse(url).netloc.lower().split(":")[0]
    except Exception:
        return url


def _normalize_url(url: str) -> str:
    """AMP suffix'ini silerek URL'yi normalize et."""
    u = url.rstrip("/")
    if u.endswith("/amp"):
        u = u[:-4].rstrip("/")
    return u


def get_duplicates(db: Session, site_id: int) -> dict[str, Any]:
    """Aynı subdomain içinde aynı title/description paylaşan URL gruplarını döner.

    m.doviz.com/xxx ile haber.doviz.com/xxx aynı title taşısa da farklı
    subdomain oldukları için duplicate sayılmaz. Aynı subdomain'de aynı
    title/description varsa duplicate'tir.
    """
    from backend.models import UrlAuditRecord

    rows = (
        db.query(UrlAuditRecord.url, UrlAuditRecord.title, UrlAuditRecord.meta_description,
                 UrlAuditRecord.has_title, UrlAuditRecord.has_meta_description)
        .filter(UrlAuditRecord.site_id == site_id)
        .limit(5000)
        .all()
    )

    def _dup_groups(value_attr, has_attr):
        # key: (host, value) → normalized unique URL listesi
        groups: dict[tuple[str, str], list[str]] = {}
        for r in rows:
            val = getattr(r, value_attr) or ""
            has = getattr(r, has_attr)
            if not has or not val:
                continue
            host = _url_host(r.url)
            norm = _normalize_url(r.url)
            key = (host, val)
            bucket = groups.setdefault(key, [])
            if norm not in bucket:
                bucket.append(norm)

        result = [
            {"value": val, "count": len(urls), "urls": urls[:10]}
            for (host, val), urls in groups.items()
            if len(urls) > 1
        ]
        result.sort(key=lambda x: x["count"], reverse=True)
        return result[:50]

    return {
        "duplicate_titles": _dup_groups("title", "has_title"),
        "duplicate_descs": _dup_groups("meta_description", "has_meta_description"),
    }


def take_daily_snapshot(db: Session, site_id: int) -> int:
    """UrlAuditRecord → MetaTagSnapshot (bugünün tarihi). Aynı gün zaten varsa skip."""
    from backend.models import MetaTagSnapshot, UrlAuditRecord

    today = date.today()
    existing = (
        db.query(func.count(MetaTagSnapshot.id))
        .filter(MetaTagSnapshot.site_id == site_id, MetaTagSnapshot.snapshot_date == today)
        .scalar()
    ) or 0

    if existing > 0:
        logger.info("MetaTagSnapshot zaten var: site_id=%d, tarih=%s (%d kayıt)", site_id, today, existing)
        return 0

    rows = db.query(UrlAuditRecord).filter(UrlAuditRecord.site_id == site_id).all()
    now = datetime.utcnow()
    count = 0
    for r in rows:
        snap = MetaTagSnapshot(
            site_id=site_id,
            url=r.url,
            title=r.title or "",
            title_length=r.title_length,
            meta_description=r.meta_description or "",
            meta_description_length=r.meta_description_length,
            canonical_url=r.canonical_url or "",
            seo_score=r.seo_score,
            is_noindex=r.is_noindex,
            has_og_title=r.has_og_title,
            has_og_description=r.has_og_description,
            snapshot_date=today,
            collected_at=now,
        )
        db.add(snap)
        count += 1

    try:
        db.commit()
        logger.info("MetaTagSnapshot kaydedildi: site_id=%d, %d URL, tarih=%s", site_id, count, today)
    except Exception:
        db.rollback()
        logger.exception("MetaTagSnapshot commit hatası")
        return 0

    return count


def get_changes(db: Session, site_id: int, days: int = 7) -> list[dict[str, Any]]:
    """Son `days` gün içinde title, canonical veya noindex değişen sayfaları döner."""
    from backend.models import MetaTagSnapshot

    cutoff = date.today() - timedelta(days=days)

    # En eski ve en yeni snapshot'ı karşılaştır
    old_snaps = {
        r.url: r
        for r in db.query(MetaTagSnapshot)
        .filter(MetaTagSnapshot.site_id == site_id, MetaTagSnapshot.snapshot_date == cutoff)
        .all()
    }
    new_snaps = {
        r.url: r
        for r in db.query(MetaTagSnapshot)
        .filter(MetaTagSnapshot.site_id == site_id, MetaTagSnapshot.snapshot_date == date.today())
        .all()
    }

    if not old_snaps or not new_snaps:
        # Tek günlük veri varsa iki ardışık günü karşılaştır
        dates = (
            db.query(MetaTagSnapshot.snapshot_date)
            .filter(MetaTagSnapshot.site_id == site_id)
            .distinct()
            .order_by(MetaTagSnapshot.snapshot_date.desc())
            .limit(2)
            .all()
        )
        if len(dates) < 2:
            return []
        new_date, old_date = dates[0][0], dates[1][0]
        old_snaps = {r.url: r for r in db.query(MetaTagSnapshot).filter(MetaTagSnapshot.site_id == site_id, MetaTagSnapshot.snapshot_date == old_date).all()}
        new_snaps = {r.url: r for r in db.query(MetaTagSnapshot).filter(MetaTagSnapshot.site_id == site_id, MetaTagSnapshot.snapshot_date == new_date).all()}

    changes = []
    for url, new in new_snaps.items():
        old = old_snaps.get(url)
        if not old:
            continue
        diffs = []
        if old.title != new.title:
            diffs.append({"field": "title", "old": old.title[:80], "new": new.title[:80], "type": "title_changed"})
        if old.canonical_url != new.canonical_url:
            diffs.append({"field": "canonical", "old": old.canonical_url[:120], "new": new.canonical_url[:120], "type": "canonical_changed"})
        if old.is_noindex != new.is_noindex:
            severity = "critical" if new.is_noindex else "info"
            diffs.append({"field": "noindex", "old": old.is_noindex, "new": new.is_noindex, "type": "noindex_added" if new.is_noindex else "noindex_removed", "severity": severity})
        if diffs:
            changes.append({"url": url, "changes": diffs, "seo_score": new.seo_score})

    changes.sort(key=lambda x: any(d.get("severity") == "critical" for d in x["changes"]), reverse=True)
    return changes


def cleanup_old_snapshots(db: Session, retention_days: int = 90) -> int:
    """90 günden eski snapshot'ları siler."""
    from backend.models import MetaTagSnapshot
    cutoff = date.today() - timedelta(days=retention_days)
    deleted = db.query(MetaTagSnapshot).filter(MetaTagSnapshot.snapshot_date < cutoff).delete(synchronize_session=False)
    try:
        db.commit()
    except Exception:
        db.rollback()
    return deleted
