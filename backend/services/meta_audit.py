"""
SEO Meta Tag Monitoring servisi.
UrlAuditRecord'dan issue özetleri, duplicate tespiti, günlük snapshot ve regresyon diff'i.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import func, text
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

    return issues


def get_audit_summary(db: Session, site_id: int) -> dict[str, Any]:
    """UrlAuditRecord'dan site geneli SEO özeti döner."""
    from backend.models import UrlAuditRecord

    rows = (
        db.query(UrlAuditRecord)
        .filter(UrlAuditRecord.site_id == site_id)
        .all()
    )

    if not rows:
        return {"total_pages": 0, "score_counts": {}, "issue_counts": {}, "last_crawled": None}

    score_counts: dict[str, int] = {"good": 0, "needs_improvement": 0, "poor": 0}
    issue_counts: dict[str, int] = {
        "missing_title": 0, "short_title": 0, "long_title": 0,
        "missing_desc": 0, "short_desc": 0, "long_desc": 0,
        "missing_canonical": 0, "broken_canonical": 0,
        "noindex": 0, "missing_og": 0,
    }

    last_crawled = None
    for r in rows:
        score_counts[r.seo_score] = score_counts.get(r.seo_score, 0) + 1
        if not r.has_title:
            issue_counts["missing_title"] += 1
        elif r.title_length < TITLE_MIN:
            issue_counts["short_title"] += 1
        elif r.title_length > TITLE_MAX:
            issue_counts["long_title"] += 1

        if not r.has_meta_description:
            issue_counts["missing_desc"] += 1
        elif r.meta_description_length < DESC_MIN:
            issue_counts["short_desc"] += 1
        elif r.meta_description_length > DESC_MAX:
            issue_counts["long_desc"] += 1

        if not r.has_canonical:
            issue_counts["missing_canonical"] += 1
        elif not r.canonical_matches_final:
            issue_counts["broken_canonical"] += 1

        if r.is_noindex:
            issue_counts["noindex"] += 1
        if not r.has_og_title or not r.has_og_description:
            issue_counts["missing_og"] += 1

        if last_crawled is None or r.collected_at > last_crawled:
            last_crawled = r.collected_at

    # Duplicate tespiti
    title_dup_count = (
        db.query(UrlAuditRecord.title)
        .filter(UrlAuditRecord.site_id == site_id, UrlAuditRecord.has_title == True)
        .group_by(UrlAuditRecord.title)
        .having(func.count(UrlAuditRecord.title) > 1)
        .count()
    )
    desc_dup_count = (
        db.query(UrlAuditRecord.meta_description)
        .filter(UrlAuditRecord.site_id == site_id, UrlAuditRecord.has_meta_description == True)
        .group_by(UrlAuditRecord.meta_description)
        .having(func.count(UrlAuditRecord.meta_description) > 1)
        .count()
    )

    total_issues = sum(issue_counts.values())

    return {
        "total_pages": len(rows),
        "score_counts": score_counts,
        "issue_counts": issue_counts,
        "total_issues": total_issues,
        "duplicate_title_groups": title_dup_count,
        "duplicate_desc_groups": desc_dup_count,
        "last_crawled": last_crawled.isoformat() if last_crawled else None,
    }


_FILTER_MAP = {
    "poor": lambda q, M: q.filter(M.seo_score == "poor"),
    "needs_improvement": lambda q, M: q.filter(M.seo_score == "needs_improvement"),
    "missing_title": lambda q, M: q.filter(M.has_title == False),
    "short_title": lambda q, M: q.filter(M.has_title == True, M.title_length < TITLE_MIN),
    "long_title": lambda q, M: q.filter(M.has_title == True, M.title_length > TITLE_MAX),
    "missing_desc": lambda q, M: q.filter(M.has_meta_description == False),
    "short_desc": lambda q, M: q.filter(M.has_meta_description == True, M.meta_description_length < DESC_MIN),
    "long_desc": lambda q, M: q.filter(M.has_meta_description == True, M.meta_description_length > DESC_MAX),
    "missing_canonical": lambda q, M: q.filter(M.has_canonical == False),
    "broken_canonical": lambda q, M: q.filter(M.has_canonical == True, M.canonical_matches_final == False),
    "noindex": lambda q, M: q.filter(M.is_noindex == True),
    "missing_og": lambda q, M: q.filter((M.has_og_title == False) | (M.has_og_description == False)),
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

    rows = q.order_by(UrlAuditRecord.seo_score, UrlAuditRecord.url).offset(offset).limit(limit).all()

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
            "collected_at": r.collected_at.isoformat() if r.collected_at else "",
            "issues": _issues_for(r),
        })
    return result


def get_audit_issues_count(db: Session, site_id: int, filter_key: str = "all") -> int:
    from backend.models import UrlAuditRecord
    q = db.query(func.count(UrlAuditRecord.id)).filter(UrlAuditRecord.site_id == site_id)
    if filter_key in _FILTER_MAP:
        q = _FILTER_MAP[filter_key](q, UrlAuditRecord)
    return q.scalar() or 0


def get_duplicates(db: Session, site_id: int) -> dict[str, Any]:
    """Aynı title veya meta description paylaşan URL gruplarını döner."""
    from backend.models import UrlAuditRecord

    def _dup_groups(field, has_field):
        groups = (
            db.query(field, func.count(field).label("cnt"), func.group_concat(UrlAuditRecord.url).label("urls"))
            .filter(UrlAuditRecord.site_id == site_id, has_field == True, field != "")
            .group_by(field)
            .having(func.count(field) > 1)
            .order_by(func.count(field).desc())
            .limit(50)
            .all()
        )
        return [
            {
                "value": g[0],
                "count": g[1],
                "urls": (g[2] or "").split(",")[:10],
            }
            for g in groups
        ]

    return {
        "duplicate_titles": _dup_groups(UrlAuditRecord.title, UrlAuditRecord.has_title),
        "duplicate_descs": _dup_groups(UrlAuditRecord.meta_description, UrlAuditRecord.has_meta_description),
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
