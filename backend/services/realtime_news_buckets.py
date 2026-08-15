"""Haber ID bazında 30 dakikalık AYRIK realtime kovaları.

GA4 Realtime API yalnızca son ~30 dakikalık **kayan** pencereyi döndürür. Mevcut
``RealtimePageSnapshot`` / ``RealtimeNewsSnapshot`` kayıtları bu kayan pencereleri
üst üste binecek şekilde biriktirdiği için toplanmaları mükerrer sayım üretir
(bu yüzden kod tabanının geri kalanı zirve/``max`` kullanır).

Burada her satır 00/30 şebekesine hizalı **ayrık** bir yarım saati temsil eder:

* ``(site_id, profile, article_id, bucket_start)`` tekildir → iş tekrar çalışsa
  veya elle tetiklense bile satır çoğalmaz, üzerine yazılır.
* Kovalar ayrık olduğu için ``pageviews`` toplamları güvenle toplanabilir.
* Aynı GA4 property'si iki profile atanmışsa (mweb → web fallback) yalnızca bir
  kez toplanır; aksi halde aynı trafik iki profille iki kez sayılırdı.

Not: GA4 Realtime tek ``MinuteRange`` için en fazla 29 dakika geriye gidebildiğinden
her 30 dakikalık kova pratikte 29 dakikalık örnek taşır. Bu, eksik yönde ve sabit
bir sapmadır; hiçbir koşulda çift sayıma yol açmaz. Gerçek pencere ``window_minutes``
kolonunda saklanır.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import func as sqlfunc
from sqlalchemy.orm import Session

from backend.services.doviz_news_admin import news_id_in_scope
from backend.services.notification_content_traffic import (
    extract_article_id_from_path,
    normalize_article_id,
)
from backend.services.realtime_news_paths import _normalize_news_tab_title

LOGGER = logging.getLogger(__name__)

BUCKET_MINUTES = 30
DOVIZ_NEWS_SITE_ID = 1

# Cron :00/:30'da tetiklenen iş birkaç saniye erken uyanabilir; şebeke hizasını
# kaybetmemek için ileri tolerans.
_EARLY_FIRE_TOLERANCE_SEC = 150
# Uzun kuyruğu da yakalamak için profil başına istenen satır sayısı.
_MAX_ROWS_PER_PROFILE = 200
# Haber makaleleri web içeriğidir; app profilleri (ios/android) ekran adı döner.
_COLLECT_PROFILES = ("web", "mweb")
_ID_CHUNK = 400

_WS_RE = re.compile(r"\s+")


def resolve_bucket(
    now: datetime | None = None,
    *,
    window_minutes: int = BUCKET_MINUTES,
) -> tuple[datetime, datetime]:
    """Kapanmış son yarım saatlik dilimi (naive UTC) döndürür.

    12:30:04'te çalışan iş ``(12:00, 12:30)`` kovasını yazar.
    """
    base = now or datetime.utcnow()
    if base.tzinfo is not None:
        base = base.astimezone(timezone.utc).replace(tzinfo=None)
    grid = max(1, int(window_minutes))
    anchor = (base + timedelta(seconds=_EARLY_FIRE_TOLERANCE_SEC)).replace(
        second=0, microsecond=0
    )
    end = anchor - timedelta(minutes=anchor.minute % grid)
    return end - timedelta(minutes=grid), end


def _title_key(title: str) -> str:
    """GA4 başlık varyantlarını tek anahtara indirger (birebir eşleşme için)."""
    text = _normalize_news_tab_title(str(title or ""))
    return _WS_RE.sub(" ", text).strip().casefold()


def build_article_index(rows: list[dict[str, Any]]) -> tuple[set[str], dict[str, str]]:
    """Haber snapshot'ından ``(id kümesi, başlık→id)`` indeksleri kurar.

    Aynı başlığa sahip birden fazla haber varsa o başlık indeksten düşülür;
    tahminle yanlış habere yazmaktansa eşleşmemesi yeğdir.
    """
    ids: set[str] = set()
    by_title: dict[str, str] = {}
    ambiguous: set[str] = set()

    for row in rows or []:
        aid = normalize_article_id(str(row.get("id") or ""))
        if not aid:
            continue
        ids.add(aid)
        key = _title_key(str(row.get("title") or ""))
        if not key:
            continue
        current = by_title.get(key)
        if current is None:
            by_title[key] = aid
        elif current != aid:
            ambiguous.add(key)

    for key in ambiguous:
        by_title.pop(key, None)
    return ids, by_title


def resolve_row_article_id(
    row: dict[str, Any],
    *,
    known_ids: set[str],
    title_index: dict[str, str],
) -> tuple[str, str]:
    """Realtime satırını haber ID'sine bağlar → ``(article_id, yöntem)``.

    Önce path/URL içindeki sayısal ID (birebir), sonra normalize başlık eşleşmesi.
    Hiçbiri tutmazsa boş döner; tahmin yapılmaz.
    """
    for candidate in (row.get("page_path"), row.get("link_url"), row.get("page")):
        aid = extract_article_id_from_path(str(candidate or ""))
        if aid and (aid in known_ids or news_id_in_scope(aid)):
            return aid, "path"

    aid = title_index.get(_title_key(str(row.get("page") or "")))
    if aid:
        return aid, "title"
    return "", ""


def _upsert_bucket(
    db: Session,
    *,
    site_id: int,
    profile: str,
    article_id: str,
    bucket_start: datetime,
    bucket_end: datetime,
    label: str,
    match_method: str,
    active_users: float,
    pageviews: float,
    window_minutes: int,
) -> bool:
    """Kovayı yazar/günceller. ``True`` → yeni satır, ``False`` → üzerine yazıldı."""
    from backend.models import RealtimeNewsArticleBucket

    existing = (
        db.query(RealtimeNewsArticleBucket)
        .filter(
            RealtimeNewsArticleBucket.site_id == site_id,
            RealtimeNewsArticleBucket.profile == profile,
            RealtimeNewsArticleBucket.article_id == article_id,
            RealtimeNewsArticleBucket.bucket_start == bucket_start,
        )
        .one_or_none()
    )
    created = existing is None
    if existing is None:
        existing = RealtimeNewsArticleBucket(
            site_id=site_id,
            profile=profile,
            article_id=article_id,
            bucket_start=bucket_start,
        )
        db.add(existing)

    existing.bucket_end = bucket_end
    existing.label = str(label or "")[:500]
    existing.match_method = match_method or "path"
    existing.active_users = float(active_users or 0)
    existing.pageviews = float(pageviews or 0)
    existing.window_minutes = int(window_minutes or BUCKET_MINUTES)
    existing.collected_at = datetime.utcnow()
    return created


def collect_news_article_buckets(
    db: Session,
    *,
    site_id: int = DOVIZ_NEWS_SITE_ID,
    window_minutes: int = BUCKET_MINUTES,
    now: datetime | None = None,
    fetch: Callable[..., dict[str, Any]] | None = None,
    news_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Realtime haber trafiğini tek bir ayrık kovaya yazar (idempotent)."""
    from backend.models import Site as SiteModel
    from backend.services.ga4_auth import get_ga4_credentials_record, load_ga4_properties
    from backend.services.ga4_realtime_quota import scheduler_profiles_for_site

    bucket_start, bucket_end = resolve_bucket(now, window_minutes=window_minutes)
    summary: dict[str, Any] = {
        "ok": False,
        "site_id": site_id,
        "bucket_start": bucket_start.isoformat(),
        "bucket_end": bucket_end.isoformat(),
        "profiles": [],
        "matched_rows": 0,
        "unmatched_rows": 0,
        "created": 0,
        "updated": 0,
        "articles": 0,
    }

    site = db.query(SiteModel).filter(SiteModel.id == site_id).one_or_none()
    if site is None:
        summary["error"] = "site_not_found"
        return summary

    properties = load_ga4_properties(get_ga4_credentials_record(db, site.id))
    allowed = scheduler_profiles_for_site(site.domain, properties)
    base_web = (properties.get("web") or "").strip()

    if news_rows is None:
        from backend.services.doviz_news_sheet import fetch_doviz_news_rows

        news_rows = fetch_doviz_news_rows()
    known_ids, title_index = build_article_index(news_rows or [])

    fetch_fn = fetch
    if fetch_fn is None:
        from backend.services.ga4_realtime import fetch_realtime_top_news_pages

        fetch_fn = fetch_realtime_top_news_pages

    seen_properties: set[str] = set()
    totals: dict[str, dict[str, Any]] = {}

    for profile in _COLLECT_PROFILES:
        if profile not in allowed:
            continue
        property_id = (properties.get(profile) or "").strip() or base_web
        if not property_id:
            continue
        # Aynı property iki profile atanmışsa tek kez topla — yoksa çift sayım olur.
        if property_id in seen_properties:
            continue
        seen_properties.add(property_id)

        try:
            result = fetch_fn(
                property_id,
                site_domain=site.domain,
                profile=profile,
                window_minutes=window_minutes,
                limit=_MAX_ROWS_PER_PROFILE,
            )
        except Exception as exc:
            LOGGER.warning(
                "Realtime haber kovası atlandı [%s/%s]: %s", site.domain, profile, exc
            )
            summary["profiles"].append({"profile": profile, "error": str(exc)})
            continue

        pages = result.get("pages") or []
        actual_window = int(result.get("window_minutes") or window_minutes)
        matched = 0
        unmatched = 0
        per_article: dict[str, dict[str, Any]] = {}

        for row in pages:
            aid, method = resolve_row_article_id(
                row, known_ids=known_ids, title_index=title_index
            )
            if not aid:
                unmatched += 1
                continue
            matched += 1
            entry = per_article.setdefault(
                aid,
                {
                    "active_users": 0.0,
                    "pageviews": 0.0,
                    "label": row.get("page_path") or row.get("page") or "",
                    "method": method,
                },
            )
            entry["active_users"] += float(row.get("activeUsers") or 0)
            entry["pageviews"] += float(row.get("screenPageViews") or 0)

        for aid, entry in per_article.items():
            created = _upsert_bucket(
                db,
                site_id=site.id,
                profile=profile,
                article_id=aid,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                label=str(entry["label"]),
                match_method=str(entry["method"]),
                active_users=entry["active_users"],
                pageviews=entry["pageviews"],
                window_minutes=actual_window,
            )
            summary["created" if created else "updated"] += 1
            agg = totals.setdefault(aid, {"pageviews": 0.0})
            agg["pageviews"] += float(entry["pageviews"])

        summary["matched_rows"] += matched
        summary["unmatched_rows"] += unmatched
        summary["profiles"].append(
            {
                "profile": profile,
                "property_id": property_id,
                "rows": len(pages),
                "matched": matched,
                "unmatched": unmatched,
                "window_minutes": actual_window,
            }
        )

    try:
        db.commit()
    except Exception:
        db.rollback()
        LOGGER.exception("Realtime haber kovası kaydedilemedi (site_id=%s)", site_id)
        summary["error"] = "commit_failed"
        return summary

    summary["ok"] = True
    summary["articles"] = len(totals)
    return summary


def get_article_realtime_totals(
    db: Session,
    article_ids: list[str] | set[str],
    *,
    site_id: int = DOVIZ_NEWS_SITE_ID,
    since: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    """Haber ID → ayrık kovaların toplamı.

    ``rt_views`` ayrık kovaların toplamıdır (çift sayım yok). ``rt_peak_users``
    eşzamanlı kullanıcı olduğu için toplanmaz, zirve alınır.
    """
    from backend.models import RealtimeNewsArticleBucket

    wanted = [normalize_article_id(str(a or "")) for a in (article_ids or [])]
    wanted = [a for a in wanted if a]
    if not wanted:
        return {}

    out: dict[str, dict[str, Any]] = {}
    unique_ids = list(dict.fromkeys(wanted))

    for i in range(0, len(unique_ids), _ID_CHUNK):
        chunk = unique_ids[i : i + _ID_CHUNK]
        query = (
            db.query(
                RealtimeNewsArticleBucket.article_id,
                sqlfunc.sum(RealtimeNewsArticleBucket.pageviews),
                sqlfunc.max(RealtimeNewsArticleBucket.active_users),
                sqlfunc.count(sqlfunc.distinct(RealtimeNewsArticleBucket.bucket_start)),
                sqlfunc.max(RealtimeNewsArticleBucket.bucket_end),
            )
            .filter(
                RealtimeNewsArticleBucket.site_id == site_id,
                RealtimeNewsArticleBucket.article_id.in_(chunk),
            )
            .group_by(RealtimeNewsArticleBucket.article_id)
        )
        if since is not None:
            query = query.filter(RealtimeNewsArticleBucket.bucket_start >= since)

        for aid, views, peak_users, bucket_count, last_end in query.all():
            out[str(aid)] = {
                "rt_views": int(round(float(views or 0))),
                "rt_peak_users": int(round(float(peak_users or 0))),
                "rt_buckets": int(bucket_count or 0),
                "rt_last_bucket": last_end.isoformat() if last_end else None,
            }
    return out
