"""Sinemalar management/getModerationSummary + getModerationDetail."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.models import (
    SinemalarModerationDailyRow,
    SinemalarModerationDetailItem,
    SinemalarModerationMeta,
)

TR = ZoneInfo("Europe/Istanbul")
DETAIL_BASE_URL = "https://www.sinemalar.com/management/getModerationDetail"

METRIC_TYPES: tuple[tuple[str, str], ...] = (
    ("movie", "Film"),
    ("person", "Sanatçı"),
    ("list", "Liste"),
    ("trailer", "Fragman"),
    ("summary", "Film Özeti"),
    ("bio", "Biyografi"),
    ("movie_cast_add", "Kadroya Ekleme"),
    ("movie_cast_remove", "Kadrodan Çıkarma"),
    ("movie_poster", "Film Afişi"),
    ("person_image", "Sanatçı Fotoğrafı"),
    ("movie_tmdb", "Film TMDB ID"),
    ("person_tmdb", "Sanatçı TMDB ID"),
    ("news", "Haber"),
)

# TMDB sütunlarının detay tablosu: ID | İsim | Aksiyon | Eski TMDB ID | Yeni TMDB ID | Tarih
TMDB_METRIC_TYPES: frozenset[str] = frozenset({"movie_tmdb", "person_tmdb"})

METRIC_DISPLAY_LABELS: dict[str, str] = {
    "movie": "Movie",
    "person": "Artist",
    "list": "List",
    "trailer": "Trailer",
    "summary": "Movie summary",
    "bio": "Biography",
    "movie_cast_add": "Cast add",
    "movie_cast_remove": "Cast remove",
    "movie_poster": "Movie poster",
    "person_image": "Artist photo",
    "movie_tmdb": "Movie TMDB ID",
    "person_tmdb": "Artist TMDB ID",
    "news": "News",
}

METRIC_LABEL_BY_TYPE = {k: v for k, v in METRIC_TYPES}
METRIC_TYPE_BY_LABEL = {v: k for k, v in METRIC_TYPES}
METRIC_TYPE_BY_LABEL.update({v: k for k, v in METRIC_DISPLAY_LABELS.items()})
METRIC_TYPE_BY_LABEL.update({
    "Film TMDB ID": "movie_tmdb",
    "Film TMDB Id": "movie_tmdb",
    "Film TMDB": "movie_tmdb",
    "Movie TMDB ID": "movie_tmdb",
    "Movie TMDB Id": "movie_tmdb",
    "Movie TMDB": "movie_tmdb",
    "Sanatçı TMDB ID": "person_tmdb",
    "Sanatçı TMDB Id": "person_tmdb",
    "Sanatçı TMDB": "person_tmdb",
    "Artist TMDB ID": "person_tmdb",
    "Artist TMDB Id": "person_tmdb",
    "Artist TMDB": "person_tmdb",
})
METRIC_TYPE_KEYS = tuple(k for k, _ in METRIC_TYPES)


def metric_display_label(metric_type: str, fallback: str = "") -> str:
    key = str(metric_type or "").strip()
    return METRIC_DISPLAY_LABELS.get(key) or METRIC_LABEL_BY_TYPE.get(key) or fallback or key

BACKFILL_START = date(2026, 1, 1)
DEFAULT_DETAIL_END = date(2026, 8, 13)  # legacy ingest fallback; panel uses today_tr()


def today_tr() -> date:
    return datetime.now(TR).date()


def yesterday_tr() -> date:
    return today_tr() - timedelta(days=1)


def _norm_key(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", "", s)
    s = s.rstrip(".")
    return s


def _norm_username(name: str) -> str:
    return _norm_key(name)


# user_id, username — tanım _norm_key sonrası
TRACKED_MODERATORS: tuple[tuple[int, str], ...] = (
    (53, "Sinemalar_Yonetim"),
    (748975, "ivicincim"),
    (873391, "gezginozlem"),
    (883754, "berend"),
    (245939, "Aquamarine"),
    (935786, "Gözde."),
)

TRACKED_USERNAMES: tuple[str, ...] = tuple(u for _, u in TRACKED_MODERATORS)
KNOWN_USER_IDS: dict[str, int] = {_norm_key(u): uid for uid, u in TRACKED_MODERATORS}
USER_ID_TO_NAME: dict[int, str] = {uid: u for uid, u in TRACKED_MODERATORS}
# Sinemalar özet tablosundaki yazım varyantları
USERNAME_ALIASES: dict[str, int] = {
    "aquuamarine": 245939,
}

# Katılım tarihinden önceki günler «boş» sayılmaz (aktif/boş grafiği).
MODERATOR_JOIN_DATES: dict[int, date] = {
    935786: date(2026, 5, 4),  # Gözde.
}


def moderator_join_date(user_id: int) -> date | None:
    return MODERATOR_JOIN_DATES.get(int(user_id))


def _eligible_days_for_moderator(uid: int, all_days: list[str]) -> list[str]:
    join = moderator_join_date(uid)
    if join is None:
        return all_days
    join_s = join.isoformat()
    return [d for d in all_days if d >= join_s]


def resolve_user_id(username: str, raw_id: Any = None) -> int:
    try:
        uid = int(raw_id) if raw_id not in (None, "") else 0
    except (TypeError, ValueError):
        uid = 0
    if uid:
        return uid
    alias = USERNAME_ALIASES.get(_norm_username(username))
    if alias:
        return alias
    return int(KNOWN_USER_IDS.get(_norm_username(username), 0))


def resolve_username(user_id: int | Any) -> str:
    try:
        uid = int(user_id)
    except (TypeError, ValueError):
        return ""
    return USER_ID_TO_NAME.get(uid, "")


def is_tracked_user_id(user_id: int | Any) -> bool:
    try:
        return int(user_id) in USER_ID_TO_NAME
    except (TypeError, ValueError):
        return False


def is_tracked_username(name: str) -> bool:
    n = _norm_username(name)
    if n in {_norm_username(u) for u in TRACKED_USERNAMES}:
        return True
    return n in USERNAME_ALIASES


def detail_url(
    user_id: int,
    *,
    start: date,
    end: date,
    metric_type: str,
) -> str:
    return (
        f"{DETAIL_BASE_URL}?userId={user_id}"
        f"&startDate={start.isoformat()}&endDate={end.isoformat()}&type={metric_type}"
    )


def _meta(db: Session) -> SinemalarModerationMeta:
    row = db.query(SinemalarModerationMeta).filter(SinemalarModerationMeta.id == 1).first()
    if row is None:
        row = SinemalarModerationMeta(id=1)
        db.add(row)
        db.flush()
    return row


def _parse_event_dt(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")[:19])
    except ValueError:
        return None


def parse_detail_rows(
    raw_rows: list[dict[str, Any]],
    *,
    user_id: int,
    username: str,
    metric_type: str,
    source_url: str | None = None,
) -> list[dict[str, Any]]:
    """getModerationDetail tablo satırlarını normalize et."""
    out: list[dict[str, Any]] = []
    label = METRIC_LABEL_BY_TYPE.get(metric_type, metric_type)
    for row in raw_rows or []:
        if not isinstance(row, dict):
            continue
        cells = row.get("cells") or []
        if not isinstance(cells, list) or len(cells) < 2:
            continue
        texts = [str((c or {}).get("text") or "").strip() for c in cells]
        hrefs = [(c or {}).get("href") for c in cells]
        item_id = texts[0]
        if not item_id or not item_id.isdigit():
            continue
        title = texts[1] if len(texts) > 1 else ""
        subtitle = texts[2] if len(texts) > 3 else ""
        if metric_type in TMDB_METRIC_TYPES and len(texts) >= 6:
            old_id, new_id = texts[3].strip(), texts[4].strip()
            change = f"{old_id or '—'} → {new_id or '—'}"
            subtitle = f"{subtitle} · {change}" if subtitle else change
        event_raw = texts[-1] if texts else ""
        event_at = _parse_event_dt(event_raw)
        if event_at is None:
            continue
        admin_url = hrefs[0] if hrefs else None
        if not admin_url:
            for h in hrefs:
                if h:
                    admin_url = h
                    break
        out.append(
            {
                "user_id": user_id,
                "username": username,
                "metric_type": metric_type,
                "metric_label": label,
                "item_id": item_id,
                "title": title[:512],
                "subtitle": subtitle[:256],
                "event_at": event_at.isoformat(sep=" "),
                "admin_url": admin_url,
                "source_url": source_url,
            }
        )
    return out


def aggregate_detail_items_to_daily(items: list[dict[str, Any]]) -> dict[tuple[str, int, str], int]:
    counts: dict[tuple[str, int, str], int] = {}
    for item in items:
        dt = _parse_event_dt(str(item.get("event_at") or ""))
        if dt is None:
            continue
        uid = int(item.get("user_id") or 0)
        mtype = str(item.get("metric_type") or "")
        if not uid or not mtype:
            continue
        key = (dt.date().isoformat(), uid, mtype)
        counts[key] = counts.get(key, 0) + 1
    return counts


def parse_summary_rows(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Scrape satırlarını ingest formatına çevir (yalnız izlenen moderatörler)."""
    out: list[dict[str, Any]] = []
    for row in raw_rows or []:
        if not isinstance(row, dict):
            continue
        moderator = str(row.get("moderator") or "").strip()
        if not is_tracked_username(moderator):
            continue
        metrics = row.get("metrics") or {}
        if not isinstance(metrics, dict):
            continue
        row_uid = resolve_user_id(moderator, row.get("moderatorUserId"))
        for label, block in metrics.items():
            if not isinstance(block, dict):
                continue
            mtype = str(block.get("type") or METRIC_TYPE_BY_LABEL.get(label) or "").strip()
            if not mtype:
                continue
            user_id = resolve_user_id(moderator, block.get("userId")) or row_uid
            if not user_id:
                continue
            try:
                count = int(block.get("count") or 0)
            except (TypeError, ValueError):
                count = 0
            out.append(
                {
                    "username": resolve_username(user_id) or moderator,
                    "user_id": user_id,
                    "metric_type": mtype,
                    "metric_label": METRIC_LABEL_BY_TYPE.get(mtype, label),
                    "count": count,
                    "detail_url": block.get("href"),
                }
            )
    return out


def _upsert_daily_count(
    db: Session,
    *,
    report_date: date,
    user_id: int,
    username: str,
    metric_type: str,
    count: int,
    detail_url: str | None,
    scraped_at: datetime,
) -> None:
    existing = (
        db.query(SinemalarModerationDailyRow)
        .filter(
            SinemalarModerationDailyRow.report_date == report_date,
            SinemalarModerationDailyRow.user_id == user_id,
            SinemalarModerationDailyRow.metric_type == metric_type,
        )
        .first()
    )
    if existing is None:
        existing = SinemalarModerationDailyRow(
            report_date=report_date,
            user_id=user_id,
            metric_type=metric_type,
        )
        db.add(existing)
    existing.username = username[:64]
    existing.metric_label = METRIC_LABEL_BY_TYPE.get(metric_type, metric_type)[:64]
    existing.count = int(count)
    existing.detail_url = str(detail_url)[:512] if detail_url else existing.detail_url
    existing.scraped_at = scraped_at


def _rebuild_daily_from_details(
    db: Session,
    *,
    user_id: int,
    username: str,
    metric_type: str,
    range_start: date,
    range_end: date,
    source_url: str | None,
    scraped_at: datetime,
) -> int:
    db.query(SinemalarModerationDailyRow).filter(
        SinemalarModerationDailyRow.user_id == user_id,
        SinemalarModerationDailyRow.metric_type == metric_type,
        SinemalarModerationDailyRow.report_date >= range_start,
        SinemalarModerationDailyRow.report_date <= range_end,
    ).delete(synchronize_session=False)

    rows = (
        db.query(SinemalarModerationDetailItem)
        .filter(
            SinemalarModerationDetailItem.user_id == user_id,
            SinemalarModerationDetailItem.metric_type == metric_type,
            SinemalarModerationDetailItem.event_at >= datetime.combine(range_start, datetime.min.time()),
            SinemalarModerationDetailItem.event_at
            < datetime.combine(range_end + timedelta(days=1), datetime.min.time()),
        )
        .all()
    )
    day_counts: dict[date, int] = {}
    for r in rows:
        d = r.event_at.date()
        day_counts[d] = day_counts.get(d, 0) + 1
    src = source_url or detail_url(user_id, start=range_start, end=range_end, metric_type=metric_type)
    for d, cnt in day_counts.items():
        _upsert_daily_count(
            db,
            report_date=d,
            user_id=user_id,
            username=username,
            metric_type=metric_type,
            count=cnt,
            detail_url=src,
            scraped_at=scraped_at,
        )
    return len(day_counts)


def _sync_daily_for_day_from_details(
    db: Session,
    *,
    user_id: int,
    username: str,
    metric_type: str,
    report_date: date,
    source_url: str | None,
    scraped_at: datetime,
) -> int:
    """Tek günün daily satırını mevcut detay kayıtlarından say — silme yok."""
    start = datetime.combine(report_date, datetime.min.time())
    end = datetime.combine(report_date + timedelta(days=1), datetime.min.time())
    cnt = (
        db.query(SinemalarModerationDetailItem)
        .filter(
            SinemalarModerationDetailItem.user_id == user_id,
            SinemalarModerationDetailItem.metric_type == metric_type,
            SinemalarModerationDetailItem.event_at >= start,
            SinemalarModerationDetailItem.event_at < end,
        )
        .count()
    )
    canonical = resolve_username(user_id) or username
    src = source_url or detail_url(user_id, start=report_date, end=report_date, metric_type=metric_type)
    _upsert_daily_count(
        db,
        report_date=report_date,
        user_id=user_id,
        username=canonical,
        metric_type=metric_type,
        count=int(cnt),
        detail_url=src,
        scraped_at=scraped_at,
    )
    return 1


def ingest_detail_batch(
    db: Session,
    *,
    user_id: int,
    username: str,
    metric_type: str,
    items: list[dict[str, Any]],
    range_start: date,
    range_end: date,
    source_url: str | None = None,
    scraped_at: datetime | None = None,
    rebuild_daily: bool = False,
    recompute_daily: bool = False,
    sync_daily_date: date | None = None,
) -> dict[str, Any]:
    now = scraped_at or datetime.utcnow()
    canonical_name = resolve_username(user_id) or username
    if items and isinstance(items[0], dict) and "cells" in items[0]:
        parsed = parse_detail_rows(
            items,
            user_id=user_id,
            username=canonical_name,
            metric_type=metric_type,
            source_url=source_url,
        )
    else:
        parsed = list(items or [])

    items_inserted = 0
    items_skipped = 0
    for item in parsed:
        event_at = item.get("event_at")
        if isinstance(event_at, str):
            event_at = _parse_event_dt(event_at)
        if not isinstance(event_at, datetime):
            continue
        subtitle = str(item.get("subtitle") or "")[:256]
        existing = (
            db.query(SinemalarModerationDetailItem)
            .filter(
                SinemalarModerationDetailItem.user_id == user_id,
                SinemalarModerationDetailItem.metric_type == metric_type,
                SinemalarModerationDetailItem.item_id == str(item.get("item_id") or ""),
                SinemalarModerationDetailItem.event_at == event_at,
                SinemalarModerationDetailItem.subtitle == subtitle,
            )
            .first()
        )
        if existing is not None:
            items_skipped += 1
            continue
        row = SinemalarModerationDetailItem(
            user_id=user_id,
            metric_type=metric_type,
            item_id=str(item.get("item_id") or "")[:32],
            event_at=event_at,
            subtitle=subtitle,
        )
        db.add(row)
        row.username = canonical_name[:64]
        row.metric_label = str(item.get("metric_label") or METRIC_LABEL_BY_TYPE.get(metric_type, metric_type))[:64]
        row.title = str(item.get("title") or "")[:512]
        admin = item.get("admin_url") or item.get("source_url")
        row.admin_url = str(admin)[:512] if admin else None
        row.source_url = str(source_url or item.get("source_url") or "")[:512] or None
        row.scraped_at = now
        items_inserted += 1

    daily_upserted = 0
    if sync_daily_date is not None:
        daily_upserted = _sync_daily_for_day_from_details(
            db,
            user_id=user_id,
            username=canonical_name,
            metric_type=metric_type,
            report_date=sync_daily_date,
            source_url=source_url,
            scraped_at=now,
        )
    elif recompute_daily:
        daily_upserted = _rebuild_daily_from_details(
            db,
            user_id=user_id,
            username=canonical_name,
            metric_type=metric_type,
            range_start=range_start,
            range_end=range_end,
            source_url=source_url,
            scraped_at=now,
        )
    elif rebuild_daily:
        db.query(SinemalarModerationDailyRow).filter(
            SinemalarModerationDailyRow.user_id == user_id,
            SinemalarModerationDailyRow.metric_type == metric_type,
            SinemalarModerationDailyRow.report_date >= range_start,
            SinemalarModerationDailyRow.report_date <= range_end,
        ).delete(synchronize_session=False)

        day_counts: dict[date, int] = {}
        for item in parsed:
            event_at = item.get("event_at")
            if isinstance(event_at, str):
                event_at = _parse_event_dt(event_at)
            if not isinstance(event_at, datetime):
                continue
            d = event_at.date()
            if d < range_start or d > range_end:
                continue
            day_counts[d] = day_counts.get(d, 0) + 1

        src = source_url or detail_url(user_id, start=range_start, end=range_end, metric_type=metric_type)
        for d, cnt in day_counts.items():
            _upsert_daily_count(
                db,
                report_date=d,
                user_id=user_id,
                username=canonical_name,
                metric_type=metric_type,
                count=cnt,
                detail_url=src,
                scraped_at=now,
            )
            daily_upserted += 1

    meta = _meta(db)
    meta.last_scraped_at = now
    meta.last_mode = "detail_incremental" if sync_daily_date else "detail_range"
    meta.message = (
        f"detail {canonical_name}/{metric_type} · +{items_inserted} yeni"
        + (f" · {items_skipped} mevcut" if items_skipped else "")
    )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return {
            "ok": False,
            "message": f"duplicate constraint · {canonical_name}/{metric_type}",
            "user_id": user_id,
            "metric_type": metric_type,
            "items_inserted": 0,
            "items_skipped": items_skipped,
        }
    return {
        "ok": True,
        "user_id": user_id,
        "metric_type": metric_type,
        "items_inserted": items_inserted,
        "items_skipped": items_skipped,
        "items_upserted": items_inserted,
        "daily_upserted": daily_upserted,
    }


def ingest_daily_batch(
    db: Session,
    *,
    report_date: str,
    rows: list[dict[str, Any]],
    mode: str = "incremental",
    scraped_at: datetime | None = None,
) -> dict[str, Any]:
    """Tek gün için upsert; aynı gün tekrar gelirse günceller."""
    try:
        day = date.fromisoformat(str(report_date)[:10])
    except ValueError:
        return {"ok": False, "message": f"Geçersiz report_date: {report_date}"}

    now = scraped_at or datetime.utcnow()
    parsed = parse_summary_rows(rows)
    upserted = 0
    for item in parsed:
        uid = resolve_user_id(str(item.get("username") or ""), item.get("user_id"))
        mtype = str(item.get("metric_type") or "")
        if not uid or not mtype:
            continue
        _upsert_daily_count(
            db,
            report_date=day,
            user_id=uid,
            username=resolve_username(uid) or str(item.get("username") or ""),
            metric_type=mtype,
            count=int(item.get("count") or 0),
            detail_url=item.get("detail_url"),
            scraped_at=now,
        )
        upserted += 1

    meta = _meta(db)
    meta.last_scraped_at = now
    meta.last_mode = mode[:32]
    meta.message = f"{day.isoformat()} · {upserted} metrik"
    db.commit()
    return {"ok": True, "report_date": day.isoformat(), "upserted": upserted, "tracked_rows": len(parsed)}


def ingest_backfill_payload(
    db: Session,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Çok günlük ingest, detail_batches veya tek gün."""
    if payload.get("purge_first"):
        purge_all_data(db)

    detail_batches = payload.get("detail_batches")
    if isinstance(detail_batches, list) and detail_batches:
        try:
            range_start = date.fromisoformat(str(payload.get("range_start") or BACKFILL_START)[:10])
            range_end = date.fromisoformat(str(payload.get("range_end") or DEFAULT_DETAIL_END)[:10])
        except ValueError:
            return {"ok": False, "message": "Geçersiz range_start/range_end"}

        total_items = 0
        total_daily = 0
        scraped_at = _parse_dt(payload.get("scraped_at"))
        for batch in detail_batches:
            if not isinstance(batch, dict):
                continue
            uid = int(batch.get("user_id") or 0)
            uname = str(batch.get("username") or resolve_username(uid))
            mtype = str(batch.get("metric_type") or "")
            if not uid or not mtype:
                continue
            recompute = batch.get("_recompute_daily")
            sync_raw = batch.get("_sync_daily_date")
            sync_day: date | None = None
            if sync_raw:
                try:
                    sync_day = date.fromisoformat(str(sync_raw)[:10])
                except ValueError:
                    sync_day = None
            if sync_day is not None:
                recompute = False
            elif recompute is None:
                # Panel totals come from detail rows; per-batch daily rebuild is slow and
                # caused Railway 500s on large ingests.
                recompute = False
            res = ingest_detail_batch(
                db,
                user_id=uid,
                username=uname,
                metric_type=mtype,
                items=list(batch.get("items") or []),
                range_start=range_start,
                range_end=range_end,
                source_url=batch.get("source_url"),
                scraped_at=scraped_at,
                recompute_daily=bool(recompute),
                sync_daily_date=sync_day,
            )
            if res.get("ok"):
                total_items += int(res.get("items_inserted") or res.get("items_upserted") or 0)
                total_daily += int(res.get("daily_upserted") or 0)

        meta = _meta(db)
        if payload.get("backfill_complete"):
            meta.backfill_complete = True
            meta.backfill_cursor = None
        meta.message = f"detail_range · {total_items} kayıt · {total_daily} gün"
        db.commit()
        return {"ok": True, "items_upserted": total_items, "daily_upserted": total_daily, "batches": len(detail_batches)}

    if (
        str(payload.get("mode") or "") == "detail_incremental"
        and isinstance(detail_batches, list)
        and not detail_batches
        and not (payload.get("days") or [])
    ):
        meta = _meta(db)
        scraped_at = _parse_dt(payload.get("scraped_at")) or datetime.utcnow()
        meta.last_scraped_at = scraped_at
        meta.last_mode = "detail_incremental"
        meta.message = str(payload.get("message") or "detail_incremental · 0 kayıt")[:500]
        if payload.get("backfill_complete"):
            meta.backfill_complete = True
            meta.backfill_cursor = None
        db.commit()
        return {"ok": True, "items_upserted": 0, "daily_upserted": 0, "batches": 0, "heartbeat": True}

    days = payload.get("days")
    if isinstance(days, list) and days:
        total = 0
        for block in days:
            if not isinstance(block, dict):
                continue
            res = ingest_daily_batch(
                db,
                report_date=str(block.get("date") or ""),
                rows=list(block.get("rows") or []),
                mode=str(payload.get("mode") or "backfill"),
                scraped_at=_parse_dt(payload.get("scraped_at")),
            )
            if res.get("ok"):
                total += int(res.get("upserted") or 0)
        meta = _meta(db)
        if payload.get("backfill_complete"):
            meta.backfill_complete = True
            meta.backfill_cursor = None
        elif payload.get("backfill_cursor"):
            meta.backfill_cursor = str(payload.get("backfill_cursor"))[:10]
        meta.message = f"backfill batch · {total} upsert"
        db.commit()
        return {"ok": True, "upserted": total, "days": len(days)}

    return ingest_daily_batch(
        db,
        report_date=str(payload.get("report_date") or ""),
        rows=list(payload.get("rows") or []),
        mode=str(payload.get("mode") or "incremental"),
        scraped_at=_parse_dt(payload.get("scraped_at")),
    )


def _parse_dt(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def backfill_pending_dates(db: Session, *, through: date | None = None) -> list[date]:
    meta = _meta(db)
    if meta.backfill_complete:
        return []
    end = through or yesterday_tr()
    if end < BACKFILL_START:
        return []
    cursor = BACKFILL_START
    if meta.backfill_cursor:
        try:
            cursor = max(BACKFILL_START, date.fromisoformat(str(meta.backfill_cursor)[:10]))
        except ValueError:
            cursor = BACKFILL_START
    out: list[date] = []
    d = cursor
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def mark_backfill_progress(db: Session, *, last_date: date, complete: bool = False) -> None:
    meta = _meta(db)
    if complete:
        meta.backfill_complete = True
        meta.backfill_cursor = None
    else:
        meta.backfill_cursor = (last_date + timedelta(days=1)).isoformat()
    db.commit()


def purge_all_data(db: Session) -> dict[str, Any]:
    """Tüm moderasyon verisini sil — sıfırdan çekim öncesi."""
    deleted_details = db.query(SinemalarModerationDetailItem).delete(synchronize_session=False)
    deleted_daily = db.query(SinemalarModerationDailyRow).delete(synchronize_session=False)
    meta = _meta(db)
    meta.backfill_complete = False
    meta.backfill_cursor = None
    meta.last_mode = "purged"
    meta.message = f"silindi · {deleted_details} detay · {deleted_daily} günlük"
    db.commit()
    return {
        "ok": True,
        "deleted_details": int(deleted_details or 0),
        "deleted_daily": int(deleted_daily or 0),
    }


def get_meta_summary(db: Session) -> dict[str, Any]:
    meta = _meta(db)
    return {
        "backfill_complete": bool(meta.backfill_complete),
        "backfill_cursor": meta.backfill_cursor,
        "last_scraped_at": meta.last_scraped_at.isoformat() if meta.last_scraped_at else None,
        "last_mode": meta.last_mode or "",
        "message": meta.message or "",
    }


def get_detail_payload(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
    user_id: int | None = None,
    metric_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    end_d = today_tr()
    start_d = BACKFILL_START
    if end:
        try:
            end_d = date.fromisoformat(str(end)[:10])
        except ValueError:
            pass
    if start:
        try:
            start_d = date.fromisoformat(str(start)[:10])
        except ValueError:
            pass

    q = db.query(SinemalarModerationDetailItem).filter(
        SinemalarModerationDetailItem.event_at >= datetime.combine(start_d, datetime.min.time()),
        SinemalarModerationDetailItem.event_at < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
    )
    if user_id:
        q = q.filter(SinemalarModerationDetailItem.user_id == int(user_id))
    if metric_type:
        q = q.filter(SinemalarModerationDetailItem.metric_type == metric_type)
    total = q.count()
    rows = (
        q.order_by(SinemalarModerationDetailItem.event_at.desc())
        .offset(max(0, offset))
        .limit(min(10000, max(1, limit)))
        .all()
    )
    return {
        "ok": True,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [
            {
                "user_id": r.user_id,
                "username": r.username,
                "metric_type": r.metric_type,
                "metric_label": r.metric_label,
                "item_id": r.item_id,
                "title": r.title,
                "subtitle": r.subtitle,
                "event_at": r.event_at.isoformat(sep=" "),
                "admin_url": r.admin_url,
            }
            for r in rows
        ],
    }


def _parse_range(start: str | None, end: str | None) -> tuple[date, date]:
    end_d = today_tr()
    if end:
        try:
            end_d = date.fromisoformat(str(end)[:10])
        except ValueError:
            pass
    if start:
        try:
            start_d = date.fromisoformat(str(start)[:10])
        except ValueError:
            start_d = BACKFILL_START
    else:
        start_d = BACKFILL_START
    if start_d > end_d:
        start_d, end_d = end_d, start_d
    return start_d, end_d


def summary_totals_map(rows: list[dict[str, Any]]) -> dict[str, int]:
    """parse_summary_rows çıktısını user_id|metric_type → count haritasına çevir."""
    out: dict[str, int] = {}
    for item in rows or []:
        uid = int(item.get("user_id") or 0)
        mtype = str(item.get("metric_type") or "")
        if not uid or not mtype:
            continue
        out[f"{uid}|{mtype}"] = int(item.get("count") or 0)
    return out


def get_detail_coverage(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """DB'deki detay kayıt sayıları — gap-fill karşılaştırması için."""
    start_d, end_d = _parse_range(start, end)
    from sqlalchemy import func

    rows = (
        db.query(
            SinemalarModerationDetailItem.user_id,
            SinemalarModerationDetailItem.metric_type,
            func.count().label("cnt"),
        )
        .filter(
            SinemalarModerationDetailItem.event_at >= datetime.combine(start_d, datetime.min.time()),
            SinemalarModerationDetailItem.event_at
            < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
        )
        .group_by(
            SinemalarModerationDetailItem.user_id,
            SinemalarModerationDetailItem.metric_type,
        )
        .all()
    )
    counts = {f"{int(r.user_id)}|{r.metric_type}": int(r.cnt or 0) for r in rows}
    items: list[dict[str, Any]] = []
    for uid, uname in TRACKED_MODERATORS:
        for mkey, mlabel in METRIC_TYPES:
            key = f"{uid}|{mkey}"
            items.append(
                {
                    "user_id": uid,
                    "username": uname,
                    "metric_type": mkey,
                    "metric_label": mlabel,
                    "detail_count": counts.get(key, 0),
                }
            )
    return {
        "ok": True,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "counts": counts,
        "items": items,
    }


def compute_gaps(
    expected: dict[str, int],
    actual: dict[str, int],
    *,
    user_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """Sinemalar özet sayıları ile DB detay sayılarını karşılaştır."""
    allowed = set(user_ids) if user_ids else None
    gaps: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key, exp in expected.items():
        if key in seen:
            continue
        seen.add(key)
        parts = key.split("|", 1)
        if len(parts) != 2:
            continue
        try:
            uid = int(parts[0])
        except ValueError:
            continue
        if allowed is not None and uid not in allowed:
            continue
        mtype = parts[1]
        got = int(actual.get(key) or 0)
        if exp > got:
            gaps.append(
                {
                    "user_id": uid,
                    "username": resolve_username(uid),
                    "metric_type": mtype,
                    "metric_label": METRIC_LABEL_BY_TYPE.get(mtype, mtype),
                    "expected": exp,
                    "actual": got,
                    "missing": exp - got,
                }
            )
    gaps.sort(key=lambda g: (-int(g.get("missing") or 0), g.get("username") or ""))
    return gaps


def build_panel_analytics(
    detail_rows: list[SinemalarModerationDetailItem],
    *,
    start_d: date,
    end_d: date,
) -> dict[str, Any]:
    """Detay satırlarından grafik verisi üret."""
    by_user_day: dict[int, dict[str, int]] = {uid: {} for uid, _ in TRACKED_MODERATORS}
    by_user_metric: dict[int, dict[str, int]] = {uid: {k: 0 for k, _ in METRIC_TYPES} for uid, _ in TRACKED_MODERATORS}
    by_user_weekday: dict[int, dict[int, int]] = {uid: {w: 0 for w in range(7)} for uid, _ in TRACKED_MODERATORS}
    by_day_total: dict[str, int] = {}
    by_day_user: dict[str, dict[str, int]] = {}

    for r in detail_rows:
        if not is_tracked_user_id(r.user_id):
            continue
        uid = int(r.user_id)
        day = r.event_at.date().isoformat()
        by_user_day.setdefault(uid, {})
        by_user_day[uid][day] = by_user_day[uid].get(day, 0) + 1
        by_user_metric.setdefault(uid, {k: 0 for k, _ in METRIC_TYPES})
        by_user_metric[uid][r.metric_type] = by_user_metric[uid].get(r.metric_type, 0) + 1
        by_user_weekday.setdefault(uid, {w: 0 for w in range(7)})
        by_user_weekday[uid][r.event_at.weekday()] = by_user_weekday[uid].get(r.event_at.weekday(), 0) + 1
        by_day_total[day] = by_day_total.get(day, 0) + 1
        if day not in by_day_user:
            by_day_user[day] = {}
        uname = resolve_username(uid) or r.username or resolve_username(uid)
        by_day_user[day][uname] = by_day_user[day].get(uname, 0) + 1

    all_days: list[str] = []
    d = start_d
    while d <= end_d:
        all_days.append(d.isoformat())
        d += timedelta(days=1)

    weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    rankings_by_metric: dict[str, list[dict[str, Any]]] = {}
    for mkey, mlabel in METRIC_TYPES:
        ranked = sorted(
            [(uid, by_user_metric.get(uid, {}).get(mkey, 0)) for uid, _ in TRACKED_MODERATORS],
            key=lambda x: (-x[1], resolve_username(x[0])),
        )
        rankings_by_metric[mkey] = [
            {
                "user_id": uid,
                "username": resolve_username(uid),
                "metric_label": metric_display_label(mkey, mlabel),
                "count": cnt,
                "rank": i + 1,
            }
            for i, (uid, cnt) in enumerate(ranked)
            if cnt > 0
        ]

    overall_ranked = sorted(
        [(uid, sum(by_user_metric.get(uid, {}).values())) for uid, _ in TRACKED_MODERATORS],
        key=lambda x: (-x[1], resolve_username(x[0])),
    )
    overall_rank = [
        {"user_id": uid, "username": resolve_username(uid), "count": cnt, "rank": i + 1}
        for i, (uid, cnt) in enumerate(overall_ranked)
        if cnt > 0
    ]

    calendars: dict[str, dict[str, Any]] = {}
    for uid, uname in TRACKED_MODERATORS:
        days_data = [{"date": day, "count": by_user_day.get(uid, {}).get(day, 0)} for day in all_days]
        eligible = _eligible_days_for_moderator(uid, all_days)
        active = sum(1 for day in eligible if by_user_day.get(uid, {}).get(day, 0) > 0)
        inactive = len(eligible) - active
        join_d = moderator_join_date(uid)
        calendars[str(uid)] = {
            "username": uname,
            "days": days_data,
            "active_days": active,
            "inactive_days": inactive,
            "eligible_days": len(eligible),
            "pre_join_days": len(all_days) - len(eligible),
            "joined_at": join_d.isoformat() if join_d else None,
        }

    shares: dict[str, dict[str, float]] = {}
    for uid, uname in TRACKED_MODERATORS:
        total = sum(by_user_metric.get(uid, {}).values())
        if total <= 0:
            shares[str(uid)] = {k: 0.0 for k, _ in METRIC_TYPES}
        else:
            shares[str(uid)] = {
                k: round(100.0 * by_user_metric[uid].get(k, 0) / total, 1) for k, _ in METRIC_TYPES
            }

    cumulative: dict[str, list[dict[str, Any]]] = {}
    for uid, uname in TRACKED_MODERATORS:
        running = 0
        series: list[dict[str, Any]] = []
        for day in all_days:
            running += by_user_day.get(uid, {}).get(day, 0)
            series.append({"date": day, "cumulative": running})
        cumulative[str(uid)] = series

    return {
        "calendar_days": all_days,
        "calendars": calendars,
        "rankings_by_metric": rankings_by_metric,
        "overall_rank": overall_rank,
        "shares_by_metric": shares,
        "weekday_labels": weekday_labels,
        "weekday_by_user": {
            str(uid): [by_user_weekday.get(uid, {}).get(w, 0) for w in range(7)] for uid, _ in TRACKED_MODERATORS
        },
        "daily_totals": by_day_total,
        "daily_by_user": {
            str(uid): [by_user_day.get(uid, {}).get(day, 0) for day in all_days] for uid, _ in TRACKED_MODERATORS
        },
        "cumulative_by_user": cumulative,
        "usernames": {str(uid): uname for uid, uname in TRACKED_MODERATORS},
    }


def get_panel_payload(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    meta = get_meta_summary(db)
    start_d, end_d = _parse_range(start, end)

    q = (
        db.query(SinemalarModerationDailyRow)
        .filter(
            SinemalarModerationDailyRow.report_date >= start_d,
            SinemalarModerationDailyRow.report_date <= end_d,
        )
        .order_by(
            SinemalarModerationDailyRow.report_date.desc(),
            SinemalarModerationDailyRow.username.asc(),
            SinemalarModerationDailyRow.metric_type.asc(),
        )
    )
    rows = q.all()

    users: dict[str, dict[str, Any]] = {}
    daily: dict[str, dict[str, int]] = {}
    for r in rows:
        if not is_tracked_user_id(r.user_id) and not is_tracked_username(r.username):
            continue
        uname = r.username or resolve_username(r.user_id)
        if uname not in users:
            users[uname] = {
                "username": uname,
                "user_id": r.user_id,
                "totals": {k: 0 for k, _ in METRIC_TYPES},
                "total_all": 0,
            }
        users[uname]["totals"][r.metric_type] = users[uname]["totals"].get(r.metric_type, 0) + int(r.count or 0)
        users[uname]["total_all"] += int(r.count or 0)
        day_key = r.report_date.isoformat()
        if day_key not in daily:
            daily[day_key] = {}
        key = f"{uname}|{r.metric_type}"
        daily[day_key][key] = daily[day_key].get(key, 0) + int(r.count or 0)

    detail_total = (
        db.query(SinemalarModerationDetailItem)
        .filter(
            SinemalarModerationDetailItem.event_at >= datetime.combine(start_d, datetime.min.time()),
            SinemalarModerationDetailItem.event_at
            < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
        )
        .count()
    )

    detail_rows: list[SinemalarModerationDetailItem] = []
    analytics: dict[str, Any] = {}
    if detail_total > 0:
        detail_rows = (
            db.query(SinemalarModerationDetailItem)
            .filter(
                SinemalarModerationDetailItem.event_at >= datetime.combine(start_d, datetime.min.time()),
                SinemalarModerationDetailItem.event_at
                < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
            )
            .all()
        )
        analytics = build_panel_analytics(detail_rows, start_d=start_d, end_d=end_d)
        if not users:
            for r in detail_rows:
                if not is_tracked_user_id(r.user_id):
                    continue
                uname = resolve_username(r.user_id) or r.username
                if uname not in users:
                    users[uname] = {
                        "username": uname,
                        "user_id": r.user_id,
                        "totals": {k: 0 for k, _ in METRIC_TYPES},
                        "total_all": 0,
                    }
                users[uname]["totals"][r.metric_type] = users[uname]["totals"].get(r.metric_type, 0) + 1
                users[uname]["total_all"] += 1

    ordered_users = []
    for uid, want in TRACKED_MODERATORS:
        for uname, block in users.items():
            if block.get("user_id") == uid or _norm_username(uname) == _norm_username(want):
                ordered_users.append(block)
                break
        else:
            ordered_users.append(
                {
                    "username": want,
                    "user_id": uid,
                    "totals": {k: 0 for k, _ in METRIC_TYPES},
                    "total_all": 0,
                }
            )

    return {
        "ok": True,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "metric_types": [{"key": k, "label": metric_display_label(k, v)} for k, v in METRIC_TYPES],
        "moderators": [{"user_id": uid, "username": uname} for uid, uname in TRACKED_MODERATORS],
        "users": ordered_users,
        "daily": daily,
        "analytics": analytics,
        "meta": get_meta_summary(db),
        "range_min": BACKFILL_START.isoformat(),
        "row_count": len(rows),
        "detail_item_count": detail_total,
    }
