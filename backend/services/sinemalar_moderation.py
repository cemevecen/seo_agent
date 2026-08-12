"""Sinemalar management/getModerationSummary + getModerationDetail."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

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
    ("news", "Haber"),
    ("list", "Liste"),
    ("trailer", "Fragman"),
    ("summary", "Film Özeti"),
    ("bio", "Biyografi"),
    ("movie_cast_add", "Kadroya Ekleme"),
    ("movie_cast_remove", "Kadrodan Çıkarma"),
    ("movie_poster", "Film Afişi"),
    ("person_image", "Sanatçı Fotoğrafı"),
)

METRIC_LABEL_BY_TYPE = {k: v for k, v in METRIC_TYPES}
METRIC_TYPE_BY_LABEL = {v: k for k, v in METRIC_TYPES}
METRIC_TYPE_KEYS = tuple(k for k, _ in METRIC_TYPES)

BACKFILL_START = date(2026, 1, 1)
DEFAULT_DETAIL_END = date(2026, 8, 13)


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


def resolve_user_id(username: str, raw_id: Any = None) -> int:
    try:
        uid = int(raw_id) if raw_id not in (None, "") else 0
    except (TypeError, ValueError):
        uid = 0
    if uid:
        return uid
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
    return n in {_norm_username(u) for u in TRACKED_USERNAMES}


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
                    "username": moderator,
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
    rebuild_daily: bool = True,
) -> dict[str, Any]:
    now = scraped_at or datetime.utcnow()
    if items and isinstance(items[0], dict) and "cells" in items[0]:
        parsed = parse_detail_rows(
            items,
            user_id=user_id,
            username=username,
            metric_type=metric_type,
            source_url=source_url,
        )
    else:
        parsed = list(items or [])

    item_upserted = 0
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
        if existing is None:
            existing = SinemalarModerationDetailItem(
                user_id=user_id,
                metric_type=metric_type,
                item_id=str(item.get("item_id") or "")[:32],
                event_at=event_at,
                subtitle=subtitle,
            )
            db.add(existing)
        existing.username = str(item.get("username") or username)[:64]
        existing.metric_label = str(item.get("metric_label") or METRIC_LABEL_BY_TYPE.get(metric_type, metric_type))[:64]
        existing.title = str(item.get("title") or "")[:512]
        admin = item.get("admin_url") or item.get("source_url")
        existing.admin_url = str(admin)[:512] if admin else None
        existing.source_url = str(source_url or item.get("source_url") or "")[:512] or None
        existing.scraped_at = now
        item_upserted += 1

    daily_upserted = 0
    if rebuild_daily:
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
                username=username,
                metric_type=metric_type,
                count=cnt,
                detail_url=src,
                scraped_at=now,
            )
            daily_upserted += 1

    meta = _meta(db)
    meta.last_scraped_at = now
    meta.last_mode = "detail_range"
    meta.message = f"detail {username}/{metric_type} · {item_upserted} kayıt"
    db.commit()
    return {
        "ok": True,
        "user_id": user_id,
        "metric_type": metric_type,
        "items_upserted": item_upserted,
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
            username=str(item.get("username") or resolve_username(uid)),
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
                rebuild_daily=True,
            )
            if res.get("ok"):
                total_items += int(res.get("items_upserted") or 0)
                total_daily += int(res.get("daily_upserted") or 0)

        meta = _meta(db)
        if payload.get("backfill_complete"):
            meta.backfill_complete = True
            meta.backfill_cursor = None
        meta.message = f"detail_range · {total_items} kayıt · {total_daily} gün"
        db.commit()
        return {"ok": True, "items_upserted": total_items, "daily_upserted": total_daily, "batches": len(detail_batches)}

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


def today_tr() -> date:
    return datetime.now(TR).date()


def yesterday_tr() -> date:
    return today_tr() - timedelta(days=1)


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
        .limit(min(500, max(1, limit)))
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


def get_panel_payload(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    meta = get_meta_summary(db)
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

    detail_total = (
        db.query(SinemalarModerationDetailItem)
        .filter(
            SinemalarModerationDetailItem.event_at >= datetime.combine(start_d, datetime.min.time()),
            SinemalarModerationDetailItem.event_at < datetime.combine(end_d + timedelta(days=1), datetime.min.time()),
        )
        .count()
    )

    return {
        "ok": True,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "metric_types": [{"key": k, "label": v} for k, v in METRIC_TYPES],
        "moderators": [{"user_id": uid, "username": uname} for uid, uname in TRACKED_MODERATORS],
        "users": ordered_users,
        "daily": daily,
        "meta": get_meta_summary(db),
        "row_count": len(rows),
        "detail_item_count": detail_total,
    }
