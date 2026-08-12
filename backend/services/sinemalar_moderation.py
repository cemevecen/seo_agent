"""Sinemalar management/getModerationSummary — günlük moderatör metrikleri."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from backend.models import SinemalarModerationDailyRow, SinemalarModerationMeta

TR = ZoneInfo("Europe/Istanbul")

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

# Panelde gösterilecek moderatörler (kullanıcı adı eşleşmesi normalize)
TRACKED_USERNAMES: tuple[str, ...] = ("gezginozlem", "berend", "Gözde.")

BACKFILL_START = date(2026, 1, 1)


def _norm_username(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.rstrip(".")
    return s


def is_tracked_username(name: str) -> bool:
    n = _norm_username(name)
    return n in {_norm_username(u) for u in TRACKED_USERNAMES}


def _meta(db: Session) -> SinemalarModerationMeta:
    row = db.query(SinemalarModerationMeta).filter(SinemalarModerationMeta.id == 1).first()
    if row is None:
        row = SinemalarModerationMeta(id=1)
        db.add(row)
        db.flush()
    return row


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
        for label, block in metrics.items():
            if not isinstance(block, dict):
                continue
            mtype = str(block.get("type") or METRIC_TYPE_BY_LABEL.get(label) or "").strip()
            if not mtype:
                continue
            uid_raw = block.get("userId")
            try:
                user_id = int(uid_raw) if uid_raw is not None else 0
            except (TypeError, ValueError):
                user_id = 0
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
        uid = int(item.get("user_id") or 0)
        mtype = str(item.get("metric_type") or "")
        if not uid or not mtype:
            continue
        existing = (
            db.query(SinemalarModerationDailyRow)
            .filter(
                SinemalarModerationDailyRow.report_date == day,
                SinemalarModerationDailyRow.user_id == uid,
                SinemalarModerationDailyRow.metric_type == mtype,
            )
            .first()
        )
        if existing is None:
            existing = SinemalarModerationDailyRow(
                report_date=day,
                user_id=uid,
                metric_type=mtype,
            )
            db.add(existing)
        existing.username = str(item.get("username") or existing.username or "")[:64]
        existing.metric_label = str(item.get("metric_label") or METRIC_LABEL_BY_TYPE.get(mtype, mtype))[:64]
        existing.count = int(item.get("count") or 0)
        detail = item.get("detail_url")
        existing.detail_url = str(detail)[:512] if detail else None
        existing.scraped_at = now
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
    """Çok günlük ingest — days: [{date, rows}] veya tek gün."""
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


def get_panel_payload(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    end_d = today_tr()
    start_d = end_d - timedelta(days=30)
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
        if not is_tracked_username(r.username):
            continue
        uname = r.username
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
    for want in TRACKED_USERNAMES:
        for uname, block in users.items():
            if _norm_username(uname) == _norm_username(want):
                ordered_users.append(block)
                break

    return {
        "ok": True,
        "start": start_d.isoformat(),
        "end": end_d.isoformat(),
        "metric_types": [{"key": k, "label": v} for k, v in METRIC_TYPES],
        "users": ordered_users,
        "daily": daily,
        "meta": get_meta_summary(db),
        "row_count": len(rows),
    }
