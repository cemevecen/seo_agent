"""Google Sheets'ten günlük açılış/kapanış — DB'ye upsert."""

from __future__ import annotations

import csv
import io
import logging
import re
import unicodedata
from datetime import date, datetime, timezone
from typing import Any, Iterable

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backend.database import SessionLocal, engine
from backend.models import MarketDailyQuote
from backend.services.backlink_csv import fetch_public_sheet_csv
from backend.services.market_sheets_config import MARKET_SHEET_SERIES, MarketSheetSeries, SERIES_BY_KEY

LOGGER = logging.getLogger(__name__)
_IS_PG = "postgresql" in str(engine.url)

_TR_MONTHS: dict[str, int] = {
    "ocak": 1,
    "oca": 1,
    "subat": 2,
    "sub": 2,
    "şubat": 2,
    "şub": 2,
    "mart": 3,
    "mar": 3,
    "nisan": 4,
    "nis": 4,
    "mayis": 5,
    "mayıs": 5,
    "may": 5,
    "haziran": 6,
    "haz": 6,
    "temmuz": 7,
    "tem": 7,
    "agustos": 8,
    "ağustos": 8,
    "agu": 8,
    "ağu": 8,
    "eylul": 9,
    "eylül": 9,
    "eyl": 9,
    "ekim": 10,
    "eki": 10,
    "kasim": 11,
    "kasım": 11,
    "kas": 11,
    "aralik": 12,
    "aralık": 12,
    "ara": 12,
}


def _norm_header(cell: str) -> str:
    s = unicodedata.normalize("NFKD", (cell or "").strip().lower())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def _parse_tr_number(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'")
    if not s or s in ("-", "—", "N/A"):
        return None
    s = s.replace("\u00a0", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_tr_date_cell(raw: str | None) -> date | None:
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if iso:
        return date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
    dmy = re.match(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$", s)
    if dmy:
        return date(int(dmy.group(3)), int(dmy.group(2)), int(dmy.group(1)))
    parts = s.split()
    if len(parts) < 3:
        return None
    try:
        day = int(parts[0])
    except ValueError:
        return None
    mon = _TR_MONTHS.get(parts[1].lower())
    if not mon:
        mon = _TR_MONTHS.get(_norm_header(parts[1]))
    if not mon:
        return None
    try:
        year = int(parts[2])
    except ValueError:
        return None
    return date(year, mon, day)


def _locate_header_row(rows: list[list[str]]) -> tuple[int, dict[str, int]] | None:
    for i, row in enumerate(rows):
        if not row:
            continue
        norm = [_norm_header(c) for c in row]
        if "tarih" not in norm:
            continue
        idx: dict[str, int] = {}
        for j, h in enumerate(norm):
            if h == "tarih":
                idx["tarih"] = j
            elif h.startswith("acil"):
                idx["acilis"] = j
            elif h.startswith("kapan"):
                idx["kapanis"] = j
        if "tarih" in idx and "kapanis" in idx:
            return i, idx
    return None


def parse_market_sheet_csv(text: str) -> list[dict[str, Any]]:
    """Satır listesi: report_date, open_price, close_price."""
    reader = csv.reader(io.StringIO(text or ""))
    rows = [list(r) for r in reader]
    located = _locate_header_row(rows)
    if not located:
        return []
    header_i, col = located
    out: list[dict[str, Any]] = []
    for row in rows[header_i + 1 :]:
        if not row or not any(str(c or "").strip() for c in row):
            continue
        di = col.get("tarih", 0)
        oi = col.get("acilis")
        ci = col.get("kapanis")
        if ci is None or ci >= len(row):
            continue
        d = _parse_tr_date_cell(row[di] if di < len(row) else "")
        close = _parse_tr_number(row[ci])
        if not d or close is None:
            continue
        open_p = _parse_tr_number(row[oi]) if oi is not None and oi < len(row) else None
        out.append({"report_date": d, "open_price": open_p, "close_price": close})
    return out


def _upsert_rows(db: Session, series_key: str, sheet_id: str, rows: Iterable[dict[str, Any]]) -> int:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    n = 0
    for item in rows:
        d = item["report_date"]
        close = float(item["close_price"])
        open_p = item.get("open_price")
        open_f = float(open_p) if open_p is not None else None
        if _IS_PG:
            stmt = pg_insert(MarketDailyQuote).values(
                series_key=series_key,
                report_date=d,
                open_price=open_f,
                close_price=close,
                source_sheet_id=sheet_id,
                synced_at=now,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["series_key", "report_date"],
                set_={
                    "open_price": open_f,
                    "close_price": close,
                    "source_sheet_id": sheet_id,
                    "synced_at": now,
                },
            )
            db.execute(stmt)
        else:
            existing = (
                db.query(MarketDailyQuote)
                .filter(MarketDailyQuote.series_key == series_key, MarketDailyQuote.report_date == d)
                .one_or_none()
            )
            if existing:
                existing.open_price = open_f
                existing.close_price = close
                existing.source_sheet_id = sheet_id
                existing.synced_at = now
            else:
                db.add(
                    MarketDailyQuote(
                        series_key=series_key,
                        report_date=d,
                        open_price=open_f,
                        close_price=close,
                        source_sheet_id=sheet_id,
                        synced_at=now,
                    )
                )
        n += 1
    return n


def _sheet_id_from_url(url: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url or "")
    return m.group(1) if m else ""


def sync_series_from_sheet(db: Session, spec: MarketSheetSeries) -> dict[str, Any]:
    csv_text = fetch_public_sheet_csv(spec.sheet_url)
    parsed = parse_market_sheet_csv(csv_text)
    if not parsed:
        return {
            "series_key": spec.key,
            "ok": False,
            "error": "Satır okunamadı (Tarih / Açılış / Kapanış)",
            "parsed": 0,
            "upserted": 0,
        }
    sheet_id = _sheet_id_from_url(spec.sheet_url)
    upserted = _upsert_rows(db, spec.key, sheet_id, parsed)
    db.commit()
    dates = [p["report_date"] for p in parsed]
    return {
        "series_key": spec.key,
        "ok": True,
        "parsed": len(parsed),
        "upserted": upserted,
        "min_date": min(dates).isoformat() if dates else None,
        "max_date": max(dates).isoformat() if dates else None,
    }


def sync_all_market_sheets(*, commit: bool = True) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    total = 0
    with SessionLocal() as db:
        for spec in MARKET_SHEET_SERIES:
            try:
                out = sync_series_from_sheet(db, spec)
                results.append(out)
                if out.get("ok"):
                    total += int(out.get("upserted") or 0)
            except Exception as exc:  # noqa: BLE001
                db.rollback()
                LOGGER.warning("Market sheet sync failed %s: %s", spec.key, exc)
                results.append({"series_key": spec.key, "ok": False, "error": str(exc)})
        if commit:
            db.commit()
    ok_count = sum(1 for r in results if r.get("ok"))
    return {
        "ok": ok_count == len(MARKET_SHEET_SERIES),
        "series_count": len(MARKET_SHEET_SERIES),
        "ok_count": ok_count,
        "rows_upserted": total,
        "results": results,
        "synced_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
    }


def query_overlay(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
    series_keys: list[str] | None = None,
) -> dict[str, Any]:
    keys = series_keys or list(SERIES_BY_KEY.keys())
    keys = [k for k in keys if k in SERIES_BY_KEY]
    q = db.query(MarketDailyQuote).filter(MarketDailyQuote.series_key.in_(keys))
    if start:
        q = q.filter(MarketDailyQuote.report_date >= date.fromisoformat(start[:10]))
    if end:
        q = q.filter(MarketDailyQuote.report_date <= date.fromisoformat(end[:10]))
    q = q.order_by(MarketDailyQuote.series_key, MarketDailyQuote.report_date)
    rows = q.all()
    by_key: dict[str, list[MarketDailyQuote]] = {k: [] for k in keys}
    for r in rows:
        by_key.setdefault(r.series_key, []).append(r)
    latest_sync = db.query(MarketDailyQuote.synced_at).order_by(MarketDailyQuote.synced_at.desc()).limit(1).scalar()
    series_out: dict[str, Any] = {}
    for k in keys:
        spec = SERIES_BY_KEY[k]
        pts = by_key.get(k) or []
        series_out[k] = {
            "key": k,
            "label": spec.label,
            "unit": spec.unit,
            "by_date": [
                {
                    "date": p.report_date.isoformat(),
                    "open": p.open_price,
                    "close": p.close_price,
                }
                for p in pts
            ],
        }
    return {
        "synced_at": latest_sync.isoformat() if latest_sync else None,
        "range": {"start": start, "end": end},
        "series": series_out,
    }
