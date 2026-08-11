"""Piyasa günlük açılış/kapanış — tarama ingest + overlay sorgusu."""

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

from backend.database import engine
from backend.models import MarketDailyQuote
from backend.services.backlink_csv import fetch_public_sheet_csv
from backend.services.market_sheets_config import (
    MARKET_SHEET_SERIES,
    MarketSheetSeries,
    SERIES_BY_KEY,
    TARAMA_SOURCE_ID,
)

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
    s = (cell or "").strip().lower().replace("ı", "i").replace("İ", "i")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def _parse_tr_number(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'")
    if not s or s in ("-", "—", "N/A"):
        return None
    s = s.replace("\u00a0", "").replace(" ", "")
    s = re.sub(r"^[%$€£₺]+", "", s)
    s = s.replace("₺", "").replace("$", "").replace("€", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        s = s.replace(".", "")
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
            elif "son" in h and "deger" in h:
                idx.setdefault("kapanis", j)
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


def parse_archive_payload(payload: Any) -> list[dict[str, Any]]:
    """doviz.com /assets/{key}/archive JSON → report_date / open / close."""
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    archive = data.get("archive") if isinstance(data, dict) else None
    if archive is None:
        return []
    if isinstance(archive, dict):
        points: Iterable[Any] = archive.values()
    elif isinstance(archive, list):
        points = archive
    else:
        return []
    out: list[dict[str, Any]] = []
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Europe/Istanbul")
    except Exception:
        tz = timezone.utc
    for point in points:
        if not isinstance(point, dict):
            continue
        close = point.get("close")
        if close is None:
            close = point.get("value") or point.get("last") or point.get("price")
        try:
            close_f = float(close) if close is not None else None
        except (TypeError, ValueError):
            close_f = None
        if close_f is None:
            continue
        open_raw = point.get("open")
        try:
            open_f = float(open_raw) if open_raw is not None else None
        except (TypeError, ValueError):
            open_f = None
        d: date | None = None
        ts = point.get("update_date") or point.get("date") or point.get("time")
        if isinstance(ts, (int, float)) and ts > 0:
            d = datetime.fromtimestamp(int(ts), tz=tz).date()
        elif isinstance(ts, str):
            d = _parse_tr_date_cell(ts[:10] if len(ts) >= 10 else ts)
        if not d:
            continue
        out.append({"report_date": d, "open_price": open_f, "close_price": close_f})
    out.sort(key=lambda r: r["report_date"])
    return out


def parse_historical_table_matrix(
    headers: list[str],
    body_rows: list[list[str]],
) -> list[dict[str, Any]]:
    """Tablo başlık + satırlar (Tarih / Açılış / Kapanış veya Son Değer)."""
    if not headers or not body_rows:
        return []
    norm = [_norm_header(h) for h in headers]
    col: dict[str, int] = {}
    for j, h in enumerate(norm):
        if h == "tarih" or h.startswith("tarih"):
            col["tarih"] = j
        elif h.startswith("acil"):
            col["acilis"] = j
        elif h.startswith("kapan"):
            col["kapanis"] = j
        elif "son" in h and "deger" in h:
            col.setdefault("kapanis", j)
        elif h in ("son", "kapanis", "close", "fiyat"):
            col.setdefault("kapanis", j)
    if "tarih" not in col or "kapanis" not in col:
        # 2 kolon: Tarih + değer
        if len(norm) >= 2 and "tarih" in col:
            col["kapanis"] = 1 if col["tarih"] == 0 else 0
        else:
            return []
    out: list[dict[str, Any]] = []
    for row in body_rows:
        if not row or not any(str(c or "").strip() for c in row):
            continue
        di = col["tarih"]
        ci = col["kapanis"]
        d = _parse_tr_date_cell(row[di] if di < len(row) else "")
        close = _parse_tr_number(row[ci] if ci < len(row) else None)
        if not d or close is None:
            continue
        oi = col.get("acilis")
        open_p = _parse_tr_number(row[oi]) if oi is not None and oi < len(row) else None
        out.append({"report_date": d, "open_price": open_p, "close_price": close})
    return out


def ingest_market_tarama_payload(
    db: Session,
    series_items: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> dict[str, Any]:
    """Mac köprüsü tarama gövdesini MarketDailyQuote'a yazar."""
    results: list[dict[str, Any]] = []
    total = 0
    for item in series_items or []:
        key = str(item.get("key") or item.get("series_key") or "").strip()
        if key not in SERIES_BY_KEY:
            results.append({"series_key": key, "ok": False, "error": "Bilinmeyen seri"})
            continue
        raw_rows = item.get("rows") or []
        parsed: list[dict[str, Any]] = []
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            d = row.get("report_date") or row.get("date")
            if isinstance(d, datetime):
                d = d.date()
            elif isinstance(d, str):
                d = _parse_tr_date_cell(d) or (
                    date.fromisoformat(d[:10]) if len(d) >= 10 else None
                )
            if not isinstance(d, date):
                continue
            close = row.get("close_price", row.get("close"))
            try:
                close_f = float(close) if close is not None else None
            except (TypeError, ValueError):
                close_f = None
            if close_f is None:
                continue
            open_raw = row.get("open_price", row.get("open"))
            try:
                open_f = float(open_raw) if open_raw is not None else None
            except (TypeError, ValueError):
                open_f = None
            parsed.append({"report_date": d, "open_price": open_f, "close_price": close_f})
        if not parsed:
            results.append({"series_key": key, "ok": False, "error": "Satır yok", "parsed": 0, "upserted": 0})
            continue
        upserted = _upsert_rows(db, key, TARAMA_SOURCE_ID, parsed)
        dates = [p["report_date"] for p in parsed]
        total += upserted
        results.append(
            {
                "series_key": key,
                "ok": True,
                "parsed": len(parsed),
                "upserted": upserted,
                "min_date": min(dates).isoformat(),
                "max_date": max(dates).isoformat(),
            }
        )
    if commit:
        db.commit()
    ok_count = sum(1 for r in results if r.get("ok"))
    return {
        "ok": ok_count == len(series_items) and bool(series_items),
        "series_count": len(series_items),
        "ok_count": ok_count,
        "rows_upserted": total,
        "results": results,
        "synced_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "source": TARAMA_SOURCE_ID,
    }


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
    """Google Sheets çekimi kapatıldı — seri güncellemesi tarama ingest ile yapılır."""
    del commit  # API uyumu
    return {
        "ok": False,
        "disabled": True,
        "message": "Piyasa serileri tarama ile güncellenir (tablo kaynağı kapalı).",
        "series_count": len(MARKET_SHEET_SERIES),
        "ok_count": 0,
        "rows_upserted": 0,
        "results": [],
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
