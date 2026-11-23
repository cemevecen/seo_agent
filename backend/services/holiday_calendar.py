"""Investing.com borsa tatil takvimi — parse, sakla, panel context."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import HolidayCalendarCache

LOGGER = logging.getLogger(__name__)

CACHE_KEY = "current"
SOURCE_URL = "https://tr.investing.com/holiday-calendar/"
_DATE_TR_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")


def parse_date_tr(value: str | None) -> str | None:
    """dd.mm.YYYY → YYYY-MM-DD."""
    raw = (value or "").strip()
    m = _DATE_TR_RE.match(raw)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None


def normalize_rows(raw_rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    last_date_tr = ""
    last_iso = ""
    seen: set[tuple[str, str, str, str]] = set()
    for item in raw_rows or []:
        if not isinstance(item, dict):
            continue
        date_tr = str(item.get("date_tr") or "").strip()
        if date_tr:
            last_date_tr = date_tr
            last_iso = parse_date_tr(date_tr) or ""
        else:
            date_tr = last_date_tr
        country = str(item.get("country") or "").strip()
        exchange = str(item.get("exchange") or "").strip()
        holiday = str(item.get("holiday") or "").strip()
        if not date_tr or not country or not holiday:
            continue
        iso = parse_date_tr(date_tr) or last_iso
        key = (date_tr, country, exchange, holiday)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "date_tr": date_tr,
                "date_iso": iso,
                "country": country,
                "country_flag": str(item.get("country_flag") or "").strip(),
                "country_href": str(item.get("country_href") or "").strip(),
                "exchange": exchange,
                "holiday": holiday,
            }
        )
    out.sort(key=lambda r: (r.get("date_iso") or "9999", r.get("country") or "", r.get("exchange") or ""))
    return out


def save_holiday_payload(db: Session, payload: dict[str, Any], *, commit: bool = True) -> dict[str, Any]:
    rows = normalize_rows(payload.get("rows") if isinstance(payload, dict) else None)
    body = {
        "source": str((payload or {}).get("source") or SOURCE_URL),
        "country": str((payload or {}).get("country") or "all"),
        "date_from": str((payload or {}).get("date_from") or ""),
        "date_to": str((payload or {}).get("date_to") or ""),
        "fetched_at": str((payload or {}).get("fetched_at") or datetime.utcnow().isoformat()),
        "row_count": len(rows),
        "rows": rows,
        "countries": sorted({r["country"] for r in rows}),
    }
    row = db.get(HolidayCalendarCache, CACHE_KEY)
    now = datetime.utcnow()
    if row is None:
        row = HolidayCalendarCache(
            cache_key=CACHE_KEY,
            payload_json=json.dumps(body, ensure_ascii=False),
            updated_at=now,
        )
        db.add(row)
    else:
        row.payload_json = json.dumps(body, ensure_ascii=False)
        row.updated_at = now
    if commit:
        db.commit()
    return {"ok": True, "row_count": len(rows), "updated_at": now.isoformat()}


def load_holiday_payload(db: Session) -> dict[str, Any] | None:
    row = db.get(HolidayCalendarCache, CACHE_KEY)
    if row is None:
        return None
    try:
        payload = json.loads(row.payload_json or "{}")
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    payload["updated_at"] = row.updated_at.isoformat() if row.updated_at else ""
    payload["rows"] = normalize_rows(payload.get("rows"))
    payload["row_count"] = len(payload["rows"])
    return payload


def build_holidays_page_context(db: Session) -> dict[str, Any]:
    payload = load_holiday_payload(db) or {
        "rows": [],
        "row_count": 0,
        "countries": [],
        "fetched_at": "",
        "updated_at": "",
        "date_from": "",
        "date_to": "",
        "source": SOURCE_URL,
    }
    rows = list(payload.get("rows") or [])
    # Investing düzeni: aynı gün tekrarında tarih hücresi boş
    display_rows: list[dict[str, Any]] = []
    last_iso = ""
    for r in rows:
        iso = str(r.get("date_iso") or "")
        show_date = iso != last_iso
        if show_date:
            last_iso = iso
        display_rows.append(
            {
                **r,
                "show_date": show_date,
                "date_display": r.get("date_tr") if show_date else "",
            }
        )
    countries = sorted({str(r.get("country") or "") for r in rows if r.get("country")})
    return {
        "rows": display_rows,
        "row_count": len(display_rows),
        "countries": countries,
        "country_count": len(countries),
        "fetched_at": payload.get("fetched_at") or payload.get("updated_at") or "",
        "updated_at": payload.get("updated_at") or "",
        "date_from": payload.get("date_from") or "",
        "date_to": payload.get("date_to") or "",
        "source_url": SOURCE_URL,
        "empty": not display_rows,
    }
