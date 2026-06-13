"""Notification Analytics — paylaşımlı workspace (tüm admin oturumları aynı veriyi görür)."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from backend.models import NotificationAnalyticsWorkspace

LOGGER = logging.getLogger(__name__)
WORKSPACE_ID = 1


def _n(value: Any) -> float:
    """Oran / CTR — ondalık korunur (3,877 → 3.877)."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value) if value == value else 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    s = re.sub(r"[%\s]", "", s)
    has_dot = "." in s
    has_comma = "," in s
    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif has_dot:
        parts = s.split(".")
        if len(parts) > 2 and all(re.fullmatch(r"\d{3}", p) for p in parts[1:]):
            s = "".join(parts)
        elif len(parts) == 2 and re.fullmatch(r"\d{3}", parts[1]):
            if len(parts[0]) > 3:
                s = "".join(parts)
            else:
                s = parts[0] + "." + parts[1]
    elif has_comma:
        parts = s.split(",")
        if len(parts) > 1 and all(re.fullmatch(r"\d{3}", p) for p in parts[1:]):
            s = "".join(parts)
        else:
            s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _n_count(value: Any) -> float:
    """Click / impression — tam sayı; 48.521 → 48521, 1.670 → 1670."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        f = float(value)
        return f if f == f else 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    s = re.sub(r"[%\s]", "", s)
    has_dot = "." in s
    has_comma = "," in s
    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    if has_dot:
        parts = s.split(".")
        if len(parts) >= 2 and all(re.fullmatch(r"\d+", p) for p in parts):
            if all(re.fullmatch(r"\d{3}", p) for p in parts[1:]):
                return float("".join(parts))
    if has_comma:
        parts = s.split(",")
        if len(parts) >= 2 and all(re.fullmatch(r"\d{3}", p) for p in parts[1:]):
            return float("".join(parts))
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _normalize_id(raw: Any) -> str:
    return re.sub(r"[\s\u00a0.,·']", "", str(raw or "").strip())


def _normalize_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (h or "").lower())


def _parse_date_smart(raw: str) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    try:
        direct = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return direct.isoformat()
    except ValueError:
        pass
    m = re.match(
        r"^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})(?:\s+(\d{1,2}):(\d{2}))?$",
        s,
    )
    if not m:
        return None
    year = int(m.group(3))
    if year < 100:
        year += 2000
    try:
        dt = datetime(year, int(m.group(2)), int(m.group(1)), int(m.group(4) or 0), int(m.group(5) or 0))
        return dt.isoformat()
    except ValueError:
        return None


def _detect_delimiter(header_line: str) -> str:
    best = ","
    best_count = -1
    for delim in (",", ";", "\t"):
        count = len(header_line.split(delim))
        if count > best_count:
            best = delim
            best_count = count
    return best


def parse_csv_text(text: str) -> list[dict]:
    raw = (text or "").strip()
    if not raw:
        return []
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    if len(lines) < 2:
        return []
    delim = _detect_delimiter(lines[0])
    headers = [_normalize_header(h) for h in lines[0].split(delim)]

    def pick(names: list[str]) -> int:
        for i, h in enumerate(headers):
            if h in names:
                return i
        return -1

    idx = {
        "id": pick(["id", "bildirimid", "notificationid"]),
        "text": pick(
            [
                "text",
                "title",
                "headline",
                "baslik",
                "icerik",
                "metin",
                "bildirimmetni",
                "notificationtext",
                "message",
                "content",
            ]
        ),
        "date": pick(
            [
                "date",
                "datetime",
                "timestamp",
                "tarih",
                "gonderimtarihi",
                "sentat",
                "publishdate",
                "gun",
            ]
        ),
        "ai": pick(["androidappimpression"]),
        "ac": pick(["androidappclick"]),
        "atr": pick(["androidappctr"]),
        "ic": pick(["iosappclick"]),
        "itr": pick(["iosappctr"]),
        "di": pick(["desktopimpression"]),
        "dc": pick(["desktopclick"]),
        "dtr": pick(["desktopctr"]),
        "mi": pick(["mobilewebimpression"]),
        "mc": pick(["mobilewebclick"]),
        "mtr": pick(["mobilewebctr"]),
    }
    if idx["text"] < 0 or idx["date"] < 0:
        return []

    def col(cols: list[str], i: int) -> str:
        return cols[i] if 0 <= i < len(cols) else ""

    out: list[dict] = []
    for line in lines[1:]:
        cols = line.split(delim)
        iso = _parse_date_smart(col(cols, idx["date"]))
        if not iso:
            continue
        item = {
            "id": _normalize_id(col(cols, idx["id"])) if idx["id"] >= 0 else "",
            "text": col(cols, idx["text"]).strip(),
            "date": iso,
            "platforms": {
                "android": {
                    "impression": _n_count(col(cols, idx["ai"])),
                    "click": _n_count(col(cols, idx["ac"])),
                    "ctr": _n(col(cols, idx["atr"])),
                },
                "ios": {
                    "click": _n_count(col(cols, idx["ic"])),
                    "ctr": _n(col(cols, idx["itr"])),
                },
                "desktop": {
                    "impression": _n_count(col(cols, idx["di"])),
                    "click": _n_count(col(cols, idx["dc"])),
                    "ctr": _n(col(cols, idx["dtr"])),
                },
                "mobileweb": {
                    "impression": _n_count(col(cols, idx["mi"])),
                    "click": _n_count(col(cols, idx["mc"])),
                    "ctr": _n(col(cols, idx["mtr"])),
                },
            },
        }
        if item["text"]:
            out.append(_sanitize_row(item))
    return out


def _highest_id(rows: list[dict]) -> int:
    best = 0
    for row in rows:
        try:
            val = int(_n(row.get("id")))
        except (TypeError, ValueError):
            val = 0
        if val > best:
            best = val
    return best


def _row_key(row: dict) -> str:
    return f"{row.get('id') or ''}|{row.get('text')}|{row.get('date')}"


def _sanitize_row(row: dict) -> dict:
    """iOS yalnızca click tutulur; impression alanı kullanılmaz."""
    platforms = row.get("platforms")
    if not isinstance(platforms, dict):
        return row
    ios = platforms.get("ios")
    if not isinstance(ios, dict) or "impression" not in ios:
        return row
    clean_ios = {k: v for k, v in ios.items() if k != "impression"}
    return {**row, "platforms": {**platforms, "ios": clean_ios}}


def _merge_rows(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for row in existing + incoming:
        merged[_row_key(row)] = _sanitize_row(row)
    return sorted(merged.values(), key=lambda r: r.get("date") or "")


def _get_workspace(db: Session) -> NotificationAnalyticsWorkspace:
    row = db.get(NotificationAnalyticsWorkspace, WORKSPACE_ID)
    if row is None:
        row = NotificationAnalyticsWorkspace(id=WORKSPACE_ID)
        db.add(row)
        db.flush()
    return row


def _load_rows(row: NotificationAnalyticsWorkspace) -> list[dict]:
    try:
        data = json.loads(row.rows_json or "[]")
        return [_sanitize_row(r) for r in data if isinstance(r, dict)]
    except json.JSONDecodeError:
        return []


def _row_day_key(iso: str | None) -> str:
    return str(iso or "")[:10]


def _rows_date_bounds(rows: list[dict]) -> tuple[str | None, str | None]:
    days = sorted({_row_day_key(r.get("date")) for r in rows} - {""})
    if not days:
        return None, None
    return days[0], days[-1]


def filter_rows_by_date(
    rows: list[dict],
    *,
    start: str | None = None,
    end: str | None = None,
) -> list[dict]:
    """Tarih aralığı (YYYY-MM-DD); boş = filtre yok."""
    s = (start or "").strip()[:10] or None
    e = (end or "").strip()[:10] or None
    if not s and not e:
        return rows
    out: list[dict] = []
    for r in rows:
        d = _row_day_key(r.get("date"))
        if not d:
            continue
        if s and d < s:
            continue
        if e and d > e:
            continue
        out.append(r)
    return out


def workspace_rows_chunk(
    db: Session,
    *,
    offset: int,
    limit: int,
    start: str | None = None,
    end: str | None = None,
) -> dict:
    row = _get_workspace(db)
    all_rows = _load_rows(row)
    rows = filter_rows_by_date(all_rows, start=start, end=end)
    total = len(rows)
    off = max(0, int(offset))
    end_idx = min(total, off + int(limit))
    chunk = rows[off:end_idx]
    return {
        "ok": True,
        "rows": chunk,
        "offset": off,
        "limit": int(limit),
        "total": total,
        "total_unfiltered": len(all_rows),
        "has_more": end_idx < total,
        "filter_start": (start or "")[:10],
        "filter_end": (end or "")[:10],
    }


def workspace_state(db: Session, *, include_rows: bool = True) -> dict:
    row = _get_workspace(db)
    rows = _load_rows(row)
    _min_d, _max_d = _rows_date_bounds(rows)
    out: dict[str, Any] = {
        "ok": True,
        "last_id": int(row.last_id or 0),
        "start": row.filter_start or "",
        "end": row.filter_end or "",
        "preset": row.preset or "1y",
        "row_count": len(rows),
        "data_min_date": _min_d or "",
        "data_max_date": _max_d or "",
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
    if include_rows:
        out["rows"] = rows
    return out


def save_workspace(
    db: Session,
    *,
    rows: list[dict] | None = None,
    last_id: int | None = None,
    start: str | None = None,
    end: str | None = None,
    preset: str | None = None,
) -> dict:
    row = _get_workspace(db)
    if rows is not None:
        row.rows_json = json.dumps(rows, ensure_ascii=False)
        if last_id is None:
            last_id = _highest_id(rows)
    if last_id is not None:
        row.last_id = int(last_id)
    if start is not None:
        row.filter_start = str(start or "")[:10]
    if end is not None:
        row.filter_end = str(end or "")[:10]
    if preset is not None:
        row.preset = str(preset or "1y")[:10]
    row.updated_at = datetime.utcnow()
    db.commit()
    return workspace_state(db)


def append_rows(db: Session, incoming: list[dict]) -> dict:
    row = _get_workspace(db)
    existing = _load_rows(row)
    max_id_before = max(int(row.last_id or 0), _highest_id(existing))
    filtered: list[dict] = []
    for item in incoming:
        rid = _n(item.get("id"))
        if rid > max_id_before or not rid:
            filtered.append(item)
    if not filtered:
        return {
            **workspace_state(db),
            "added": 0,
            "message": f"Yeni satır yok (son ID: {max_id_before}).",
        }
    merged = _merge_rows(existing, filtered)
    row.rows_json = json.dumps(merged, ensure_ascii=False)
    row.last_id = max(max_id_before, _highest_id(merged))
    row.updated_at = datetime.utcnow()
    db.commit()
    return {
        **workspace_state(db),
        "added": len(filtered),
        "message": f"{len(filtered)} yeni satır eklendi.",
    }


def upload_csv_text(db: Session, csv_text: str) -> dict:
    """CSV satırlarını id|text|date anahtarıyla birleştirir (mevcut ID'ler de güncellenir)."""
    parsed = parse_csv_text(csv_text)
    if not parsed:
        return {
            **workspace_state(db, include_rows=False),
            "added": 0,
            "updated": 0,
            "parsed": 0,
            "message": (
                "CSV parse edilemedi. Başlık satırında metin (text/title/başlık) ve tarih (date/tarih) "
                "sütunları ve en az bir veri satırı gerekli."
            ),
        }
    row = _get_workspace(db)
    existing = _load_rows(row)
    existing_keys = {_row_key(r) for r in existing}
    added = 0
    updated = 0
    seen_incoming: set[str] = set()
    for item in parsed:
        key = _row_key(item)
        if key in seen_incoming:
            continue
        seen_incoming.add(key)
        if key in existing_keys:
            updated += 1
        else:
            added += 1
            existing_keys.add(key)
    merged = _merge_rows(existing, parsed)
    min_day, max_day = _rows_date_bounds(merged)
    fe = (row.filter_end or "").strip()[:10]
    if fe and max_day and max_day > fe:
        row.filter_end = max_day
    row.rows_json = json.dumps(merged, ensure_ascii=False)
    row.last_id = max(int(row.last_id or 0), _highest_id(merged))
    row.updated_at = datetime.utcnow()
    db.commit()
    return {
        **workspace_state(db, include_rows=False),
        "added": added,
        "updated": updated,
        "parsed": len(parsed),
        "data_min_date": min_day or "",
        "data_max_date": max_day or "",
        "message": f"{len(parsed)} satır işlendi: {added} yeni, {updated} güncellendi.",
    }


def decode_csv_bytes(raw: bytes) -> str:
    """UTF-8 / Windows Türkçe CSV kodlamalarını dene."""
    if not raw:
        return ""
    for enc in ("utf-8-sig", "utf-8", "cp1254", "iso-8859-9", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def reset_workspace(db: Session) -> dict:
    row = _get_workspace(db)
    row.rows_json = "[]"
    row.last_id = 0
    row.updated_at = datetime.utcnow()
    db.commit()
    return workspace_state(db)
