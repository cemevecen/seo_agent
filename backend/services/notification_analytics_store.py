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
        if len(parts) > 1 and all(re.fullmatch(r"\d{3}", p) for p in parts[1:]):
            s = "".join(parts)
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
        "id": pick(["id"]),
        "text": pick(["text", "title", "headline"]),
        "date": pick(["date", "datetime", "timestamp"]),
        "ai": pick(["androidappimpression"]),
        "ac": pick(["androidappclick"]),
        "atr": pick(["androidappctr"]),
        "ii": pick(["iosappimpression"]),
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
                    "impression": _n(col(cols, idx["ai"])),
                    "click": _n(col(cols, idx["ac"])),
                    "ctr": _n(col(cols, idx["atr"])),
                },
                "ios": {
                    "impression": _n(col(cols, idx["ii"])),
                    "click": _n(col(cols, idx["ic"])),
                    "ctr": _n(col(cols, idx["itr"])),
                },
                "desktop": {
                    "impression": _n(col(cols, idx["di"])),
                    "click": _n(col(cols, idx["dc"])),
                    "ctr": _n(col(cols, idx["dtr"])),
                },
                "mobileweb": {
                    "impression": _n(col(cols, idx["mi"])),
                    "click": _n(col(cols, idx["mc"])),
                    "ctr": _n(col(cols, idx["mtr"])),
                },
            },
        }
        if item["text"]:
            out.append(item)
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


def _merge_rows(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for row in existing + incoming:
        key = f"{row.get('id') or ''}|{row.get('text')}|{row.get('date')}"
        merged[key] = row
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
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def workspace_state(db: Session) -> dict:
    row = _get_workspace(db)
    rows = _load_rows(row)
    return {
        "ok": True,
        "rows": rows,
        "last_id": int(row.last_id or 0),
        "start": row.filter_start or "",
        "end": row.filter_end or "",
        "preset": row.preset or "1y",
        "row_count": len(rows),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


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
    parsed = parse_csv_text(csv_text)
    if not parsed:
        return {**workspace_state(db), "added": 0, "message": "CSV parse edilemedi."}
    return append_rows(db, parsed)


def reset_workspace(db: Session) -> dict:
    row = _get_workspace(db)
    row.rows_json = "[]"
    row.last_id = 0
    row.updated_at = datetime.utcnow()
    db.commit()
    return workspace_state(db)
