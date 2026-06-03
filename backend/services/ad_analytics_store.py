"""Reklam raporu (Excel/CSV) — DB saklama, filtreleme ve özet agregasyon."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Iterator

from openpyxl import load_workbook
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from backend.database import engine
from backend.models import AdReportCatalog, AdReportRow

_IS_PG = "postgresql" in str(engine.url)

LOGGER = logging.getLogger(__name__)

HEADERS_MAP = {
    "adunit": "ad_unit",
    "month": "month",
    "date": "report_date",
    "incometype": "income_type",
    "adrequest": "ad_request",
    "matchedrequest": "matched_request",
    "impression": "impression",
    "click": "click",
    "adrequestecpm": "ad_request_ecpm",
    "adimpressionecpm": "ad_impression_ecpm",
    "adimpressionecpmtl": "ad_impression_ecpm",
    "ctr": "ctr",
    "coverage": "coverage",
    "viewability": "viewability",
    "netrevenue": "net_revenue",
    "netrevenuetl": "net_revenue",
    "empowerpageview": "empower_pageview",
    "empoweruniquevisitor": "empower_unique_visitor",
    "pageviewecpm": "pageview_ecpm",
    "uniquevisitorecpm": "unique_visitor_ecpm",
    "abovethefoldratio": "above_the_fold_ratio",
}

# Bilinen alan dışı sütunlar extra_metrics JSON'da saklanır (ham başlık anahtarı).
_CORE_FIELDS = frozenset(
    {
        "ad_unit",
        "month",
        "report_date",
        "income_type",
        "ad_request",
        "matched_request",
        "impression",
        "click",
        "ad_request_ecpm",
        "ad_impression_ecpm",
        "ctr",
        "coverage",
        "viewability",
        "net_revenue",
        "empower_pageview",
        "empower_unique_visitor",
        "pageview_ecpm",
        "unique_visitor_ecpm",
        "above_the_fold_ratio",
    }
)

_HEADER_SUBSTRING_RULES: list[tuple[str, str]] = [
    ("netrevenue", "net_revenue"),
    ("adunit", "ad_unit"),
    ("incometype", "income_type"),
    ("adrequest", "ad_request"),
    ("matchedrequest", "matched_request"),
    ("impression", "impression"),
    ("click", "click"),
    ("adrequestecpm", "ad_request_ecpm"),
    ("adimpressionecpm", "ad_impression_ecpm"),
    ("pageviewecpm", "pageview_ecpm"),
    ("uniquevisitorecpm", "unique_visitor_ecpm"),
    ("empowerpageview", "empower_pageview"),
    ("empoweruniquevisitor", "empower_unique_visitor"),
    ("abovethefold", "above_the_fold_ratio"),
    ("viewability", "viewability"),
    ("coverage", "coverage"),
]


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


def _normalize_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (h or "").lower())


def _slug_metric_key(raw: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", (raw or "").strip()).strip("_").lower()
    return s[:80] or "metric"


def _resolve_field(norm: str, raw: str) -> str | None:
    if norm in HEADERS_MAP:
        return HEADERS_MAP[norm]
    for needle, field in _HEADER_SUBSTRING_RULES:
        if needle in norm:
            return field
    return None


def _map_header_row(raw_headers: list[Any]) -> tuple[dict[str, int], list[tuple[str, int]]]:
    col_map: dict[str, int] = {}
    extras: list[tuple[str, int]] = []
    for i, raw in enumerate(raw_headers):
        label = str(raw or "").strip()
        norm = _normalize_header(label)
        field = _resolve_field(norm, label)
        if field:
            if field not in col_map:
                col_map[field] = i
        elif label:
            extras.append((label, i))
    return col_map, extras


def _excel_serial_to_date(val: Any) -> date | None:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        serial = float(val)
    except (TypeError, ValueError):
        s = str(val).strip()
        m = re.match(r"^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})$", s)
        if not m:
            return None
        y = int(m.group(3))
        if y < 100:
            y += 2000
        return date(y, int(m.group(2)), int(m.group(1)))
    if serial < 1000:
        return None
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=serial)).date()


def _month_from_serial(val: Any) -> str:
    d = _excel_serial_to_date(val)
    return d.strftime("%Y-%m") if d else ""


def _detect_channel(filename: str) -> str:
    low = (filename or "").lower()
    if "dovizcom" in low or "doviz.com" in low or "doviz_com" in low:
        return "dovizcom"
    if "android" in low:
        return "android"
    if "ios" in low:
        return "ios"
    if "web" in low:
        return "web"
    return "other"


def _detect_surface(ad_unit: str, channel: str) -> str:
    u = (ad_unit or "").lower()
    if u.startswith("m_"):
        return "mweb"
    if u.startswith("web_"):
        return "web"
    if "app-ios" in u or "doviz_ios" in u or "_ios_" in u or u.startswith("ios_"):
        return "ios_app"
    if "android" in u or "app-android" in u:
        return "android_app"
    if channel == "android":
        return "android_app"
    if channel == "ios":
        return "ios_app"
    if channel == "dovizcom":
        if u.startswith("m_"):
            return "mweb"
        if u.startswith("web_"):
            return "web"
        return "site"
    return "unknown"


def _platform_from_surface(surface: str, channel: str) -> str:
    if surface in ("web", "mweb", "site"):
        return "web"
    if surface in ("android_app", "ios_app") or channel in ("android", "ios"):
        return "app"
    if channel == "dovizcom":
        return "web"
    return channel or "other"


def _row_fingerprint(
    *,
    report_date: date,
    ad_unit: str,
    income_type: str,
    platform: str,
    source_file: str,
) -> str:
    raw = f"{report_date.isoformat()}|{ad_unit}|{income_type}|{platform}|{source_file}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _dict_from_mapped(
    mapped: dict[str, Any],
    source_file: str,
    *,
    channel: str,
    platform: str,
    surface: str,
    extra_metrics: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    ad_unit = (mapped.get("ad_unit") or "").strip()
    income_type = (mapped.get("income_type") or "").strip()
    rd = mapped.get("report_date")
    if isinstance(rd, str):
        rd = _excel_serial_to_date(rd)
    if not isinstance(rd, date):
        rd = _excel_serial_to_date(mapped.get("month"))
    if not ad_unit or not income_type or not rd:
        return None
    month_key = mapped.get("month_key") or _month_from_serial(mapped.get("month")) or rd.strftime("%Y-%m")
    fp = _row_fingerprint(
        report_date=rd,
        ad_unit=ad_unit,
        income_type=income_type,
        platform=platform,
        source_file=source_file,
    )
    extras = dict(extra_metrics or {})
    for key in list(extras.keys()):
        if key in _CORE_FIELDS:
            extras.pop(key, None)
    row: dict[str, Any] = {
        "fingerprint": fp,
        "source_file": source_file,
        "platform": platform,
        "channel": channel,
        "surface": surface,
        "ad_unit": ad_unit[:500],
        "month_key": month_key[:7],
        "report_date": rd,
        "income_type": income_type[:120],
        "ad_request": _n(mapped.get("ad_request")),
        "matched_request": _n(mapped.get("matched_request")),
        "impression": _n(mapped.get("impression")),
        "click": _n(mapped.get("click")),
        "ad_request_ecpm": _n(mapped.get("ad_request_ecpm")),
        "ad_impression_ecpm": _n(mapped.get("ad_impression_ecpm")),
        "ctr": _n(mapped.get("ctr")),
        "coverage": _n(mapped.get("coverage")),
        "viewability": _n(mapped.get("viewability")),
        "net_revenue": _n(mapped.get("net_revenue")),
    }
    for opt in (
        "empower_pageview",
        "empower_unique_visitor",
        "pageview_ecpm",
        "unique_visitor_ecpm",
        "above_the_fold_ratio",
    ):
        if opt in mapped and mapped.get(opt) not in (None, ""):
            extras[_slug_metric_key(opt)] = _n(mapped.get(opt))
    row["extra_metrics"] = json.dumps(extras, ensure_ascii=False)
    return row


def _row_from_values(
    values: tuple[Any, ...],
    col_map: dict[str, int],
    extras_idx: list[tuple[str, int]],
    *,
    source_file: str,
    channel: str,
) -> dict[str, Any] | None:
    mapped: dict[str, Any] = {}
    extra_metrics: dict[str, Any] = {}
    for field, idx in col_map.items():
        if idx < len(values):
            mapped[field] = values[idx]
    if "month" in col_map and col_map["month"] < len(values):
        mapped["month_key"] = _month_from_serial(values[col_map["month"]])
    for label, idx in extras_idx:
        if idx < len(values):
            key = _slug_metric_key(label)
            val = values[idx]
            extra_metrics[key] = _n(val) if isinstance(val, (int, float, str)) else val
    ad_unit = (mapped.get("ad_unit") or "").strip()
    surface = _detect_surface(ad_unit, channel)
    platform = _platform_from_surface(surface, channel)
    return _dict_from_mapped(
        mapped,
        source_file,
        channel=channel,
        platform=platform,
        surface=surface,
        extra_metrics=extra_metrics,
    )


def iter_xlsx_rows(data: bytes, *, filename: str = "upload.xlsx") -> Iterator[dict[str, Any]]:
    wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    try:
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        header_row = next(rows_iter, None)
        if not header_row:
            return
        col_map, extras_idx = _map_header_row(list(header_row))
        if "ad_unit" not in col_map or "income_type" not in col_map:
            return
        channel = _detect_channel(filename)
        for row in rows_iter:
            if not row:
                continue
            item = _row_from_values(row, col_map, extras_idx, source_file=filename, channel=channel)
            if item:
                yield item
    finally:
        wb.close()


def parse_xlsx_bytes(data: bytes, *, filename: str = "upload.xlsx") -> list[dict[str, Any]]:
    return list(iter_xlsx_rows(data, filename=filename))


def parse_csv_text(text: str, *, filename: str = "upload.csv") -> list[dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return []
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    if len(lines) < 2:
        return []
    delim = ","
    if lines[0].count(";") > lines[0].count(","):
        delim = ";"
    reader = csv.reader(lines, delimiter=delim)
    header_row = next(reader, None)
    if not header_row:
        return []
    col_map, extras_idx = _map_header_row(header_row)
    if "ad_unit" not in col_map or "income_type" not in col_map:
        return []
    channel = _detect_channel(filename)
    out: list[dict[str, Any]] = []
    for cols in reader:
        item = _row_from_values(tuple(cols), col_map, extras_idx, source_file=filename, channel=channel)
        if item:
            out.append(item)
    return out


def _upsert_catalog(db: Session, filename: str, channel: str, columns: list[str], row_count: int) -> None:
    payload = json.dumps(columns, ensure_ascii=False)
    row = db.execute(
        select(AdReportCatalog).where(AdReportCatalog.source_file == filename)
    ).scalars().first()
    if row:
        row.channel = channel
        row.columns_json = payload
        row.row_count = row_count
        row.imported_at = datetime.utcnow()
    else:
        db.add(
            AdReportCatalog(
                source_file=filename,
                channel=channel,
                columns_json=payload,
                row_count=row_count,
            )
        )


def _flush_batch(db: Session, batch: list[dict[str, Any]]) -> tuple[int, int]:
    if not batch:
        return 0, 0
    if _IS_PG:
        stmt = pg_insert(AdReportRow).values(batch)
        stmt = stmt.on_conflict_do_nothing(index_elements=["fingerprint"])
        res = db.execute(stmt)
        rc = res.rowcount
        inserted = len(batch) if rc is None or rc < 0 else int(rc)
        skipped = max(0, len(batch) - inserted)
        db.flush()
        return inserted, skipped
    fps = [r["fingerprint"] for r in batch]
    existing = {
        r[0]
        for r in db.execute(
            select(AdReportRow.fingerprint).where(AdReportRow.fingerprint.in_(fps))
        ).all()
    }
    inserted = 0
    skipped = 0
    to_add: list[AdReportRow] = []
    for r in batch:
        if r["fingerprint"] in existing:
            skipped += 1
            continue
        to_add.append(AdReportRow(**r))
        existing.add(r["fingerprint"])
    if to_add:
        db.add_all(to_add)
        db.flush()
        inserted = len(to_add)
    return inserted, skipped


def import_rows(db: Session, rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    inserted = 0
    skipped = 0
    parsed = 0
    batch: list[dict[str, Any]] = []
    for r in rows:
        parsed += 1
        batch.append(r)
        if len(batch) >= 800:
            ins, sk = _flush_batch(db, batch)
            inserted += ins
            skipped += sk
            batch.clear()
    if batch:
        ins, sk = _flush_batch(db, batch)
        inserted += ins
        skipped += sk
    db.commit()
    return {
        "inserted": inserted,
        "skipped": skipped,
        "parsed": parsed,
        "total": count_rows(db),
    }


def import_upload_file(db: Session, data: bytes, *, filename: str) -> dict[str, Any]:
    low = filename.lower()
    channel = _detect_channel(filename)
    if low.endswith(".xlsx") or low.endswith(".xlsm"):
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        try:
            header_row = next(wb.active.iter_rows(values_only=True), None)
            columns = [str(h or "").strip() for h in (header_row or []) if str(h or "").strip()]
        finally:
            wb.close()
        result = import_rows(db, iter_xlsx_rows(data, filename=filename))
    elif low.endswith(".csv") or low.endswith(".txt"):
        text = data.decode("utf-8", errors="replace")
        lines = [ln for ln in text.splitlines() if ln.strip()]
        columns = []
        if lines:
            delim = ";" if lines[0].count(";") > lines[0].count(",") else ","
            columns = [c.strip() for c in lines[0].split(delim)]
        result = import_rows(db, parse_csv_text(text, filename=filename))
    else:
        raise ValueError("Yalnızca .xlsx veya .csv desteklenir")
    _upsert_catalog(db, filename, channel, columns, result.get("parsed", 0))
    db.commit()
    result["filename"] = filename
    result["channel"] = channel
    result["columns"] = columns
    return result


def count_rows(db: Session) -> int:
    return int(db.scalar(select(func.count()).select_from(AdReportRow)) or 0)


def reset_all(db: Session) -> dict[str, int]:
    db.execute(delete(AdReportRow))
    db.execute(delete(AdReportCatalog))
    db.commit()
    return {"total": 0}


def date_bounds(db: Session) -> dict[str, str | None]:
    row = db.execute(
        select(
            func.min(AdReportRow.report_date),
            func.max(AdReportRow.report_date),
        )
    ).one()
    dmin, dmax = row[0], row[1]
    return {
        "min_date": dmin.isoformat() if dmin else None,
        "max_date": dmax.isoformat() if dmax else None,
    }


def facets(db: Session) -> dict[str, Any]:
    income = [
        r[0]
        for r in db.execute(
            select(AdReportRow.income_type).distinct().order_by(AdReportRow.income_type)
        ).all()
        if r[0]
    ]
    platforms = [
        r[0]
        for r in db.execute(
            select(AdReportRow.platform).distinct().order_by(AdReportRow.platform)
        ).all()
        if r[0]
    ]
    sources = [
        r[0]
        for r in db.execute(
            select(AdReportRow.source_file).distinct().order_by(AdReportRow.source_file.desc())
        ).all()
        if r[0]
    ]
    channels = [
        r[0]
        for r in db.execute(
            select(AdReportRow.channel).distinct().order_by(AdReportRow.channel)
        ).all()
        if r[0]
    ]
    surfaces = [
        r[0]
        for r in db.execute(
            select(AdReportRow.surface).distinct().order_by(AdReportRow.surface)
        ).all()
        if r[0]
    ]
    catalogs = db.execute(select(AdReportCatalog)).scalars().all()
    extra_columns: list[str] = []
    seen_cols: set[str] = set()
    for cat in catalogs:
        try:
            cols = json.loads(cat.columns_json or "[]")
        except json.JSONDecodeError:
            cols = []
        for c in cols:
            norm = _normalize_header(str(c))
            if norm in HEADERS_MAP or _resolve_field(norm, str(c)):
                continue
            slug = _slug_metric_key(str(c))
            if slug not in seen_cols:
                seen_cols.add(slug)
                extra_columns.append(str(c))
    return {
        "income_types": income,
        "platforms": platforms,
        "channels": channels,
        "surfaces": surfaces,
        "source_files": sources,
        "extra_columns": extra_columns,
        "imports": [
            {
                "source_file": c.source_file,
                "channel": c.channel,
                "row_count": c.row_count,
                "columns": json.loads(c.columns_json or "[]"),
            }
            for c in catalogs
        ],
        **date_bounds(db),
        "total_rows": count_rows(db),
    }


def _parse_filter_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _apply_filters(
    q,
    *,
    start: str | None,
    end: str | None,
    income_types: list[str],
    ad_units: list[str],
    platforms: list[str],
    channels: list[str],
    surfaces: list[str],
    sources: list[str],
    search: str | None,
):
    if start:
        try:
            q = q.where(AdReportRow.report_date >= date.fromisoformat(start[:10]))
        except ValueError:
            pass
    if end:
        try:
            q = q.where(AdReportRow.report_date <= date.fromisoformat(end[:10]))
        except ValueError:
            pass
    if income_types:
        q = q.where(AdReportRow.income_type.in_(income_types))
    if ad_units:
        q = q.where(AdReportRow.ad_unit.in_(ad_units))
    if platforms:
        q = q.where(AdReportRow.platform.in_(platforms))
    if channels:
        q = q.where(AdReportRow.channel.in_(channels))
    if surfaces:
        q = q.where(AdReportRow.surface.in_(surfaces))
    if sources:
        q = q.where(AdReportRow.source_file.in_(sources))
    if search:
        q = q.where(AdReportRow.ad_unit.ilike(f"%{search}%"))
    return q


def query_summary(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
    income_types: str | None = None,
    ad_units: str | None = None,
    platforms: str | None = None,
    channels: str | None = None,
    surfaces: str | None = None,
    sources: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    it = _parse_filter_list(income_types)
    au = _parse_filter_list(ad_units)
    pl = _parse_filter_list(platforms)
    ch = _parse_filter_list(channels)
    su = _parse_filter_list(surfaces)
    sf = _parse_filter_list(sources)

    base = select(AdReportRow)
    base = _apply_filters(
        base,
        start=start,
        end=end,
        income_types=it,
        ad_units=au,
        platforms=pl,
        channels=ch,
        surfaces=su,
        sources=sf,
        search=search,
    )
    sub = base.subquery()

    totals = db.execute(
        select(
            func.coalesce(func.sum(sub.c.net_revenue), 0),
            func.coalesce(func.sum(sub.c.impression), 0),
            func.coalesce(func.sum(sub.c.click), 0),
            func.coalesce(func.sum(sub.c.ad_request), 0),
        )
    ).one()
    net_rev, impr, clicks, ad_req = [float(x or 0) for x in totals]
    avg_ecpm = (net_rev / impr * 1000.0) if impr > 0 else 0.0
    ctr = (clicks / impr * 100.0) if impr > 0 else 0.0

    by_date = db.execute(
        select(
            sub.c.report_date,
            func.sum(sub.c.net_revenue),
            func.sum(sub.c.impression),
            func.sum(sub.c.click),
        )
        .group_by(sub.c.report_date)
        .order_by(sub.c.report_date)
    ).all()

    by_income = db.execute(
        select(
            sub.c.income_type,
            func.sum(sub.c.net_revenue),
            func.sum(sub.c.impression),
        )
        .group_by(sub.c.income_type)
        .order_by(func.sum(sub.c.net_revenue).desc())
    ).all()

    by_unit = db.execute(
        select(
            sub.c.ad_unit,
            func.sum(sub.c.net_revenue),
            func.sum(sub.c.impression),
        )
        .group_by(sub.c.ad_unit)
        .order_by(func.sum(sub.c.net_revenue).desc())
        .limit(25)
    ).all()

    by_month = db.execute(
        select(
            sub.c.month_key,
            func.sum(sub.c.net_revenue),
            func.sum(sub.c.impression),
        )
        .group_by(sub.c.month_key)
        .order_by(sub.c.month_key)
    ).all()

    by_surface = db.execute(
        select(
            sub.c.surface,
            func.sum(sub.c.net_revenue),
            func.sum(sub.c.impression),
        )
        .group_by(sub.c.surface)
        .order_by(func.sum(sub.c.net_revenue).desc())
    ).all()

    by_channel = db.execute(
        select(
            sub.c.channel,
            func.sum(sub.c.net_revenue),
            func.sum(sub.c.impression),
        )
        .group_by(sub.c.channel)
        .order_by(func.sum(sub.c.net_revenue).desc())
    ).all()

    return {
        "kpis": {
            "net_revenue": round(net_rev, 2),
            "impressions": int(impr),
            "clicks": int(clicks),
            "ad_requests": int(ad_req),
            "avg_ad_ecpm": round(avg_ecpm, 3),
            "ctr_pct": round(ctr, 3),
        },
        "by_date": [
            {
                "date": r[0].isoformat(),
                "net_revenue": round(float(r[1] or 0), 2),
                "impression": int(r[2] or 0),
                "click": int(r[3] or 0),
            }
            for r in by_date
        ],
        "by_income_type": [
            {
                "income_type": r[0],
                "net_revenue": round(float(r[1] or 0), 2),
                "impression": int(r[2] or 0),
            }
            for r in by_income
        ],
        "by_ad_unit": [
            {
                "ad_unit": r[0],
                "net_revenue": round(float(r[1] or 0), 2),
                "impression": int(r[2] or 0),
            }
            for r in by_unit
        ],
        "by_month": [
            {
                "month": r[0],
                "net_revenue": round(float(r[1] or 0), 2),
                "impression": int(r[2] or 0),
            }
            for r in by_month
        ],
        "by_surface": [
            {
                "surface": r[0],
                "net_revenue": round(float(r[1] or 0), 2),
                "impression": int(r[2] or 0),
            }
            for r in by_surface
        ],
        "by_channel": [
            {
                "channel": r[0],
                "net_revenue": round(float(r[1] or 0), 2),
                "impression": int(r[2] or 0),
            }
            for r in by_channel
        ],
    }


def query_table(
    db: Session,
    *,
    start: str | None = None,
    end: str | None = None,
    income_types: str | None = None,
    ad_units: str | None = None,
    platforms: str | None = None,
    channels: str | None = None,
    surfaces: str | None = None,
    sources: str | None = None,
    search: str | None = None,
    breakdown: str | "date,month,ad_unit,income_type",
    limit: int = 500,
    offset: int = 0,
) -> dict[str, Any]:
    it = _parse_filter_list(income_types)
    au = _parse_filter_list(ad_units)
    pl = _parse_filter_list(platforms)
    ch = _parse_filter_list(channels)
    su = _parse_filter_list(surfaces)
    sf = _parse_filter_list(sources)
    parts = [p.strip() for p in (breakdown or "").split(",") if p.strip()]
    allowed = {"date", "month", "ad_unit", "income_type", "platform", "channel", "surface"}
    group_cols = [p for p in parts if p in allowed] or ["date", "ad_unit", "income_type"]

    sub = select(AdReportRow)
    sub = _apply_filters(
        sub,
        start=start,
        end=end,
        income_types=it,
        ad_units=au,
        platforms=pl,
        channels=ch,
        surfaces=su,
        sources=sf,
        search=search,
    )
    sub = sub.subquery()

    select_cols = []
    group_by_cols = []
    if "date" in group_cols:
        select_cols.append(sub.c.report_date.label("report_date"))
        group_by_cols.append(sub.c.report_date)
    if "month" in group_cols:
        select_cols.append(sub.c.month_key.label("month_key"))
        group_by_cols.append(sub.c.month_key)
    if "ad_unit" in group_cols:
        select_cols.append(sub.c.ad_unit.label("ad_unit"))
        group_by_cols.append(sub.c.ad_unit)
    if "income_type" in group_cols:
        select_cols.append(sub.c.income_type.label("income_type"))
        group_by_cols.append(sub.c.income_type)
    if "platform" in group_cols:
        select_cols.append(sub.c.platform.label("platform"))
        group_by_cols.append(sub.c.platform)
    if "channel" in group_cols:
        select_cols.append(sub.c.channel.label("channel"))
        group_by_cols.append(sub.c.channel)
    if "surface" in group_cols:
        select_cols.append(sub.c.surface.label("surface"))
        group_by_cols.append(sub.c.surface)

    metrics = [
        func.sum(sub.c.ad_request).label("ad_request"),
        func.sum(sub.c.matched_request).label("matched_request"),
        func.sum(sub.c.impression).label("impression"),
        func.sum(sub.c.click).label("click"),
        func.sum(sub.c.net_revenue).label("net_revenue"),
        func.avg(sub.c.ad_impression_ecpm).label("ad_impression_ecpm"),
        func.avg(sub.c.ctr).label("ctr"),
        func.avg(sub.c.coverage).label("coverage"),
        func.avg(sub.c.viewability).label("viewability"),
    ]
    q = select(*select_cols, *metrics).group_by(*group_by_cols)
    if "report_date" in [c.name for c in group_by_cols]:
        q = q.order_by(sub.c.report_date.desc())
    else:
        q = q.order_by(func.sum(sub.c.net_revenue).desc())

    grouped = q.subquery()
    total = int(db.scalar(select(func.count()).select_from(grouped)) or 0)
    rows = db.execute(
        select(grouped).limit(min(limit, 2000)).offset(max(offset, 0))
    ).all()

    out_rows = []
    for r in rows:
        m = r._mapping if hasattr(r, "_mapping") else dict(zip(grouped.c.keys(), r))
        item: dict[str, Any] = {}
        if "report_date" in m and m["report_date"] is not None:
            item["date"] = m["report_date"].isoformat()
        if "month_key" in m:
            item["month"] = m["month_key"]
        if "ad_unit" in m:
            item["ad_unit"] = m["ad_unit"]
        if "income_type" in m:
            item["income_type"] = m["income_type"]
        if "platform" in m:
            item["platform"] = m["platform"]
        if "channel" in m:
            item["channel"] = m["channel"]
        if "surface" in m:
            item["surface"] = m["surface"]
        impr = float(m["impression"] or 0)
        rev = float(m["net_revenue"] or 0)
        item.update({
            "ad_request": int(m["ad_request"] or 0),
            "matched_request": int(m["matched_request"] or 0),
            "impression": int(impr),
            "click": int(m["click"] or 0),
            "net_revenue": round(rev, 2),
            "ad_impression_ecpm": round(float(m["ad_impression_ecpm"] or 0), 3),
            "ctr": round(float(m["ctr"] or 0) * (100 if float(m["ctr"] or 0) <= 1 else 1), 3),
            "coverage": round(float(m["coverage"] or 0) * (100 if float(m["coverage"] or 0) <= 1 else 1), 2),
            "viewability": round(float(m["viewability"] or 0) * (100 if float(m["viewability"] or 0) <= 1 else 1), 2),
        })
        if impr > 0:
            item["computed_ecpm"] = round(rev / impr * 1000, 3)
        else:
            item["computed_ecpm"] = 0.0
        out_rows.append(item)

    return {"rows": out_rows, "total": int(total), "limit": limit, "offset": offset}
