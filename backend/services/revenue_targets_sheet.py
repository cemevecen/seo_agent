"""Google Sheets — aylık gelir hedef / kazanç tablosu (Döviz & Sinemalar)."""

from __future__ import annotations

import calendar
import csv
import io
import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from backend.services.backlink_csv import fetch_public_sheet_csv
from backend.services.market_sheets_sync import _norm_header, _parse_tr_number

logger = logging.getLogger(__name__)
_TR = ZoneInfo("Europe/Istanbul")

# Ad target KPI kaynağı (gid=244461752 — satır 26/27 Doviz & Sinemalar).
REVENUE_TARGETS_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1ITl0rUlLylTspsztMtaaFGEdvT_gINoUHDPodspEa5Y/edit?gid=244461752#gid=244461752"
)
# Önceki herkese açık tablolar (erişim yoksa yedek).
REVENUE_TARGETS_SHEET_URL_FALLBACK = (
    "https://docs.google.com/spreadsheets/d/11IWNTk3mjjX0N-4LO03wyeSPkLoW4jagc2Prcs9ifcY/edit?gid=0#gid=0"
)
REVENUE_TARGETS_SHEET_URL_FALLBACK_2 = (
    "https://docs.google.com/spreadsheets/d/1ulWizYIfbdeUERkEwqEi70abtSkXJt7oYtHnn07OyuA/edit#gid=0"
)
REVENUE_TARGETS_SHEET_URL_PENDING = REVENUE_TARGETS_SHEET_URL

_CACHE: dict[str, Any] | None = None
_CACHE_TTL_SEC = 900.0

# Mac Firefox scrape ingest (Railway disk)
_INGEST_PATH = Path(__file__).resolve().parents[2] / "data" / "revenue_targets_ingest.json"
_INGEST_MAX_AGE_SEC = 36 * 3600.0

_TR_MONTHS: dict[str, int] = {
    "ocak": 1,
    "subat": 2,
    "şubat": 2,
    "mart": 3,
    "nisan": 4,
    "mayis": 5,
    "mayıs": 5,
    "haziran": 6,
    "temmuz": 7,
    "agustos": 8,
    "ağustos": 8,
    "eylul": 9,
    "eylül": 9,
    "ekim": 10,
    "kasim": 11,
    "kasım": 11,
    "aralik": 12,
    "aralık": 12,
}


def _today_tr() -> date:
    return datetime.now(_TR).date()


def _parse_pct(raw: str | None) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace("%", "").strip()
    if not s or s in ("-", "—"):
        return None
    return _parse_tr_number(s)


def _parse_tr_money(raw: str | None) -> float | None:
    """TR binlik ayırıcı (550.000) ve ondalık (12,5) formatları."""
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'")
    if not s or s in ("-", "—"):
        return None
    s = s.replace("\u00a0", "").replace(" ", "")
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        return float(s.replace(".", ""))
    if "," in s and "." in s:
        return _parse_tr_number(s)
    if "," in s:
        return _parse_tr_number(s)
    if "." in s:
        whole, frac = s.rsplit(".", 1)
        if frac.isdigit() and len(frac) == 3 and whole.replace(".", "").isdigit():
            return float(s.replace(".", ""))
    return _parse_tr_number(s)


def _normalize_project(raw: str | None) -> tuple[str, str] | None:
    name = str(raw or "").strip()
    if not name:
        return None
    low = _norm_header(name)
    # Canlidoviz vb. eşleşmesin
    if "sinema" in low:
        return "sinemalar", "Sinemalar.com"
    if low in ("doviz.com", "doviz", "www.doviz.com") or low.startswith("doviz.com"):
        return "doviz", "Doviz.com"
    return None


_TR_MONTH_LABELS: dict[int, str] = {
    1: "Ocak",
    2: "Şubat",
    3: "Mart",
    4: "Nisan",
    5: "Mayıs",
    6: "Haziran",
    7: "Temmuz",
    8: "Ağustos",
    9: "Eylül",
    10: "Ekim",
    11: "Kasım",
    12: "Aralık",
}

# Eski hedef paneli zaman aralığı (çok aylık grafik/liste) — Şubat 2023+
REVENUE_TARGETS_HISTORY_FROM = "2023-02"
# Biten ayı sheet'ten panoya yazma: her ayın 1 ve 2'si (TR)
REVENUE_TARGETS_CLOSED_MONTH_DAYS = frozenset({1, 2})


def previous_month_period_key(today: date | None = None) -> str:
    """Takvimde bir önceki ay (TR). Örn. Ağustos → 2026-07 değil; Eylül'de → 2026-08."""
    today = today or _today_tr()
    if today.month == 1:
        return f"{today.year - 1:04d}-12"
    return f"{today.year:04d}-{today.month - 1:02d}"


def is_closed_month_sync_day(today: date | None = None) -> bool:
    today = today or _today_tr()
    return int(today.day) in REVENUE_TARGETS_CLOSED_MONTH_DAYS


def current_month_period_key(today: date | None = None) -> str:
    today = today or _today_tr()
    return f"{today.year:04d}-{today.month:02d}"


def _period_tuple(year: int, month: int) -> tuple[str, int, int, str]:
    label = f"{_TR_MONTH_LABELS.get(month, str(month))} {year}"
    return label, year, month, f"{year:04d}-{month:02d}"


def _parse_period_cell(raw: str | None) -> tuple[str, int, int, str] | None:
    s = str(raw or "").strip()
    if not s:
        return None
    parts = s.split()
    if len(parts) < 2:
        return None
    year_s = parts[-1]
    if not re.match(r"^\d{4}$", year_s):
        return None
    year = int(year_s)
    month_name = " ".join(parts[:-1]).strip()
    mon = _TR_MONTHS.get(_norm_header(month_name))
    if not mon:
        return None
    return _period_tuple(year, mon)


def parse_sheet_tab_period(name: str | None) -> tuple[str, int, int, str] | None:
    """Sekme adı → dönem. Örn. «Ağustos'26», «Temmuz'26», «Şubat 2023»."""
    raw = str(name or "").strip()
    if not raw:
        return None
    low = _norm_header(raw)
    if low in ("site_settings", "settings") or "ordino" in low:
        return None
    # «Ağustos 2026»
    direct = _parse_period_cell(raw)
    if direct:
        return direct
    # «Ağustos'26» / «Mayıs’26»
    m = re.match(
        r"^(?P<mon>.+?)(?:['\u2019\u2032]\s*|\s+)(?P<y>\d{2}|\d{4})\s*$",
        raw,
    )
    if not m:
        return None
    mon = _TR_MONTHS.get(_norm_header(m.group("mon")))
    if not mon:
        return None
    y_raw = m.group("y")
    year = int(y_raw) if len(y_raw) == 4 else 2000 + int(y_raw)
    if year < 2000 or year > 2100:
        return None
    return _period_tuple(year, mon)


def parse_revenue_targets_csv(
    csv_text: str,
    *,
    period_hint: str | None = None,
) -> list[dict[str, Any]]:
    """CSV satırlarını normalize edilmiş hedef kayıtlarına çevirir.

    İki düzen:
      A) Eski: dönem | proje | hedef | …
      B) MCM aylık sekme: satır0 = «Ağustos 2026,Hedef,…» veya «,Hedef,…» + period_hint
    """
    reader = csv.reader(io.StringIO(csv_text or ""))
    rows_in = list(reader)
    if not rows_in:
        return []

    # MCM başlık: ikinci hücre "Hedef"; dönem col0 veya sekme adından
    mcm = False
    header_period: tuple[str, int, int, str] | None = None
    if rows_in:
        h0 = list(rows_in[0]) + [""] * 4
        if _norm_header(h0[1]) in ("hedef", "target"):
            mcm = True
            header_period = _parse_period_cell(h0[0])
            if header_period is None and period_hint:
                header_period = parse_sheet_tab_period(period_hint) or _parse_period_cell(
                    period_hint
                )

    out: list[dict[str, Any]] = []
    current_period: tuple[str, int, int, str] | None = header_period if mcm else None

    for i, row in enumerate(rows_in):
        if not row or len(row) < 2:
            continue
        cells = list(row) + [""] * (12 - len(row))

        if mcm:
            if i == 0:
                continue
            # TOPLAM / network satırları — sadece doviz/sinemalar
            proj = _normalize_project(cells[0])
            if not proj or current_period is None:
                continue
            project_key, project_label = proj
            period_label, year, month, period_key = current_period
            hedef = _parse_tr_money(cells[1])
            hedef_80 = _parse_tr_money(cells[2])
            kazanc = _parse_tr_money(cells[3])
            if hedef is None and kazanc is None:
                continue
            out.append(
                {
                    "period": period_label,
                    "period_key": period_key,
                    "year": year,
                    "month": month,
                    "project": project_key,
                    "project_label": project_label,
                    "hedef": hedef,
                    "hedef_80": hedef_80,
                    "kazanc": kazanc,
                    "tamamlama_orani": _parse_pct(cells[4]),
                    "gunluk_kazanc": _parse_tr_money(cells[5]),
                    "kalan": _parse_tr_money(cells[6]),
                    "kalan_80": _parse_tr_money(cells[7]),
                    "gunluk_kalan": _parse_tr_money(cells[8]),
                    "gunluk_kalan_80": _parse_tr_money(cells[9]),
                    "sheet_row": i + 1,
                }
            )
            continue

        if i == 0 and _norm_header(cells[1]) == "proje":
            continue

        period_cell = str(cells[0] or "").strip()
        if period_cell:
            parsed = _parse_period_cell(period_cell)
            if parsed:
                current_period = parsed

        proj = _normalize_project(cells[1])
        if not proj or current_period is None:
            continue

        project_key, project_label = proj
        period_label, year, month, period_key = current_period
        hedef = _parse_tr_money(cells[2])
        hedef_80 = _parse_tr_money(cells[3])
        kazanc = _parse_tr_money(cells[4])
        if hedef is None and kazanc is None:
            continue

        out.append(
            {
                "period": period_label,
                "period_key": period_key,
                "year": year,
                "month": month,
                "project": project_key,
                "project_label": project_label,
                "hedef": hedef,
                "hedef_80": hedef_80,
                "kazanc": kazanc,
                "tamamlama_orani": _parse_pct(cells[5]),
                "gunluk_kazanc": _parse_tr_money(cells[6]),
                "kalan": _parse_tr_money(cells[7]),
                "sheet_row": i + 1,
            }
        )

    out.sort(key=lambda r: (r.get("period_key") or "", r.get("project") or ""))
    return out


def _cache_rows() -> list[dict[str, Any]] | None:
    if not _CACHE:
        return None
    if (time.monotonic() - float(_CACHE.get("ts") or 0)) >= _CACHE_TTL_SEC:
        return None
    rows = _CACHE.get("rows")
    return rows if isinstance(rows, list) else None


def load_ingested_revenue_targets(
    *,
    max_age_sec: float = _INGEST_MAX_AGE_SEC,
) -> dict[str, Any] | None:
    """Mac Firefox scrape ingest — önce Postgres, sonra lokal dosya."""
    data: dict[str, Any] | None = None

    try:
        from backend.database import SessionLocal
        from backend.models import RevenueTargetsCache

        with SessionLocal() as db:
            row = db.get(RevenueTargetsCache, "current")
            if row and row.payload_json:
                parsed = json.loads(row.payload_json)
                if isinstance(parsed, dict):
                    data = parsed
    except Exception as exc:  # noqa: BLE001
        logger.warning("revenue targets postgres cache read failed: %s", exc)

    if data is None and _INGEST_PATH.is_file():
        try:
            parsed = json.loads(_INGEST_PATH.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = None

    if not data:
        return None
    fetched_at = str(data.get("fetched_at") or "")
    try:
        ts = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=_TR)
        age = (datetime.now(tz=ts.tzinfo) - ts).total_seconds()
        if age > max_age_sec:
            return None
    except Exception:
        pass
    rows = data.get("rows")
    if not isinstance(rows, list) or not rows:
        return None
    return data


def save_ingested_revenue_targets(
    csv_text: str | None = None,
    *,
    rows: list[dict[str, Any]] | None = None,
    source: str = "mac_firefox_selenium",
    source_url: str | None = None,
) -> dict[str, Any]:
    """CSV ve/veya hazır satır listesini cache’e yazar (çok aylık MCM birleşimi)."""
    parsed_rows: list[dict[str, Any]] = list(rows or [])
    if not parsed_rows and csv_text:
        parsed_rows = parse_revenue_targets_csv(csv_text)
    if not parsed_rows:
        raise ValueError("No Doviz/Sinemalar rows to ingest")

    # Aynı period+project için son gelen kazanır
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for r in parsed_rows:
        pk = str(r.get("period_key") or "")
        proj = str(r.get("project") or "")
        if not pk or proj not in ("doviz", "sinemalar"):
            continue
        by_key[(pk, proj)] = r
    merged = sorted(by_key.values(), key=lambda r: (r.get("period_key") or "", r.get("project") or ""))

    url = source_url or REVENUE_TARGETS_SHEET_URL
    payload = {
        "fetched_at": datetime.now(_TR).isoformat(),
        "source": source,
        "source_url": url,
        "row_count": len(merged),
        "rows": merged,
        "period_keys": sorted({str(r.get("period_key") or "") for r in merged if r.get("period_key")}),
        "csv": csv_text if csv_text and len(csv_text) < 500_000 else None,
    }
    payload_json = json.dumps(payload, ensure_ascii=False)

    # Lokal dosya (Mac / docker volume)
    try:
        _INGEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _INGEST_PATH.write_text(payload_json, encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("revenue targets file cache write failed: %s", exc)

    # Postgres — Railway deploy disk silinse de kalsın
    pg_ok = False
    try:
        from datetime import datetime as _dt

        from backend.database import SessionLocal
        from backend.models import RevenueTargetsCache

        with SessionLocal() as db:
            row = db.get(RevenueTargetsCache, "current")
            if row is None:
                row = RevenueTargetsCache(cache_key="current", payload_json=payload_json)
                db.add(row)
            else:
                row.payload_json = payload_json
                row.updated_at = _dt.utcnow()
            db.commit()
            pg_ok = True
    except Exception as exc:  # noqa: BLE001
        logger.warning("revenue targets postgres cache write failed: %s", exc)

    global _CACHE
    _CACHE = {
        "ts": time.monotonic(),
        "rows": merged,
        "source_url": url,
        "warning": None,
        "pending_error": None,
        "fetched_at": payload["fetched_at"],
        "ingest_source": source,
    }
    return {
        "ok": True,
        "rows": len(merged),
        "fetched_at": payload["fetched_at"],
        "path": str(_INGEST_PATH),
        "postgres": pg_ok,
        "period_keys": payload["period_keys"],
    }


def fetch_revenue_targets_rows(*, force: bool = False) -> list[dict[str, Any]]:
    global _CACHE
    if not force:
        cached = _cache_rows()
        if cached is not None:
            return cached

    ingested = load_ingested_revenue_targets()
    if ingested and isinstance(ingested.get("rows"), list):
        rows = ingested["rows"]
        _CACHE = {
            "ts": time.monotonic(),
            "rows": rows,
            "source_url": ingested.get("source_url") or REVENUE_TARGETS_SHEET_URL,
            "warning": None,
            "pending_error": None,
            "fetched_at": ingested.get("fetched_at"),
            "ingest_source": ingested.get("source"),
        }
        return rows

    urls = [
        REVENUE_TARGETS_SHEET_URL,
        REVENUE_TARGETS_SHEET_URL_FALLBACK,
        REVENUE_TARGETS_SHEET_URL_FALLBACK_2,
    ]
    last_err: Exception | None = None
    csv_text = ""
    used_url = REVENUE_TARGETS_SHEET_URL
    primary_error: str | None = None
    for url in urls:
        try:
            csv_text = fetch_public_sheet_csv(url)
            used_url = url
            break
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            logger.warning("revenue targets sheet fetch failed url=%s: %s", url, exc)
            if url == REVENUE_TARGETS_SHEET_URL:
                primary_error = str(exc)
    else:
        raise ValueError(str(last_err) if last_err else "Sheet okunamadı") from last_err

    rows = parse_revenue_targets_csv(csv_text)
    warning = None
    if used_url != REVENUE_TARGETS_SHEET_URL and primary_error:
        warning = (
            "Primary ad-target sheet is private or unreachable. "
            "Showing fallback table — Mac Firefox scrape "
            "(`revenue_targets_scrape.py --sync`) or share the sheet with the GA4 service account."
        )
    _CACHE = {
        "ts": time.monotonic(),
        "rows": rows,
        "source_url": used_url,
        "warning": warning,
        "pending_error": primary_error,
        "fetched_at": datetime.now(_TR).isoformat(),
    }
    return rows


def _completion_pct(row: dict[str, Any]) -> float | None:
    raw = row.get("tamamlama_orani")
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    hedef = row.get("hedef")
    kazanc = row.get("kazanc")
    try:
        h = float(hedef) if hedef is not None else 0.0
        k = float(kazanc) if kazanc is not None else None
    except (TypeError, ValueError):
        return None
    if h > 0 and k is not None:
        return (k / h) * 100.0
    return None


def enrich_month_target_kpi(
    row: dict[str, Any] | None,
    *,
    today: date | None = None,
) -> dict[str, Any] | None:
    """Aylık hedef satırından panel KPI alanları (EN etiketler UI’da)."""
    if not row:
        return None
    today = today or _today_tr()
    year = int(row.get("year") or 0)
    month = int(row.get("month") or 0)
    if year < 2000 or month < 1 or month > 12:
        return None

    days_in_month = calendar.monthrange(year, month)[1]
    in_month = today.year == year and today.month == month
    day_of_month = today.day if in_month else days_in_month
    days_elapsed = max(1, min(day_of_month, days_in_month))
    days_remaining = max(0, days_in_month - day_of_month + (1 if in_month else 0))
    if not in_month and today > date(year, month, days_in_month):
        days_remaining = 0
    if not in_month and today < date(year, month, 1):
        days_elapsed = 0
        days_remaining = days_in_month

    hedef = row.get("hedef")
    hedef_80 = row.get("hedef_80")
    kazanc = row.get("kazanc")
    try:
        h = float(hedef) if hedef is not None else None
    except (TypeError, ValueError):
        h = None
    try:
        h80 = float(hedef_80) if hedef_80 is not None else (h * 0.8 if h is not None else None)
    except (TypeError, ValueError):
        h80 = h * 0.8 if h is not None else None
    try:
        k = float(kazanc) if kazanc is not None else None
    except (TypeError, ValueError):
        k = None

    kalan = row.get("kalan")
    try:
        rem = float(kalan) if kalan is not None else None
    except (TypeError, ValueError):
        rem = None
    if rem is None and h is not None and k is not None:
        rem = max(0.0, h - k)

    kalan_80_raw = row.get("kalan_80")
    try:
        rem80 = float(kalan_80_raw) if kalan_80_raw is not None else None
    except (TypeError, ValueError):
        rem80 = None
    if rem80 is None and h80 is not None and k is not None:
        rem80 = max(0.0, h80 - k)

    gunluk = row.get("gunluk_kazanc")
    try:
        daily = float(gunluk) if gunluk is not None else None
    except (TypeError, ValueError):
        daily = None
    if daily is not None and k is not None and abs(daily - k) < 0.5:
        daily = None
    if daily is None and k is not None and days_elapsed > 0:
        daily = k / days_elapsed

    # Sheet «Günlük Kalan» / «Günlük Kalan (%80)» — yoksa hesapla
    gk = row.get("gunluk_kalan")
    gk80 = row.get("gunluk_kalan_80")
    try:
        needed_daily = float(gk) if gk is not None else None
    except (TypeError, ValueError):
        needed_daily = None
    try:
        needed_daily_80 = float(gk80) if gk80 is not None else None
    except (TypeError, ValueError):
        needed_daily_80 = None
    if needed_daily is None:
        if rem is not None and days_remaining > 0:
            needed_daily = rem / days_remaining
        elif rem is not None and days_remaining == 0:
            needed_daily = 0.0
    if needed_daily_80 is None:
        if rem80 is not None and days_remaining > 0:
            needed_daily_80 = rem80 / days_remaining
        elif rem80 is not None and days_remaining == 0:
            needed_daily_80 = 0.0

    pct100 = _completion_pct(row)
    pct80 = None
    if h80 and h80 > 0 and k is not None:
        pct80 = (k / h80) * 100.0
    remaining_pct_100 = (100.0 - pct100) if pct100 is not None else None
    remaining_pct_80 = (100.0 - pct80) if pct80 is not None else None

    return {
        "project": row.get("project"),
        "project_label": row.get("project_label"),
        "period": row.get("period"),
        "period_key": row.get("period_key"),
        "year": year,
        "month": month,
        "sheet_row": row.get("sheet_row"),
        "in_current_month": in_month,
        "days_in_month": days_in_month,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "target_100": h,
        "target_80": h80,
        "achieved": k,
        "remaining": rem,
        "remaining_80": rem80,
        "remaining_100": rem,
        "completion_pct_100": pct100,
        "completion_pct_80": pct80,
        "remaining_pct_100": remaining_pct_100,
        "remaining_pct_80": remaining_pct_80,
        "daily_avg": daily,
        "needed_daily": needed_daily,
        "needed_daily_80": needed_daily_80,
        "needed_daily_100": needed_daily,
    }


def current_month_target_rows(
    all_rows: list[dict[str, Any]],
    *,
    today: date | None = None,
) -> dict[str, dict[str, Any] | None]:
    """İçinde bulunulan ayın Doviz / Sinemalar satırları (önceki aya düşme yok).

    Fallback sheet Temmuz gösterirken Ağustos KPI’sı uydurulmasın.
    """
    today = today or _today_tr()
    want_key = f"{today.year:04d}-{today.month:02d}"
    by_proj: dict[str, dict[str, Any]] = {}
    for r in all_rows:
        pk = str(r.get("project") or "")
        if pk not in ("doviz", "sinemalar"):
            continue
        if str(r.get("period_key") or "") == want_key:
            by_proj[pk] = r
    return {
        "doviz": enrich_month_target_kpi(by_proj.get("doviz"), today=today),
        "sinemalar": enrich_month_target_kpi(by_proj.get("sinemalar"), today=today),
    }


def revenue_targets_payload(
    *,
    project: str | None = None,
    year: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    all_rows = fetch_revenue_targets_rows(force=force)
    source_url = REVENUE_TARGETS_SHEET_URL
    warning = None
    fetched_at = None
    if _CACHE:
        if _CACHE.get("source_url"):
            source_url = str(_CACHE["source_url"])
        warning = _CACHE.get("warning")
        fetched_at = _CACHE.get("fetched_at")
    rows = all_rows
    pk = (project or "").strip().lower()
    if pk in ("doviz", "sinemalar"):
        rows = [r for r in rows if r.get("project") == pk]
    if year is not None:
        rows = [r for r in rows if int(r.get("year") or 0) == int(year)]

    years = sorted({int(r["year"]) for r in all_rows if r.get("year")})
    current = current_month_target_rows(all_rows)
    if not warning and (current.get("doviz") is None and current.get("sinemalar") is None):
        want = f"{_today_tr().year:04d}-{_today_tr().month:02d}"
        warning = (
            f"No {want} rows in the loaded sheet (often an older fallback month). "
            "Run Mac Firefox scrape for the current-month tab."
        )
    return {
        "source_url": source_url,
        "source_pending_url": REVENUE_TARGETS_SHEET_URL,
        "using_fallback": source_url != REVENUE_TARGETS_SHEET_URL,
        "warning": warning,
        "fetched_at": fetched_at,
        "ingest_source": (_CACHE or {}).get("ingest_source"),
        "rows": rows,
        "years": years,
        "current_month": current,
        "projects": [
            {"key": "doviz", "label": "Doviz.com"},
            {"key": "sinemalar", "label": "Sinemalar.com"},
        ],
    }


def prefetch_revenue_targets(*, force: bool = True) -> dict[str, Any]:
    """05:00 / 13:00 cron — sheet cache yenile."""
    try:
        payload = revenue_targets_payload(force=force)
        return {
            "ok": True,
            "rows": len(payload.get("rows") or []),
            "source_url": payload.get("source_url"),
            "using_fallback": payload.get("using_fallback"),
            "warning": payload.get("warning"),
            "fetched_at": payload.get("fetched_at"),
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("revenue targets prefetch failed: %s", exc)
        return {"ok": False, "error": str(exc)[:240]}
