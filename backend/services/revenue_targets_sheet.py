"""Google Sheets — aylık gelir hedef / kazanç tablosu (Döviz & Sinemalar)."""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from typing import Any

from backend.services.backlink_csv import fetch_public_sheet_csv
from backend.services.market_sheets_sync import _norm_header, _parse_tr_number

logger = logging.getLogger(__name__)

# Herkese açık yedek (şu an okunabilen).
REVENUE_TARGETS_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1ulWizYIfbdeUERkEwqEi70abtSkXJt7oYtHnn07OyuA/edit#gid=0"
)
# Güncel tablo — «Bağlantısı olan herkes → Görüntüleyici» olunca buradan okunur.
REVENUE_TARGETS_SHEET_URL_PENDING = (
    "https://docs.google.com/spreadsheets/d/11IWNTk3mjjX0N-4LO03wyeSPkLoW4jagc2Prcs9ifcY/edit?gid=0#gid=0"
)

_CACHE: dict[str, Any] | None = None
_CACHE_TTL_SEC = 900.0

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
    if "doviz" in low or "döviz" in name.lower():
        return "doviz", "Doviz.com"
    if "sinema" in low:
        return "sinemalar", "Sinemalar.com"
    return None


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
    period_key = f"{year:04d}-{mon:02d}"
    return s, year, mon, period_key


def parse_revenue_targets_csv(csv_text: str) -> list[dict[str, Any]]:
    """CSV satırlarını normalize edilmiş hedef kayıtlarına çevirir."""
    reader = csv.reader(io.StringIO(csv_text or ""))
    rows_in = list(reader)
    if not rows_in:
        return []

    out: list[dict[str, Any]] = []
    current_period: tuple[str, int, int, str] | None = None

    for i, row in enumerate(rows_in):
        if not row or len(row) < 2:
            continue
        cells = list(row) + [""] * (12 - len(row))
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


def fetch_revenue_targets_rows(*, force: bool = False) -> list[dict[str, Any]]:
    global _CACHE
    if not force:
        cached = _cache_rows()
        if cached is not None:
            return cached

    # Önce güncel sheet; kapalıysa (401) herkese açık yedek tabloya düş.
    urls = [REVENUE_TARGETS_SHEET_URL_PENDING, REVENUE_TARGETS_SHEET_URL]
    last_err: Exception | None = None
    csv_text = ""
    used_url = REVENUE_TARGETS_SHEET_URL
    pending_error: str | None = None
    for url in urls:
        try:
            csv_text = fetch_public_sheet_csv(url)
            used_url = url
            break
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            logger.warning("revenue targets sheet fetch failed url=%s: %s", url, exc)
            if url == REVENUE_TARGETS_SHEET_URL_PENDING:
                pending_error = str(exc)
    else:
        raise ValueError(str(last_err) if last_err else "Sheet okunamadı") from last_err

    rows = parse_revenue_targets_csv(csv_text)
    warning = None
    if used_url != REVENUE_TARGETS_SHEET_URL_PENDING and pending_error:
        warning = (
            "Güncel Google Sheet okunamadı (paylaşım kapalı). "
            "Yedek tablo gösteriliyor — Temmuz/Ağustos gibi yeni satırlar eksik olabilir. "
            "Sheet’te: Paylaş → Bağlantısı olan herkes → Görüntüleyici."
        )
    _CACHE = {
        "ts": time.monotonic(),
        "rows": rows,
        "source_url": used_url,
        "warning": warning,
        "pending_error": pending_error,
    }
    return rows


def revenue_targets_payload(
    *,
    project: str | None = None,
    year: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    all_rows = fetch_revenue_targets_rows(force=force)
    source_url = REVENUE_TARGETS_SHEET_URL
    warning = None
    if _CACHE:
        if _CACHE.get("source_url"):
            source_url = str(_CACHE["source_url"])
        warning = _CACHE.get("warning")
    rows = all_rows
    pk = (project or "").strip().lower()
    if pk in ("doviz", "sinemalar"):
        rows = [r for r in rows if r.get("project") == pk]
    if year is not None:
        rows = [r for r in rows if int(r.get("year") or 0) == int(year)]

    years = sorted({int(r["year"]) for r in all_rows if r.get("year")})
    return {
        "source_url": source_url,
        "source_pending_url": REVENUE_TARGETS_SHEET_URL_PENDING,
        "using_fallback": source_url != REVENUE_TARGETS_SHEET_URL_PENDING,
        "warning": warning,
        "rows": rows,
        "years": years,
        "projects": [
            {"key": "doviz", "label": "Doviz.com"},
            {"key": "sinemalar", "label": "Sinemalar.com"},
        ],
    }
