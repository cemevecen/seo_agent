"""Metrik kıyası — önceki eşit aralık ve geçen yılın aynı günleri.

Kıyas yalnızca depodaki tarih aralığı o pencereyi kapsıyorsa kurulur.
Eksik günler 0 sayılmaz; aralık yoksa payload available=False döner.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterable

COMPARE_MODES = frozenset({"previous_period", "previous_year"})
# Piyasa serileri hafta sonu kapanmaz; kenarda birkaç gün pay bırak.
DEFAULT_EDGE_SLACK_DAYS = 3

MISSING_NOT_IN_WAREHOUSE = "Karşılaştırma aralığı depoda yok"
MISSING_NO_POINTS = "İlgili dönem için karşılaştırma verisi bulunamadı"


def shift_years(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year + years)
    except ValueError:
        return day.replace(year=day.year + years, day=28)


def compare_bounds(start: date, end: date, mode: str | None) -> tuple[date, date] | None:
    key = (mode or "").strip()
    if start > end or key not in COMPARE_MODES:
        return None
    if key == "previous_period":
        span = (end - start).days + 1
        pe = start - timedelta(days=1)
        ps = pe - timedelta(days=span - 1)
        return ps, pe
    return shift_years(start, -1), shift_years(end, -1)


def parse_iso_dates(raw_dates: Iterable[Any]) -> list[date]:
    out: list[date] = []
    for raw in raw_dates:
        s = str(raw or "").strip()[:10]
        if len(s) != 10:
            continue
        try:
            out.append(date.fromisoformat(s))
        except ValueError:
            continue
    return out


def dates_cover_window(
    raw_dates: Iterable[Any],
    start: date,
    end: date,
    *,
    edge_slack_days: int = DEFAULT_EDGE_SLACK_DAYS,
) -> bool:
    """Pencere depoda var mı: ilk kayıt pencere başını, son kayıt bitişini kapsar."""
    if start > end:
        return False
    dates = parse_iso_dates(raw_dates)
    if not dates:
        return False
    lo, hi = min(dates), max(dates)
    slack = timedelta(days=max(0, edge_slack_days))
    if lo > start + slack or hi < end - slack:
        return False
    return any(start - slack <= d <= end + slack for d in dates)


def unavailable_payload(
    mode: str,
    start: date,
    end: date,
    *,
    reason: str = MISSING_NOT_IN_WAREHOUSE,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total": None,
        "delta_pct": None,
        "series": [],
        "available": False,
        "missing_reason": reason,
    }


def delta_pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    if not previous:
        return None
    return round((float(current) - float(previous)) / abs(float(previous)) * 100.0, 2)
