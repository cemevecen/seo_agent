"""Konsol scrape kapsaması — «elimde hangi günler var?».

`history_seal.scheduled_fetch_window` boşluk doldurma modunu (`gap_fill`) zaten
destekliyor, ama çalışması için `known_dates` verilmesi gerekiyor ve bugüne
kadar bunu kimse geçmiyordu. Sonuç: köprü bir gün çalışmazsa o gün kalıcı
boşluk olarak kalıyordu — ertesi gün yalnızca yeni dün çekiliyor, atlanan gün
geri gelmiyordu.

Bu modül panelin elindeki günleri çıkarır; Mac'teki scraper scrape öncesi bunu
sorup `known_dates` olarak geçince `gap_fill` devreye girer.

ASC ve Firebase verisi tek satırda JSON blob olarak duruyor ve iki farklı şekle
sahip (ASC: `panels.explorer_facts`, Firebase: platform blokları içinde
`series` listeleri). Bu yüzden tarih toplama şekle bağlı değil: yapı özyinelemeli
gezilir ve sözlüklerdeki tarih alanları toplanır. Böylece şekil değişirse
kapsama sessizce boşalmaz.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

# Tarih taşıyan alan adları — iki store'da da bunlar kullanılıyor
DATE_KEYS = ("date", "day", "report_date", "date_iso")
_MAX_DEPTH = 8


def _parse_iso(raw: Any) -> date | None:
    if isinstance(raw, date):
        return raw
    s = str(raw or "").strip()[:10]
    if len(s) != 10:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _collect_dates(node: Any, out: dict[date, int], depth: int = 0) -> None:
    """Yapıyı gez, sözlüklerdeki tarih alanlarını say."""
    if depth > _MAX_DEPTH:
        return
    if isinstance(node, dict):
        hit = None
        for key in DATE_KEYS:
            if key in node:
                hit = _parse_iso(node.get(key))
                if hit:
                    break
        if hit:
            out[hit] = out.get(hit, 0) + 1
        for value in node.values():
            if isinstance(value, (dict, list)):
                _collect_dates(value, out, depth + 1)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, (dict, list)):
                _collect_dates(item, out, depth + 1)


def _load_blob(raw: str | None) -> Any:
    try:
        return json.loads(raw or "") if raw else None
    except Exception:  # noqa: BLE001
        return None


def missing_between(known: set[date], start: date, end: date) -> list[date]:
    """[start, end] aralığında elde olmayan günler."""
    if start > end:
        return []
    out: list[date] = []
    cur = start
    while cur <= end:
        if cur not in known:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def _coverage_payload(
    pipeline: str,
    counts: dict[date, int],
    *,
    start: str | None,
    end: str | None,
) -> dict[str, Any]:
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    known = set(counts)
    yday = calendar_yesterday()
    seal = pipeline_seal_through(pipeline)

    # Varsayılan pencere: mühürden sonraki ilk günden düne kadar — gap_fill'in
    # baktığı aralığın aynısı, böylece panel ile scraper aynı şeyi konuşur.
    win_start = _parse_iso(start) or (seal + timedelta(days=1))
    win_end = _parse_iso(end) or yday
    if win_start > win_end:
        win_start = win_end

    missing = missing_between(known, win_start, win_end)
    return {
        "ok": True,
        "pipeline": pipeline,
        "start": win_start.isoformat(),
        "end": win_end.isoformat(),
        "yesterday": yday.isoformat(),
        "seal_through": seal.isoformat(),
        "known_count": len(known),
        "dates": sorted(d.isoformat() for d in known),
        "counts": {d.isoformat(): n for d, n in sorted(counts.items())},
        "missing": [d.isoformat() for d in missing],
        "missing_count": len(missing),
        "has_gap": bool(missing),
    }


def asc_coverage(db: Session, *, start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """App Store Connect — kayıtlı günler ve eksikler."""
    from backend.models import AscConsoleWorkspace

    counts: dict[date, int] = {}
    row = db.query(AscConsoleWorkspace).filter(AscConsoleWorkspace.id == 1).first()
    if row is not None:
        _collect_dates(_load_blob(row.metrics_json), counts)
    return _coverage_payload("asc", counts, start=start, end=end)


def firebase_coverage(db: Session, *, start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """Firebase Console — kayıtlı günler ve eksikler."""
    from backend.models import FirebaseConsoleWorkspace

    counts: dict[date, int] = {}
    row = db.query(FirebaseConsoleWorkspace).filter(FirebaseConsoleWorkspace.id == 1).first()
    if row is not None:
        _collect_dates(_load_blob(row.metrics_json), counts)
    return _coverage_payload("firebase", counts, start=start, end=end)


COVERAGE_BY_PIPELINE = {
    "asc": asc_coverage,
    "firebase": firebase_coverage,
}
