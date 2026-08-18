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

    yday = calendar_yesterday()
    seal = pipeline_seal_through(pipeline)
    # Bugün hiçbir zaman tam gün değildir ve kalıcı kaydedilmez. Ham blob'da
    # yarım gün olarak görünebiliyor; kapsamaya alınırsa gerçek bir boşluğu
    # maskeler ya da "bugün elimde var" yanılgısı üretir.
    counts = {d: n for d, n in counts.items() if d <= yday}
    known = set(counts)

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


# ── Scraper tarafı: paneldeki kapsamayı sor, boşluğu SINIRLI tut ─────────────

# Boşluk doldurmanın amacı kaçan bir turu telafi etmek; geçmişi yeniden çekmek
# değil (o `force_full` işi). Sınır olmazsa mühür aylar öncesine düşebildiği
# için tek bir eksik gün yüzünden yüzlerce günlük tarama tetiklenebilir.
DEFAULT_GAP_LOOKBACK_DAYS = 14
_HTTP_TIMEOUT_SEC = 30


def coverage_url(pipeline: str, base_url: str | None = None) -> str:
    import os

    key = (pipeline or "").strip().lower()
    explicit = (os.environ.get(f"{key.upper()}_CONSOLE_COVERAGE_URL") or "").strip()
    if explicit:
        return explicit
    base = (
        base_url
        or os.environ.get("PROJECT_CONTROL_BASE_URL")
        or os.environ.get("NOTIFICATION_INGEST_BASE_URL")
        or "https://projectcontrol.up.railway.app"
    ).strip().rstrip("/")
    return f"{base}/api/{key}-console/coverage"


def fetch_remote_coverage(pipeline: str, *, base_url: str | None = None) -> dict[str, Any] | None:
    """Paneldeki kapsamayı getir. Her hata durumunda None — scrape asla bozulmaz."""
    import os

    token = (os.environ.get("NOTIFICATION_INGEST_TOKEN") or "").strip()
    if not token:
        LOGGER.info("Kapsama sorgusu atlandı: ingest token yok (%s)", pipeline)
        return None
    try:
        import requests

        resp = requests.get(
            coverage_url(pipeline, base_url),
            headers={"X-Notification-Ingest-Token": token},
            timeout=_HTTP_TIMEOUT_SEC,
        )
        if resp.status_code != 200:
            LOGGER.warning("Kapsama sorgusu HTTP %s (%s)", resp.status_code, pipeline)
            return None
        data = resp.json()
        return data if isinstance(data, dict) and data.get("ok") else None
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Kapsama sorgusu başarısız (%s): %s", pipeline, exc)
        return None


def bounded_known_dates(
    stored: Any,
    *,
    pipeline: str,
    lookback_days: int = DEFAULT_GAP_LOOKBACK_DAYS,
) -> list[date]:
    """Kayıtlı günler + pencere dışındaki tüm günler «biliniyor» sayılır.

    `scheduled_fetch_window` boşlukları mühür+1'den düne kadar arıyor. Yalnızca
    son N günü telafi etmek istediğimiz için, N'den eski günleri bilerek
    «var» işaretliyoruz — böylece test edilmiş gap_fill mantığı olduğu gibi
    kullanılır ama tarama penceresi sınırlı kalır.
    """
    from backend.services.history_seal import (
        calendar_yesterday,
        iter_dates_inclusive,
        pipeline_seal_through,
    )

    yday = calendar_yesterday()
    span = max(1, int(lookback_days))
    boundary = yday - timedelta(days=span - 1)

    actual: set[date] = set()
    for raw in stored or []:
        parsed = _parse_iso(raw)
        if parsed and parsed <= yday:      # bugün ve ötesi hiç sayılmaz
            actual.add(parsed)

    known = set(actual)
    gap_start = pipeline_seal_through(pipeline) + timedelta(days=1)

    # (a) Lookback penceresinin dışı: geçmiş backfill force_full'un işi
    if boundary > gap_start:
        known.update(iter_dates_inclusive(gap_start, boundary - timedelta(days=1)))

    # (b) İlk kayıttan öncesi: boşluk, elde olan günlerin ARASINDAKİ deliktir.
    # Hattın geçmişi hiç yoksa (yeni kurulmuş, kaynak o günleri vermiyor) o
    # günler "eksik" değildir — aksi halde her tur boşuna geniş tarama açar.
    # Not: gerçek kayıtlardan hesaplanır, (a)'da eklenen sentetik günlerden değil.
    if actual:
        earliest = min(actual)
        if earliest > gap_start:
            known.update(iter_dates_inclusive(gap_start, earliest - timedelta(days=1)))
    return sorted(known)


def known_dates_for_scrape(
    pipeline: str,
    *,
    lookback_days: int = DEFAULT_GAP_LOOKBACK_DAYS,
    base_url: str | None = None,
) -> list[date] | None:
    """`scheduled_fetch_window(known_dates=...)` için hazır liste.

    None dönerse çağıran taraf mevcut davranışta kalır (yalnızca dün) — yani
    kapsama ucuna ulaşılamaması asla yanlış/eksik veri üretmez.
    """
    cov = fetch_remote_coverage(pipeline, base_url=base_url)
    if cov is None:
        return None
    return bounded_known_dates(
        cov.get("dates") or [], pipeline=pipeline, lookback_days=lookback_days
    )


def oldest_missing_within(
    pipeline: str,
    *,
    lookback_days: int = DEFAULT_GAP_LOOKBACK_DAYS,
    base_url: str | None = None,
) -> date | None:
    """Sınır içindeki en eski eksik gün — Firebase gibi gün sayısı isteyenler için."""
    known = known_dates_for_scrape(
        pipeline, lookback_days=lookback_days, base_url=base_url
    )
    if known is None:
        return None
    from backend.services.history_seal import calendar_yesterday, pipeline_seal_through

    yday = calendar_yesterday()
    # Arama alanı `scheduled_fetch_window` ile AYNI olmalı: mühür+1'den düne.
    # Lookback sınırı zaten `bounded_known_dates` içinde uygulanıyor; burada da
    # sınırdan başlamak, mühürden önceki günleri yanlışlıkla eksik gösterip
    # her turda gereksiz geniş pencere açıyordu.
    gap_start = pipeline_seal_through(pipeline) + timedelta(days=1)
    missing = missing_between(set(known), gap_start, yday)
    return missing[0] if missing else None
