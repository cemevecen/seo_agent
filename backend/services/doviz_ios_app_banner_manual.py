"""doviz iOS — GA4 app download yerine manuel günlük Total Downloads tablosu."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.ga4_app_attribution import (
    CAMPAIGN_DIMENSION,
    _parse_iso_date,
    _series_from_buckets,
)

LOGGER = logging.getLogger(__name__)

_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "doviz_ios_app_banner_daily.json"


def _load_daily_map() -> dict[str, float]:
    raw = json.loads(_DATA_PATH.read_text(encoding="utf-8"))
    out: dict[str, float] = {}
    for row in raw.get("daily") or []:
        if not isinstance(row, dict):
            continue
        d = str(row.get("date") or "")[:10]
        if not d:
            continue
        try:
            out[d] = float(int(row.get("downloads") or 0))
        except (TypeError, ValueError):
            continue
    return out


def _manual_span(daily: dict[str, float]) -> tuple[date, date] | tuple[None, None]:
    if not daily:
        return None, None
    keys = sorted(daily.keys())
    return _parse_iso_date(keys[0]), _parse_iso_date(keys[-1])


def fetch_doviz_ios_app_banner_manual(
    *,
    start: str,
    end: str,
    top_campaigns: int = 10,
) -> dict[str, Any]:
    """Manuel tablo — GA4 app first_open API çağrısı yok."""
    del top_campaigns  # kampanya kırılımı henüz yok; yalnızca toplam

    req_start = _parse_iso_date(start)
    req_end = _parse_iso_date(end)
    if req_end < req_start:
        raise ValueError("Bitiş tarihi başlangıçtan önce olamaz.")

    if not _DATA_PATH.is_file():
        raise ValueError("iOS manuel download veri dosyası bulunamadı.")

    total_map = _load_daily_map()
    data_start, data_end = _manual_span(total_map)
    if data_start is None or data_end is None:
        raise ValueError("iOS manuel download verisi boş.")

    clip_start = max(req_start, data_start)
    clip_end = min(req_end, data_end)
    if clip_end < clip_start:
        raise ValueError(
            f"Seçilen aralık manuel veri dışında ({data_start.isoformat()} – {data_end.isoformat()})."
        )

    clipped = {
        k: v for k, v in total_map.items() if clip_start.isoformat() <= k <= clip_end.isoformat()
    }
    period_total = int(round(sum(clipped.values())))

    meta = json.loads(_DATA_PATH.read_text(encoding="utf-8"))

    return {
        "metric_mode": "first_opens",
        "metric_label": "Download",
        "dimension": CAMPAIGN_DIMENSION,
        "dimension_label": "First user campaign",
        "start": req_start.isoformat(),
        "end": req_end.isoformat(),
        "top_campaigns": 0,
        "total_daily": _series_from_buckets(clipped, start=clip_start, end=clip_end),
        "campaigns": [],
        "manual_period_total": period_total,
        "fetched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "data_source": "manual_ios_table",
        "manual_table_note": (
            "iOS app download — GA4 yerine manuel Total Downloads "
            f"({data_start.isoformat()} – {data_end.isoformat()}, dönem toplamı {period_total}). "
            "Kampanya kırılımı ayrı paylaşılırsa eklenebilir."
        ),
        "manual_coverage": {
            "from": data_start.isoformat(),
            "to": data_end.isoformat(),
            "source_total": int(meta.get("period_total") or sum(total_map.values())),
        },
    }
