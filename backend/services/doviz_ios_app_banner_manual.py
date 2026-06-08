"""doviz iOS — GA4 app first_open yerine manuel tablo (5 günlük dilimler)."""

from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Any

from backend.services.ga4_app_attribution import (
    CAMPAIGN_DIMENSION,
    _calendar_dates,
    _parse_iso_date,
    _series_from_buckets,
)

# Eki 2025 → Eyl 2026, her ay 5 dilim: 1-7, 8-14, 15-21, 22-28, 29-son
_MANUAL_MONTHS: list[tuple[int, int]] = [
    (2025, 10),
    (2025, 11),
    (2025, 12),
    (2026, 1),
    (2026, 2),
    (2026, 3),
    (2026, 4),
    (2026, 5),
    (2026, 6),
    (2026, 7),
    (2026, 8),
    (2026, 9),
]

_IOS_BUCKET_TOTALS: list[list[int]] = [
    [16, 10, 12, 11, 9],
    [10, 7, 13, 11, 9],
    [11, 10, 14, 13, 11],
    [10, 11, 12, 11, 10],
    [11, 9, 10, 11, 3],
    [12, 10, 11, 11, 8],
    [10, 11, 11, 10, 7],
    [11, 10, 12, 11, 9],
    [10, 11, 11, 10, 8],
    [11, 12, 13, 12, 10],
    [12, 11, 13, 12, 11],
    [11, 10, 12, 11, 9],
]

_IOS_D_BUCKET: list[list[int]] = [
    [15, 9, 11, 10, 8],
    [9, 6, 12, 10, 8],
    [10, 9, 13, 12, 10],
    [9, 10, 11, 10, 9],
    [10, 8, 9, 10, 2],
    [11, 9, 10, 10, 7],
    [9, 10, 10, 9, 6],
    [10, 9, 11, 10, 8],
    [9, 10, 10, 9, 7],
    [10, 11, 12, 11, 9],
    [11, 10, 12, 11, 10],
    [10, 9, 11, 10, 8],
]

_CAMPAIGN_ROWS: list[tuple[str, list[list[int]]]] = [
    ("ios.d", _IOS_D_BUCKET),
]


def _bucket_ranges(year: int, month: int) -> list[tuple[int, int]]:
    """Beş dilim; Şubat gibi kısa aylarda 5. dilim son güne yazılır."""
    last = monthrange(year, month)[1]
    if last >= 29:
        tail = (29, last)
    else:
        tail = (last, last)
    return [
        (1, min(7, last)),
        (8, min(14, last)),
        (15, min(21, last)),
        (22, min(28, last)),
        tail,
    ]


def _split_bucket_total(total: int, day_count: int) -> list[int]:
    if day_count <= 0:
        return []
    if total <= 0:
        return [0] * day_count
    base, rem = divmod(int(total), day_count)
    out = [base] * day_count
    for i in range(rem):
        out[i] += 1
    return out


def _expand_buckets_to_daily(
    months: list[tuple[int, int]],
    bucket_rows: list[list[int]],
) -> dict[str, float]:
    daily: dict[str, float] = {}
    for (year, month), buckets in zip(months, bucket_rows, strict=True):
        for bucket_total, (d0, d1) in zip(buckets, _bucket_ranges(year, month), strict=True):
            days = [date(year, month, d) for d in range(d0, d1 + 1)]
            parts = _split_bucket_total(bucket_total, len(days))
            for d, val in zip(days, parts, strict=True):
                daily[d.isoformat()] = daily.get(d.isoformat(), 0.0) + float(val)
    return daily


def _manual_span() -> tuple[date, date]:
    start = date(_MANUAL_MONTHS[0][0], _MANUAL_MONTHS[0][1], 1)
    y, m = _MANUAL_MONTHS[-1]
    end = date(y, m, monthrange(y, m)[1])
    return start, end


def fetch_doviz_ios_app_banner_manual(
    *,
    start: str,
    end: str,
    top_campaigns: int = 10,
) -> dict[str, Any]:
    """Manuel tablo — GA4 app first_open API çağrısı yok."""
    req_start = _parse_iso_date(start)
    req_end = _parse_iso_date(end)
    if req_end < req_start:
        raise ValueError("Bitiş tarihi başlangıçtan önce olamaz.")

    data_start, data_end = _manual_span()
    clip_start = max(req_start, data_start)
    clip_end = min(req_end, data_end)

    total_map = _expand_buckets_to_daily(_MANUAL_MONTHS, _IOS_BUCKET_TOTALS)
    campaigns_out: list[dict[str, Any]] = []
    for name, rows in _CAMPAIGN_ROWS[: max(1, top_campaigns)]:
        cmap = _expand_buckets_to_daily(_MANUAL_MONTHS, rows)
        campaigns_out.append(
            {
                "campaign": name,
                "total": int(round(sum(cmap.values()))),
                "daily": _series_from_buckets(cmap, start=clip_start, end=clip_end),
            }
        )

    clipped_total = {
        k: v for k, v in total_map.items() if clip_start.isoformat() <= k <= clip_end.isoformat()
    }

    return {
        "metric_mode": "first_opens",
        "metric_label": "Download",
        "dimension": CAMPAIGN_DIMENSION,
        "dimension_label": "First user campaign",
        "start": req_start.isoformat(),
        "end": req_end.isoformat(),
        "top_campaigns": top_campaigns,
        "total_daily": _series_from_buckets(clipped_total, start=clip_start, end=clip_end),
        "campaigns": campaigns_out,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "data_source": "manual_ios_table",
        "manual_table_note": (
            "iOS app download — GA4 yerine manuel tablo (Eki 2025–Eyl 2026, 5 günlük dilim). "
            "Kampanya: ios.d. Ek kampanya satırları için tabloyu JSON/CSV ile genişletilebilir."
        ),
        "manual_coverage": {
            "from": data_start.isoformat(),
            "to": data_end.isoformat(),
        },
    }
