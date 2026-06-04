"""Zaman serisi spike / peak / dip tespiti — /ad drill grafik özetleri için."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SeriesPoint:
    label: str
    value: float


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2:
        return float(s[mid])
    return (float(s[mid - 1]) + float(s[mid])) / 2.0


def _mad(vals: list[float]) -> float:
    if not vals:
        return 0.0
    med = _median(vals)
    dev = [abs(v - med) for v in vals]
    return _median(dev) or 0.0


def analyze_series(
    points: list[SeriesPoint],
    *,
    is_rate_pct: bool = False,
    min_points: int = 5,
) -> dict[str, Any]:
    """
    peaks: yerel zirveler
    valleys: yerel dipler (coverage düşüşleri)
    spikes_up / spikes_down: komşuya göre ani sıçrama (en fazla 5'er)
    """
    if len(points) < min_points:
        return {
            "peaks": [],
            "valleys": [],
            "spikes_up": [],
            "spikes_down": [],
            "range_min": None,
            "range_max": None,
        }

    ys = [float(p.value) for p in points]
    labels = [p.label for p in points]
    diffs: list[float] = []
    for i in range(1, len(ys)):
        diffs.append(ys[i] - ys[i - 1])

    mad = _mad(diffs)
    std_floor = 8.0 if is_rate_pct else max(abs(_median(ys)) * 0.15, 1e-6)
    jump_thr = max(std_floor, mad * 4.0, 12.0 if is_rate_pct else mad * 3.0)
    local_thr = max(5.0 if is_rate_pct else mad * 2.5, mad * 2.0)

    peaks: list[dict[str, Any]] = []
    valleys: list[dict[str, Any]] = []
    for i in range(1, len(ys) - 1):
        if ys[i] >= ys[i - 1] and ys[i] > ys[i + 1]:
            prominence = ys[i] - max(ys[i - 1], ys[i + 1])
            if prominence >= local_thr:
                peaks.append(
                    {
                        "index": i,
                        "label": labels[i],
                        "value": ys[i],
                        "prominence": round(prominence, 4),
                    }
                )
        if ys[i] <= ys[i - 1] and ys[i] < ys[i + 1]:
            prominence = min(ys[i - 1], ys[i + 1]) - ys[i]
            if prominence >= local_thr:
                valleys.append(
                    {
                        "index": i,
                        "label": labels[i],
                        "value": ys[i],
                        "prominence": round(prominence, 4),
                    }
                )

    spikes_up: list[dict[str, Any]] = []
    spikes_down: list[dict[str, Any]] = []
    for i in range(1, len(ys)):
        d = ys[i] - ys[i - 1]
        if d >= jump_thr:
            spikes_up.append(
                {
                    "index": i,
                    "label": labels[i],
                    "value": ys[i],
                    "delta": round(d, 4),
                    "from_label": labels[i - 1],
                }
            )
        elif d <= -jump_thr:
            spikes_down.append(
                {
                    "index": i,
                    "label": labels[i],
                    "value": ys[i],
                    "delta": round(d, 4),
                    "from_label": labels[i - 1],
                }
            )

    def _top(items: list[dict], key: str, n: int = 5) -> list[dict]:
        return sorted(items, key=lambda x: abs(x.get(key) or 0), reverse=True)[:n]

    return {
        "peaks": _top(peaks, "prominence"),
        "valleys": _top(valleys, "prominence"),
        "spikes_up": _top(spikes_up, "delta"),
        "spikes_down": _top(spikes_down, "delta"),
        "range_min": {"label": labels[ys.index(min(ys))], "value": min(ys)},
        "range_max": {"label": labels[ys.index(max(ys))], "value": max(ys)},
    }
