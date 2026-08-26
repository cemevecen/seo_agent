"""Ayılma hemşireleri — aylık çalışma çizelgesi motoru.

Hücre kodları (örnek SSE ile uyumlu):
  8  → 08:00–16:00 (8 saat)
  16 → 16:00–08:00 (16 saat)
  24 → 08:00–08:00 (24 saat)
  Yİ → yıllık izin
  RP → rapor
  '' → boş / dinlenme
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from typing import Any

LEAD_NURSE = "Gülten Çelik"

STAFF_NURSES: tuple[str, ...] = (
    "Nuray Durna",
    "Sema Evecen",
    "Şengül Zamur",
    "Emine Türker",
    "Semanur Çınar",
    "Rabia Kumtepe",
)

ALL_NURSES: tuple[str, ...] = (LEAD_NURSE, *STAFF_NURSES)

LEAVE_CODES = frozenset({"Yİ", "RP"})
WORK_CODES = frozenset({"8", "16", "24"})
MAX_MONTHLY_HOURS = 300


@dataclass(frozen=True)
class DayMeta:
    day: int
    iso: str
    weekday: int  # Mon=0 … Sun=6
    is_weekend: bool
    is_weekday: bool


def _hours_for(code: str | None) -> int:
    c = (code or "").strip().upper()
    if c == "8":
        return 8
    if c == "16":
        return 16
    if c == "24":
        return 24
    return 0


def _norm_code(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    up = s.upper()
    if up in ("YI", "Yİ"):
        return "Yİ"
    if up == "RP":
        return "RP"
    if s in ("8", "16", "24"):
        return s
    return ""


def month_days(year: int, month: int) -> list[DayMeta]:
    n = calendar.monthrange(year, month)[1]
    out: list[DayMeta] = []
    for d in range(1, n + 1):
        dt = date(year, month, d)
        wd = dt.weekday()
        out.append(
            DayMeta(
                day=d,
                iso=dt.isoformat(),
                weekday=wd,
                is_weekend=wd >= 5,
                is_weekday=wd < 5,
            )
        )
    return out


def count_weekdays(year: int, month: int) -> int:
    return sum(1 for dm in month_days(year, month) if dm.is_weekday)


def ideal_hours(year: int, month: int) -> int:
    return count_weekdays(year, month) * 8


def _empty_grid(year: int, month: int) -> dict[str, dict[str, str]]:
    days = month_days(year, month)
    grid: dict[str, dict[str, str]] = {}
    for name in ALL_NURSES:
        grid[name] = {dm.iso: "" for dm in days}
    return grid


def _apply_leaves(
    grid: dict[str, dict[str, str]],
    leaves: dict[str, dict[str, str]] | None,
) -> None:
    if not leaves:
        return
    for name, by_day in leaves.items():
        if name not in grid or not isinstance(by_day, dict):
            continue
        for iso, code in by_day.items():
            nc = _norm_code(code)
            if nc in LEAVE_CODES and iso in grid[name]:
                grid[name][iso] = nc


def _is_leave(code: str) -> bool:
    return (code or "").strip().upper() in {"YI", "Yİ", "RP"} or (code or "") in LEAVE_CODES


def _blocked_by_rest(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    *,
    prefer_48h_after_24: bool,
) -> bool:
    """Önceki gece 16/24 bitişi bugünün sabahına denk gelir → bugün başlama."""
    if day_index <= 0:
        return False
    prev = days[day_index - 1]
    prev_code = grid[name].get(prev.iso, "")
    if prev_code == "24":
        return True
    if prev_code == "16":
        return True
    if prefer_48h_after_24 and day_index >= 2:
        prev2 = days[day_index - 2]
        if grid[name].get(prev2.iso, "") == "24":
            # 24 sonrası ideal 2 gün boş — zorunlu değil; sadece puanlamada tercih
            pass
    return False


def _rest_penalty(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Düşük = daha iyi aday. 24 sonrası 2. gün hâlâ cezalı (ideal 48s)."""
    if day_index >= 2 and grid[name].get(days[day_index - 2].iso, "") == "24":
        return 40
    if day_index >= 1 and grid[name].get(days[day_index - 1].iso, "") in ("16", "24"):
        return 999
    return 0


def generate_ayilma_schedule(
    year: int,
    month: int,
    *,
    leaves: dict[str, dict[str, str]] | None = None,
    day_only: list[str] | None = None,
    prefer_48h_after_24: bool = True,
) -> dict[str, Any]:
    if not (1 <= month <= 12):
        raise ValueError("month 1–12 olmalı")
    if year < 2000 or year > 2100:
        raise ValueError("year geçersiz")

    days = month_days(year, month)
    grid = _empty_grid(year, month)
    _apply_leaves(grid, leaves)
    day_only_set = {str(x).strip() for x in (day_only or []) if str(x).strip()}

    # Sorumlu: her gün 08–16 (izin yazılmışsa dokunma)
    for dm in days:
        if not grid[LEAD_NURSE][dm.iso]:
            grid[LEAD_NURSE][dm.iso] = "8"

    hours = {n: 0 for n in STAFF_NURSES}
    warnings: list[str] = []

    for idx, dm in enumerate(days):
        # Dinlenme: önceki 16/24 → bugün zorla boş (izin değilse)
        for name in STAFF_NURSES:
            if grid[name][dm.iso]:
                continue
            if _blocked_by_rest(name, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24):
                # bilinçli boş bırak
                pass

        available = [
            n
            for n in STAFF_NURSES
            if not grid[n][dm.iso]
            and not _blocked_by_rest(n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24)
        ]

        def rank(n: str, *, for_night: bool) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            if for_night and n in day_only_set:
                pen += 500
            # Saat dengesi + 300 tavan
            h = hours[n]
            over = max(0, h - MAX_MONTHLY_HOURS)
            return (pen, over, h, n)

        # 1) Kat 1 gündüz: her gün bir 08–16 (8 veya 24 sabahı kapsar)
        morning_cands = sorted(available, key=lambda n: rank(n, for_night=False))
        morning = morning_cands[0] if morning_cands else None

        # Gece için en az 2 nöbetçi (2. + 3. kat). Sabahçı 24 alırsa 1 sayılır.
        night_needed = 2
        assigned_night: list[str] = []

        if morning:
            # Sabahçıyı 24 yapmayı dene (geceye katkı) — saat uygunsa
            can_24 = morning not in day_only_set and hours[morning] + 24 <= MAX_MONTHLY_HOURS + 24
            # Hafta içi yoğunlukta 24 tercih; hafta sonu da mümkün
            prefer_24 = can_24 and (
                hours[morning] + 24 <= ideal_hours(year, month) + 80
                or len([n for n in available if n != morning and n not in day_only_set]) < 2
            )
            if prefer_24:
                grid[morning][dm.iso] = "24"
                hours[morning] += 24
                assigned_night.append(morning)
                night_needed -= 1
            else:
                grid[morning][dm.iso] = "8"
                hours[morning] += 8
            available = [n for n in available if n != morning]

        # 2) Kalan gece kadrosu: 16 veya 24
        night_cands = sorted(
            [n for n in available if n not in day_only_set],
            key=lambda n: rank(n, for_night=True),
        )
        for n in night_cands:
            if night_needed <= 0:
                break
            # 16 tercih (daha az yük) — sabahçı yoksa veya denge için ara sıra 24
            use_24 = hours[n] + 16 > MAX_MONTHLY_HOURS and hours[n] + 24 <= MAX_MONTHLY_HOURS + 16
            if use_24 or (night_needed >= 2 and hours[n] < hours.get(morning or "", 10**9)):
                # İlk geceyi 16 ile doldur; ikinci gerekirse 16
                code = "16"
            else:
                code = "16"
            # Ayın son günü: 16/24 yazılırsa uyarı (sonraki ay 1 boş olmalı)
            grid[n][dm.iso] = code
            hours[n] += _hours_for(code)
            assigned_night.append(n)
            night_needed -= 1
            available = [n2 for n2 in available if n2 != n]

        if morning is None:
            warnings.append(f"{dm.iso}: Kat-1 gündüz hemşiresi atanamadı (izin/dinlenme).")
        if night_needed > 0:
            warnings.append(
                f"{dm.iso}: Gece nöbeti eksik ({2 - night_needed}/2). "
                "İzin veya dinlenme nedeniyle yetersiz kadro."
            )

    # Ay sonu: son gün 16/24 olanlar → next_month_must_rest
    last = days[-1]
    next_month_rest = [
        n
        for n in STAFF_NURSES
        if grid[n].get(last.iso, "") in ("16", "24")
    ]

    ideal = ideal_hours(year, month)
    rows: list[dict[str, Any]] = []
    for name in ALL_NURSES:
        worked = sum(_hours_for(grid[name][dm.iso]) for dm in days)
        is_lead = name == LEAD_NURSE
        target = 0 if is_lead else ideal
        overtime = 0 if is_lead else max(0, worked - target)
        rows.append(
            {
                "name": name,
                "role": "lead" if is_lead else "staff",
                "day_only": name in day_only_set,
                "cells": {dm.iso: grid[name][dm.iso] for dm in days},
                "worked_hours": worked,
                "ideal_hours": target,
                "overtime_hours": overtime,
                "over_cap": (not is_lead) and worked > MAX_MONTHLY_HOURS,
            }
        )

    return {
        "ok": True,
        "year": year,
        "month": month,
        "month_label": f"{calendar.month_name[month]} {year}",
        "days": [
            {
                "day": dm.day,
                "iso": dm.iso,
                "weekday": dm.weekday,
                "weekday_tr": ("Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz")[dm.weekday],
                "is_weekend": dm.is_weekend,
            }
            for dm in days
        ],
        "nurses": list(ALL_NURSES),
        "staff": list(STAFF_NURSES),
        "lead": LEAD_NURSE,
        "ideal_hours_staff": ideal,
        "weekday_count": count_weekdays(year, month),
        "max_monthly_hours": MAX_MONTHLY_HOURS,
        "rows": rows,
        "warnings": warnings,
        "next_month_must_rest": next_month_rest,
        "legend": {
            "8": "08:00–16:00",
            "16": "16:00–08:00",
            "24": "08:00–08:00",
            "Yİ": "Yıllık izin",
            "RP": "Rapor",
        },
    }


def roster_defaults() -> dict[str, Any]:
    today = date.today()
    return {
        "lead": LEAD_NURSE,
        "staff": list(STAFF_NURSES),
        "all": list(ALL_NURSES),
        "default_year": today.year,
        "default_month": today.month,
        "max_monthly_hours": MAX_MONTHLY_HOURS,
        "legend": {
            "8": "08:00–16:00",
            "16": "16:00–08:00",
            "24": "08:00–08:00",
            "Yİ": "Yıllık izin",
            "RP": "Rapor",
        },
    }
