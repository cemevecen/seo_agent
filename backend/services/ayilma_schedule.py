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
    """6 personel + sorumlu.

    Gülten (sorumlu) yalnızca kendi satırına yazılır; kadro / fazla mesai /
    gece hesabına hiç girmez.

    Tercih sırası:
      1) Her gün mümkünse bir personel «8» (kat-1 gündüz)
      2) Gece (2 nöbetçi) için mümkünse «24»
      3) «16» yalnızca başka çare yoksa
    """
    if not (1 <= month <= 12):
        raise ValueError("month 1–12 olmalı")
    if year < 2000 or year > 2100:
        raise ValueError("year geçersiz")

    days = month_days(year, month)
    grid = _empty_grid(year, month)
    _apply_leaves(grid, leaves)
    day_only_set = {str(x).strip() for x in (day_only or []) if str(x).strip()}

    # Sorumlu: yalnız kendi satırı — personel döngüsüne dokunmaz
    for dm in days:
        if not grid[LEAD_NURSE][dm.iso]:
            grid[LEAD_NURSE][dm.iso] = "8"

    hours = {n: 0 for n in STAFF_NURSES}
    warnings: list[str] = []

    for idx, dm in enumerate(days):
        available = [
            n
            for n in STAFF_NURSES
            if not grid[n][dm.iso]
            and not _blocked_by_rest(n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24)
        ]

        def rank(n: str, *, prefer_day_only: bool = False) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            if prefer_day_only and n not in day_only_set:
                pen += 5  # gündüz 8 için day_only adayları hafif öne al
            if not prefer_day_only and n in day_only_set:
                pen += 500
            h = hours[n]
            over = max(0, h - MAX_MONTHLY_HOURS)
            return (pen, over, h, n)

        morning: str | None = None
        night_needed = 2

        # 1) Kat-1 gündüz: mümkün olduğunca düz «8»
        morning_cands = sorted(available, key=lambda n: rank(n, prefer_day_only=True))
        if morning_cands:
            morning = morning_cands[0]
            grid[morning][dm.iso] = "8"
            hours[morning] += 8
            available = [n for n in available if n != morning]

        def _assign(n: str, code: str) -> None:
            nonlocal night_needed, available
            grid[n][dm.iso] = code
            hours[n] += _hours_for(code)
            if code in ("16", "24"):
                night_needed -= 1
            available = [x for x in available if x != n]

        # 2) Gece: önce «24» (2 kişi)
        while night_needed > 0:
            c24 = sorted(
                [n for n in available if n not in day_only_set],
                key=lambda n: rank(n, prefer_day_only=False),
            )
            if not c24:
                break
            _assign(c24[0], "24")

        # 3) Hâlâ gece eksiği: sabahçıyı 8→24 yükselt (zorunlu; 16'ya göre tercih)
        if (
            night_needed > 0
            and morning
            and grid[morning][dm.iso] == "8"
            and morning not in day_only_set
        ):
            grid[morning][dm.iso] = "24"
            hours[morning] += 16  # zaten 8 yazılmıştı
            night_needed -= 1

        # 4) Son çare: «16»
        while night_needed > 0:
            c16 = sorted(
                [n for n in available if n not in day_only_set],
                key=lambda n: rank(n, prefer_day_only=False),
            )
            if not c16:
                break
            _assign(c16[0], "16")

        # Sabah hiç yoksa ama 24 yazıldıysa kat-1 gündüz yine karşılanır
        has_morning = any(
            grid[n][dm.iso] in ("8", "24") for n in STAFF_NURSES
        )
        if not has_morning:
            warnings.append(f"{dm.iso}: Kat-1 gündüz hemşiresi atanamadı (izin/dinlenme).")
        if night_needed > 0:
            warnings.append(
                f"{dm.iso}: Gece nöbeti eksik ({2 - night_needed}/2). "
                "İzin veya dinlenme nedeniyle yetersiz kadro."
            )

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
        # Sorumlu: ideal/fazla mesai hesabı yok — yalnız kendi satırı
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
                "exclude_from_staff_balance": is_lead,
            }
        )

    code_counts = {"8": 0, "16": 0, "24": 0}
    for name in STAFF_NURSES:
        for dm in days:
            c = grid[name][dm.iso]
            if c in code_counts:
                code_counts[c] += 1

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
        "staff_code_counts": code_counts,
        "legend": {
            "8": "08:00–16:00",
            "16": "16:00–08:00 (son çare)",
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
