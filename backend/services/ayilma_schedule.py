"""Ayılma hemşireleri — aylık çalışma çizelgesi motoru.

Hücre kodları (örnek SSE ile uyumlu):
  8  → 08:00–16:00 (8 saat)
  16 → 16:00–08:00 (16 saat)
  24 → 08:00–08:00 (24 saat)
  Yİ → yıllık izin
  RP → rapor
  İST → özel gün isteği / rezervasyon (çalıştırılmaz)
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

LEAVE_CODES = frozenset({"Yİ", "RP", "İST"})
WORK_CODES = frozenset({"8", "16", "24"})
MAX_MONTHLY_HOURS = 300
# Yıllık izin günü = 8 saat mesai kullanılmış sayılır
YI_DAY_HOURS = 8
# Personel arası toplam (mesai+Yİ) farkı hedefi: ortalama ±16
HOURS_BALANCE_TOLERANCE = 16


def _yi_hours_from_grid(grid: dict[str, dict[str, str]], name: str, days: list[DayMeta]) -> int:
    return sum(YI_DAY_HOURS for dm in days if grid[name].get(dm.iso, "") == "Yİ")


def _shift_hours_from_grid(grid: dict[str, dict[str, str]], name: str, days: list[DayMeta]) -> int:
    return sum(_hours_for(grid[name].get(dm.iso, "")) for dm in days)


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
    if up in ("IST", "İST"):
        return "İST"
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
    return False


def _rest_penalty(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Düşük = daha iyi aday. 24 sonrası 2. gün hâlâ hafif cezalı (ideal 48s)."""
    if day_index >= 1 and grid[name].get(days[day_index - 1].iso, "") in ("16", "24"):
        return 999
    if day_index >= 2 and grid[name].get(days[day_index - 2].iso, "") == "24":
        return 25
    return 0


def _gun_asiri_24_penalty(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """24 + (1 gün boş) + 24 = 'gün aşırı nöbet' — sağlık için kaçın."""
    if day_index < 2:
        return 0
    if grid[name].get(days[day_index - 2].iso, "") != "24":
        return 0
    mid = grid[name].get(days[day_index - 1].iso, "")
    # Ortadaki gün boş / izin / istek ise klasik gün aşırı kalıbı
    if mid in ("", "Yİ", "RP", "İST"):
        return 220
    return 0


def _prefer_8_after_24_gap(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """24 sonrası ilk uygun günde 8 serpiştir (düşük = tercih)."""
    if day_index >= 2 and grid[name].get(days[day_index - 2].iso, "") == "24":
        return 0
    if day_index >= 3 and grid[name].get(days[day_index - 3].iso, "") == "24":
        return 1
    return 4


def generate_ayilma_schedule(
    year: int,
    month: int,
    *,
    leaves: dict[str, dict[str, str]] | None = None,
    day_only: list[str] | None = None,
    prefer_48h_after_24: bool = True,
) -> dict[str, Any]:
    """6 personel + sorumlu.

    Gülten yalnız kendi satırı (kadroya karışmaz).

    Her gün: mümkünse 1×«8» + 2×«24» (6 kişi arasında döner).
    Gün aşırı 24 (24+boş+24) kaçınılır; 24 aralarına 8 serpiştirilir.
    «16» yalnızca başka çare yoksa.
    """
    if not (1 <= month <= 12):
        raise ValueError("month 1–12 olmalı")
    if year < 2000 or year > 2100:
        raise ValueError("year geçersiz")

    days = month_days(year, month)
    grid = _empty_grid(year, month)
    _apply_leaves(grid, leaves)
    day_only_set = {str(x).strip() for x in (day_only or []) if str(x).strip()}

    for dm in days:
        if not grid[LEAD_NURSE][dm.iso]:
            grid[LEAD_NURSE][dm.iso] = "8"

    # Yİ peşin: her gün 8s sayılır; zorunlu nöbet saati = aylık kota − Yİ
    ideal = ideal_hours(year, month)
    yi_hours = {n: _yi_hours_from_grid(grid, n, days) for n in STAFF_NURSES}
    min_shift = {n: max(0, ideal - yi_hours[n]) for n in STAFF_NURSES}

    hours = {n: 0 for n in STAFF_NURSES}
    n8 = {n: 0 for n in STAFF_NURSES}
    n24 = {n: 0 for n in STAFF_NURSES}
    n16 = {n: 0 for n in STAFF_NURSES}
    warnings: list[str] = []

    def accounted(n: str) -> int:
        """Mesai + Yİ (8s/gün) — dengelenecek toplam."""
        return hours[n] + yi_hours[n]

    for idx, dm in enumerate(days):
        available = [
            n
            for n in STAFF_NURSES
            if not grid[n][dm.iso]
            and not _blocked_by_rest(n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24)
        ]

        def rank_for_8(n: str) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            # 24 sonrası dönüşte 8 tercih (gün aşırı 24'ü kırmak)
            after24 = _prefer_8_after_24_gap(n, idx, days, grid)
            return (pen, after24, n8[n], accounted(n), n)

        def rank_for_24(n: str) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            if n in day_only_set:
                pen += 500
            # Gün aşırı 24'ü güçlü cezalandır
            pen += _gun_asiri_24_penalty(n, idx, days, grid)
            behind = 0 if hours[n] < min_shift[n] else 1
            return (pen, behind, accounted(n), n24[n], n)

        morning: str | None = None
        night_needed = 2

        morning_cands = sorted(available, key=rank_for_8)
        if morning_cands:
            morning = morning_cands[0]
            grid[morning][dm.iso] = "8"
            hours[morning] += 8
            n8[morning] += 1
            available = [n for n in available if n != morning]

        def _assign24(n: str) -> None:
            nonlocal night_needed, available
            grid[n][dm.iso] = "24"
            hours[n] += 24
            n24[n] += 1
            night_needed -= 1
            available = [x for x in available if x != n]

        def _assign16(n: str) -> None:
            nonlocal night_needed, available
            grid[n][dm.iso] = "16"
            hours[n] += 16
            n16[n] += 1
            night_needed -= 1
            available = [x for x in available if x != n]

        while night_needed > 0:
            c24_all = [n for n in available if n not in day_only_set]
            # Gün aşırı adayları ele — başka kimse yoksa mecburen kullan
            c24_ok = [
                n for n in c24_all
                if _gun_asiri_24_penalty(n, idx, days, grid) == 0
            ]
            pool = c24_ok if c24_ok else c24_all
            c24 = sorted(pool, key=rank_for_24)
            if not c24:
                break
            _assign24(c24[0])

        if (
            night_needed > 0
            and morning
            and grid[morning][dm.iso] == "8"
            and morning not in day_only_set
            # Gün aşırıya düşecekse sabahçıyı 24'e yükseltme
            and _gun_asiri_24_penalty(morning, idx, days, grid) == 0
        ):
            grid[morning][dm.iso] = "24"
            hours[morning] += 16
            n8[morning] -= 1
            n24[morning] += 1
            night_needed -= 1

        while night_needed > 0:
            c16 = sorted(
                [n for n in available if n not in day_only_set],
                key=rank_for_24,
            )
            if not c16:
                break
            _assign16(c16[0])

        has_morning = any(grid[n][dm.iso] in ("8", "24") for n in STAFF_NURSES)
        if not has_morning:
            warnings.append(f"{dm.iso}: Kat-1 gündüz hemşiresi atanamadı (izin/dinlenme).")
        if night_needed > 0:
            warnings.append(
                f"{dm.iso}: Gece nöbeti eksik ({2 - night_needed}/2). "
                "İzin veya dinlenme nedeniyle yetersiz kadro."
            )

    last = days[-1]
    next_month_rest = [
        n for n in STAFF_NURSES if grid[n].get(last.iso, "") in ("16", "24")
    ]

    accounted_list = [hours[n] + yi_hours[n] for n in STAFF_NURSES]
    if accounted_list:
        spread = max(accounted_list) - min(accounted_list)
        if spread > HOURS_BALANCE_TOLERANCE * 2:
            warnings.append(
                f"Personel toplam saat farkı {spread:.0f}s "
                f"(hedef ≤±{HOURS_BALANCE_TOLERANCE} ≈ {HOURS_BALANCE_TOLERANCE * 2}s aralık)."
            )

    gun_asiri = 0
    for name in STAFF_NURSES:
        for i in range(2, len(days)):
            if grid[name].get(days[i].iso, "") != "24":
                continue
            if grid[name].get(days[i - 2].iso, "") != "24":
                continue
            mid = grid[name].get(days[i - 1].iso, "")
            if mid in ("", "Yİ", "RP", "İST"):
                gun_asiri += 1
    if gun_asiri:
        warnings.append(
            f"Gün aşırı 24 kalıbı {gun_asiri} kez kaldı (kaçınıldı ama tamamen sıfırlanamadı)."
        )

    rows: list[dict[str, Any]] = []
    for name in ALL_NURSES:
        is_lead = name == LEAD_NURSE
        if is_lead:
            shift_h = sum(_hours_for(grid[name][dm.iso]) for dm in days)
            leave_h = 0
            acc = shift_h
            target = 0
            min_s = 0
            overtime = 0
        else:
            shift_h = hours[name]
            leave_h = yi_hours[name]
            acc = shift_h + leave_h  # Yİ günleri 8s gibi yazılır
            target = ideal
            min_s = min_shift[name]
            overtime = acc - target  # eksi = eksik, artı = fazla
            if shift_h < min_s:
                warnings.append(
                    f"{name}: zorunlu mesai eksiği — en az {min_s}s nöbet "
                    f"(kota {ideal} − Yİ {leave_h}), şu an {shift_h}s."
                )
        rows.append(
            {
                "name": name,
                "role": "lead" if is_lead else "staff",
                "day_only": name in day_only_set,
                "cells": {dm.iso: grid[name][dm.iso] for dm in days},
                "worked_hours": acc,
                "shift_hours": shift_h,
                "leave_hours": leave_h,
                "ideal_hours": target,
                "min_shift_hours": min_s,
                "overtime_hours": overtime,
                "over_cap": (not is_lead) and shift_h > MAX_MONTHLY_HOURS,
                "exclude_from_staff_balance": is_lead,
                "count_8": 0 if is_lead else n8[name],
                "count_24": 0 if is_lead else n24[name],
                "count_16": 0 if is_lead else n16[name],
            }
        )

    code_counts = {"8": sum(n8.values()), "16": sum(n16.values()), "24": sum(n24.values())}

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
        "hours_balance_tolerance": HOURS_BALANCE_TOLERANCE,
        "yi_day_hours": YI_DAY_HOURS,
        "rows": rows,
        "warnings": warnings,
        "next_month_must_rest": next_month_rest,
        "staff_code_counts": code_counts,
        "legend": {
            "8": "08:00–16:00 (6 kişiye dağıtılır)",
            "16": "16:00–08:00 (son çare)",
            "24": "08:00–08:00 (6 kişiye dağıtılır)",
            "Yİ": f"Yıllık izin ({YI_DAY_HOURS}s sayılır)",
            "RP": "Rapor",
            "İST": "Özel gün isteği / rezervasyon",
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
            "İST": "Özel gün isteği",
        },
    }
