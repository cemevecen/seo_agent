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
# Personel arası toplam (mesai+Yİ) / fazla mesai bandı hedefi (~16s)
HOURS_BALANCE_TOLERANCE = 16
# Kişi başı düz 8 sayısı (3×8≈24s + boş gün üretir; az tut)
EIGHT_PER_PERSON_MIN = 2
EIGHT_PER_PERSON_TARGET = 3
EIGHT_PER_PERSON_MAX = 4
# Gün aşırı zinciri: 24+boş+24+boş+24 (en fazla 3×24); 4. uzatma kaçınılır
GUN_ASIRI_STREAK_MAX = 3
# Haftada bu kadar Yİ/RP/İST hücresi varsa streak limiti gevşer
LEAVE_HEAVY_WEEK_THRESHOLD = 6


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
    """24 + (1 gün boş) + 24 = 'gün aşırı nöbet' — yumuşak ceza (16 yazmak için değil)."""
    if day_index < 2:
        return 0
    if grid[name].get(days[day_index - 2].iso, "") != "24":
        return 0
    mid = grid[name].get(days[day_index - 1].iso, "")
    if mid in ("", "Yİ", "RP", "İST"):
        return 1
    return 0


def _gun_asiri_streak_if_24(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Bugün 24 yazılırsa gün aşırı zincirinde toplam kaç 24 olur (geri+ileri)."""
    gap = ("", "Yİ", "RP", "İST")

    back = 1  # today
    j = day_index
    while j >= 2:
        if grid[name].get(days[j - 2].iso, "") != "24":
            break
        mid = grid[name].get(days[j - 1].iso, "")
        if mid not in gap:
            break
        back += 1
        j -= 2

    forward = 0
    j = day_index
    while j + 2 < len(days):
        mid = grid[name].get(days[j + 1].iso, "")
        if mid not in gap:
            break
        if grid[name].get(days[j + 2].iso, "") != "24":
            break
        forward += 1
        j += 2

    return back + forward


def _week_leave_cell_count(
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """İçinde bulunulan Pzt–Paz haftasındaki personel izin/istek hücresi sayısı."""
    wd = days[day_index].weekday
    start = max(0, day_index - wd)
    end = min(len(days) - 1, start + 6)
    n = 0
    for i in range(start, end + 1):
        iso = days[i].iso
        for name in STAFF_NURSES:
            if grid[name].get(iso, "") in ("Yİ", "RP", "İST"):
                n += 1
    return n


def _week_is_leave_heavy(
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    return _week_leave_cell_count(day_index, days, grid) >= LEAVE_HEAVY_WEEK_THRESHOLD


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

    Gülten yalnız hafta içi 8 (hafta sonu boş); kadroya karışmaz.

    Hafta içi: mümkünse 1×«8» + 2×«24». Hafta sonu: yalnız 2×«24» (kat-1 / 8 yok).
    Düz «8» kişi başı aylık ~2–4 (hedef 3). Fazla mesai personelde aynı ~16s bantta.
    Gün aşırı zinciri en fazla 3×24; izin yoğun haftada gevşer.
    «16» yalnızca 24 yazacak kimse yoksa — çok uç çare.
    """
    if not (1 <= month <= 12):
        raise ValueError("month 1–12 olmalı")
    if year < 2000 or year > 2100:
        raise ValueError("year geçersiz")

    days = month_days(year, month)
    grid = _empty_grid(year, month)
    _apply_leaves(grid, leaves)
    day_only_set = {str(x).strip() for x in (day_only or []) if str(x).strip()}

    # Gülten: yalnız hafta içi 8; hafta sonu yazma
    for dm in days:
        if grid[LEAD_NURSE][dm.iso]:
            continue
        if dm.is_weekday:
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
    eight_budget = EIGHT_PER_PERSON_TARGET * len(STAFF_NURSES)

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
        leave_heavy = _week_is_leave_heavy(idx, days, grid)

        def is_gun_asiri_candidate(n: str) -> bool:
            return _gun_asiri_24_penalty(n, idx, days, grid) > 0

        def streak_if_24(n: str) -> int:
            return _gun_asiri_streak_if_24(n, idx, days, grid)

        def over_streak(n: str) -> bool:
            return (not leave_heavy) and streak_if_24(n) > GUN_ASIRI_STREAK_MAX

        def rank_for_8(n: str) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            break_streak = 0 if over_streak(n) else 1
            break24 = 0 if is_gun_asiri_candidate(n) else 1
            after24 = _prefer_8_after_24_gap(n, idx, days, grid)
            # Az toplam saat alan önce (fazla mesai dengesi)
            return (pen, break_streak, accounted(n), n8[n], break24, after24, n)

        def rank_for_24(n: str) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            if n in day_only_set:
                pen += 500
            over = 1 if over_streak(n) else 0
            gun = _gun_asiri_24_penalty(n, idx, days, grid)
            behind = 0 if hours[n] < min_shift[n] else 1
            # accounted erken: fazla mesai bandı
            return (pen, over, accounted(n), streak_if_24(n), gun, behind, n24[n], n)

        morning: str | None = None
        night_needed = 2

        def _want_morning_8() -> bool:
            # Hafta sonu kat-1 / 8 yok — yalnız 2×24
            if dm.is_weekend:
                return False
            under_max = [n for n in available if n8[n] < EIGHT_PER_PERSON_MAX]
            if not under_max:
                return False
            total8 = sum(n8.values())
            remaining = sum(1 for d in days[idx:] if d.is_weekday)
            need_min = sum(max(0, EIGHT_PER_PERSON_MIN - n8[n]) for n in STAFF_NURSES)
            if any(over_streak(n) for n in under_max):
                return True
            if any(is_gun_asiri_candidate(n) and n8[n] < EIGHT_PER_PERSON_TARGET for n in under_max):
                return True
            if need_min > 0 and remaining <= need_min + 1:
                return True
            if total8 >= eight_budget:
                return False
            weekday_i = sum(1 for d in days[: idx + 1] if d.is_weekday)
            weekday_n = sum(1 for d in days if d.is_weekday) or 1
            paced = (weekday_i / weekday_n) * eight_budget
            if total8 >= paced + 0.75:
                return False
            return any(n8[n] < EIGHT_PER_PERSON_TARGET for n in under_max)

        morning_cands = [
            n for n in sorted(available, key=rank_for_8) if n8[n] < EIGHT_PER_PERSON_MAX
        ]
        if morning_cands and _want_morning_8():
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
            pool = [n for n in available if n not in day_only_set]
            if not leave_heavy:
                capped = [n for n in pool if not over_streak(n)]
                if capped:
                    pool = capped
            if not pool:
                # Coverage: streak aşan dahil
                pool = [n for n in available if n not in day_only_set]
            if not pool:
                break
            _assign24(sorted(pool, key=rank_for_24)[0])

        if (
            night_needed > 0
            and morning
            and grid[morning][dm.iso] == "8"
            and morning not in day_only_set
            and (leave_heavy or not over_streak(morning))
        ):
            grid[morning][dm.iso] = "24"
            hours[morning] += 16
            n8[morning] -= 1
            n24[morning] += 1
            night_needed -= 1
            morning = None

        while night_needed > 0:
            c16 = sorted(
                [n for n in available if n not in day_only_set],
                key=rank_for_24,
            )
            if not c16:
                break
            _assign16(c16[0])

        if dm.is_weekday:
            has_morning = any(grid[n][dm.iso] in ("8", "24") for n in STAFF_NURSES)
            if not has_morning:
                warnings.append(f"{dm.iso}: Kat-1 gündüz hemşiresi atanamadı (izin/dinlenme).")
        if night_needed > 0:
            warnings.append(f"{dm.iso}: Gece nöbeti eksik ({2 - night_needed}/2).")

    # ── Post: streak > 3 olan 24'leri mümkünse 8 ile takas ──
    for _pass in range(4):
        swapped = False
        for name in STAFF_NURSES:
            for i in range(2, len(days)):
                if grid[name].get(days[i].iso, "") != "24":
                    continue
                if _week_is_leave_heavy(i, days, grid):
                    continue
                if _gun_asiri_streak_if_24(name, i, days, grid) <= GUN_ASIRI_STREAK_MAX:
                    # Klasik gün aşırı yumuşak takas (8 bütçesi içinde)
                    if _gun_asiri_24_penalty(name, i, days, grid) == 0:
                        continue
                    if n8[name] >= EIGHT_PER_PERSON_TARGET:
                        continue
                else:
                    # Streak > 3: 8 max'a kadar zorla kır
                    if n8[name] >= EIGHT_PER_PERSON_MAX:
                        continue
                iso = days[i].iso
                prev_iso = days[i - 1].iso
                partners = [
                    o
                    for o in STAFF_NURSES
                    if o != name
                    and grid[o].get(iso, "") == "8"
                    and grid[o].get(prev_iso, "") not in ("16", "24")
                    and (
                        i + 1 >= len(days)
                        or grid[o].get(days[i + 1].iso, "") not in ("8", "16", "24")
                    )
                    and _gun_asiri_streak_if_24(o, i, days, grid) <= GUN_ASIRI_STREAK_MAX
                ]
                if not partners:
                    continue
                other = sorted(partners, key=lambda o: (n24[o], hours[o], o))[0]
                grid[name][iso] = "8"
                grid[other][iso] = "24"
                hours[name] -= 16
                hours[other] += 16
                n8[name] += 1
                n24[name] -= 1
                n8[other] -= 1
                n24[other] += 1
                swapped = True
        if not swapped:
            break

    # ── Fazla 8 budama (hedef üstü / max): gündüz 24 ile örtülüyse 8'i kaldır ──
    for name in STAFF_NURSES:
        while n8[name] > EIGHT_PER_PERSON_TARGET:
            trimmed = False
            for dm in days:
                if grid[name].get(dm.iso, "") != "8":
                    continue
                day_24 = sum(1 for o in STAFF_NURSES if grid[o].get(dm.iso, "") == "24")
                if day_24 < 1:
                    continue
                grid[name][dm.iso] = ""
                hours[name] -= 8
                n8[name] -= 1
                trimmed = True
                break
            if not trimmed:
                break
    # Sert tavan 4 (örtü yoksa bile bırakmamak için son çare budama yok)
    for name in STAFF_NURSES:
        while n8[name] > EIGHT_PER_PERSON_MAX:
            trimmed = False
            for dm in days:
                if grid[name].get(dm.iso, "") != "8":
                    continue
                day_24 = sum(1 for o in STAFF_NURSES if grid[o].get(dm.iso, "") == "24")
                if day_24 < 1:
                    continue
                grid[name][dm.iso] = ""
                hours[name] -= 8
                n8[name] -= 1
                trimmed = True
                break
            if not trimmed:
                break

    # ── Hafta sonu personel 8 temizle (kat-1 yok) ──
    for name in STAFF_NURSES:
        for dm in days:
            if not dm.is_weekend:
                continue
            if grid[name].get(dm.iso, "") != "8":
                continue
            grid[name][dm.iso] = ""
            hours[name] -= 8
            n8[name] -= 1

    # ── Eksik 8 (min 2): yalnız hafta içi boş güne ──
    for name in STAFF_NURSES:
        for i, dm in enumerate(days):
            if n8[name] >= EIGHT_PER_PERSON_MIN:
                break
            if dm.is_weekend:
                continue
            if grid[name].get(dm.iso, ""):
                continue
            if _blocked_by_rest(name, i, days, grid, prefer_48h_after_24=prefer_48h_after_24):
                continue
            if n8[name] >= EIGHT_PER_PERSON_MAX:
                break
            grid[name][dm.iso] = "8"
            hours[name] += 8
            n8[name] += 1

    # ── Fazla mesai bandı: yüksek ↔ düşük (hedef ≤16s) ──
    def _strip_next_8_if_safe(recv: str, i: int) -> bool:
        """recv yarın 8 ise ve gündüz başka örtü varsa 8'i kaldır → 24 alabilsin."""
        if i + 1 >= len(days):
            return True
        nxt = days[i + 1]
        code = grid[recv].get(nxt.iso, "")
        if code in ("16", "24"):
            return False
        if code != "8":
            return True
        other_morn = any(
            o != recv and grid[o].get(nxt.iso, "") in ("8", "24") for o in STAFF_NURSES
        )
        if not other_morn and not nxt.is_weekend:
            return False
        grid[recv][nxt.iso] = ""
        hours[recv] -= 8
        n8[recv] -= 1
        return True

    def _can_take_24(recv: str, i: int) -> bool:
        if grid[recv].get(days[i].iso, ""):
            return False
        if recv in day_only_set:
            return False
        if _blocked_by_rest(recv, i, days, grid, prefer_48h_after_24=prefer_48h_after_24):
            return False
        if i + 1 < len(days) and grid[recv].get(days[i + 1].iso, "") in ("16", "24"):
            return False
        if (not _week_is_leave_heavy(i, days, grid)) and (
            _gun_asiri_streak_if_24(recv, i, days, grid) > GUN_ASIRI_STREAK_MAX
        ):
            return False
        if not _strip_next_8_if_safe(recv, i):
            return False
        return True

    for _bal in range(80):
        vals = {n: accounted(n) for n in STAFF_NURSES}
        hi = max(STAFF_NURSES, key=lambda n: (vals[n], n))
        lo = min(STAFF_NURSES, key=lambda n: (vals[n], n))
        if vals[hi] - vals[lo] <= HOURS_BALANCE_TOLERANCE:
            break
        moved = False

        # Aynı gün hi=24 lo=8 → takas
        for i, dm in enumerate(days):
            if grid[hi].get(dm.iso, "") != "24":
                continue
            if grid[lo].get(dm.iso, "") != "8":
                continue
            if i >= 1 and grid[lo].get(days[i - 1].iso, "") in ("16", "24"):
                continue
            if n8[hi] >= EIGHT_PER_PERSON_MAX:
                continue
            # Takas sonrası lo'nun streak'i (bugün 24 olacak)
            if (not _week_is_leave_heavy(i, days, grid)) and (
                _gun_asiri_streak_if_24(lo, i, days, grid) > GUN_ASIRI_STREAK_MAX
            ):
                continue
            if not _strip_next_8_if_safe(lo, i):
                continue
            grid[hi][dm.iso] = "8"
            grid[lo][dm.iso] = "24"
            hours[hi] -= 16
            hours[lo] += 16
            n24[hi] -= 1
            n24[lo] += 1
            n8[hi] += 1
            n8[lo] -= 1
            moved = True
            break
        if moved:
            continue

        # hi 24 → lo boş
        for i, dm in enumerate(days):
            if grid[hi].get(dm.iso, "") != "24":
                continue
            if not _can_take_24(lo, i):
                continue
            grid[hi][dm.iso] = ""
            grid[lo][dm.iso] = "24"
            hours[hi] -= 24
            hours[lo] += 24
            n24[hi] -= 1
            n24[lo] += 1
            moved = True
            break
        if moved:
            continue

        # hi 8 → lo boş (hafta içi)
        for i, dm in enumerate(days):
            if dm.is_weekend:
                continue
            if grid[hi].get(dm.iso, "") != "8":
                continue
            if grid[lo].get(dm.iso, ""):
                continue
            if _blocked_by_rest(lo, i, days, grid, prefer_48h_after_24=prefer_48h_after_24):
                continue
            if n8[lo] >= EIGHT_PER_PERSON_MAX or n8[hi] <= EIGHT_PER_PERSON_MIN:
                continue
            grid[hi][dm.iso] = ""
            grid[lo][dm.iso] = "8"
            hours[hi] -= 8
            hours[lo] += 8
            n8[hi] -= 1
            n8[lo] += 1
            moved = True
            break
        if moved:
            continue

        # hi=24 mid=8
        mids = [n for n in sorted(STAFF_NURSES, key=lambda n: vals[n]) if n not in (hi, lo)]
        for mid in mids:
            for i, dm in enumerate(days):
                if grid[hi].get(dm.iso, "") != "24":
                    continue
                if grid[mid].get(dm.iso, "") != "8":
                    continue
                if i >= 1 and grid[mid].get(days[i - 1].iso, "") in ("16", "24"):
                    continue
                if n8[hi] >= EIGHT_PER_PERSON_MAX:
                    continue
                if (not _week_is_leave_heavy(i, days, grid)) and (
                    _gun_asiri_streak_if_24(mid, i, days, grid) > GUN_ASIRI_STREAK_MAX
                ):
                    continue
                if not _strip_next_8_if_safe(mid, i):
                    continue
                grid[hi][dm.iso] = "8"
                grid[mid][dm.iso] = "24"
                hours[hi] -= 16
                hours[mid] += 16
                n24[hi] -= 1
                n24[mid] += 1
                n8[hi] += 1
                n8[mid] -= 1
                moved = True
                break
            if moved:
                break
        if not moved:
            break

    # Denge sonrası: min 8 ve hafta sonu 8 temizliği
    for name in STAFF_NURSES:
        for dm in days:
            if dm.is_weekend and grid[name].get(dm.iso, "") == "8":
                grid[name][dm.iso] = ""
                hours[name] -= 8
                n8[name] -= 1
    for name in STAFF_NURSES:
        for i, dm in enumerate(days):
            if n8[name] >= EIGHT_PER_PERSON_MIN:
                break
            if dm.is_weekend or grid[name].get(dm.iso, ""):
                continue
            if _blocked_by_rest(name, i, days, grid, prefer_48h_after_24=prefer_48h_after_24):
                continue
            grid[name][dm.iso] = "8"
            hours[name] += 8
            n8[name] += 1

    # Streak > 3 kır: zincirdeki son 24'ü başka kişiye ver veya 8 yap
    for _sk in range(20):
        broke = False
        for name in STAFF_NURSES:
            for i in range(len(days)):
                if grid[name].get(days[i].iso, "") != "24":
                    continue
                if _week_is_leave_heavy(i, days, grid):
                    continue
                if _gun_asiri_streak_if_24(name, i, days, grid) <= GUN_ASIRI_STREAK_MAX:
                    continue
                # Bu gün zinciri uzatıyor — taşı veya 8'e düş
                iso = days[i].iso
                takers = [
                    o
                    for o in STAFF_NURSES
                    if o != name
                    and grid[o].get(iso, "") == ""
                    and o not in day_only_set
                    and not _blocked_by_rest(o, i, days, grid, prefer_48h_after_24=prefer_48h_after_24)
                    and (
                        i + 1 >= len(days)
                        or grid[o].get(days[i + 1].iso, "") not in ("8", "16", "24")
                    )
                    and _gun_asiri_streak_if_24(o, i, days, grid) <= GUN_ASIRI_STREAK_MAX
                ]
                if takers:
                    other = sorted(takers, key=lambda o: (accounted(o), n24[o], o))[0]
                    grid[name][iso] = ""
                    grid[other][iso] = "24"
                    hours[name] -= 24
                    hours[other] += 24
                    n24[name] -= 1
                    n24[other] += 1
                    broke = True
                    break
                # 8'e düş (gündüz başka 24 varsa)
                if (
                    n8[name] < EIGHT_PER_PERSON_MAX
                    and sum(1 for o in STAFF_NURSES if o != name and grid[o].get(iso, "") == "24")
                    >= 1
                ):
                    grid[name][iso] = "8"
                    hours[name] -= 16
                    n24[name] -= 1
                    n8[name] += 1
                    broke = True
                    break
            if broke:
                break
        if not broke:
            break

    # Streak kırımı sonrası tekrar kısa denge
    for _bal2 in range(40):
        vals = {n: accounted(n) for n in STAFF_NURSES}
        if max(vals.values()) - min(vals.values()) <= HOURS_BALANCE_TOLERANCE:
            break
        moved = False
        his = sorted(STAFF_NURSES, key=lambda n: (-vals[n], -n24[n], n))
        los = sorted(STAFF_NURSES, key=lambda n: (vals[n], n24[n], n))
        for hi in his:
            for lo in los:
                if hi == lo or vals[hi] - vals[lo] <= HOURS_BALANCE_TOLERANCE:
                    continue
                for i, dm in enumerate(days):
                    if grid[hi].get(dm.iso, "") != "24":
                        continue
                    if not _can_take_24(lo, i):
                        continue
                    grid[hi][dm.iso] = ""
                    grid[lo][dm.iso] = "24"
                    hours[hi] -= 24
                    hours[lo] += 24
                    n24[hi] -= 1
                    n24[lo] += 1
                    moved = True
                    break
                if moved:
                    break
                for i, dm in enumerate(days):
                    if grid[hi].get(dm.iso, "") != "24" or grid[lo].get(dm.iso, "") != "8":
                        continue
                    if i >= 1 and grid[lo].get(days[i - 1].iso, "") in ("16", "24"):
                        continue
                    if n8[hi] >= EIGHT_PER_PERSON_MAX:
                        continue
                    if (not _week_is_leave_heavy(i, days, grid)) and (
                        _gun_asiri_streak_if_24(lo, i, days, grid) > GUN_ASIRI_STREAK_MAX
                    ):
                        continue
                    if not _strip_next_8_if_safe(lo, i):
                        continue
                    grid[hi][dm.iso] = "8"
                    grid[lo][dm.iso] = "24"
                    hours[hi] -= 16
                    hours[lo] += 16
                    n24[hi] -= 1
                    n24[lo] += 1
                    n8[hi] += 1
                    n8[lo] -= 1
                    moved = True
                    break
                if moved:
                    break
            if moved:
                break
        if not moved:
            break

    last = days[-1]
    next_month_rest = [
        n for n in STAFF_NURSES if grid[n].get(last.iso, "") in ("16", "24")
    ]

    accounted_list = [hours[n] + yi_hours[n] for n in STAFF_NURSES]
    if accounted_list:
        spread = max(accounted_list) - min(accounted_list)
        if spread > HOURS_BALANCE_TOLERANCE:
            warnings.append(
                f"Personel fazla/toplam saat bandı {spread:.0f}s "
                f"(hedef ≤{HOURS_BALANCE_TOLERANCE}s aralık)."
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
            f"Gün aşırı 24 kalıbı {gun_asiri} kez (zincir hedefi ≤{GUN_ASIRI_STREAK_MAX}; "
            "izin yoğun haftada esnek; 16 tercih edilmedi)."
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
    if today.month == 12:
        default_year, default_month = today.year + 1, 1
    else:
        default_year, default_month = today.year, today.month + 1
    return {
        "lead": LEAD_NURSE,
        "staff": list(STAFF_NURSES),
        "all": list(ALL_NURSES),
        "default_year": default_year,
        "default_month": default_month,
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


def build_ayilma_xlsx_bytes(
    *,
    year: int,
    month: int,
    days: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> bytes:
    """Win + Mac Excel / Numbers / LibreOffice uyumlu .xlsx."""
    from io import BytesIO

    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill

    wb = Workbook()
    ws = wb.active
    ws.title = f"{month:02d}-{year}"[:31]

    month_tr = (
        "",
        "Ocak",
        "Şubat",
        "Mart",
        "Nisan",
        "Mayıs",
        "Haziran",
        "Temmuz",
        "Ağustos",
        "Eylül",
        "Ekim",
        "Kasım",
        "Aralık",
    )
    ws["A1"] = f"Ayılma hemşireleri — {month_tr[month]} {year}"
    ws["A1"].font = Font(bold=True, size=14)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max(4, 1 + len(days) + 3))

    headers = ["Ad Soyadı"] + [str(d.get("day", "")) for d in days] + ["Çalıştığı", "Aylık", "Fazla"]
    for col, h in enumerate(headers, start=1):
        cell = ws.cell(row=3, column=col, value=h)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", wrap_text=True)

    # Hafta sonu başlık boyası
    wknd_fill = PatternFill("solid", fgColor="BBF7D0")
    for i, d in enumerate(days):
        if d.get("is_weekend"):
            ws.cell(row=3, column=2 + i).fill = wknd_fill

    fills = {
        "Yİ": PatternFill("solid", fgColor="FDE68A"),
        "RP": PatternFill("solid", fgColor="FECACA"),
        "İST": PatternFill("solid", fgColor="BAE6FD"),
        "8": PatternFill("solid", fgColor="E0E7FF"),
        "16": PatternFill("solid", fgColor="FEF3C7"),
        "24": PatternFill("solid", fgColor="DDD6FE"),
    }

    for r_i, row in enumerate(rows, start=4):
        name = row.get("name") or ""
        cells_map = row.get("cells") or {}
        ws.cell(row=r_i, column=1, value=name).font = Font(bold=True)
        worked = 0
        for c_i, d in enumerate(days):
            code = cells_map.get(d.get("iso") or "", "") or ""
            cell = ws.cell(row=r_i, column=2 + c_i, value=code)
            cell.alignment = Alignment(horizontal="center")
            if code in fills:
                cell.fill = fills[code]
            if code == "8":
                worked += 8
            elif code == "16":
                worked += 16
            elif code == "24":
                worked += 24
            elif code == "Yİ":
                worked += 8
        ideal = int(row.get("ideal_hours") or 0)
        if row.get("role") == "lead":
            ideal = 0
        ot = max(0, worked - ideal) if row.get("role") != "lead" else 0
        # İstemci ideal göndermediyse satırdaki değerleri kullan
        if "worked_hours" in row:
            worked = int(row.get("worked_hours") or worked)
        if "overtime_hours" in row and row.get("role") != "lead":
            ot = int(row.get("overtime_hours") or ot)
        if "ideal_hours" in row and row.get("role") != "lead":
            ideal = int(row.get("ideal_hours") or ideal)
        ws.cell(row=r_i, column=2 + len(days), value=worked)
        ws.cell(row=r_i, column=3 + len(days), value=ideal)
        ws.cell(row=r_i, column=4 + len(days), value=ot)

    ws.column_dimensions["A"].width = 18
    for i in range(len(days)):
        ws.column_dimensions[ws.cell(row=3, column=2 + i).column_letter].width = 4.2
    for j in range(3):
        ws.column_dimensions[ws.cell(row=3, column=2 + len(days) + j).column_letter].width = 10

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()
