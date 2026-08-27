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
import random
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
MAX_MONTHLY_HOURS = 400
# Yıllık izin / rapor günü = 8 saat kredi (toplam hesabına eklenir)
YI_DAY_HOURS = 8
RP_DAY_HOURS = 8
# Personel arası toplam (mesai+Yİ) / fazla mesai bandı hedefi (~16s)
HOURS_BALANCE_TOLERANCE = 16
# İST (istek) günü kotadan düşülmez; mesai bandı fiili mesai ile takip edilir
REQUEST_CODES = frozenset({"İST"})
ABSENCE_CREDIT_CODES = frozenset({"Yİ", "RP"})
# Kişi başı düz 8 sayısı (3×8≈24s + boş gün üretir; az tut)
EIGHT_PER_PERSON_MIN = 2
EIGHT_PER_PERSON_TARGET = 3
EIGHT_PER_PERSON_MAX = 4
# Gün aşırı zinciri: 24+boş+24+… — yumuşak ≤3, normal tavan 4; çok sıkışıkta 5, asla 5 üstü yok
GUN_ASIRI_STREAK_SOFT = 3
GUN_ASIRI_STREAK_MAX = 4
GUN_ASIRI_STREAK_ABSOLUTE = 5  # yalnız gece doldurma son çare; 5 aşılmaz
# Üst üste «8»: olabildiğince yok (yumuşak 1); mecbur kalınırsa en fazla 2 gün
CONSECUTIVE_8_STREAK_SOFT = 1
CONSECUTIVE_8_STREAK_MAX = 2
# Haftada bu kadar Yİ/RP/İST hücresi varsa streak limiti gevşer
LEAVE_HEAVY_WEEK_THRESHOLD = 6
# Aynı ikili aynı gün 24 — yumuşak çeşitlilik (sert kural değil)
PAIR24_RECENT_GAP = 4  # son eşleşmeden bu kadar gün içinde tekrar → ek ceza
PAIR24_PRIOR_WEIGHT = 7  # ay içi önceki birlikte 24 (her biri)
PAIR24_NEAR_REPEAT = 18  # yakın aralıkta tekrar eşleşme
PAIR24_THIRD_NEAR = 28  # ay içi 2+ kez eşleşmiş ikilinin yakın 3. kez
PAIR24_MONTHLY_SOFT = 4  # post-pass: hedef üstü aylık birlikte 24
# 24 arası boş hücre: KATİ tavan 3 (4+ yasak); hedef 2+24+2
IDLE_24_GAP_MAX = 3
IDLE_24_GAP_SOFT = 2  # tercih: 2 gün boş + 24 + 2 gün boş; ayrıntı docs/ayilma-schedule-rules.md


def _yi_hours_from_grid(grid: dict[str, dict[str, str]], name: str, days: list[DayMeta]) -> int:
    return sum(YI_DAY_HOURS for dm in days if grid[name].get(dm.iso, "") == "Yİ")


def _rp_hours_from_grid(grid: dict[str, dict[str, str]], name: str, days: list[DayMeta]) -> int:
    return sum(RP_DAY_HOURS for dm in days if grid[name].get(dm.iso, "") == "RP")


def _ist_day_count(grid: dict[str, dict[str, str]], name: str, days: list[DayMeta]) -> int:
    return sum(1 for dm in days if grid[name].get(dm.iso, "") == "İST")


def _uses_ist_only_leave(
    name: str,
    *,
    ist_count: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
) -> bool:
    """Yalnızca İST var; Yİ/RP yok — mesai kotası istekten düşülmez."""
    return ist_count[name] > 0 and yi_hours[name] == 0 and rp_hours[name] == 0


def _has_yi_or_rp(
    name: str,
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
) -> bool:
    """Yıllık izin veya rapor — ortalama mesai havuzuna dahil edilmez."""
    return yi_hours.get(name, 0) > 0 or rp_hours.get(name, 0) > 0


def _balance_peer_names(
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
) -> list[str]:
    """Ortalama / bant hesabı: Yİ/RP alanlar hariç (hepsi izinse tüm kadro)."""
    peers = [n for n in STAFF_NURSES if not _has_yi_or_rp(n, yi_hours, rp_hours)]
    return peers if peers else list(STAFF_NURSES)


def _balance_metric(
    name: str,
    hours: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
) -> int:
    """Fazla mesai dengesi: İST → fiili mesai; Yİ/RP → mesai + izin kredisi."""
    if _uses_ist_only_leave(name, ist_count=ist_count, yi_hours=yi_hours, rp_hours=rp_hours):
        return hours[name]
    return hours[name] + yi_hours[name] + rp_hours[name]


def _peer_balance_goal(
    hours: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
) -> int:
    peers = _balance_peer_names(yi_hours, rp_hours)
    vals = sorted(
        _balance_metric(n, hours, yi_hours, rp_hours, ist_count) for n in peers
    )
    return vals[len(vals) // 2]


def _peer_hours_spread(
    hours: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
) -> int:
    peers = _balance_peer_names(yi_hours, rp_hours)
    vals = [_balance_metric(n, hours, yi_hours, rp_hours, ist_count) for n in peers]
    if not vals:
        return 0
    return max(vals) - min(vals)


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
        # Gülten satırı yalnızca görünür; izin/mesai yazılmaz.
        if name == LEAD_NURSE or name not in grid or not isinstance(by_day, dict):
            continue
        for iso, code in by_day.items():
            nc = _norm_code(code)
            if nc in LEAVE_CODES and iso in grid[name]:
                grid[name][iso] = nc


def _clear_lead_row(grid: dict[str, dict[str, str]]) -> None:
    """Gülten Çelik çizelgede kalır ama tüm hücreler boş; hesaba dahil edilmez."""
    cells = grid.get(LEAD_NURSE)
    if not cells:
        return
    for iso in cells:
        cells[iso] = ""


def _blocked_by_rest(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    *,
    prefer_48h_after_24: bool,
) -> bool:
    """Önceki gece 16/24 bitişi bugün başlatamaz; ertesi gün 16/24 varsa bugün 24 yazma."""
    if day_index > 0:
        prev_code = grid[name].get(days[day_index - 1].iso, "")
        if prev_code in ("16", "24"):
            return True
    if day_index + 1 < len(days):
        next_code = grid[name].get(days[day_index + 1].iso, "")
        if next_code in ("16", "24"):
            return True
    return False


def _rest_allows_24(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    """24 yazılabilir mi (dry-run; grid değiştirmez)."""
    if day_index > 0 and grid[name].get(days[day_index - 1].iso, "") in ("16", "24"):
        return False
    if day_index + 1 < len(days):
        nxt = days[day_index + 1]
        nxt_code = grid[name].get(nxt.iso, "")
        if nxt_code in ("16", "24"):
            return False
        if nxt_code == "8" and nxt.is_weekday:
            other = any(
                o != name and grid[o].get(nxt.iso, "") in ("8", "24")
                for o in STAFF_NURSES
            )
            if not other:
                return False
    return True


def _enforce_rest_before_24(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
) -> bool:
    """24 yazmadan önce dinlenme kuralı; ertesi gün 8 varsa kaldır."""
    if not _rest_allows_24(name, day_index, days, grid):
        return False
    if day_index + 1 < len(days):
        nxt = days[day_index + 1]
        if grid[name].get(nxt.iso) == "8":
            grid[name][nxt.iso] = ""
            hours[name] -= 8
            n8[name] -= 1
    return True


def _days_without_24_before(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Bugünden geriye son 24'ten bu yana kaç gün geçti (izin günleri atlanır, sıfırlamaz)."""
    n = 0
    for j in range(day_index - 1, -1, -1):
        code = grid[name].get(days[j].iso, "")
        if code in LEAVE_CODES:
            continue
        if code == "24":
            return n
        n += 1
    return n


def _idle_empty_streak_before(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Bugünden önce ardışık tam boş (mesai/izin yok) gün sayısı."""
    streak = 0
    for j in range(day_index - 1, -1, -1):
        code = grid[name].get(days[j].iso, "")
        if code in LEAVE_CODES or code in WORK_CODES:
            break
        streak += 1
    return streak


def _empty_days_after_24_until_next(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """24'ten sonraki 24'e kadar kaç mesai-dışı gün (izin atlanır, sıfırlamaz)."""
    n = 0
    for j in range(day_index + 1, len(days)):
        code = grid[name].get(days[j].iso, "")
        if code in LEAVE_CODES:
            continue
        if code == "24":
            return n
        n += 1
    return n


def _is_triple_gap_sandwich_24(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    """3 boş + 24 + 3 boş kalıbı (her iki yanda da tavan kadar boşluk)."""
    if grid[name].get(days[day_index].iso, "") != "24":
        return False
    gap_b = _days_without_24_before(name, day_index, days, grid)
    gap_a = _empty_days_after_24_until_next(name, day_index, days, grid)
    return gap_b >= IDLE_24_GAP_MAX and gap_a >= IDLE_24_GAP_MAX


def _count_triple_gap_sandwiches_in_grid(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    n = 0
    for name in STAFF_NURSES:
        for i in range(len(days)):
            if _is_triple_gap_sandwich_24(name, i, days, grid):
                n += 1
    return n


def _is_locked_ist_follow_24(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    """İST ertesi gün 24 — gap/balance pass'lerinde kaydırma."""
    if day_index <= 0:
        return False
    return (
        grid[name].get(days[day_index].iso, "") == "24"
        and grid[name].get(days[day_index - 1].iso, "") == "İST"
    )


def _try_pull_24_earlier(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
) -> bool:
    """3+24+3 kır: 24'ü bir gün öne çek (2+24+…). Saat değişmez."""
    iso = days[day_index].iso
    if grid[name].get(iso, "") != "24":
        return False
    if _is_locked_ist_follow_24(name, day_index, days, grid):
        return False
    if _days_without_24_before(name, day_index, days, grid) < IDLE_24_GAP_MAX:
        return False
    j = day_index - 1
    while j >= max(0, day_index - IDLE_24_GAP_MAX):
        code = grid[name].get(days[j].iso, "")
        if code in LEAVE_CODES:
            j -= 1
            continue
        if code != "":
            return False
        break
    else:
        return False
    tgt_iso = days[j].iso
    if tgt_iso in force_avoid[name] or name in day_only_set:
        return False
    if _staff_night_count(grid, tgt_iso) >= NIGHT_SHIFTS_PER_DAY:
        return False
    grid[name][iso] = ""
    if _blocked_by_rest(
        name, j, days, grid, prefer_48h_after_24=prefer_48h_after_24
    ) or _gun_asiri_streak_over(
        name, j, days, grid, cap=GUN_ASIRI_STREAK_ABSOLUTE
    ):
        grid[name][iso] = "24"
        return False
    grid[name][tgt_iso] = "24"
    return True


def _shorten_triple_gap_sandwiches(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
) -> bool:
    """3+24+3 yerine 2+24+2: mevcut 24'leri öne çek (yeni saat eklemez)."""
    changed = False
    for _ in range(24):
        fixed = False
        for name in STAFF_NURSES:
            for i in range(len(days)):
                if not _is_triple_gap_sandwich_24(name, i, days, grid):
                    continue
                pull_targets = [i]
                for k in range(i + 1, len(days)):
                    code = grid[name].get(days[k].iso, "")
                    if code in LEAVE_CODES:
                        continue
                    if code == "24":
                        pull_targets.append(k)
                        break
                    if code in WORK_CODES:
                        break
                for pull_i in pull_targets:
                    if _try_pull_24_earlier(
                        name,
                        pull_i,
                        days,
                        grid,
                        prefer_48h_after_24=prefer_48h_after_24,
                        force_avoid=force_avoid,
                        day_only_set=day_only_set,
                    ):
                        fixed = True
                        changed = True
                        break
                if fixed:
                    break
            if fixed:
                break
        if not fixed:
            break
    return changed


def _max_days_without_24_in_grid(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    best = 0
    for name in STAFF_NURSES:
        run = 0
        for dm in days:
            code = grid[name].get(dm.iso, "")
            if code in LEAVE_CODES:
                run = 0
            elif code == "24":
                run = 0
            else:
                run += 1
                best = max(best, run)
    return best


def _max_empty_between_24_in_grid(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """İki 24 arasında veya son 24 sonrası ay sonuna kadar ardışık boş gün."""
    best = 0
    for name in STAFF_NURSES:
        for i, dm in enumerate(days):
            if grid[name].get(dm.iso, "") != "24":
                continue
            run = 0
            for j in range(i + 1, len(days)):
                code = grid[name].get(days[j].iso, "")
                if code == "24":
                    best = max(best, run)
                    break
                if code == "":
                    run += 1
                    best = max(best, run)
                elif code not in LEAVE_CODES:
                    break
            else:
                # Ay sonuna kadar sonraki 24 yok — trailing boşluk da sayılır
                best = max(best, run)
    return best


def _empty_between_24_urgency(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """İki 24 arası veya son 24 sonrası boş seri uzunsa, araya 24 koyma aciliyeti."""
    prev_i: int | None = None
    for j in range(day_index - 1, -1, -1):
        if grid[name].get(days[j].iso, "") == "24":
            prev_i = j
            break
    if prev_i is None:
        return 0
    next_i: int | None = None
    empty_count = 0
    for j in range(prev_i + 1, len(days)):
        code = grid[name].get(days[j].iso, "")
        if code == "24":
            next_i = j
            break
        if code == "":
            empty_count += 1
        elif code not in LEAVE_CODES:
            return 0
    if empty_count <= IDLE_24_GAP_MAX:
        return 0
    end = next_i if next_i is not None else len(days)
    if day_index <= prev_i or day_index >= end:
        return 0
    return empty_count * 20 + (day_index - prev_i)


def _grid_gap_ok(days: list[DayMeta], grid: dict[str, dict[str, str]]) -> bool:
    return (
        _max_days_without_24_in_grid(days, grid) <= IDLE_24_GAP_MAX
        and _max_empty_between_24_in_grid(days, grid) <= IDLE_24_GAP_MAX
    )


def _try_assign_24_for_gap(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
) -> bool:
    """24 arası boşluk kapat: boş slot varsa yaz; gece doluysa daha az acil 24'lü çıkar."""
    iso = days[day_index].iso
    code = grid[name].get(iso, "")
    if code in LEAVE_CODES or code == "24":
        return False
    if iso in force_avoid[name] or name in day_only_set:
        return False
    if _blocked_by_rest(
        name, day_index, days, grid, prefer_48h_after_24=prefer_48h_after_24
    ):
        return False
    if _gun_asiri_streak_over(
        name, day_index, days, grid, cap=GUN_ASIRI_STREAK_ABSOLUTE
    ):
        return False
    if not _enforce_rest_before_24(name, day_index, days, grid, hours, n8):
        return False

    def _apply_24() -> None:
        prev = grid[name].get(iso, "")
        grid[name][iso] = "24"
        if prev == "8":
            hours[name] += 16
            n8[name] -= 1
        else:
            hours[name] += 24
        n24[name] += 1

    if _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY:
        _apply_24()
        return True

    cand_gap = _days_without_24_before(name, day_index, days, grid)
    cand_idle = _idle_empty_streak_before(name, day_index, days, grid)
    cand_between = _empty_between_24_urgency(name, day_index, days, grid)
    cand_empty = _max_empty_between_24_for(name, days, grid)
    cand_score = cand_gap * 10 + cand_idle + cand_between
    partners = [n for n in STAFF_NURSES if n != name and grid[n].get(iso) == "24"]
    for p in sorted(
        partners,
        key=lambda n: (
            0 if _can_remove_24_without_gap_violation(n, day_index, days, grid) else 1,
            _days_without_24_before(n, day_index, days, grid) * 10
            + _idle_empty_streak_before(n, day_index, days, grid),
            n24[n],
            n,
        ),
    ):
        p_score = (
            _days_without_24_before(p, day_index, days, grid) * 10
            + _idle_empty_streak_before(p, day_index, days, grid)
        )
        safe = _can_remove_24_without_gap_violation(p, day_index, days, grid)
        if not safe:
            grid[p][iso] = ""
            p_empty_after = _max_empty_between_24_for(p, days, grid)
            grid[p][iso] = "24"
            if cand_empty <= IDLE_24_GAP_MAX:
                continue
            if p_empty_after > cand_empty:
                continue
            if p_empty_after == cand_empty and cand_between < 80:
                continue
        elif p_score >= cand_score and cand_between < 80 and cand_empty <= IDLE_24_GAP_MAX:
            continue
        grid[p][iso] = ""
        hours[p] -= 24
        n24[p] -= 1
        _apply_24()
        return True
    return False


def _enforce_idle_24_gaps(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    min_shift: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
    yi_hours: dict[str, int] | None = None,
) -> None:
    """24 arası en fazla 3 gün; hedef 2+24+2 (3+24+3'ten kaçın)."""

    def _acc(n: str) -> int:
        return hours[n] + (yi_hours or {}).get(n, 0)

    failed: set[tuple[str, int]] = set()
    for _ in range(48):
        worst: tuple[tuple[int, int], str, int] | None = None
        for name in STAFF_NURSES:
            for i, dm in enumerate(days):
                if (name, i) in failed:
                    continue
                code = grid[name].get(dm.iso, "")
                if code in LEAVE_CODES or code == "24":
                    continue
                if dm.iso in force_avoid[name] or name in day_only_set:
                    continue
                gap24 = _days_without_24_before(name, i, days, grid)
                idle_before = _idle_empty_streak_before(name, i, days, grid)
                must = gap24 >= IDLE_24_GAP_MAX or idle_before >= IDLE_24_GAP_MAX
                soft = (
                    not must
                    and gap24 >= IDLE_24_GAP_SOFT
                    and hours[name] < min_shift[name]
                )
                if not must and not soft:
                    continue
                urgency = gap24 * 10 + idle_before + (100 if must else 0)
                score = (urgency, -_acc(name))
                if worst is None or score > worst[0]:
                    worst = (score, name, i)
            # 24 sonrası uzun boş seri (görünür 4–5 gün boşluk)
            for i, dm in enumerate(days):
                if grid[name].get(dm.iso, "") != "24":
                    continue
                empty_after = 0
                target_j: int | None = None
                for j in range(i + 1, len(days)):
                    nxt = grid[name].get(days[j].iso, "")
                    if nxt == "24":
                        break
                    if nxt == "":
                        empty_after += 1
                        if empty_after == IDLE_24_GAP_SOFT and target_j is None:
                            target_j = j
                    elif nxt not in LEAVE_CODES:
                        break
                if empty_after < IDLE_24_GAP_MAX or target_j is None:
                    continue
                # Trailing / uzun boşlukta soft+1 … max de dene
                cand_targets = [target_j]
                seen_empty = 0
                for j in range(i + 1, len(days)):
                    nxt = grid[name].get(days[j].iso, "")
                    if nxt == "24":
                        break
                    if nxt == "":
                        seen_empty += 1
                        if (
                            seen_empty > IDLE_24_GAP_SOFT
                            and seen_empty <= IDLE_24_GAP_MAX
                            and j not in cand_targets
                        ):
                            cand_targets.append(j)
                    elif nxt not in LEAVE_CODES:
                        break
                for tj in cand_targets:
                    if (name, tj) in failed:
                        continue
                    if days[tj].iso in force_avoid[name] or name in day_only_set:
                        continue
                    urgency = 200 + empty_after * 10
                    score = (urgency, -_acc(name))
                    if worst is None or score > worst[0]:
                        worst = (score, name, tj)
                        break
        if worst is None:
            break
        _, name, i = worst
        placed = False
        try_indices = list(range(i, max(-1, i - IDLE_24_GAP_MAX), -1))
        run_start = i
        while run_start > 0:
            code = grid[name].get(days[run_start - 1].iso, "")
            if code in LEAVE_CODES or code == "24":
                break
            run_start -= 1
        run_end = i
        while run_end + 1 < len(days):
            code = grid[name].get(days[run_end + 1].iso, "")
            if code in LEAVE_CODES or code == "24":
                break
            run_end += 1
        if run_end - run_start + 1 > IDLE_24_GAP_MAX:
            for j in range(run_start, run_end + 1):
                if j not in try_indices:
                    try_indices.append(j)
        for pi in range(i - 1, -1, -1):
            if grid[name].get(days[pi].iso, "") == "24":
                for j in range(pi + 1, len(days)):
                    code = grid[name].get(days[j].iso, "")
                    if code == "24":
                        break
                    if code == "":
                        if j not in try_indices:
                            try_indices.append(j)
                        continue
                    if code not in LEAVE_CODES:
                        break
                break
        for j in try_indices:
            if _try_assign_24_for_gap(
                name,
                j,
                days,
                grid,
                hours,
                n8,
                n24,
                prefer_48h_after_24=prefer_48h_after_24,
                force_avoid=force_avoid,
                day_only_set=day_only_set,
            ):
                placed = True
                break
        if placed:
            failed.clear()
            continue
        failed.add((name, i))
        if len(failed) > len(STAFF_NURSES) * len(days):
            break


def _max_days_without_24_for(
    name: str,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    best = 0
    run = 0
    for dm in days:
        code = grid[name].get(dm.iso, "")
        if code in LEAVE_CODES or code == "24":
            run = 0
        else:
            run += 1
            best = max(best, run)
    return best


def _max_empty_between_24_for(
    name: str,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    best = 0
    for i, dm in enumerate(days):
        if grid[name].get(dm.iso, "") != "24":
            continue
        run = 0
        for j in range(i + 1, len(days)):
            code = grid[name].get(days[j].iso, "")
            if code == "24":
                best = max(best, run)
                break
            if code == "":
                run += 1
                best = max(best, run)
            elif code not in LEAVE_CODES:
                break
        else:
            best = max(best, run)
    return best


def _can_remove_24_without_gap_violation(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    iso = days[day_index].iso
    if grid[name].get(iso, "") != "24":
        return False
    grid[name][iso] = ""
    ok = (
        _max_days_without_24_for(name, days, grid) <= IDLE_24_GAP_MAX
        and _max_empty_between_24_for(name, days, grid) <= IDLE_24_GAP_MAX
    )
    grid[name][iso] = "24"
    return ok


def _assign_special_work_day(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    prefer_work: dict[str, set[str]],
) -> bool:
    """Özel «çalışsın» günü: dinlenme yumuşatılır; gece doluysa partner değiştirilir."""
    iso = days[day_index].iso
    code = grid[name].get(iso, "")
    if code in WORK_CODES or code in LEAVE_CODES:
        return False

    dm = days[day_index]
    prev_night = (
        day_index > 0
        and grid[name].get(days[day_index - 1].iso, "") in ("16", "24")
    )
    next_night = (
        day_index + 1 < len(days)
        and grid[name].get(days[day_index + 1].iso, "") in ("16", "24")
    )

    def _try_8() -> bool:
        if not dm.is_weekday or n8[name] >= EIGHT_PER_PERSON_MAX:
            return False
        if (
            _consecutive_8_streak_if_8(name, day_index, days, grid)
            > CONSECUTIVE_8_STREAK_MAX
        ):
            return False
        prev = grid[name].get(iso, "")
        grid[name][iso] = "8"
        if prev == "24":
            hours[name] -= 16
            n24[name] -= 1
        elif prev != "8":
            hours[name] += 8
        n8[name] += 1
        return True

    def _try_24(*, force_swap: bool) -> bool:
        if _gun_asiri_streak_over(
            name, day_index, days, grid, cap=GUN_ASIRI_STREAK_ABSOLUTE
        ):
            return False
        if day_index + 1 < len(days):
            nxt = days[day_index + 1]
            if grid[name].get(nxt.iso) == "8":
                grid[name][nxt.iso] = ""
                hours[name] -= 8
                n8[name] -= 1

        def _apply_24() -> None:
            prev = grid[name].get(iso, "")
            grid[name][iso] = "24"
            if prev == "8":
                hours[name] += 16
                n8[name] -= 1
            elif prev != "24":
                hours[name] += 24
            n24[name] += 1

        if _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY:
            _apply_24()
            return True
        if not force_swap:
            return False
        partners = [
            n for n in STAFF_NURSES if n != name and grid[n].get(iso) == "24"
        ]

        def _swap_rank(p: str) -> tuple:
            return (0 if iso in prefer_work[p] else 1, n24[p], p)

        for p in sorted(partners, key=_swap_rank):
            if iso in prefer_work[p]:
                continue
            grid[p][iso] = ""
            hours[p] -= 24
            n24[p] -= 1
            _apply_24()
            return True
        for p in sorted(partners, key=_swap_rank):
            grid[p][iso] = ""
            hours[p] -= 24
            n24[p] -= 1
            _apply_24()
            return True
        return False

    if prev_night or next_night:
        if _try_8():
            return True
        return _try_24(force_swap=True)
    if _try_24(force_swap=True):
        return True
    return _try_8()


def _enforce_special_work_days(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    prefer_work: dict[str, set[str]],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
) -> bool:
    """Özel koşul «çalışsın»: boş kalan zorunlu günlerde mesai ata (24 veya 8)."""
    if not any(prefer_work[n] for n in STAFF_NURSES):
        return False
    changed = False
    for _ in range(64):
        placed_any = False
        targets: list[tuple[int, int, str]] = []
        for name in STAFF_NURSES:
            pw = prefer_work[name]
            if not pw:
                continue
            missing = sum(
                1
                for iso in pw
                if grid[name].get(iso, "") not in WORK_CODES
                and grid[name].get(iso, "") not in LEAVE_CODES
            )
            if not missing:
                continue
            for i, dm in enumerate(days):
                iso = dm.iso
                if iso not in pw:
                    continue
                code = grid[name].get(iso, "")
                if code in WORK_CODES or code in LEAVE_CODES:
                    continue
                if iso in force_avoid[name] or name in day_only_set:
                    continue
                targets.append((missing, i, name))
        targets.sort()
        for _, i, name in targets:
            if _assign_special_work_day(
                name, i, days, grid, hours, n8, n24, prefer_work
            ):
                placed_any = True
                changed = True
                break
        if not placed_any:
            break
    return changed


def _boost_ist_shift_hours(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
) -> bool:
    """İST kullanan personel: istek günleri kotadan düşülmez; kalan günlerde banda çek."""
    targets = [
        n
        for n in STAFF_NURSES
        if _uses_ist_only_leave(n, ist_count=ist_count, yi_hours=yi_hours, rp_hours=rp_hours)
    ]
    if not targets:
        return False

    def accounted(n: str) -> int:
        return _balance_metric(n, hours, yi_hours, rp_hours, ist_count)

    changed = False
    for _ in range(96):
        goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
        vals = [accounted(n) for n in STAFF_NURSES]
        if max(vals) - min(vals) <= HOURS_BALANCE_TOLERANCE:
            break
        fixed = False
        behind = sorted(
            [n for n in targets if accounted(n) < goal - 4],
            key=lambda n: (accounted(n), n),
        )
        for name in behind:
            slots: list[tuple[int, int]] = []
            for i, dm in enumerate(days):
                iso = dm.iso
                if iso in force_avoid[name] or name in day_only_set:
                    continue
                code = grid[name].get(iso, "")
                if code in LEAVE_CODES or code in WORK_CODES:
                    continue
                slots.append((_idle_empty_streak_before(name, i, days, grid), i))
            slots.sort(reverse=True)
            for _, i in slots:
                if _try_assign_24_catchup(
                    name,
                    i,
                    days,
                    grid,
                    hours,
                    n8,
                    n24,
                    force_avoid=force_avoid,
                    day_only_set=day_only_set,
                    prefer_48h_after_24=prefer_48h_after_24,
                    accounted_fn=accounted,
                    goal=goal,
                    relax_rest=True,
                ):
                    fixed = True
                    changed = True
                    break
                dm = days[i]
                if (
                    dm.is_weekday
                    and n8[name] < EIGHT_PER_PERSON_MAX
                    and _consecutive_8_streak_if_8(name, i, days, grid)
                    <= CONSECUTIVE_8_STREAK_MAX
                ):
                    iso = dm.iso
                    grid[name][iso] = "8"
                    hours[name] += 8
                    n8[name] += 1
                    fixed = True
                    changed = True
                    break
            if fixed:
                break
        if not fixed:
            break
    return changed


def _enforce_24_after_ist(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n16: dict[str, int],
    n24: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
    *,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
    prefer_48h_after_24: bool,
) -> bool:
    """İST ertesi takvim günü mutlaka 24 nöbet."""
    changed = False

    def accounted(n: str) -> int:
        return _balance_metric(n, hours, yi_hours, rp_hours, ist_count)

    for _ in range(32):
        placed = False
        for name in STAFF_NURSES:
            for i in range(1, len(days)):
                if grid[name].get(days[i - 1].iso, "") != "İST":
                    continue
                iso = days[i].iso
                code = grid[name].get(iso, "")
                if code in LEAVE_CODES or iso in force_avoid[name] or name in day_only_set:
                    continue
                if code == "24":
                    continue
                if code == "8":
                    grid[name][iso] = ""
                    hours[name] -= 8
                    n8[name] -= 1
                elif code == "16":
                    grid[name][iso] = ""
                    hours[name] -= 16
                    n16[name] -= 1
                if _gun_asiri_streak_over(
                    name, i, days, grid, cap=GUN_ASIRI_STREAK_ABSOLUTE
                ):
                    continue
                if i + 1 < len(days):
                    nxt = days[i + 1]
                    if grid[name].get(nxt.iso) == "8":
                        grid[name][nxt.iso] = ""
                        hours[name] -= 8
                        n8[name] -= 1

                def _apply_24() -> None:
                    grid[name][iso] = "24"
                    hours[name] += 24
                    n24[name] += 1

                if _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY:
                    _apply_24()
                    placed = True
                    changed = True
                    break

                partners = [
                    n for n in STAFF_NURSES if n != name and grid[n].get(iso) == "24"
                ]
                for p in sorted(
                    partners,
                    key=lambda n: (
                        0
                        if _can_remove_24_without_gap_violation(n, i, days, grid)
                        else 1,
                        accounted(n),
                        n24[n],
                        n,
                    ),
                ):
                    if not _can_remove_24_without_gap_violation(p, i, days, grid):
                        continue
                    grid[p][iso] = ""
                    hours[p] -= 24
                    n24[p] -= 1
                    _apply_24()
                    placed = True
                    changed = True
                    break
                if not placed:
                    for p in sorted(
                        partners,
                        key=lambda n: (accounted(n), n24[n], n),
                    ):
                        grid[p][iso] = ""
                        hours[p] -= 24
                        n24[p] -= 1
                        _apply_24()
                        placed = True
                        changed = True
                        break
                if placed:
                    break
            if placed:
                break
        if not placed:
            break
    return changed


def _ensure_two_nights_per_day(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n16: dict[str, int],
    n24: dict[str, int],
    *,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
    prefer_48h_after_24: bool,
) -> None:
    """Her gün 2× gece nöbeti (son pass — İST/özel koşul sonrası)."""
    for idx, dm in enumerate(days):
        iso = dm.iso
        while _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY:
            upgraded = False
            for n in STAFF_NURSES:
                if grid[n].get(iso) != "8":
                    continue
                if iso in force_avoid[n] or n in day_only_set:
                    continue
                grid[n][iso] = "24"
                hours[n] += 16
                n8[n] -= 1
                n24[n] += 1
                upgraded = True
                break
            if upgraded:
                continue
            filled = False
            for cap in (GUN_ASIRI_STREAK_MAX, GUN_ASIRI_STREAK_ABSOLUTE):
                pool = [
                    n
                    for n in STAFF_NURSES
                    if n not in day_only_set
                    and iso not in force_avoid[n]
                    and grid[n].get(iso, "") not in LEAVE_CODES
                    and grid[n].get(iso, "") in ("", "8")
                    and not _blocked_by_rest(
                        n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                    )
                    and _gun_asiri_streak_if_24(n, idx, days, grid) <= cap
                    and _rest_allows_24(n, idx, days, grid)
                ]
                if not pool:
                    continue
                pick = sorted(pool, key=lambda n: (n24[n], n))[0]
                if not _enforce_rest_before_24(pick, idx, days, grid, hours, n8):
                    continue
                prev = grid[pick].get(iso, "")
                grid[pick][iso] = "24"
                if prev == "8":
                    hours[pick] += 16
                    n8[pick] -= 1
                else:
                    hours[pick] += 24
                n24[pick] += 1
                filled = True
                break
            if filled:
                continue
            pool16 = [
                n
                for n in STAFF_NURSES
                if n not in day_only_set
                and iso not in force_avoid[n]
                and not grid[n].get(iso)
                and not _blocked_by_rest(
                    n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                )
            ]
            if pool16:
                pick = sorted(pool16, key=lambda n: (n16[n], n))[0]
                grid[pick][iso] = "16"
                hours[pick] += 16
                n16[pick] += 1
            else:
                # Son çare: dinlenme yumuşat — günde 2 gece zorunlu
                hard16 = [
                    n
                    for n in STAFF_NURSES
                    if n not in day_only_set
                    and iso not in force_avoid[n]
                    and grid[n].get(iso, "") == ""
                ]
                if hard16:
                    pick = sorted(hard16, key=lambda n: (n16[n], n))[0]
                    grid[pick][iso] = "16"
                    hours[pick] += 16
                    n16[pick] += 1
                else:
                    break


def _try_assign_24_catchup(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    *,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
    prefer_48h_after_24: bool,
    accounted_fn,
    goal: int,
    relax_rest: bool = False,
) -> bool:
    """Çalışmasın koşulu olan personele, izin verilen günde mesai ekle (denge için)."""
    iso = days[day_index].iso
    code = grid[name].get(iso, "")
    if code in LEAVE_CODES or code == "24":
        return False
    if iso in force_avoid[name] or name in day_only_set:
        return False
    if (
        not relax_rest
        and _blocked_by_rest(
            name, day_index, days, grid, prefer_48h_after_24=prefer_48h_after_24
        )
    ):
        return False
    if _gun_asiri_streak_over(
        name, day_index, days, grid, cap=GUN_ASIRI_STREAK_ABSOLUTE
    ):
        return False
    if relax_rest:
        if day_index + 1 < len(days):
            nxt = days[day_index + 1]
            nxt_code = grid[name].get(nxt.iso, "")
            if nxt_code == "8":
                grid[name][nxt.iso] = ""
                hours[name] -= 8
                n8[name] -= 1
            elif nxt_code == "24" and nxt.iso not in force_avoid[name]:
                grid[name][nxt.iso] = ""
                hours[name] -= 24
                n24[name] -= 1
    elif not _enforce_rest_before_24(name, day_index, days, grid, hours, n8):
        return False

    def _apply_24() -> None:
        prev = grid[name].get(iso, "")
        grid[name][iso] = "24"
        if prev == "8":
            hours[name] += 16
            n8[name] -= 1
        else:
            hours[name] += 24
        n24[name] += 1

    if _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY:
        _apply_24()
        return True

    partners = [
        n for n in STAFF_NURSES if n != name and grid[n].get(iso) == "24"
    ]
    recv_acc = accounted_fn(name)
    for p in sorted(partners, key=lambda n: (-accounted_fn(n), n24[n], n)):
        if accounted_fn(p) <= recv_acc + 8:
            continue
        grid[p][iso] = ""
        hours[p] -= 24
        n24[p] -= 1
        _apply_24()
        return True
    return False


def _boost_special_avoid_hours(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
    force_avoid: dict[str, set[str]],
    *,
    prefer_48h_after_24: bool,
    day_only_set: set[str],
) -> bool:
    """Çalışmasın günleri olan personel: diğer günlerde ortalama mesai bandına çek."""
    targets = [n for n in STAFF_NURSES if force_avoid[n]]
    if not targets:
        return False

    def accounted(n: str) -> int:
        return _balance_metric(n, hours, yi_hours, rp_hours, ist_count)

    changed = False
    for _ in range(96):
        goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
        vals = [accounted(n) for n in STAFF_NURSES]
        if max(vals) - min(vals) <= HOURS_BALANCE_TOLERANCE:
            break
        fixed = False
        behind = sorted(
            [n for n in targets if accounted(n) < goal - 4],
            key=lambda n: (accounted(n), n),
        )
        for name in behind:
            slots: list[tuple[int, int, int]] = []
            for i, dm in enumerate(days):
                iso = dm.iso
                if iso in force_avoid[name] or name in day_only_set:
                    continue
                code = grid[name].get(iso, "")
                if code in LEAVE_CODES or code in WORK_CODES:
                    continue
                empty_run = _idle_empty_streak_before(name, i, days, grid)
                slots.append((empty_run, i, accounted(name)))
            slots.sort(reverse=True)
            for _, i, _ in slots:
                if _try_assign_24_catchup(
                    name,
                    i,
                    days,
                    grid,
                    hours,
                    n8,
                    n24,
                    force_avoid=force_avoid,
                    day_only_set=day_only_set,
                    prefer_48h_after_24=prefer_48h_after_24,
                    accounted_fn=accounted,
                    goal=goal,
                    relax_rest=True,
                ):
                    fixed = True
                    changed = True
                    break
                dm = days[i]
                if (
                    dm.is_weekday
                    and n8[name] < EIGHT_PER_PERSON_MAX
                    and _consecutive_8_streak_if_8(name, i, days, grid)
                    <= CONSECUTIVE_8_STREAK_MAX
                ):
                    iso = dm.iso
                    grid[name][iso] = "8"
                    hours[name] += 8
                    n8[name] += 1
                    fixed = True
                    changed = True
                    break
            if fixed:
                break
        if not fixed:
            break
    return changed


def _enforce_hours_balance(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
    max_passes: int = 120,
) -> bool:
    """Aktif personel (Yİ/RP hariç) arası mesai bandı; İST kotadan düşülmez."""

    peers = _balance_peer_names(yi_hours, rp_hours)

    def accounted(n: str) -> int:
        return _balance_metric(n, hours, yi_hours, rp_hours, ist_count)

    def _strip_next_8_if_safe(recv: str, i: int) -> bool:
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
        if days[i].iso in force_avoid[recv]:
            return False
        if recv in day_only_set:
            return False
        if _blocked_by_rest(recv, i, days, grid, prefer_48h_after_24=prefer_48h_after_24):
            return False
        if i + 1 < len(days) and grid[recv].get(days[i + 1].iso, "") in ("16", "24"):
            return False
        if _gun_asiri_streak_over(recv, i, days, grid):
            return False
        return _strip_next_8_if_safe(recv, i)

    changed = False
    for _ in range(max_passes):
        vals = {n: accounted(n) for n in peers}
        if max(vals.values()) - min(vals.values()) <= HOURS_BALANCE_TOLERANCE:
            break
        moved = False
        hi = max(peers, key=lambda n: (vals[n], n))
        lo = min(peers, key=lambda n: (vals[n], n))
        if vals[hi] - vals[lo] <= HOURS_BALANCE_TOLERANCE:
            break

        for i, dm in enumerate(days):
            if grid[hi].get(dm.iso, "") != "24" or grid[lo].get(dm.iso, "") != "8":
                continue
            if i >= 1 and grid[lo].get(days[i - 1].iso, "") in ("16", "24"):
                continue
            if n8[hi] >= EIGHT_PER_PERSON_MAX or dm.iso in force_avoid[hi]:
                continue
            if not _can_assign_8(hi, i, days, grid):
                continue
            if _gun_asiri_streak_over(lo, i, days, grid):
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
            changed = True
            continue

        for i, dm in enumerate(days):
            if grid[hi].get(dm.iso, "") != "24":
                continue
            if not _can_remove_24_without_gap_violation(hi, i, days, grid):
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
            changed = True
            continue

        for i, dm in enumerate(days):
            if dm.is_weekend:
                continue
            if grid[hi].get(dm.iso, "") != "8" or grid[lo].get(dm.iso, ""):
                continue
            if dm.iso in force_avoid[lo]:
                continue
            if _blocked_by_rest(lo, i, days, grid, prefer_48h_after_24=prefer_48h_after_24):
                continue
            if n8[lo] >= EIGHT_PER_PERSON_MAX or n8[hi] <= EIGHT_PER_PERSON_MIN:
                continue
            if sum(n8.values()) <= EIGHT_PER_PERSON_MIN * len(STAFF_NURSES):
                continue
            if not _can_assign_8(lo, i, days, grid):
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
            changed = True
            continue

        mids = [n for n in sorted(peers, key=lambda n: vals[n]) if n not in (hi, lo)]
        for mid in mids:
            for i, dm in enumerate(days):
                if grid[hi].get(dm.iso, "") != "24" or grid[mid].get(dm.iso, "") != "8":
                    continue
                if i >= 1 and grid[mid].get(days[i - 1].iso, "") in ("16", "24"):
                    continue
                if n8[hi] >= EIGHT_PER_PERSON_MAX or dm.iso in force_avoid[hi]:
                    continue
                if not _can_assign_8(hi, i, days, grid):
                    continue
                if _gun_asiri_streak_over(mid, i, days, grid):
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
        if moved:
            changed = True
            continue

        his = sorted(peers, key=lambda n: (-vals[n], -n24[n], n))
        los = sorted(peers, key=lambda n: (vals[n], n24[n], n))
        for hi2 in his:
            for lo2 in los:
                if hi2 == lo2 or vals[hi2] - vals[lo2] <= HOURS_BALANCE_TOLERANCE:
                    continue
                for i, dm in enumerate(days):
                    if grid[hi2].get(dm.iso, "") != "24":
                        continue
                    if not _can_remove_24_without_gap_violation(hi2, i, days, grid):
                        continue
                    if not _can_take_24(lo2, i):
                        continue
                    grid[hi2][dm.iso] = ""
                    grid[lo2][dm.iso] = "24"
                    hours[hi2] -= 24
                    hours[lo2] += 24
                    n24[hi2] -= 1
                    n24[lo2] += 1
                    moved = True
                    break
                if moved:
                    break
                for i, dm in enumerate(days):
                    if grid[hi2].get(dm.iso, "") != "24" or grid[lo2].get(dm.iso, "") != "8":
                        continue
                    if i >= 1 and grid[lo2].get(days[i - 1].iso, "") in ("16", "24"):
                        continue
                    if n8[hi2] >= EIGHT_PER_PERSON_MAX or dm.iso in force_avoid[hi2]:
                        continue
                    if _gun_asiri_streak_over(lo2, i, days, grid):
                        continue
                    if not _strip_next_8_if_safe(lo2, i):
                        continue
                    if not _can_assign_8(hi2, i, days, grid):
                        continue
                    grid[hi2][dm.iso] = "8"
                    grid[lo2][dm.iso] = "24"
                    hours[hi2] -= 16
                    hours[lo2] += 16
                    n24[hi2] -= 1
                    n24[lo2] += 1
                    n8[hi2] += 1
                    n8[lo2] -= 1
                    moved = True
                    break
                if moved:
                    break
            if moved:
                break
        if not moved:
            break
        changed = True
    return changed


def _boost_peer_hours(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n24: dict[str, int],
    yi_hours: dict[str, int],
    rp_hours: dict[str, int],
    ist_count: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
) -> bool:
    """Aktif personeli (Yİ/RP hariç) ortalama bant altına düşmüşse catch-up 24 ekle."""
    peers = _balance_peer_names(yi_hours, rp_hours)
    if len(peers) < 2:
        return False

    def accounted(n: str) -> int:
        return _balance_metric(n, hours, yi_hours, rp_hours, ist_count)

    changed = False
    for _ in range(96):
        goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
        if _peer_hours_spread(hours, yi_hours, rp_hours, ist_count) <= HOURS_BALANCE_TOLERANCE:
            break
        behind = sorted(
            [n for n in peers if accounted(n) < goal - 4],
            key=lambda n: (accounted(n), n),
        )
        if not behind:
            break
        fixed = False
        for name in behind:
            if hours[name] >= MAX_MONTHLY_HOURS - 8:
                continue
            slots: list[tuple[int, int]] = []
            for i, dm in enumerate(days):
                iso = dm.iso
                if iso in force_avoid[name] or name in day_only_set:
                    continue
                code = grid[name].get(iso, "")
                if code in LEAVE_CODES or code == "24":
                    continue
                empty_run = _idle_empty_streak_before(name, i, days, grid)
                gap24 = _days_without_24_before(name, i, days, grid)
                slots.append((empty_run + gap24, i))
            slots.sort(reverse=True)
            for _, i in slots:
                if _try_assign_24_catchup(
                    name,
                    i,
                    days,
                    grid,
                    hours,
                    n8,
                    n24,
                    force_avoid=force_avoid,
                    day_only_set=day_only_set,
                    prefer_48h_after_24=prefer_48h_after_24,
                    accounted_fn=accounted,
                    goal=goal,
                    relax_rest=True,
                ):
                    fixed = True
                    changed = True
                    break
            if fixed:
                break
        if not fixed:
            break
    return changed


def _enforce_gun_asiri_streak_caps(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    n16: dict[str, int],
    n24: dict[str, int],
    *,
    prefer_48h_after_24: bool,
    force_avoid: dict[str, set[str]],
    day_only_set: set[str],
    accounted_fn,
) -> bool:
    """Gün aşırı 24 zinciri ABSOLUTE (5) aşmasın; mümkünse MAX (4) hedefi."""
    changed = False
    for _enf in range(32):
        step = False
        for name in STAFF_NURSES:
            for i in range(len(days)):
                iso = days[i].iso
                if grid[name].get(iso) != "24":
                    continue
                streak = _gun_asiri_streak_if_24(name, i, days, grid)
                if streak > GUN_ASIRI_STREAK_ABSOLUTE:
                    grid[name][iso] = ""
                    hours[name] -= 24
                    n24[name] -= 1
                    step = True
                    changed = True
                    idx = i
                    dm = days[idx]
                    while _staff_night_count(grid, dm.iso) < NIGHT_SHIFTS_PER_DAY:
                        pool = [
                            n
                            for n in STAFF_NURSES
                            if n != name
                            and n not in day_only_set
                            and dm.iso not in force_avoid[n]
                            and grid[n].get(dm.iso, "") not in LEAVE_CODES
                            and grid[n].get(dm.iso, "") in ("", "8")
                            and not _blocked_by_rest(
                                n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                            )
                            and _gun_asiri_streak_if_24(n, idx, days, grid)
                            <= GUN_ASIRI_STREAK_ABSOLUTE
                            and _rest_allows_24(n, idx, days, grid)
                        ]
                        if not pool:
                            break
                        pick = sorted(pool, key=lambda n: (accounted_fn(n), n24[n], n))[0]
                        if not _enforce_rest_before_24(pick, idx, days, grid, hours, n8):
                            break
                        prev = grid[pick].get(dm.iso, "")
                        grid[pick][dm.iso] = "24"
                        if prev == "8":
                            hours[pick] += 16
                            n8[pick] -= 1
                        else:
                            hours[pick] += 24
                        n24[pick] += 1
                    break
                if streak <= GUN_ASIRI_STREAK_MAX:
                    continue
                for taker_cap in (
                    GUN_ASIRI_STREAK_MAX,
                    GUN_ASIRI_STREAK_ABSOLUTE,
                ):
                    takers = [
                        o
                        for o in STAFF_NURSES
                        if o != name
                        and o not in day_only_set
                        and iso not in force_avoid[o]
                        and grid[o].get(iso, "") in ("", "8")
                        and not _blocked_by_rest(
                            o, i, days, grid, prefer_48h_after_24=prefer_48h_after_24
                        )
                        and _gun_asiri_streak_if_24(o, i, days, grid) <= taker_cap
                    ]
                    if not takers:
                        if streak > GUN_ASIRI_STREAK_ABSOLUTE:
                            grid[name][iso] = ""
                            hours[name] -= 24
                            n24[name] -= 1
                            step = True
                            changed = True
                        break
                    other = sorted(takers, key=lambda o: (accounted_fn(o), n24[o], o))[0]
                    prev_o = grid[other].get(iso, "")
                    grid[name][iso] = ""
                    grid[other][iso] = "24"
                    hours[name] -= 24
                    if prev_o == "8":
                        hours[other] += 16
                        n8[other] -= 1
                    else:
                        hours[other] += 24
                    n24[name] -= 1
                    n24[other] += 1
                    step = True
                    changed = True
                    break
                if step:
                    break
            if step:
                break
        if not step:
            break

    for _force in range(16):
        offender: tuple[str, int] | None = None
        worst = 0
        for name in STAFF_NURSES:
            for i in range(len(days)):
                if grid[name].get(days[i].iso, "") != "24":
                    continue
                streak = _gun_asiri_streak_if_24(name, i, days, grid)
                if streak > GUN_ASIRI_STREAK_ABSOLUTE and streak > worst:
                    worst = streak
                    offender = (name, i)
        if not offender:
            break
        changed = True
        name, i = offender
        iso = days[i].iso
        grid[name][iso] = ""
        hours[name] -= 24
        n24[name] -= 1
        idx = i
        dm = days[idx]
        while _staff_night_count(grid, dm.iso) < NIGHT_SHIFTS_PER_DAY:
            pool = [
                n
                for n in STAFF_NURSES
                if n != name
                and n not in day_only_set
                and dm.iso not in force_avoid[n]
                and grid[n].get(dm.iso, "") not in LEAVE_CODES
                and grid[n].get(dm.iso, "") in ("", "8")
                and not _blocked_by_rest(
                    n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                )
                and _gun_asiri_streak_if_24(n, idx, days, grid) <= GUN_ASIRI_STREAK_ABSOLUTE
                and _rest_allows_24(n, idx, days, grid)
            ]
            if not pool:
                break
            pick = sorted(pool, key=lambda n: (accounted_fn(n), n24[n], n))[0]
            if not _enforce_rest_before_24(pick, idx, days, grid, hours, n8):
                pool = [x for x in pool if x != pick]
                if not pool:
                    break
                pick = sorted(pool, key=lambda n: (accounted_fn(n), n24[n], n))[0]
                if not _enforce_rest_before_24(pick, idx, days, grid, hours, n8):
                    break
            prev = grid[pick].get(dm.iso, "")
            grid[pick][dm.iso] = "24"
            if prev == "8":
                hours[pick] += 16
                n8[pick] -= 1
            else:
                hours[pick] += 24
            n24[pick] += 1
    return changed


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


def _gun_asiri_streak_over(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    *,
    cap: int | None = None,
) -> bool:
    """Bugün 24 yazılırsa cap aşılır mı."""
    limit = cap if cap is not None else GUN_ASIRI_STREAK_MAX
    return _gun_asiri_streak_if_24(name, day_index, days, grid) > limit


def _pair_key(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a < b else (b, a)


def _partners_on_24_today(
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> list[str]:
    iso = days[day_index].iso
    return [n for n in STAFF_NURSES if grid[n].get(iso) == "24"]


def _pair24_prior_count(
    a: str,
    b: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Bugünden önce bu ay a+b kaç kez aynı gün 24 tutmuş."""
    n = 0
    for j in range(day_index - 1, -1, -1):
        iso = days[j].iso
        if grid[a].get(iso) == "24" and grid[b].get(iso) == "24":
            n += 1
    return n


def _pair24_last_day(
    a: str,
    b: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int | None:
    for j in range(day_index - 1, -1, -1):
        iso = days[j].iso
        if grid[a].get(iso) == "24" and grid[b].get(iso) == "24":
            return j
    return None


def _pair24_near_streak(
    a: str,
    b: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Bugün eşleşirlerse son ardışık «yakın» (≤GAP gün) birlikte 24 kaçı olur."""
    idxs = [
        j
        for j in range(day_index)
        if grid[a].get(days[j].iso, "") == "24" and grid[b].get(days[j].iso, "") == "24"
    ]
    if not idxs:
        return 0
    streak = 1
    for k in range(len(idxs) - 1, 0, -1):
        if idxs[k] - idxs[k - 1] <= PAIR24_RECENT_GAP:
            streak += 1
        else:
            break
    return streak


def _pair24_soft_penalty(
    candidate: str,
    partner: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Aynı gün 24'te partner ile eşleşme — düşük öncelikli yumuşak ceza."""
    if candidate == partner:
        return 999
    prior = _pair24_prior_count(candidate, partner, day_index, days, grid)
    if prior == 0:
        return 0
    pen = prior * PAIR24_PRIOR_WEIGHT
    near_streak = _pair24_near_streak(candidate, partner, day_index, days, grid)
    if near_streak >= 2:
        # Üst üste 3. kez aynı ikili (ör. gün 6-8-10) — güçlü ama yumuşak ceza
        pen += PAIR24_THIRD_NEAR + 16
    else:
        last_j = _pair24_last_day(candidate, partner, day_index, days, grid)
        if last_j is not None and day_index - last_j <= PAIR24_RECENT_GAP:
            pen += PAIR24_NEAR_REPEAT
    return pen


def _pair24_month_count(
    a: str,
    b: str,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    return sum(
        1
        for dm in days
        if grid[a].get(dm.iso, "") == "24" and grid[b].get(dm.iso, "") == "24"
    )


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
    """24 sonrası boş günde 8 serpiştir (düşük = tercih)."""
    if day_index >= 2 and grid[name].get(days[day_index - 2].iso, "") == "24":
        mid = grid[name].get(days[day_index - 1].iso, "")
        if mid in ("", "Yİ", "RP", "İST"):
            return 0
    if day_index >= 3 and grid[name].get(days[day_index - 3].iso, "") == "24":
        return 1
    return 4


def _prev_day_is_8(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    if day_index <= 0:
        return False
    return grid[name].get(days[day_index - 1].iso, "") == "8"


def _consecutive_8_streak_if_8(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    """Bugün «8» yazılırsa kaç gün üst üste 8 olur."""
    streak = 1
    j = day_index - 1
    while j >= 0 and grid[name].get(days[j].iso, "") == "8":
        streak += 1
        j -= 1
    return streak


def _can_assign_8(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    if day_index > 0 and grid[name].get(days[day_index - 1].iso, "") in ("16", "24"):
        return False
    return (
        _consecutive_8_streak_if_8(name, day_index, days, grid) <= CONSECUTIVE_8_STREAK_MAX
    )


def _max_consecutive_8_streak(
    name: str,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> int:
    best = 0
    streak = 0
    for dm in days:
        if grid[name].get(dm.iso, "") == "8":
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def _first_day_after_leave(
    name: str,
    day_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
) -> bool:
    """Yİ/RP/İST bloğundan sonraki ilk takvim günü (hafta sonu dahil → 24)."""
    if day_index <= 0:
        return False
    today = grid[name].get(days[day_index].iso, "")
    if today in LEAVE_CODES:
        return False
    prev = grid[name].get(days[day_index - 1].iso, "")
    return prev in ("Yİ", "RP", "İST")


def _count_consecutive_8_runs(
    grid: dict[str, dict[str, str]],
    days: list[DayMeta],
    *,
    staff_only: bool = True,
) -> int:
    """Ardışık 8 çifti sayısı (üst üste 8+8)."""
    names = list(STAFF_NURSES) if staff_only else list(ALL_NURSES)
    runs = 0
    for name in names:
        for i in range(1, len(days)):
            if grid[name].get(days[i].iso, "") != "8":
                continue
            if grid[name].get(days[i - 1].iso, "") == "8":
                runs += 1
    return runs


def _try_relocate_8_to_break_pair(
    name: str,
    from_index: int,
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    *,
    prefer_48h_after_24: bool,
) -> bool:
    """Üst üste 8+8 içindeki bir 8'i başka güne taşı (mümkünse)."""
    from_iso = days[from_index].iso
    if grid[name].get(from_iso, "") != "8":
        return False
    candidates: list[tuple[int, int, int]] = []
    for j, dm in enumerate(days):
        if j == from_index or dm.is_weekend or grid[name].get(dm.iso, ""):
            continue
        if _blocked_by_rest(
            name, j, days, grid, prefer_48h_after_24=prefer_48h_after_24
        ):
            continue
        tmp = grid[name][from_iso]
        grid[name][from_iso] = ""
        ok = _can_assign_8(name, j, days, grid) and (
            _consecutive_8_streak_if_8(name, j, days, grid) <= CONSECUTIVE_8_STREAK_SOFT
        )
        grid[name][from_iso] = tmp
        if not ok:
            continue
        gap24 = _prefer_8_after_24_gap(name, j, days, grid)
        candidates.append((gap24, _consecutive_8_streak_if_8(name, j, days, grid), j))
    if not candidates:
        return False
    _, _, dest = sorted(candidates)[0]
    grid[name][from_iso] = ""
    grid[name][days[dest].iso] = "8"
    return True


def _enforce_soft_no_consecutive_8_pairs(
    days: list[DayMeta],
    grid: dict[str, dict[str, str]],
    hours: dict[str, int],
    n8: dict[str, int],
    *,
    prefer_48h_after_24: bool,
) -> bool:
    """8+8 çiftlerini mümkünse dağıt; değişiklik oldu mu."""
    changed = False
    for name in STAFF_NURSES:
        for i in range(1, len(days)):
            iso = days[i].iso
            if grid[name].get(iso, "") != "8":
                continue
            if grid[name].get(days[i - 1].iso, "") != "8":
                continue
            # Önce ikinci 8'i taşı; olmazsa birinciyi
            for idx in (i, i - 1):
                if _try_relocate_8_to_break_pair(
                    name,
                    idx,
                    days,
                    grid,
                    prefer_48h_after_24=prefer_48h_after_24,
                ):
                    changed = True
                    break
            if changed:
                break
        if changed:
            break
    return changed


NIGHT_SHIFTS_PER_DAY = 2


def _staff_night_count(grid: dict[str, dict[str, str]], iso: str) -> int:
    return sum(1 for n in STAFF_NURSES if grid[n].get(iso, "") in ("16", "24"))


def resolve_special_day_sets(
    year: int,
    month: int,
    special_rules: list[dict[str, Any]] | None,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Özel koşul → kişi başına çalışsın / çalışmasın ISO gün kümeleri.

    Kural: {name, mode: work|avoid, dates: [iso…], weekly: bool}
    weekly=True → seçilen her tarihin hafta günü ay boyunca tekrarlanır.
    """
    work: dict[str, set[str]] = {n: set() for n in STAFF_NURSES}
    avoid: dict[str, set[str]] = {n: set() for n in STAFF_NURSES}
    if not special_rules:
        return work, avoid
    days = month_days(year, month)
    by_weekday: dict[int, list[str]] = {}
    for dm in days:
        by_weekday.setdefault(dm.weekday, []).append(dm.iso)
    iso_to_wd = {dm.iso: dm.weekday for dm in days}
    month_isos = set(iso_to_wd)

    for raw in special_rules:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        if name not in STAFF_NURSES:
            continue
        mode = str(raw.get("mode") or "").strip().lower()
        if mode in ("calissin", "çalışsın", "work"):
            mode = "work"
        elif mode in ("calismasin", "çalışmasın", "avoid"):
            mode = "avoid"
        else:
            continue
        dates_raw = raw.get("dates") or []
        if not isinstance(dates_raw, list):
            continue
        picked = [str(x).strip() for x in dates_raw if str(x).strip()]
        weekly = bool(raw.get("weekly"))
        expanded: set[str] = set()
        for iso in picked:
            if weekly and iso in iso_to_wd:
                expanded.update(by_weekday.get(iso_to_wd[iso], []))
            elif iso in month_isos:
                expanded.add(iso)
        if mode == "work":
            work[name] |= expanded
        else:
            avoid[name] |= expanded
    # Aynı gün hem work hem avoid → avoid kazanır
    for n in STAFF_NURSES:
        clash = work[n] & avoid[n]
        if clash:
            work[n] -= clash
    return work, avoid


def generate_ayilma_schedule(
    year: int,
    month: int,
    *,
    leaves: dict[str, dict[str, str]] | None = None,
    day_only: list[str] | None = None,
    prefer_48h_after_24: bool = True,
    special_rules: list[dict[str, Any]] | None = None,
    variant: int = 0,
) -> dict[str, Any]:
    """6 personel + sorumlu.

    Gülten panelde gizlenir; indirmede boş satır olarak gelir. Tüm hesaplar 6 personel kadrosuyla yapılır.

    Hafta içi: mümkünse 1×«8» + 2×«24». Hafta sonu: yalnız 2×«24» (kat-1 / 8 yok).
    Yİ/RP/İST bitişinin ertesi takvim günü nöbet (24) — hafta sonu kuralı yok.
    Yİ/RP: zorunlu nöbet = ideal−izin; kalan günlerle taban doldurulur, üstüne ek mesai olabilir.
    Düz «8» kişi başı aylık ~2–4 (hedef 3). Fazla mesai personelde aynı ~16s bantta.
    Üst üste «8» kaçınılır; mecbur kalınırsa en fazla 2 gün.
    Gün aşırı 24 zinciri: yumuşak ≤3, normal tavan 4; çok sıkışıkta 5, asla 5 üstü yok.
    «16» yalnızca 24 yazacak kimse yoksa — çok uç çare.
    Özel koşul: çalışmasın (sert) / çalışsın (yumuşak tercih).
    Çalışmasın + haftalık tekrar: bloklu günler dışında ortalama mesai bandına yetişir.
    İST: ertesi gün 24 nöbet; istek günleri kotadan düşülmez, kalan günlerle denge.
    Aynı ikili 24 nöbette mümkün olduğunca az ve üst üste tekrar etmesin (yumuşak).
    24 arası boş hücre KATİ en fazla 3 (4+ yasak); hedef 2+24+2 (zorunlu tercih, kapatılamaz).
    Aylık mesai üst sınırı 400s.
    variant>0 → eşitlikte farklı aday seç (yeniden oluştur).
    """
    # 24 sonrası 2 gün boşluk tercihi her zaman açık (UI seçeneği yok).
    prefer_48h_after_24 = True
    if not (1 <= month <= 12):
        raise ValueError("month 1–12 olmalı")
    if year < 2000 or year > 2100:
        raise ValueError("year geçersiz")

    days = month_days(year, month)
    grid = _empty_grid(year, month)
    _apply_leaves(grid, leaves)
    _clear_lead_row(grid)
    day_only_set = {str(x).strip() for x in (day_only or []) if str(x).strip()}
    prefer_work, force_avoid = resolve_special_day_sets(year, month, special_rules)
    rng = random.Random((year * 100 + month) * 10007 + int(variant or 0))
    # variant>0 iken kişi başına sabit tie-break (sıralamada her karşılaştırmada yeni random üretme)
    person_tie = {n: (rng.random() if variant else 0.0) for n in STAFF_NURSES}

    def _tie(n: str) -> float:
        return person_tie[n]

    # Yİ/RP peşin kredi; İST kotadan düşülmez.
    # Zorunlu nöbet tabanı = ideal − Yİ − RP; kalan günlerle bu taban doldurulur,
    # üzerine mevcut denge kurallarıyla ek mesai gelebilir.
    ideal = ideal_hours(year, month)
    yi_hours = {n: _yi_hours_from_grid(grid, n, days) for n in STAFF_NURSES}
    rp_hours = {n: _rp_hours_from_grid(grid, n, days) for n in STAFF_NURSES}
    ist_count = {n: _ist_day_count(grid, n, days) for n in STAFF_NURSES}
    min_shift = {
        n: max(0, ideal - yi_hours[n] - rp_hours[n]) for n in STAFF_NURSES
    }

    hours = {n: 0 for n in STAFF_NURSES}
    n8 = {n: 0 for n in STAFF_NURSES}
    n24 = {n: 0 for n in STAFF_NURSES}
    n16 = {n: 0 for n in STAFF_NURSES}
    warnings: list[str] = []
    eight_budget = EIGHT_PER_PERSON_TARGET * len(STAFF_NURSES)

    def accounted(n: str) -> int:
        return _balance_metric(n, hours, yi_hours, rp_hours, ist_count)

    def _ist_behind(n: str) -> int:
        if not _uses_ist_only_leave(
            n, ist_count=ist_count, yi_hours=yi_hours, rp_hours=rp_hours
        ):
            return 1
        peer = sum(hours[o] for o in STAFF_NURSES) / len(STAFF_NURSES)
        return 0 if hours[n] < peer - 4 else 1

    for idx, dm in enumerate(days):
        available = [
            n
            for n in STAFF_NURSES
            if not grid[n][dm.iso]
            and dm.iso not in force_avoid[n]
            and not _blocked_by_rest(n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24)
        ]

        def is_gun_asiri_candidate(n: str) -> bool:
            return _gun_asiri_24_penalty(n, idx, days, grid) > 0

        def streak_if_24(n: str) -> int:
            return _gun_asiri_streak_if_24(n, idx, days, grid)

        def over_streak(n: str) -> bool:
            return _gun_asiri_streak_over(n, idx, days, grid)

        def special_work_rank(n: str) -> int:
            # 0 = çalışsın günü (tercih); 1 = nötr; 2 = başka güne kaydır (çalışsın günü varsa)
            if dm.iso in prefer_work[n]:
                return 0
            if prefer_work[n]:
                return 2
            return 1

        def avoid_catchup_rank(n: str) -> int:
            """Çalışmasın koşulu: izin verilen günlerde ortalama mesai bandına yetiş."""
            if not force_avoid[n] or dm.iso in force_avoid[n]:
                return 1
            goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
            acc = accounted(n)
            if acc < goal - 4:
                return 0
            if acc > goal + 4:
                return 2
            return 1

        def balance_rank(n: str) -> int:
            if not any(force_avoid[x] for x in STAFF_NURSES):
                return 1
            goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
            acc = accounted(n)
            if acc < goal - 4:
                return 0
            if acc > goal + 4:
                return 2
            return 1

        def ist_catchup_rank(n: str) -> int:
            if not _uses_ist_only_leave(
                n, ist_count=ist_count, yi_hours=yi_hours, rp_hours=rp_hours
            ):
                return 1
            goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
            acc = accounted(n)
            if acc < goal - 4:
                return 0
            if acc > goal + 4:
                return 2
            return 1

        def rank_for_8(n: str) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            break24 = 0 if is_gun_asiri_candidate(n) else 1
            streak8 = _consecutive_8_streak_if_8(n, idx, days, grid)
            pair8 = 0 if streak8 <= CONSECUTIVE_8_STREAK_SOFT else 1
            after24 = _prefer_8_after_24_gap(n, idx, days, grid)
            # İzinden dönüş günü 8 değil nöbet (24)
            after_leave = 1 if _first_day_after_leave(n, idx, days, grid) else 0
            return (
                pen,
                after_leave,
                avoid_catchup_rank(n),
                ist_catchup_rank(n),
                balance_rank(n),
                pair8,
                special_work_rank(n),
                streak8,
                accounted(n),
                after24,
                n8[n],
                break24,
                _tie(n),
                n,
            )

        def rank_for_24(n: str) -> tuple:
            pen = _rest_penalty(n, idx, days, grid)
            if n in day_only_set:
                pen += 500
            partners_today = _partners_on_24_today(idx, days, grid)
            pair_pen = sum(
                _pair24_soft_penalty(n, p, idx, days, grid) for p in partners_today
            )
            over = 1 if over_streak(n) else 0
            gun = _gun_asiri_24_penalty(n, idx, days, grid)
            soft_streak = max(0, streak_if_24(n) - GUN_ASIRI_STREAK_SOFT)
            behind = 0 if hours[n] < min_shift[n] else 1
            ist_behind = _ist_behind(n)
            # İzin/istek/rapor dönüşü → önce nöbet
            after_leave = 0 if _first_day_after_leave(n, idx, days, grid) else 1
            gap24 = _days_without_24_before(n, idx, days, grid)
            gap24_prio = -min(gap24, IDLE_24_GAP_MAX)
            gap_ideal = abs(gap24 - IDLE_24_GAP_SOFT)
            return (
                pen,
                after_leave,
                ist_behind,
                avoid_catchup_rank(n),
                ist_catchup_rank(n),
                balance_rank(n),
                gap24_prio,
                gap_ideal,
                pair_pen,
                special_work_rank(n),
                over,
                soft_streak,
                accounted(n),
                streak_if_24(n),
                gun,
                behind,
                n24[n],
                _tie(n),
                n,
            )

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
            n
            for n in sorted(available, key=rank_for_8)
            if n8[n] < EIGHT_PER_PERSON_MAX and _can_assign_8(n, idx, days, grid)
        ]
        # İzinden dönüş günü: kat-1 8 yerine nöbet (24) beklenir
        if morning_cands and not dm.is_weekend:
            no_return = [
                n for n in morning_cands if not _first_day_after_leave(n, idx, days, grid)
            ]
            if no_return:
                morning_cands = no_return
        if morning_cands and _want_morning_8():
            morning = morning_cands[0]
            grid[morning][dm.iso] = "8"
            hours[morning] += 8
            n8[morning] += 1
            available = [n for n in available if n != morning]

        def _assign24(n: str) -> bool:
            nonlocal night_needed, available
            if not _enforce_rest_before_24(n, idx, days, grid, hours, n8):
                return False
            grid[n][dm.iso] = "24"
            hours[n] += 24
            n24[n] += 1
            night_needed -= 1
            available = [x for x in available if x != n]
            return True

        while night_needed > 0:
            pool = [n for n in available if n not in day_only_set]
            capped = [n for n in pool if not over_streak(n)]
            if capped:
                pool = capped
            if not pool:
                pool = [
                    n
                    for n in STAFF_NURSES
                    if n not in day_only_set
                    and dm.iso not in force_avoid[n]
                    and grid[n].get(dm.iso, "") in ("", "8")
                    and grid[n].get(dm.iso, "") not in LEAVE_CODES
                    and not _blocked_by_rest(
                        n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                    )
                ]
            if not pool:
                break
            return_pool = [n for n in pool if _first_day_after_leave(n, idx, days, grid)]
            pick_from = return_pool if return_pool else pool
            pick_from = [n for n in pick_from if _rest_allows_24(n, idx, days, grid)]
            if not pick_from:
                break
            pick = sorted(pick_from, key=rank_for_24)[0]
            prev = grid[pick].get(dm.iso, "")
            if prev == "8":
                if not _enforce_rest_before_24(pick, idx, days, grid, hours, n8):
                    available = [x for x in available if x != pick]
                    continue
                grid[pick][dm.iso] = "24"
                hours[pick] += 16
                n8[pick] -= 1
                n24[pick] += 1
                night_needed -= 1
                available = [x for x in available if x != pick]
            else:
                if not _assign24(pick):
                    available = [x for x in available if x != pick]
                    continue

        if (
            night_needed > 0
            and morning
            and grid[morning][dm.iso] == "8"
            and morning not in day_only_set
            and not over_streak(morning)
        ):
            grid[morning][dm.iso] = "24"
            hours[morning] += 16
            n8[morning] -= 1
            n24[morning] += 1
            night_needed -= 1
            morning = None

        if dm.is_weekday:
            has_morning = any(grid[n][dm.iso] in ("8", "24") for n in STAFF_NURSES)
            if not has_morning:
                warnings.append(f"{dm.iso}: Kat-1 gündüz hemşiresi atanamadı (izin/dinlenme).")
        if night_needed > 0:
            warnings.append(f"{dm.iso}: Gece nöbeti eksik ({2 - night_needed}/2).")

    # ── Post: aynı ikili çok sık 24 — mümkünse bir günü başka hemşireye kaydır ──
    for _pass in range(3):
        swapped = False
        for i, a in enumerate(STAFF_NURSES):
            for b in STAFF_NURSES[i + 1 :]:
                if _pair24_month_count(a, b, days, grid) < PAIR24_MONTHLY_SOFT:
                    continue
                pair_days = [
                    j
                    for j, dm in enumerate(days)
                    if grid[a].get(dm.iso, "") == "24" and grid[b].get(dm.iso, "") == "24"
                ]
                for j in reversed(pair_days):
                    iso = days[j].iso
                    for who, other in ((a, b), (b, a)):
                        alts = [
                            o
                            for o in STAFF_NURSES
                            if o not in (a, b)
                            and grid[o].get(iso) in ("", "8")
                            and iso not in force_avoid[o]
                            and not _blocked_by_rest(
                                o, j, days, grid, prefer_48h_after_24=prefer_48h_after_24
                            )
                            and (
                                _gun_asiri_streak_if_24(o, j, days, grid) <= GUN_ASIRI_STREAK_MAX
                            )
                        ]
                        if not alts:
                            continue

                        def alt_score(o: str, *, _other=other, _who=who) -> tuple:
                            return (
                                _pair24_prior_count(o, _other, j, days, grid)
                                + _pair24_prior_count(o, _who, j, days, grid),
                                _pair24_soft_penalty(o, _other, j, days, grid)
                                + _pair24_soft_penalty(o, _who, j, days, grid),
                                n24[o],
                                hours[o],
                                o,
                            )

                        alt = sorted(alts, key=alt_score)[0]
                        prev = grid[who].get(iso, "")
                        if prev == "8":
                            grid[who][iso] = ""
                            hours[who] -= 8
                            n8[who] -= 1
                        else:
                            grid[who][iso] = ""
                            hours[who] -= 24
                            n24[who] -= 1
                        prev_alt = grid[alt].get(iso, "")
                        if prev_alt == "8":
                            grid[alt][iso] = "24"
                            hours[alt] += 16
                            n8[alt] -= 1
                        else:
                            grid[alt][iso] = "24"
                            hours[alt] += 24
                        n24[alt] += 1
                        swapped = True
                        break
                    if swapped:
                        break
                if swapped:
                    break
            if swapped:
                break
        if not swapped:
            break

    # ── Post: streak > GUN_ASIRI_STREAK_MAX olan 24'leri mümkünse 8 ile takas ──
    for _pass in range(4):
        swapped = False
        for name in STAFF_NURSES:
            for i in range(2, len(days)):
                if grid[name].get(days[i].iso, "") != "24":
                    continue
                if _gun_asiri_streak_if_24(name, i, days, grid) <= GUN_ASIRI_STREAK_MAX:
                    # Klasik gün aşırı yumuşak takas (8 bütçesi içinde)
                    if _gun_asiri_24_penalty(name, i, days, grid) == 0:
                        continue
                    if n8[name] >= EIGHT_PER_PERSON_TARGET:
                        continue
                else:
                    # Streak > tavan: 8 max'a kadar zorla kır (16 ile değil)
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
                if iso in force_avoid[name]:
                    continue
                if not _can_assign_8(name, i, days, grid):
                    continue
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

    # ── Eksik 8 (min 2): hafta içi; üst üste 8 yazmamaya çalış ──
    for name in STAFF_NURSES:
        while n8[name] < EIGHT_PER_PERSON_MIN:
            slots: list[tuple[int, int, int]] = []
            for i, dm in enumerate(days):
                if dm.is_weekend or grid[name].get(dm.iso, ""):
                    continue
                if dm.iso in force_avoid[name]:
                    continue
                if _blocked_by_rest(
                    name, i, days, grid, prefer_48h_after_24=prefer_48h_after_24
                ):
                    continue
                if not _can_assign_8(name, i, days, grid):
                    continue
                if n8[name] >= EIGHT_PER_PERSON_MAX:
                    break
                streak8 = _consecutive_8_streak_if_8(name, i, days, grid)
                pair8 = 0 if streak8 <= CONSECUTIVE_8_STREAK_SOFT else 1
                slots.append((pair8, streak8, i))
            if not slots:
                break
            _, _, pick_i = sorted(slots)[0]
            dm = days[pick_i]
            grid[name][dm.iso] = "8"
            hours[name] += 8
            n8[name] += 1

    # ── Fazla mesai bandı: yüksek ↔ düşük (hedef ≤16s) ──
    _enforce_hours_balance(
        days,
        grid,
        hours,
        n8,
        n24,
        yi_hours,
        rp_hours,
        ist_count,
        prefer_48h_after_24=prefer_48h_after_24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
    )

    # Denge sonrası: min 8 ve hafta sonu 8 temizliği
    for name in STAFF_NURSES:
        for dm in days:
            if dm.is_weekend and grid[name].get(dm.iso, "") == "8":
                grid[name][dm.iso] = ""
                hours[name] -= 8
                n8[name] -= 1
    for name in STAFF_NURSES:
        while n8[name] < EIGHT_PER_PERSON_MIN:
            slots: list[tuple[int, int, int]] = []
            for i, dm in enumerate(days):
                if dm.is_weekend or grid[name].get(dm.iso, ""):
                    continue
                if dm.iso in force_avoid[name]:
                    continue
                if _blocked_by_rest(
                    name, i, days, grid, prefer_48h_after_24=prefer_48h_after_24
                ):
                    continue
                if not _can_assign_8(name, i, days, grid):
                    continue
                streak8 = _consecutive_8_streak_if_8(name, i, days, grid)
                pair8 = 0 if streak8 <= CONSECUTIVE_8_STREAK_SOFT else 1
                slots.append((pair8, streak8, i))
            if not slots:
                break
            _, _, pick_i = sorted(slots)[0]
            dm = days[pick_i]
            grid[name][dm.iso] = "8"
            hours[name] += 8
            n8[name] += 1

    # Üst üste 8+8: mümkünse dağıt (yumuşak)
    for _pair8 in range(24):
        if not _enforce_soft_no_consecutive_8_pairs(
            days,
            grid,
            hours,
            n8,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    # Gün aşırı zincir > MAX kır: 24'ü başka kişiye taşı
    for _sk in range(20):
        broke = False
        for name in STAFF_NURSES:
            for i in range(len(days)):
                if grid[name].get(days[i].iso, "") != "24":
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
                # 24→8 düşürme: günde zaten 2. gece varken kapsamayı 1'e indirir — yapma
            if broke:
                break
        if not broke:
            break

    # Streak kırımı sonrası tekrar denge
    _enforce_hours_balance(
        days,
        grid,
        hours,
        n8,
        n24,
        yi_hours,
        rp_hours,
        ist_count,
        prefer_48h_after_24=prefer_48h_after_24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        max_passes=80,
    )

    # Üst üste 3+ «8» kır (mecburen en fazla 2)
    for _fix8long in range(20):
        fixed = False
        for name in STAFF_NURSES:
            if _max_consecutive_8_streak(name, days, grid) <= CONSECUTIVE_8_STREAK_MAX:
                continue
            target_i: int | None = None
            run_start: int | None = None
            for i, dm in enumerate(days):
                if grid[name].get(dm.iso, "") == "8":
                    if run_start is None:
                        run_start = i
                    if i - run_start + 1 > CONSECUTIVE_8_STREAK_MAX:
                        target_i = i
                else:
                    run_start = None
            if target_i is None:
                continue
            iso = days[target_i].iso
            covered = any(
                o != name and grid[o].get(iso, "") in ("8", "24") for o in STAFF_NURSES
            )
            if covered:
                grid[name][iso] = ""
                hours[name] -= 8
                n8[name] -= 1
                fixed = True
                continue
            moved = False
            for j, dm in enumerate(days):
                if dm.is_weekend or grid[name].get(dm.iso, ""):
                    continue
                if not _can_assign_8(name, j, days, grid):
                    continue
                if _blocked_by_rest(
                    name, j, days, grid, prefer_48h_after_24=prefer_48h_after_24
                ):
                    continue
                if _prefer_8_after_24_gap(name, j, days, grid) > 0:
                    continue
                grid[name][iso] = ""
                grid[name][dm.iso] = "8"
                fixed = True
                moved = True
                break
            if not moved and not covered:
                if (
                    name not in day_only_set
                    and not _blocked_by_rest(
                        name, target_i, days, grid, prefer_48h_after_24=prefer_48h_after_24
                    )
                    and _gun_asiri_streak_if_24(name, target_i, days, grid) <= GUN_ASIRI_STREAK_MAX
                ):
                    grid[name][iso] = "24"
                    hours[name] += 16
                    n8[name] -= 1
                    n24[name] += 1
                    fixed = True
        if not fixed:
            break

    for _pair8late in range(16):
        if not _enforce_soft_no_consecutive_8_pairs(
            days,
            grid,
            hours,
            n8,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    # Çalışmasın: post-pass sızıntısını gece doldurmadan önce temizle
    for name in STAFF_NURSES:
        for iso in force_avoid[name]:
            code = grid[name].get(iso, "")
            if code not in WORK_CODES:
                continue
            h = _hours_for(code)
            grid[name][iso] = ""
            hours[name] -= h
            if code == "8":
                n8[name] -= 1
            elif code == "24":
                n24[name] -= 1
            elif code == "16":
                n16[name] -= 1

    # ── Zorunlu: her gün tam 2× gece nöbeti (post-pass sonrası boşluk kalmasın) ──
    for idx, dm in enumerate(days):
        iso = dm.iso
        bal_goal = _peer_balance_goal(hours, yi_hours, rp_hours, ist_count)
        while _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY:
            filled = False

            def _rank_night_fill(n: str) -> tuple:
                code = grid[n].get(iso, "")
                empty = 0 if code == "" else 1
                sw = 0 if iso in prefer_work[n] else (2 if prefer_work[n] else 1)
                if force_avoid[n]:
                    if iso in force_avoid[n]:
                        av = 9
                    elif accounted(n) < bal_goal - 4:
                        av = 0
                    elif accounted(n) > bal_goal + 4:
                        av = 3
                    else:
                        av = 1
                else:
                    av = 1
                acc = accounted(n)
                if any(force_avoid[x] for x in STAFF_NURSES):
                    if acc < bal_goal - 4:
                        bal = 0
                    elif acc > bal_goal + 4:
                        bal = 2
                    else:
                        bal = 1
                else:
                    bal = 1
                streak = _gun_asiri_streak_if_24(n, idx, days, grid)
                soft_over = max(0, streak - GUN_ASIRI_STREAK_SOFT)
                hard_over = max(0, streak - GUN_ASIRI_STREAK_MAX)
                gap24 = _days_without_24_before(n, idx, days, grid)
                return (
                    av,
                    bal,
                    sw,
                    empty,
                    hard_over,
                    soft_over,
                    -min(gap24, IDLE_24_GAP_MAX),
                    abs(gap24 - IDLE_24_GAP_SOFT),
                    acc,
                    n24[n],
                    n,
                )

            for cap in (GUN_ASIRI_STREAK_MAX, GUN_ASIRI_STREAK_ABSOLUTE):
                pool = [
                    n
                    for n in STAFF_NURSES
                    if n not in day_only_set
                    and iso not in force_avoid[n]
                    and grid[n].get(iso, "") not in LEAVE_CODES
                    and grid[n].get(iso, "") in ("", "8")
                    and not _blocked_by_rest(
                        n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                    )
                    and _gun_asiri_streak_if_24(n, idx, days, grid) <= cap
                    and _rest_allows_24(n, idx, days, grid)
                ]
                if not pool:
                    continue
                pick = sorted(pool, key=_rank_night_fill)[0]
                if not _enforce_rest_before_24(pick, idx, days, grid, hours, n8):
                    continue
                prev = grid[pick].get(iso, "")
                grid[pick][iso] = "24"
                if prev == "8":
                    hours[pick] += 16
                    n8[pick] -= 1
                else:
                    hours[pick] += 24
                n24[pick] += 1
                filled = True
                break

            if filled:
                continue

            # 16 yalnızca gerçekten kimse 24 alamıyorsa (streak kırmak için değil)
            pool16 = [
                n
                for n in STAFF_NURSES
                if n not in day_only_set
                and iso not in force_avoid[n]
                and not grid[n].get(iso)
                and not _blocked_by_rest(
                    n, idx, days, grid, prefer_48h_after_24=prefer_48h_after_24
                )
            ]
            if pool16:
                pick = sorted(pool16, key=lambda n: (accounted(n), n24[n], n))[0]
                grid[pick][iso] = "16"
                hours[pick] += 16
                n16[pick] += 1
                continue

            warnings.append(
                f"{iso}: Gece nöbeti eksik ({_staff_night_count(grid, iso)}/{NIGHT_SHIFTS_PER_DAY})."
            )
            break

    # ── Yİ/RP/İST dönüşü: sonraki ilk takvim gününde mutlaka 24 (hafta sonu dahil) ──
    for name in STAFF_NURSES:
        for i, dm in enumerate(days):
            if not _first_day_after_leave(name, i, days, grid):
                continue
            iso = dm.iso
            code = grid[name].get(iso, "")
            if code in WORK_CODES:
                continue
            if code in LEAVE_CODES or iso in force_avoid[name] or name in day_only_set:
                continue
            rest_block = _blocked_by_rest(
                name, i, days, grid, prefer_48h_after_24=prefer_48h_after_24
            )
            placed = False
            if (
                not rest_block
                and _staff_night_count(grid, iso) < NIGHT_SHIFTS_PER_DAY
                and _gun_asiri_streak_if_24(name, i, days, grid) <= GUN_ASIRI_STREAK_ABSOLUTE
                and _enforce_rest_before_24(name, i, days, grid, hours, n8)
            ):
                grid[name][iso] = "24"
                hours[name] += 24
                n24[name] += 1
                placed = True
            else:
                partners = [
                    n
                    for n in STAFF_NURSES
                    if n != name and grid[n].get(iso) == "24"
                ]
                for p in sorted(partners, key=lambda n: (n24[n], n)):
                    if rest_block:
                        break
                    if _gun_asiri_streak_if_24(name, i, days, grid) > GUN_ASIRI_STREAK_ABSOLUTE:
                        break
                    if not _can_remove_24_without_gap_violation(p, i, days, grid):
                        continue
                    if not _enforce_rest_before_24(name, i, days, grid, hours, n8):
                        continue
                    grid[p][iso] = ""
                    hours[p] -= 24
                    n24[p] -= 1
                    grid[name][iso] = "24"
                    hours[name] += 24
                    n24[name] += 1
                    placed = True
                    break
            if not placed and not rest_block and dm.is_weekday and _can_assign_8(name, i, days, grid):
                if n8[name] < EIGHT_PER_PERSON_MAX:
                    grid[name][iso] = "8"
                    hours[name] += 8
                    n8[name] += 1
                    placed = True
            if not placed:
                warnings.append(f"{iso}: {name} izin/istek dönüşünde çalışma atanamadı.")

    # ── 24 sonrası ertesi gün mesai temizle (gece doldurma geriye dönük çakışma) ──
    for name in STAFF_NURSES:
        for i in range(len(days) - 1):
            iso = days[i].iso
            if grid[name].get(iso) != "24":
                continue
            nxt_iso = days[i + 1].iso
            nxt_code = grid[name].get(nxt_iso, "")
            if nxt_code == "8":
                grid[name][nxt_iso] = ""
                hours[name] -= 8
                n8[name] -= 1
            elif nxt_code in ("16", "24"):
                grid[name][nxt_iso] = ""
                hours[name] -= _hours_for(nxt_code)
                if nxt_code == "24":
                    n24[name] -= 1
                elif nxt_code == "16":
                    n16[name] -= 1

    # ── Son: gün aşırı 24 + boşluk + fazla mesai bandı ──
    for _finalize in range(8):
        _enforce_gun_asiri_streak_caps(
            days,
            grid,
            hours,
            n8,
            n16,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            accounted_fn=accounted,
        )
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )
        _shorten_triple_gap_sandwiches(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )
        spread = max(accounted(n) for n in STAFF_NURSES) - min(
            accounted(n) for n in STAFF_NURSES
        )
        if (
            _finalize > 0
            and spread <= HOURS_BALANCE_TOLERANCE
            and _grid_gap_ok(days, grid)
        ):
            worst_st = 0
            for name in STAFF_NURSES:
                for i in range(len(days)):
                    if grid[name].get(days[i].iso, "") != "24":
                        continue
                    worst_st = max(
                        worst_st, _gun_asiri_streak_if_24(name, i, days, grid)
                    )
            if worst_st <= GUN_ASIRI_STREAK_ABSOLUTE:
                break

    _boost_ist_shift_hours(
        days,
        grid,
        hours,
        n8,
        n24,
        yi_hours,
        rp_hours,
        ist_count,
        prefer_48h_after_24=prefer_48h_after_24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
    )
    _boost_special_avoid_hours(
        days,
        grid,
        hours,
        n8,
        n24,
        yi_hours,
        rp_hours,
        ist_count,
        force_avoid,
        prefer_48h_after_24=prefer_48h_after_24,
        day_only_set=day_only_set,
    )
    _enforce_hours_balance(
        days,
        grid,
        hours,
        n8,
        n24,
        yi_hours,
        rp_hours,
        ist_count,
        prefer_48h_after_24=prefer_48h_after_24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        max_passes=120,
    )

    for _tail in range(8):
        _enforce_hours_balance(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            max_passes=80,
        )
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )
        _shorten_triple_gap_sandwiches(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )
        _boost_special_avoid_hours(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid,
            prefer_48h_after_24=prefer_48h_after_24,
            day_only_set=day_only_set,
        )
        _enforce_hours_balance(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            max_passes=80,
        )
        spread = max(accounted(n) for n in STAFF_NURSES) - min(
            accounted(n) for n in STAFF_NURSES
        )
        if (
            spread <= HOURS_BALANCE_TOLERANCE
            and _grid_gap_ok(days, grid)
        ):
            break

    _shorten_triple_gap_sandwiches(
        days,
        grid,
        hours,
        n8,
        n24,
        prefer_48h_after_24=prefer_48h_after_24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
    )

    for name in STAFF_NURSES:
        while n8[name] < EIGHT_PER_PERSON_MIN:
            slots: list[tuple[int, int, int]] = []
            for i, dm in enumerate(days):
                if dm.is_weekend or grid[name].get(dm.iso, ""):
                    continue
                if dm.iso in force_avoid[name]:
                    continue
                if _blocked_by_rest(
                    name, i, days, grid, prefer_48h_after_24=prefer_48h_after_24
                ):
                    continue
                if not _can_assign_8(name, i, days, grid):
                    continue
                streak8 = _consecutive_8_streak_if_8(name, i, days, grid)
                pair8 = 0 if streak8 <= CONSECUTIVE_8_STREAK_SOFT else 1
                slots.append((pair8, streak8, i))
            if not slots:
                break
            _, _, pick_i = sorted(slots)[0]
            dm = days[pick_i]
            grid[name][dm.iso] = "8"
            hours[name] += 8
            n8[name] += 1

    for _ in range(8):
        if not _enforce_special_work_days(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_work,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        ):
            break

    for _ in range(8):
        if not _boost_special_avoid_hours(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid,
            prefer_48h_after_24=prefer_48h_after_24,
            day_only_set=day_only_set,
        ):
            break
        _enforce_hours_balance(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            max_passes=120,
        )

    for _ in range(8):
        _boost_ist_shift_hours(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )
        _enforce_hours_balance(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            max_passes=120,
        )

    for _ in range(16):
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )
        _shorten_triple_gap_sandwiches(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )
        if _grid_gap_ok(days, grid):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    for _ in range(8):
        if not _enforce_24_after_ist(
            days,
            grid,
            hours,
            n8,
            n16,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    for _ in range(8):
        if not _enforce_special_work_days(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_work,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        ):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    for _ in range(4):
        if not _enforce_24_after_ist(
            days,
            grid,
            hours,
            n8,
            n16,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    for _ in range(16):
        if _grid_gap_ok(days, grid):
            break
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )
        _shorten_triple_gap_sandwiches(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )

    for _ in range(3):
        if not _enforce_24_after_ist(
            days,
            grid,
            hours,
            n8,
            n16,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    for _ in range(8):
        if _grid_gap_ok(days, grid):
            break
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )
        _shorten_triple_gap_sandwiches(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )

    for _ in range(4):
        if not _enforce_special_work_days(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_work,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        ):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    # Son denge: Yİ/RP hariç aktif kadro ortalamasına çek
    for _ in range(6):
        _boost_peer_hours(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )
        _enforce_hours_balance(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            max_passes=160,
        )
        if _peer_hours_spread(hours, yi_hours, rp_hours, ist_count) <= HOURS_BALANCE_TOLERANCE:
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    for _ in range(4):
        if _grid_gap_ok(days, grid):
            break
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )

    for _ in range(4):
        _boost_peer_hours(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )
        _enforce_hours_balance(
            days,
            grid,
            hours,
            n8,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            max_passes=80,
        )
        if _peer_hours_spread(hours, yi_hours, rp_hours, ist_count) <= HOURS_BALANCE_TOLERANCE:
            break

    for _ in range(4):
        if not _enforce_special_work_days(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_work,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        ):
            break

    for _ in range(3):
        if not _enforce_24_after_ist(
            days,
            grid,
            hours,
            n8,
            n16,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    # Kati: son denge/özel koşul sonrası 4+ boşluk kalmasın (trailing dahil)
    for _ in range(12):
        if _grid_gap_ok(days, grid):
            break
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )
        _shorten_triple_gap_sandwiches(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        )

    for _ in range(4):
        if not _enforce_special_work_days(
            days,
            grid,
            hours,
            n8,
            n24,
            prefer_work,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
        ):
            break

    for _ in range(3):
        if not _enforce_24_after_ist(
            days,
            grid,
            hours,
            n8,
            n16,
            n24,
            yi_hours,
            rp_hours,
            ist_count,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            prefer_48h_after_24=prefer_48h_after_24,
        ):
            break

    _ensure_two_nights_per_day(
        days,
        grid,
        hours,
        n8,
        n16,
        n24,
        force_avoid=force_avoid,
        day_only_set=day_only_set,
        prefer_48h_after_24=prefer_48h_after_24,
    )

    for _ in range(6):
        if _grid_gap_ok(days, grid):
            break
        _enforce_idle_24_gaps(
            days,
            grid,
            hours,
            n8,
            n24,
            min_shift,
            prefer_48h_after_24=prefer_48h_after_24,
            force_avoid=force_avoid,
            day_only_set=day_only_set,
            yi_hours=yi_hours,
        )

    last = days[-1]
    next_month_rest = [
        n for n in STAFF_NURSES if grid[n].get(last.iso, "") in ("16", "24")
    ]

    peer_spread = _peer_hours_spread(hours, yi_hours, rp_hours, ist_count)
    if peer_spread > HOURS_BALANCE_TOLERANCE:
        peers = _balance_peer_names(yi_hours, rp_hours)
        warnings.append(
            f"Aktif personel (Yİ/RP hariç, n={len(peers)}) saat bandı {peer_spread:.0f}s "
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
            f"Gün aşırı 24 kalıbı {gun_asiri} kez (zincir hedefi ≤{GUN_ASIRI_STREAK_SOFT}, "
            f"tavan ≤{GUN_ASIRI_STREAK_MAX}, uç durum ≤{GUN_ASIRI_STREAK_ABSOLUTE}; 16 tercih edilmedi)."
        )

    triple_gap = _count_triple_gap_sandwiches_in_grid(days, grid)
    if triple_gap:
        warnings.append(
            f"3+24+3 boşluk kalıbı {triple_gap} kez kaldı "
            f"(hedef 2+24+2; bkz. docs/ayilma-schedule-rules.md)."
        )

    empty_between = _max_empty_between_24_in_grid(days, grid)
    if empty_between > IDLE_24_GAP_MAX:
        warnings.append(
            f"24 arası boş gün {empty_between} (tavan {IDLE_24_GAP_MAX}; hedef {IDLE_24_GAP_SOFT}+24+{IDLE_24_GAP_SOFT})."
        )

    # Gülten yalnızca görünür satır — hesap/mesai yazımı yok.
    _clear_lead_row(grid)

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
            leave_h = yi_hours[name] + rp_hours[name]
            acc = shift_h + leave_h  # Yİ/RP kredili; İST kotadan düşülmez
            target = ideal
            min_s = min_shift[name]
            overtime = acc - target  # eksi = eksik, artı = fazla
            if shift_h < min_s:
                cred = []
                if yi_hours[name]:
                    cred.append(f"Yİ {yi_hours[name]}s")
                if rp_hours[name]:
                    cred.append(f"RP {rp_hours[name]}s")
                cred_txt = " − ".join(cred) if cred else "0"
                warnings.append(
                    f"{name}: zorunlu mesai eksiği — en az {min_s}s nöbet "
                    f"(kota {ideal} − {cred_txt}; kalan günlerle doldurulur), şu an {shift_h}s."
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
                "exclude_from_staff_balance": is_lead
                or (not is_lead and _has_yi_or_rp(name, yi_hours, rp_hours)),
                "count_8": 0 if is_lead else n8[name],
                "count_24": 0 if is_lead else n24[name],
                "count_16": 0 if is_lead else n16[name],
            }
        )

    code_counts = {"8": sum(n8.values()), "16": sum(n16.values()), "24": sum(n24.values())}

    result: dict[str, Any] = {
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
        "special_rules_applied": {
            "work": {n: sorted(prefer_work[n]) for n in STAFF_NURSES if prefer_work[n]},
            "avoid": {n: sorted(force_avoid[n]) for n in STAFF_NURSES if force_avoid[n]},
        },
        "variant": int(variant or 0),
        "legend": {
            "8": "08:00–16:00 (6 kişiye dağıtılır)",
            "16": "16:00–08:00 (son çare)",
            "24": "08:00–08:00 (6 kişiye dağıtılır)",
            "Yİ": f"Yıllık izin ({YI_DAY_HOURS}s sayılır)",
            "RP": "Rapor",
            "İST": "Özel gün isteği / rezervasyon",
        },
    }

    if int(variant or 0) == 0:
        def _active_hours(rs: list[dict[str, Any]]) -> list[int]:
            return [
                int(r["worked_hours"])
                for r in rs
                if r.get("role") == "staff" and not r.get("exclude_from_staff_balance")
            ]

        staff_h = _active_hours(rows)
        needs_retry = False
        if staff_h and max(staff_h) - min(staff_h) > HOURS_BALANCE_TOLERANCE:
            needs_retry = True
        if not _ist_followed_by_24_ok(rows, result["days"]):
            needs_retry = True
        if not _grid_gap_ok(days, grid):
            needs_retry = True
        if any(
            _uses_ist_only_leave(n, ist_count=ist_count, yi_hours=yi_hours, rp_hours=rp_hours)
            for n in STAFF_NURSES
        ):
            ist_targets = [
                r
                for r in rows
                if r["role"] == "staff"
                and _uses_ist_only_leave(
                    r["name"],
                    ist_count=ist_count,
                    yi_hours=yi_hours,
                    rp_hours=rp_hours,
                )
            ]
            if ist_targets and staff_h:
                peer_med = sorted(staff_h)[len(staff_h) // 2]
                for r in ist_targets:
                    if abs(r["worked_hours"] - peer_med) > HOURS_BALANCE_TOLERANCE:
                        needs_retry = True
                        break
        if needs_retry and staff_h:
            gap_fallback: dict[str, Any] | None = None
            gap_fallback_spread = 10**9
            best_spread_alt: dict[str, Any] | None = None
            best_spread_val = max(staff_h) - min(staff_h)
            for retry_v in range(1, 24):
                alt = generate_ayilma_schedule(
                    year,
                    month,
                    leaves=leaves,
                    day_only=day_only,
                    prefer_48h_after_24=prefer_48h_after_24,
                    special_rules=special_rules,
                    variant=retry_v,
                )
                am = _active_hours(alt["rows"])
                if not am:
                    continue
                spread = max(am) - min(am)
                spread_ok = spread <= HOURS_BALANCE_TOLERANCE
                ist_ok = _ist_followed_by_24_ok(alt["rows"], alt["days"])
                alt_grid = {
                    r["name"]: r["cells"] for r in alt["rows"] if r["role"] == "staff"
                }
                gap_ok = _grid_gap_ok(days, alt_grid)
                if spread_ok and ist_ok and gap_ok:
                    return alt
                if ist_ok and gap_ok and spread < gap_fallback_spread:
                    gap_fallback = alt
                    gap_fallback_spread = spread
                if ist_ok and gap_ok and spread < best_spread_val:
                    best_spread_alt = alt
                    best_spread_val = spread
            if gap_fallback is not None and gap_fallback_spread <= HOURS_BALANCE_TOLERANCE:
                return gap_fallback
            if best_spread_alt is not None:
                return best_spread_alt
            if gap_fallback is not None:
                return gap_fallback

    return result


def _ist_followed_by_24_ok(
    rows: list[dict[str, Any]],
    days: list[dict[str, Any]] | list[DayMeta],
) -> bool:
    """Her İST gününün ertesi gününde (izin değilse) 24 nöbet var mı."""
    day_isos = [d["iso"] if isinstance(d, dict) else d.iso for d in days]
    for row in rows:
        if row.get("role") != "staff":
            continue
        cells = row.get("cells") or {}
        for i in range(1, len(day_isos)):
            if cells.get(day_isos[i - 1]) != "İST":
                continue
            code = cells.get(day_isos[i], "")
            if code in LEAVE_CODES:
                continue
            if code != "24":
                return False
    return True


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

    title = _month_title(month, year)
    export_rows = _ensure_empty_lead_export_rows(days, rows)
    headers, body = _export_matrix(days, export_rows)

    wb = Workbook()
    ws = wb.active
    ws.title = f"{month:02d}-{year}"[:31]

    ws["A1"] = title
    ws["A1"].font = Font(bold=True, size=14)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max(4, 1 + len(days) + 3))

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

    for r_i, line in enumerate(body, start=4):
        row_meta = export_rows[r_i - 4] if r_i - 4 < len(export_rows) else {}
        ws.cell(row=r_i, column=1, value=line[0]).font = Font(bold=True)
        for c_i, d in enumerate(days):
            code = str(line[1 + c_i] or "")
            cell = ws.cell(row=r_i, column=2 + c_i, value=code)
            cell.alignment = Alignment(horizontal="center")
            if code in fills:
                cell.fill = fills[code]
        ws.cell(row=r_i, column=2 + len(days), value=line[-3])
        ws.cell(row=r_i, column=3 + len(days), value=line[-2])
        ws.cell(row=r_i, column=4 + len(days), value=line[-1])
        if row_meta.get("role") == "lead":
            ws.cell(row=r_i, column=1).font = Font(bold=True, color="4338CA")

    ws.column_dimensions["A"].width = 18
    for i in range(len(days)):
        ws.column_dimensions[ws.cell(row=3, column=2 + i).column_letter].width = 4.2
    for j in range(3):
        ws.column_dimensions[ws.cell(row=3, column=2 + len(days) + j).column_letter].width = 10

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


_MONTH_TR_NAMES = (
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


def _month_title(month: int, year: int) -> str:
    label = _MONTH_TR_NAMES[month] if 1 <= month <= 12 else str(month)
    return f"Ayılma hemşireleri — {label} {year}"


def _export_headers(days: list[dict[str, Any]]) -> list[str]:
    return ["Ad Soyadı"] + [str(d.get("day", "")) for d in days] + ["Çalıştığı", "Aylık", "Fazla"]


def _row_totals(
    row: dict[str, Any],
    days: list[dict[str, Any]],
    cells_map: dict[str, str],
) -> tuple[int, int, int]:
    worked = 0
    for d in days:
        code = cells_map.get(d.get("iso") or "", "") or ""
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
    if "worked_hours" in row:
        worked = int(row.get("worked_hours") or worked)
    if "overtime_hours" in row and row.get("role") != "lead":
        ot = int(row.get("overtime_hours") or ot)
    if "ideal_hours" in row and row.get("role") != "lead":
        ideal = int(row.get("ideal_hours") or ideal)
    return worked, ideal, ot


def _ensure_empty_lead_export_rows(
    days: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """İndirmede Gülten satırı her zaman en üstte ve tamamen boş."""
    empty_cells = {str(d.get("iso") or ""): "" for d in days if d.get("iso")}
    lead_row = {
        "name": LEAD_NURSE,
        "role": "lead",
        "cells": empty_cells,
        "worked_hours": 0,
        "ideal_hours": 0,
        "overtime_hours": 0,
    }
    others = [
        r
        for r in (rows or [])
        if (r.get("name") or "") != LEAD_NURSE and r.get("role") != "lead"
    ]
    return [lead_row, *others]


def _export_matrix(
    days: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[list[Any]]]:
    headers = _export_headers(days)
    body: list[list[Any]] = []
    for row in _ensure_empty_lead_export_rows(days, rows):
        cells_map = row.get("cells") or {}
        worked, ideal, ot = _row_totals(row, days, cells_map)
        line: list[Any] = [row.get("name") or ""]
        for d in days:
            line.append(cells_map.get(d.get("iso") or "", "") or "")
        line.extend([worked, ideal, ot])
        body.append(line)
    return headers, body


def build_ayilma_csv_bytes(
    *,
    year: int,
    month: int,
    days: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> bytes:
    """UTF-8 BOM + virgül ayırıcı; boş hücreler \"\" — bitişik virgül (,,) yok."""
    import csv
    from io import StringIO

    headers, body = _export_matrix(days, rows)
    sio = StringIO()
    sio.write("\ufeff")
    writer = csv.writer(
        sio,
        delimiter=",",
        lineterminator="\r\n",
        quoting=csv.QUOTE_ALL,
        quotechar='"',
    )
    writer.writerow([_month_title(month, year)])
    writer.writerow([])
    writer.writerow(headers)
    for line in body:
        writer.writerow(line)
    return sio.getvalue().encode("utf-8")


def build_ayilma_docx_bytes(
    *,
    year: int,
    month: int,
    days: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> bytes:
    """Word .docx — yatay A4, sabit sütun; Windows Word / Android uyumlu."""
    from io import BytesIO

    from docx import Document
    from docx.enum.section import WD_ORIENT
    from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.shared import Cm, Mm, Pt

    def _set_nowrap(cell) -> None:
        tc_pr = cell._tc.get_or_add_tcPr()
        for old in tc_pr.findall(qn("w:noWrap")):
            tc_pr.remove(old)
        tc_pr.append(OxmlElement("w:noWrap"))

    def _write_cell(cell, text: str, *, bold: bool = False, size: float = 8, align=WD_ALIGN_PARAGRAPH.CENTER) -> None:
        cell.text = ""
        p = cell.paragraphs[0]
        p.alignment = align
        run = p.add_run(text)
        run.bold = bold
        run.font.size = Pt(size)
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        _set_nowrap(cell)

    def _fixed_layout(table) -> None:
        tbl = table._tbl
        tbl_pr = tbl.tblPr
        if tbl_pr is None:
            tbl_pr = OxmlElement("w:tblPr")
            tbl.insert(0, tbl_pr)
        layout = tbl_pr.find(qn("w:tblLayout"))
        if layout is None:
            layout = OxmlElement("w:tblLayout")
            tbl_pr.append(layout)
        layout.set(qn("w:type"), "fixed")

    headers, body = _export_matrix(days, rows)
    doc = Document()

    section = doc.sections[0]
    section.orientation = WD_ORIENT.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width
    section.top_margin = Cm(1.0)
    section.bottom_margin = Cm(1.0)
    section.left_margin = Cm(0.7)
    section.right_margin = Cm(0.7)

    title = doc.add_heading(_month_title(month, year), level=1)
    for run in title.runs:
        run.font.size = Pt(14)

    table = doc.add_table(rows=1 + len(body), cols=len(headers))
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.autofit = False
    _fixed_layout(table)

    name_w = Mm(26)
    total_w = Mm(13)
    n_days = len(days)
    usable = section.page_width - section.left_margin - section.right_margin
    day_budget = usable - name_w - (total_w * 3)
    day_w = day_budget // max(n_days, 1)
    day_w = max(day_w, Mm(4.8))

    col_widths = [name_w] + [day_w] * n_days + [total_w, total_w, total_w]
    for col_idx, width in enumerate(col_widths):
        for row in table.rows:
            row.cells[col_idx].width = width

    wd_tr = ("Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz")
    hdr = table.rows[0].cells
    _write_cell(hdr[0], "Ad Soyadı", bold=True, size=8, align=WD_ALIGN_PARAGRAPH.LEFT)
    for i, d in enumerate(days):
        wd = wd_tr[d.get("weekday", 0) % 7] if isinstance(d.get("weekday"), int) else ""
        label = f"{d.get('day', '')}\n{wd}" if wd else str(d.get("day", ""))
        _write_cell(hdr[1 + i], label, bold=True, size=7)
    for j, label in enumerate(("Çalıştığı", "Aylık", "Fazla")):
        _write_cell(hdr[1 + n_days + j], label, bold=True, size=7)

    for r_i, line in enumerate(body):
        cells = table.rows[r_i + 1].cells
        _write_cell(cells[0], str(line[0]), size=8, align=WD_ALIGN_PARAGRAPH.LEFT)
        for c_i, d in enumerate(days):
            val = line[1 + c_i]
            _write_cell(cells[1 + c_i], "" if val == "" else str(val), size=8)
        for j in range(3):
            _write_cell(cells[1 + n_days + j], str(line[-3 + j]), size=8)

    buf = BytesIO()
    doc.save(buf)
    return buf.getvalue()
