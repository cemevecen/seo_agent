# -*- coding: utf-8 -*-
"""/sheet erisim + ayılma çizelge motoru."""

from backend.services.ayilma_schedule import (
    LEAD_NURSE,
    STAFF_NURSES,
    generate_ayilma_schedule,
    ideal_hours,
    roster_defaults,
)
from backend.services.sheet_page_access import (
    is_sheet_page_allowed_email,
    is_sheet_page_path,
    member_denied_sheet_access,
    resolve_sheet_menu_visible,
)


def test_sheet_page_allowed_emails():
    assert is_sheet_page_allowed_email("cemevecen@nokta.com")
    assert is_sheet_page_allowed_email("CemEvecen@Gmail.com")
    assert not is_sheet_page_allowed_email("onurtorun@nokta.com")
    assert not is_sheet_page_allowed_email("melihengin@nokta.com")
    assert not is_sheet_page_allowed_email("")


def test_roster_defaults_next_month():
    from datetime import date

    d = roster_defaults()
    today = date.today()
    if today.month == 12:
        assert d["default_year"] == today.year + 1
        assert d["default_month"] == 1
    else:
        assert d["default_year"] == today.year
        assert d["default_month"] == today.month + 1


def test_sheet_page_paths():
    assert is_sheet_page_path("/sheet")
    assert is_sheet_page_path("/sheet/")
    assert is_sheet_page_path("/api/sheet/ayilma/meta")
    assert is_sheet_page_path("/api/sheet/ayilma/generate")
    assert is_sheet_page_path("/api/sheet/ayilma/export.xlsx")
    assert not is_sheet_page_path("/ipo")
    assert not is_sheet_page_path("/")


def test_export_xlsx_opens():
    from io import BytesIO

    from openpyxl import load_workbook

    from backend.services.ayilma_schedule import build_ayilma_xlsx_bytes

    out = generate_ayilma_schedule(2026, 9)
    raw = build_ayilma_xlsx_bytes(year=2026, month=9, days=out["days"], rows=out["rows"])
    assert raw[:2] == b"PK"
    wb = load_workbook(BytesIO(raw))
    ws = wb.active
    assert "Ayılma" in str(ws["A1"].value)
    assert ws.cell(row=4, column=1).value  # first nurse name


def test_sheet_menu_visible():
    assert resolve_sheet_menu_visible(member_email="cemevecen@nokta.com") is True
    assert resolve_sheet_menu_visible(member_email="cemevecen@gmail.com") is True
    assert resolve_sheet_menu_visible(member_email="other@nokta.com") is False
    assert member_denied_sheet_access("other@nokta.com") is True
    assert member_denied_sheet_access("cemevecen@gmail.com") is False


def test_generate_august_basic_coverage():
    out = generate_ayilma_schedule(2026, 8)
    assert out["ok"] is True
    assert out["lead"] == LEAD_NURSE
    assert len(out["staff"]) == 6
    assert out["ideal_hours_staff"] == ideal_hours(2026, 8)

    lead = next(r for r in out["rows"] if r["name"] == LEAD_NURSE)
    assert lead["role"] == "lead"
    assert lead["overtime_hours"] == 0
    for dm in out["days"]:
        code = lead["cells"][dm["iso"]]
        if dm["is_weekend"]:
            assert code == "", f"lead weekend {dm['iso']}"
        else:
            assert code == "8", f"lead weekday {dm['iso']}"

    for dm in out["days"]:
        iso = dm["iso"]
        morning = 0
        night = 0
        staff8 = 0
        for name in STAFF_NURSES:
            row = next(r for r in out["rows"] if r["name"] == name)
            code = row["cells"].get(iso, "")
            if code in ("8", "24"):
                morning += 1
            if code in ("16", "24"):
                night += 1
            if code == "8":
                staff8 += 1
        assert night >= 2, f"night short {iso}: {night}"
        if dm["is_weekend"]:
            assert staff8 == 0, f"weekend staff 8 on {iso}"
        else:
            assert morning >= 1, f"morning missing {iso}"


def test_prefer_8_and_minimize_16():
    out = generate_ayilma_schedule(2026, 8)
    counts = out["staff_code_counts"]
    # Kişi başı ~2–4 → toplam ~12–24; her güne 8 şart değil
    assert 12 <= counts["8"] <= 24
    assert counts["16"] < counts["24"]
    assert counts["16"] <= 2


def test_eights_and_twentyfours_are_shared():
    """8 kişi başı 2–4; 24 altı kişiye yayılır."""
    out = generate_ayilma_schedule(2026, 8)
    staff_rows = [r for r in out["rows"] if r["role"] == "staff"]
    eights = [r["count_8"] for r in staff_rows]
    twentyfours = [r["count_24"] for r in staff_rows]
    assert min(eights) >= 2
    assert max(eights) <= 4
    assert max(eights) - min(eights) <= 2
    assert min(twentyfours) >= 2
    assert max(twentyfours) - min(twentyfours) <= 4


def test_lead_does_not_count_in_staff_night_or_overtime():
    out = generate_ayilma_schedule(2026, 8)
    lead = next(r for r in out["rows"] if r["name"] == LEAD_NURSE)
    assert lead["exclude_from_staff_balance"] is True
    assert lead["ideal_hours"] == 0
    assert lead["overtime_hours"] == 0
    for dm in out["days"]:
        iso = dm["iso"]
        night = sum(
            1
            for n in STAFF_NURSES
            if next(r for r in out["rows"] if r["name"] == n)["cells"].get(iso) in ("16", "24")
        )
        assert night >= 2
        if dm["is_weekend"]:
            assert lead["cells"][iso] == ""
        else:
            assert lead["cells"][iso] == "8"


def test_generate_respects_leave_and_rest_after_24():
    leaves = {
        "Nuray Durna": {
            "2026-08-17": "Yİ",
            "2026-08-18": "Yİ",
            "2026-08-19": "Yİ",
        }
    }
    out = generate_ayilma_schedule(2026, 8, leaves=leaves)
    nuray = next(r for r in out["rows"] if r["name"] == "Nuray Durna")
    assert nuray["cells"]["2026-08-17"] == "Yİ"
    assert nuray["cells"]["2026-08-18"] == "Yİ"

    # 24 sonrası ertesi gün boş veya izin
    for name in STAFF_NURSES:
        row = next(r for r in out["rows"] if r["name"] == name)
        cells = row["cells"]
        for dm in out["days"]:
            if dm["day"] >= 31:
                continue
            iso = dm["iso"]
            if cells.get(iso) != "24":
                continue
            nxt = f"2026-08-{dm['day'] + 1:02d}"
            if nxt in cells:
                assert cells[nxt] in ("", "Yİ", "RP", "İST"), f"{name} {iso}-> {cells[nxt]}"


def test_yi_counts_as_eight_and_lowers_min_shift():
    """5 gün Yİ = 40s; eylül 176 → en az 136s nöbet; Çalıştığı = nöbet+Yİ."""
    leaves = {
        "Nuray Durna": {
            f"2026-09-{d:02d}": "Yİ" for d in range(1, 6)  # Pzt–Cum haftası
        }
    }
    out = generate_ayilma_schedule(2026, 9, leaves=leaves)
    assert out["ideal_hours_staff"] == 176
    nuray = next(r for r in out["rows"] if r["name"] == "Nuray Durna")
    assert nuray["leave_hours"] == 40
    assert nuray["min_shift_hours"] == 136
    assert nuray["worked_hours"] == nuray["shift_hours"] + 40
    assert nuray["cells"]["2026-09-01"] == "Yİ"
    # Nöbet saati zorunlu tabana yaklaşmalı
    assert nuray["shift_hours"] >= 120


def test_staff_accounted_hours_reasonably_balanced():
    out = generate_ayilma_schedule(2026, 9)
    vals = [r["worked_hours"] for r in out["rows"] if r["role"] == "staff"]
    ots = [r["overtime_hours"] for r in out["rows"] if r["role"] == "staff"]
    assert max(vals) - min(vals) <= 16
    assert max(ots) - min(ots) <= 16


def test_ist_request_blocks_assignment():
    leaves = {"Sema Evecen": {"2026-08-10": "İST", "2026-08-11": "İST"}}
    out = generate_ayilma_schedule(2026, 8, leaves=leaves)
    sema = next(r for r in out["rows"] if r["name"] == "Sema Evecen")
    assert sema["cells"]["2026-08-10"] == "İST"
    assert sema["cells"]["2026-08-11"] == "İST"


def _count_gun_asiri(out: dict) -> int:
    days = out["days"]
    n = 0
    for row in out["rows"]:
        if row["role"] != "staff":
            continue
        cells = row["cells"]
        for i in range(2, len(days)):
            if cells.get(days[i]["iso"]) != "24":
                continue
            if cells.get(days[i - 2]["iso"]) != "24":
                continue
            mid = cells.get(days[i - 1]["iso"], "")
            if mid in ("", "Yİ", "RP", "İST"):
                n += 1
    return n


def _max_gun_asiri_streak(out: dict) -> int:
    """Bir kişide 24+boş+24… zincirinin en uzun 24 sayısı."""
    days = out["days"]
    gap = ("", "Yİ", "RP", "İST")
    best = 0
    for row in out["rows"]:
        if row["role"] != "staff":
            continue
        cells = row["cells"]
        for i, dm in enumerate(days):
            if cells.get(dm["iso"]) != "24":
                continue
            streak = 1
            j = i
            while j >= 2:
                if cells.get(days[j - 2]["iso"]) != "24":
                    break
                if cells.get(days[j - 1]["iso"], "") not in gap:
                    break
                streak += 1
                j -= 2
            best = max(best, streak)
    return best


def test_soft_avoid_gun_asiri_prefer_24_over_16():
    """16 neredeyse hiç; gün aşırı zinciri ≤3 (izin yoksa)."""
    out = generate_ayilma_schedule(2026, 9)
    counts = out["staff_code_counts"]
    assert counts["16"] <= 2
    assert counts["24"] >= counts["8"]
    assert _max_gun_asiri_streak(out) <= 3


def test_gun_asiri_streak_helpers():
    from backend.services.ayilma_schedule import (
        GUN_ASIRI_STREAK_MAX,
        _gun_asiri_streak_if_24,
        month_days,
    )

    days = month_days(2026, 9)
    grid = {n: {d.iso: "" for d in days} for n in STAFF_NURSES}
    name = STAFF_NURSES[0]
    # 12,14,16 dolu → gün 10'a yazmak zinciri 4 yapar
    grid[name][days[11].iso] = "24"
    grid[name][days[13].iso] = "24"
    grid[name][days[15].iso] = "24"
    assert _gun_asiri_streak_if_24(name, 9, days, grid) == 4  # day 10
    grid[name][days[9].iso] = "24"
    assert _gun_asiri_streak_if_24(name, 15, days, grid) == 4  # day 16 back
    assert GUN_ASIRI_STREAK_MAX == 3
