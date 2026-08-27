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
    is_sheet_only_member_email,
    is_sheet_page_allowed_email,
    is_sheet_page_path,
    member_denied_sheet_access,
    resolve_sheet_menu_visible,
    sheet_only_member_path_allowed,
)


def test_sheet_page_allowed_emails():
    assert is_sheet_page_allowed_email("cemevecen@nokta.com")
    assert is_sheet_page_allowed_email("CemEvecen@Gmail.com")
    assert is_sheet_page_allowed_email("evecensema@gmail.com")
    assert is_sheet_page_allowed_email("EvecenSema@gmail.com")
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
    assert resolve_sheet_menu_visible(member_email="evecensema@gmail.com") is True
    assert resolve_sheet_menu_visible(member_email="other@nokta.com") is False
    assert member_denied_sheet_access("other@nokta.com") is True
    assert member_denied_sheet_access("cemevecen@gmail.com") is False
    assert member_denied_sheet_access("evecensema@gmail.com") is False


def test_sheet_only_member_paths():
    assert is_sheet_only_member_email("evecensema@gmail.com")
    assert not is_sheet_only_member_email("cemevecen@gmail.com")
    assert sheet_only_member_path_allowed("/sheet")
    assert sheet_only_member_path_allowed("/api/sheet/ayilma/meta")
    assert sheet_only_member_path_allowed("/auth/logout")
    assert sheet_only_member_path_allowed("/static/js/app.js")
    assert not sheet_only_member_path_allowed("/realtime")
    assert not sheet_only_member_path_allowed("/api/panel/online-users")


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
        # Görsel: Gülten boş satır (hesaba dahil değil)
        assert lead["cells"][dm["iso"]] == "", f"lead must be empty {dm['iso']}"

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
            assert staff8 >= 1, f"kat-1 8 missing {iso}"


def test_engine_never_writes_16_without_pin():
    """Nöbet motoru 16 yazmaz; yalnız sabit pin ile gelir."""
    out = generate_ayilma_schedule(2026, 9)
    assert out["staff_code_counts"]["16"] == 0
    leaves = {
        "Semanur Çınar": {f"2026-09-{d:02d}": "Yİ" for d in range(1, 14)},
        "Şengül Zamur": {f"2026-09-{d:02d}": "Yİ" for d in range(14, 21)},
    }
    out2 = generate_ayilma_schedule(2026, 9, leaves=leaves, variant=1)
    assert out2["staff_code_counts"]["16"] == 0


def test_prefer_8_and_minimize_16():
    out = generate_ayilma_schedule(2026, 8)
    counts = out["staff_code_counts"]
    # Hafta içi her gün ~1×8 → Ağustos ~21–22
    assert counts["8"] >= 20
    assert counts["16"] == 0


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
        # Görsel: Gülten her gün boş
        assert lead["cells"][iso] == ""


def test_first_day_after_leave_gets_night_shift():
    """Yİ/RP bitişinin ertesi gün nöbet (24); hafta içi kat-1 8 değil."""
    leaves = {
        "Nuray Durna": {
            "2026-08-17": "Yİ",
            "2026-08-18": "Yİ",
            "2026-08-19": "Yİ",
        }
    }
    out = generate_ayilma_schedule(2026, 8, leaves=leaves)
    nuray = next(r for r in out["rows"] if r["name"] == "Nuray Durna")
    first_back = nuray["cells"]["2026-08-20"]
    assert first_back == "24", f"expected 24 nöbet, got {first_back!r}"
    assert first_back != "8"


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


def test_no_back_to_back_24():
    """Aynı kişide ardışık gün 24+24 olmamalı (pin yokken)."""
    leaves = {
        "Nuray Durna": {
            "2026-09-03": "İST",
            "2026-09-04": "İST",
            "2026-09-10": "İST",
            "2026-09-17": "İST",
            "2026-09-24": "İST",
        },
        "Sema Evecen": {"2026-09-12": "İST", "2026-09-13": "İST"},
        "Şengül Zamur": {"2026-09-20": "İST"},
        "Emine Türker": {f"2026-09-{d:02d}": "Yİ" for d in range(8, 13)},
        "Semanur Çınar": {f"2026-09-{d:02d}": "Yİ" for d in range(14, 25)},
    }
    for v in range(5):
        out = generate_ayilma_schedule(2026, 9, leaves=leaves, variant=v)
        for row in out["rows"]:
            if row["role"] != "staff":
                continue
            cells = row["cells"]
            days = out["days"]
            for i in range(len(days) - 1):
                a = cells.get(days[i]["iso"], "")
                b = cells.get(days[i + 1]["iso"], "")
                assert not (a == "24" and b == "24"), (
                    f"variant={v} {row['name']} {days[i]['iso']}+{days[i+1]['iso']}"
                )


def test_soft_avoid_gun_asiri_prefer_24_over_16():
    """16 yok; gün aşırı zinciri kati ≤3."""
    out = generate_ayilma_schedule(2026, 9)
    counts = out["staff_code_counts"]
    assert counts["16"] == 0
    assert counts["24"] >= counts["8"]
    assert _max_gun_asiri_streak(out) <= 3


def test_gun_asiri_streak_never_four():
    """İzinli ayda da art arda gün aşırı 24 en fazla 3."""
    leaves = {
        "Semanur Çınar": {f"2026-09-{d:02d}": "Yİ" for d in range(1, 14)},
        "Şengül Zamur": {f"2026-09-{d:02d}": "Yİ" for d in range(14, 21)},
        "Sema Evecen": {"2026-09-01": "İST", "2026-09-07": "İST", "2026-09-08": "İST"},
    }
    for v in (0, 1, 2):
        out = generate_ayilma_schedule(2026, 9, leaves=leaves, variant=v)
        assert _max_gun_asiri_streak(out) <= 3, (
            f"variant={v} streak={_max_gun_asiri_streak(out)}"
        )


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


def test_no_consecutive_eights_for_staff():
    from backend.services.ayilma_schedule import (
        CONSECUTIVE_8_STREAK_MAX,
        _max_consecutive_8_streak,
        month_days,
    )

    out = generate_ayilma_schedule(2026, 9)
    grid = {r["name"]: r["cells"] for r in out["rows"]}
    days_meta = month_days(2026, 9)
    for row in out["rows"]:
        if row["role"] != "staff":
            continue
        streak = _max_consecutive_8_streak(row["name"], days_meta, grid)
        assert streak <= CONSECUTIVE_8_STREAK_MAX, (
            f"{row['name']} has {streak} consecutive 8s (max {CONSECUTIVE_8_STREAK_MAX})"
        )


def test_no_three_eights_after_long_leave():
    """Uzun Yİ sonrası ardışık 3×8 olmamalı (ör. Semanur senaryosu)."""
    from backend.services.ayilma_schedule import _max_consecutive_8_streak, month_days

    leaves = {
        "Semanur Çınar": {f"2026-09-{d:02d}": "Yİ" for d in range(1, 14)},
    }
    out = generate_ayilma_schedule(2026, 9, leaves=leaves)
    semanur = next(r for r in out["rows"] if r["name"] == "Semanur Çınar")
    days_meta = month_days(2026, 9)
    grid = {"Semanur Çınar": semanur["cells"]}
    streak = _max_consecutive_8_streak("Semanur Çınar", days_meta, grid)
    assert streak <= 2, f"Semanur consecutive 8 streak={streak}"
    # Dönüş günü (14) nöbet veya en az tek 8; üçlü blok yok
    d14, d15, d16 = (
        semanur["cells"].get("2026-09-14", ""),
        semanur["cells"].get("2026-09-15", ""),
        semanur["cells"].get("2026-09-16", ""),
    )
    assert not (d14 == d15 == d16 == "8"), f"3×8 block: {d14}/{d15}/{d16}"


def test_two_night_shifts_every_day_with_mixed_leaves():
    """15 Eylül gibi İST/Yİ karışımında da her gün 2× gece nöbeti."""
    leaves = {
        "Semanur Çınar": {f"2026-09-{d:02d}": "Yİ" for d in range(1, 14)},
        "Sema Evecen": {"2026-09-01": "İST", "2026-09-07": "İST", "2026-09-08": "İST"},
        "Rabia Kumtepe": {"2026-09-03": "İST"},
        "Emine Türker": {"2026-09-15": "İST", "2026-09-22": "İST"},
        "Nuray Durna": {"2026-09-18": "İST"},
        "Şengül Zamur": {"2026-09-28": "İST"},
    }
    out = generate_ayilma_schedule(2026, 9, leaves=leaves)
    iso = "2026-09-15"
    night = [
        (r["name"], r["cells"].get(iso, ""))
        for r in out["rows"]
        if r["role"] == "staff" and r["cells"].get(iso, "") in ("16", "24")
    ]
    assert len(night) >= 2, f"2026-09-15 only {night}"
    for dm in out["days"]:
        n = sum(
            1
            for r in out["rows"]
            if r["role"] == "staff" and r["cells"].get(dm["iso"], "") in ("16", "24")
        )
        assert n >= 2, f"{dm['iso']}: {n} night shifts"


def test_variant_reroll_changes_schedule():
    """Yeniden oluştur (variant) deterministik ama farklı çizelge üretir."""
    leaves = {"Sema Evecen": {"2026-09-01": "İST"}}
    a = generate_ayilma_schedule(2026, 9, leaves=leaves, variant=0)
    b = generate_ayilma_schedule(2026, 9, leaves=leaves, variant=1)

    def cells(out):
        return {
            r["name"]: dict(r["cells"])
            for r in out["rows"]
            if r["role"] == "staff"
        }

    assert cells(a) != cells(b)
    assert a.get("variant") == 0
    assert b.get("variant") == 1


def test_variant_reroll_under_heavy_leaves():
    """Yoğun İST/Yİ altında da variant en az birkaç farklı öneri üretir."""
    leaves = {
        "Nuray Durna": {
            "2026-09-03": "İST",
            "2026-09-04": "İST",
            "2026-09-10": "İST",
            "2026-09-17": "İST",
            "2026-09-24": "İST",
        },
        "Sema Evecen": {"2026-09-12": "İST", "2026-09-13": "İST"},
        "Şengül Zamur": {"2026-09-20": "İST"},
        "Emine Türker": {f"2026-09-{d:02d}": "Yİ" for d in range(8, 13)},
        "Semanur Çınar": {f"2026-09-{d:02d}": "Yİ" for d in range(14, 25)},
    }

    def cells(out):
        return tuple(
            (r["name"], tuple(sorted(r["cells"].items())))
            for r in out["rows"]
            if r["role"] == "staff"
        )

    fps = {cells(generate_ayilma_schedule(2026, 9, leaves=leaves, variant=v)) for v in range(8)}
    assert len(fps) >= 3, f"unique variants={len(fps)}"


def test_special_avoid_blocks_day():
    rules = [{"name": "Emine Türker", "mode": "avoid", "dates": ["2026-09-10"], "weekly": False}]
    out = generate_ayilma_schedule(2026, 9, special_rules=rules)
    em = next(r for r in out["rows"] if r["name"] == "Emine Türker")
    assert em["cells"]["2026-09-10"] == ""


def test_special_pin_fixed_shifts():
    rules = [{
        "name": "Rabia Kumtepe",
        "mode": "pin",
        "shifts": {"2026-09-03": "8", "2026-09-05": "24"},
        "weekly": False,
    }]
    out = generate_ayilma_schedule(2026, 9, special_rules=rules)
    rab = next(r for r in out["rows"] if r["name"] == "Rabia Kumtepe")
    assert rab["cells"]["2026-09-03"] == "8"
    assert rab["cells"]["2026-09-05"] == "24"
