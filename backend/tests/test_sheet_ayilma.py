# -*- coding: utf-8 -*-
"""/sheet erisim + ayılma çizelge motoru."""

from backend.services.ayilma_schedule import (
    LEAD_NURSE,
    STAFF_NURSES,
    generate_ayilma_schedule,
    ideal_hours,
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


def test_sheet_page_paths():
    assert is_sheet_page_path("/sheet")
    assert is_sheet_page_path("/sheet/")
    assert is_sheet_page_path("/api/sheet/ayilma/meta")
    assert is_sheet_page_path("/api/sheet/ayilma/generate")
    assert not is_sheet_page_path("/ipo")
    assert not is_sheet_page_path("/")


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
    # Her gün 8 (izin yok)
    assert all(v == "8" for v in lead["cells"].values())

    for dm in out["days"]:
        iso = dm["iso"]
        morning = 0
        night = 0
        for name in STAFF_NURSES:
            row = next(r for r in out["rows"] if r["name"] == name)
            code = row["cells"].get(iso, "")
            if code in ("8", "24"):
                morning += 1
            if code in ("16", "24"):
                night += 1
        assert morning >= 1, f"morning missing {iso}"
        assert night >= 2, f"night short {iso}: {night}"


def test_prefer_8_and_minimize_16():
    out = generate_ayilma_schedule(2026, 8)
    counts = out["staff_code_counts"]
    assert counts["8"] >= len(out["days"]) // 2
    assert counts["16"] < counts["24"]


def test_eights_and_twentyfours_are_shared():
    """8 ve 24 altı kişiye yayılır; kimse neredeyse tüm 8'leri tek başına almaz."""
    out = generate_ayilma_schedule(2026, 8)
    staff_rows = [r for r in out["rows"] if r["role"] == "staff"]
    eights = [r["count_8"] for r in staff_rows]
    twentyfours = [r["count_24"] for r in staff_rows]
    assert max(eights) - min(eights) <= 4
    assert min(eights) >= 2  # herkese biraz 8
    assert min(twentyfours) >= 2  # herkese biraz 24
    assert max(eights) < len(out["days"]) - 5  # tek kişiye sürekli 8 yok


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
    assert max(vals) - min(vals) <= 48  # ±16 ideal; pratikte ≤48 kabul


def test_ist_request_blocks_assignment():
    leaves = {"Sema Evecen": {"2026-08-10": "İST", "2026-08-11": "İST"}}
    out = generate_ayilma_schedule(2026, 8, leaves=leaves)
    sema = next(r for r in out["rows"] if r["name"] == "Sema Evecen")
    assert sema["cells"]["2026-08-10"] == "İST"
    assert sema["cells"]["2026-08-11"] == "İST"
