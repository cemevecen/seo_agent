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
                assert cells[nxt] in ("", "Yİ", "RP"), f"{name} {iso}-> {cells[nxt]}"
