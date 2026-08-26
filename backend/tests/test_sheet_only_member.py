# -*- coding: utf-8 -*-
"""Sheet-only üye — yalnızca /sheet."""

from pathlib import Path

from backend.services import app_member_auth as ama
from backend.services.sheet_page_access import (
    SHEET_ONLY_MEMBER_EMAILS,
    is_sheet_only_member_email,
    sheet_only_member_path_allowed,
)


def test_sheet_only_emails():
    assert "evecensema@gmail.com" in SHEET_ONLY_MEMBER_EMAILS
    assert is_sheet_only_member_email("EvecenSema@gmail.com")
    assert not is_sheet_only_member_email("cemevecen@nokta.com")


def test_sheet_only_member_path_allowed():
    assert sheet_only_member_path_allowed("/sheet")
    assert sheet_only_member_path_allowed("/api/sheet/ayilma/generate")
    assert not sheet_only_member_path_allowed("/")
    assert not sheet_only_member_path_allowed("/ga4")


def test_base_template_sheet_only_nav():
    base = Path(__file__).resolve().parents[2] / "templates" / "base.html"
    text = base.read_text(encoding="utf-8")
    assert "is_sheet_only_member(request)" in text
