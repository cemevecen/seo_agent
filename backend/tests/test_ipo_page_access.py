from backend.services.ipo_page_access import (
    is_ipo_page_allowed_email,
    is_ipo_page_path,
    member_denied_ipo_access,
    resolve_ipo_menu_visible,
)


def test_ipo_page_allowed_emails():
    assert is_ipo_page_allowed_email("cemevecen@nokta.com")
    assert is_ipo_page_allowed_email("CEMEVECEN@Gmail.com")
    assert not is_ipo_page_allowed_email("onurtorun@nokta.com")
    assert not is_ipo_page_allowed_email("")


def test_ipo_paths():
    assert is_ipo_page_path("/ipo")
    assert is_ipo_page_path("/api/ipo/compare")
    assert not is_ipo_page_path("/api/ipo-other")


def test_ipo_menu_and_denied():
    assert resolve_ipo_menu_visible(member_email="cemevecen@gmail.com")
    assert not resolve_ipo_menu_visible(member_email="other@nokta.com")
    assert member_denied_ipo_access("other@nokta.com")
    assert not member_denied_ipo_access("cemevecen@nokta.com")
