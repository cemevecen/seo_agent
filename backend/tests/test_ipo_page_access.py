from backend.services.ipo_page_access import (
    is_ipo_page_allowed_email,
    is_ipo_page_path,
    member_denied_ipo_access,
    resolve_ipo_menu_visible,
)


def test_ipo_page_is_open_to_everyone():
    """IPO sekmesi herkese açık: e-posta kısıtı yok."""
    assert is_ipo_page_allowed_email("cemevecen@nokta.com")
    assert is_ipo_page_allowed_email("onurtorun@nokta.com")
    assert is_ipo_page_allowed_email("")


def test_ipo_paths():
    assert is_ipo_page_path("/ipo")
    assert is_ipo_page_path("/api/ipo/compare")
    assert not is_ipo_page_path("/api/ipo-other")


def test_ipo_menu_visible_for_all_members():
    assert resolve_ipo_menu_visible(member_email="cemevecen@gmail.com")
    assert resolve_ipo_menu_visible(member_email="other@nokta.com")
    assert resolve_ipo_menu_visible(member_email=None)
    assert not member_denied_ipo_access("other@nokta.com")
    assert not member_denied_ipo_access("cemevecen@nokta.com")
