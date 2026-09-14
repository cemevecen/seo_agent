from backend.services.settings_menu_access import (
    is_ai_page_allowed_email,
    is_ai_page_path,
    is_settings_menu_allowed_email,
    member_denied_ai_access,
    member_denied_settings_access,
    resolve_settings_menu_visible,
)


def test_settings_allowlist():
    assert is_settings_menu_allowed_email("cemevecen@nokta.com")
    assert is_settings_menu_allowed_email("CEMEVECEN@Gmail.com")
    assert not is_settings_menu_allowed_email("other@nokta.com")
    assert not is_settings_menu_allowed_email("")


def test_member_denied():
    assert member_denied_settings_access("ops@nokta.com")
    assert not member_denied_settings_access("cemevecen@nokta.com")


def test_nav_visible():
    assert resolve_settings_menu_visible(
        member_email="ops@nokta.com", admin_authenticated=False
    ) is False
    assert resolve_settings_menu_visible(
        member_email="cemevecen@gmail.com", admin_authenticated=False
    ) is True
    assert resolve_settings_menu_visible(member_email=None, admin_authenticated=True) is False


def test_ai_page_under_settings_allowlist():
    assert is_ai_page_path("/ai")
    assert is_ai_page_path("/ai/generate")
    assert not is_ai_page_path("/ais")
    assert is_ai_page_allowed_email("cemevecen@nokta.com")
    assert not is_ai_page_allowed_email("ops@nokta.com")
    assert member_denied_ai_access("ops@nokta.com")
    assert not member_denied_ai_access("cemevecen@gmail.com")
