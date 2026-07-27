"""GitLab board star → home chip mapping."""

from backend.services.gitlab_board_stars import (
    HOME_CHIPS,
    classify_board_list,
    home_order_project_key,
    resolve_chip_for_project,
)


def test_resolve_chip_for_doviz_ios():
    meta = resolve_chip_for_project("ios/doviz")
    assert meta == {"product": "doviz", "platform": "ios", "source_label": "iOS"}


def test_home_chips_include_web_only_once():
    assert [c["id"] for c in HOME_CHIPS["doviz"]] == ["web", "ios", "android"]
    assert [c["id"] for c in HOME_CHIPS["sinemalar"]] == ["web"]


def test_classify_board_list():
    assert classify_board_list("closed", []) == "closed"
    assert classify_board_list("opened", ["Doing"]) == "doing"
    assert classify_board_list("opened", ["Testing"]) == "testing"
    assert classify_board_list("opened", []) == "open"


def test_home_order_project_key():
    assert home_order_project_key("doviz", "ios") == "home_git_nokta::doviz::ios"
