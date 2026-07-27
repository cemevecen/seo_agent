"""GitLab board star → home chip mapping."""

from backend.services.gitlab_board_stars import (
    HOME_CHIPS,
    classify_board_list,
    resolve_chip_for_project,
)


def test_resolve_chip_for_doviz_ios():
    meta = resolve_chip_for_project("ios/doviz")
    assert meta == {"product": "doviz", "platform": "ios", "source_label": "iOS"}


def test_home_chips_include_web_mweb():
    assert [c["id"] for c in HOME_CHIPS["doviz"]] == ["web", "mweb", "ios", "android"]
    assert [c["id"] for c in HOME_CHIPS["sinemalar"]] == ["web", "mweb"]


def test_classify_board_list():
    assert classify_board_list("closed", []) == "closed"
    assert classify_board_list("opened", ["Doing"]) == "doing"
    assert classify_board_list("opened", ["Testing"]) == "testing"
    assert classify_board_list("opened", []) == "open"
