"""GitLab board star → home chip mapping."""

from backend.services.gitlab_board_stars import (
    HOME_CHIPS,
    classify_board_list,
    home_order_project_key,
    refresh_stars_from_gitlab,
    resolve_chip_for_project,
    star_to_dict,
)


def test_resolve_chip_for_doviz_ios():
    meta = resolve_chip_for_project("ios/doviz")
    assert meta == {"product": "doviz", "platform": "ios", "source_label": "iOS"}


def test_home_chips_include_web_only_once():
    assert [c["id"] for c in HOME_CHIPS["doviz"]] == ["web", "ios", "android"]
    assert [c["id"] for c in HOME_CHIPS["sinemalar"]] == ["web"]


def test_classify_board_list():
    assert classify_board_list("closed", []) == "closed"
    assert classify_board_list("closed", ["Doing"]) == "closed"
    assert classify_board_list("opened", ["Doing"]) == "doing"
    assert classify_board_list("opened", ["Testing"]) == "testing"
    assert classify_board_list("opened", []) == "open"


def test_home_order_project_key():
    assert home_order_project_key("doviz", "ios") == "home_git_nokta::doviz::ios"


def test_refresh_stars_moves_closed_from_doing(monkeypatch):
    class _Row:
        def __init__(self):
            self.project_path = "ios/doviz"
            self.issue_iid = 315
            self.product = "doviz"
            self.platform = "ios"
            self.title = "Cüzdan tanıtım"
            self.web_url = "https://git.nokta.com/ios/doviz/-/issues/315"
            self.state = "opened"
            self.labels_json = '["Doing"]'
            self.board_list = "doing"
            self.starred_at = None
            self.id = 1

    row = _Row()
    committed = {"n": 0}

    class _Query:
        def filter(self, *a, **k):
            return self

        def all(self):
            return [row]

    class _Db:
        def query(self, *a, **k):
            return _Query()

        def commit(self):
            committed["n"] += 1

    monkeypatch.setattr(
        "backend.services.gitlab_board_stars.fetch_issues_by_iids",
        lambda path, iids, timeout_sec=4.0: {
            315: {
                "iid": 315,
                "state": "closed",
                "labels": [],
                "title": "Cüzdan tanıtım ekranları",
                "web_url": row.web_url,
            }
        },
    )

    result = refresh_stars_from_gitlab(_Db())
    assert result["updated"] == 1
    assert row.board_list == "closed"
    assert row.state == "closed"
    assert committed["n"] == 1
    assert star_to_dict(row)["board_list"] == "closed"
