"""git.nokta home board — GitLab issue bucketing."""

from backend.services.priority_board import (
    PRIORITY_BOARD_COLUMNS,
    _classify_bucket,
    _top_entries,
    get_priority_board_sections,
)


def test_priority_board_columns_defined():
    assert [c["id"] for c in PRIORITY_BOARD_COLUMNS] == ["open", "doing", "testing", "closed"]


def test_get_priority_board_sections_empty_shape():
    sections = get_priority_board_sections()
    assert [s["id"] for s in sections] == ["doviz", "sinemalar"]
    for section in sections:
        assert len(section["columns"]) == 4
        for col in section["columns"]:
            assert col["count"] == 0
            assert col["entries"] == []


def test_classify_bucket_open_doing_testing_closed():
    assert _classify_bucket({"state": "closed", "labels": []}) == "closed"
    assert _classify_bucket({"state": "opened", "labels": ["Doing"]}) == "doing"
    assert _classify_bucket({"state": "opened", "labels": ["Testing"]}) == "testing"
    assert _classify_bucket({"state": "opened", "labels": []}) == "open"
    assert _classify_bucket({"state": "opened", "labels": [{"name": "Doing"}]}) == "doing"


def test_top_entries_open_and_closed_limit_three_by_date():
    issues = [
        {
            "iid": 1,
            "title": "Old open",
            "state": "opened",
            "labels": [],
            "updated_at": "2026-01-01T10:00:00Z",
            "_pc_source_label": "Web",
            "_pc_project_path": "nokta/doviz",
        },
        {
            "iid": 2,
            "title": "New open",
            "state": "opened",
            "labels": [],
            "updated_at": "2026-07-20T10:00:00Z",
            "_pc_source_label": "iOS",
            "_pc_project_path": "ios/doviz",
        },
        {
            "iid": 3,
            "title": "Mid open",
            "state": "opened",
            "labels": [],
            "updated_at": "2026-06-01T10:00:00Z",
            "_pc_source_label": "Android",
            "_pc_project_path": "android/doviz",
        },
        {
            "iid": 4,
            "title": "Doing skip",
            "state": "opened",
            "labels": ["Doing"],
            "updated_at": "2026-07-21T10:00:00Z",
            "_pc_source_label": "Web",
            "_pc_project_path": "nokta/doviz",
        },
        {
            "iid": 10,
            "title": "Closed new",
            "state": "closed",
            "labels": [],
            "closed_at": "2026-07-19T12:00:00Z",
            "updated_at": "2026-07-19T12:00:00Z",
            "_pc_source_label": "Web",
            "_pc_project_path": "nokta/doviz",
        },
        {
            "iid": 11,
            "title": "Closed old",
            "state": "closed",
            "labels": [],
            "closed_at": "2026-02-01T12:00:00Z",
            "updated_at": "2026-02-01T12:00:00Z",
            "_pc_source_label": "Web",
            "_pc_project_path": "nokta/doviz",
        },
        {
            "iid": 12,
            "title": "Closed mid",
            "state": "closed",
            "labels": [],
            "closed_at": "2026-05-01T12:00:00Z",
            "updated_at": "2026-05-01T12:00:00Z",
            "_pc_source_label": "Web",
            "_pc_project_path": "nokta/doviz",
        },
        {
            "iid": 13,
            "title": "Closed extra",
            "state": "closed",
            "labels": [],
            "closed_at": "2026-03-01T12:00:00Z",
            "updated_at": "2026-03-01T12:00:00Z",
            "_pc_source_label": "Web",
            "_pc_project_path": "nokta/doviz",
        },
    ]
    open_top = _top_entries(issues, status="open", limit=3)
    assert [e["iid"] for e in open_top] == [2, 3, 1]
    assert all(e["status"] == "open" for e in open_top)

    closed_top = _top_entries(issues, status="closed", limit=3)
    assert [e["iid"] for e in closed_top] == [10, 12, 13]
    assert len(closed_top) == 3
    assert closed_top[0]["date_label"]
