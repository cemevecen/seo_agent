"""GitLab board sütun sırası kalıcılığı."""

from unittest.mock import AsyncMock, patch

import pytest

from backend.database import Base, SessionLocal, engine
from backend.models import GitlabBoardIssueOrder, GitlabBoardProjectSettings
from backend.services.gitlab_board import (
    get_board_column_orders,
    get_board_project_settings,
    normalize_board_move_labels,
    normalize_board_sort_mode,
    save_board_column_order,
    save_board_project_settings,
    sync_column_order_to_gitlab,
)


def test_save_and_load_board_column_order():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        save_board_column_order(db, "ios/doviz", "__open__", [42, 17, 83])
        orders = get_board_column_orders(db, "ios/doviz")
        assert orders["__open__"] == [42, 17, 83]

        save_board_column_order(db, "ios/doviz", "__open__", [17, 42])
        orders = get_board_column_orders(db, "ios/doviz")
        assert orders["__open__"] == [17, 42]
        assert db.query(GitlabBoardIssueOrder).filter_by(project_path="ios/doviz", list_key="__open__").count() == 2
    finally:
        db.query(GitlabBoardIssueOrder).filter_by(project_path="ios/doviz").delete()
        db.commit()
        db.close()


def test_save_and_load_board_sort_settings():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        save_board_project_settings(db, "ios/doviz", "updated_at_desc")
        settings = get_board_project_settings(db, "ios/doviz")
        assert settings["sort_mode"] == "updated_at_desc"
        assert normalize_board_sort_mode("invalid") == "manual"
        save_board_project_settings(db, "ios/doviz", "relative_position")
        settings = get_board_project_settings(db, "ios/doviz")
        assert settings["sort_mode"] == "relative_position"
    finally:
        db.query(GitlabBoardProjectSettings).filter_by(project_path="ios/doviz").delete()
        db.commit()
        db.close()


@pytest.mark.asyncio
async def test_sync_column_order_moves_first_issue_before_current_top():
    target = [
        {"iid": 45, "id": 1045},
        {"iid": 42, "id": 1042},
        {"iid": 38, "id": 1038},
    ]
    current = [
        {"iid": 6, "id": 1006},
        {"iid": 45, "id": 1045},
        {"iid": 42, "id": 1042},
    ]
    calls = []

    async def fake_reorder(project_path, issue_iid, *, move_after_id=None, move_before_id=None):
        calls.append((issue_iid, move_after_id, move_before_id))
        return {"iid": issue_iid, "id": issue_iid + 1000}

    with patch("backend.services.gitlab_board.reorder_issue_async", new=AsyncMock(side_effect=fake_reorder)):
        result = await sync_column_order_to_gitlab("nokta/sinemalar", target, current_ordered=current)

    assert result["synced"] == 3
    assert result["failed"] == 0
    assert calls[0] == (45, None, 1006)
    assert calls[1] == (42, 1045, None)
    assert calls[2] == (38, 1042, None)


@pytest.mark.asyncio
async def test_sync_column_order_skips_already_aligned_prefix():
    target = [
        {"iid": 6, "id": 1006},
        {"iid": 45, "id": 1045},
    ]
    current = [
        {"iid": 6, "id": 1006},
        {"iid": 7, "id": 1007},
    ]
    calls = []

    async def fake_reorder(project_path, issue_iid, *, move_after_id=None, move_before_id=None):
        calls.append((issue_iid, move_after_id, move_before_id))
        return {"iid": issue_iid}

    with patch("backend.services.gitlab_board.reorder_issue_async", new=AsyncMock(side_effect=fake_reorder)):
        result = await sync_column_order_to_gitlab("nokta/sinemalar", target, current_ordered=current)

    assert result["skipped"] == 1
    assert calls == [(45, 1006, None)]


def test_normalize_board_move_labels_prefers_remove_list():
    add, remove = normalize_board_move_labels(
        from_label="Doing",
        to_label="Review",
        remove_labels=["Doing", "Backlog"],
    )
    assert add == ["Review"]
    assert remove == ["Doing", "Backlog"]


def test_normalize_board_move_labels_skips_duplicate_target():
    add, remove = normalize_board_move_labels(
        from_label="Doing",
        to_label="Review",
        remove_labels=["Doing", "Review"],
    )
    assert add == ["Review"]
    assert remove == ["Doing"]
