"""GitLab board sütun sırası kalıcılığı."""

from backend.database import Base, SessionLocal, engine
from backend.models import GitlabBoardIssueOrder, GitlabBoardProjectSettings
from backend.services.gitlab_board import (
    get_board_column_orders,
    get_board_project_settings,
    normalize_board_sort_mode,
    save_board_column_order,
    save_board_project_settings,
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
