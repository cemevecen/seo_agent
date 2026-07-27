"""Priority board sections for /boards."""

from backend.services.priority_board import PRIORITY_BOARD_COLUMNS, get_priority_board_sections


def test_priority_board_has_doviz_and_sinemalar():
    sections = get_priority_board_sections()
    assert [s["id"] for s in sections] == ["doviz", "sinemalar"]
    assert [c["id"] for c in PRIORITY_BOARD_COLUMNS] == ["open", "doing", "testing", "closed"]


def test_priority_board_columns_populated():
    sections = get_priority_board_sections()
    for section in sections:
        assert len(section["columns"]) == 4
        total = sum(c["count"] for c in section["columns"])
        assert total >= 4
        for col in section["columns"]:
            assert col["count"] == len(col["items"])
            for item in col["items"]:
                assert item["status"] == col["id"]
                assert item["title"]
                assert item.get("source_label")
