"""AI Talk page context tests."""

from backend.services.page_context_tools import format_page_context_for_prompt


def test_format_page_context_includes_path():
    text = format_page_context_for_prompt({"path": "/firebase", "label": "Firebase", "filters": {"product": "doviz"}})
    assert "firebase" in text.lower()
    assert "doviz" in text
    assert "aktif sayfa" in text.lower()


def test_format_page_context_empty():
    assert format_page_context_for_prompt(None) == ""
    assert format_page_context_for_prompt({}) == ""
