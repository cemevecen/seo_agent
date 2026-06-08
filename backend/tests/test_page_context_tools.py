"""AI Talk page context tests."""

from backend.services.page_context_tools import format_page_context_for_prompt


def test_format_page_context_includes_path():
    text = format_page_context_for_prompt({"path": "/firebase", "label": "Firebase", "filters": {"product": "doviz"}})
    assert "firebase" in text.lower()
    assert "doviz" in text
    assert "aktif sayfa" in text.lower()
    assert "analysis_hints" in text
    assert "çıkarım" in text.lower() or "öneri" in text.lower()


def test_format_page_context_ad_hints():
    text = format_page_context_for_prompt({"path": "/ad", "page_id": "ad", "label": "Monetizasyon"})
    assert "page_fetch_mz_analytics" in text
    assert "analysis_hints" in text


def test_format_page_context_empty():
    assert format_page_context_for_prompt(None) == ""
    assert format_page_context_for_prompt({}) == ""
