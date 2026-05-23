"""Inbox özet e-postası birim testleri."""

from backend.services.inbox_summary import (
    INBOX_SUMMARY_SECTIONS,
    _normalize_summary_route,
    build_inbox_summary_html,
)


class _Thread:
    def __init__(self, *, id: int, subject: str, route_tag: str, snippet: str = "", last_internal_ms: int = 0):
        self.id = id
        self.subject = subject
        self.route_tag = route_tag
        self.snippet = snippet
        self.last_internal_ms = last_internal_ms


class _FakeQuery:
    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return None


class _FakeDb:
    def query(self, model):
        return _FakeQuery()


def test_normalize_feedback_to_doviz():
    assert _normalize_summary_route("feedback") == "doviz"
    assert _normalize_summary_route("ziyaret") == "nstat"


def test_summary_html_section_order():
    keys = [s[0] for s in INBOX_SUMMARY_SECTIONS]
    assert keys == ["all", "doviz", "sinemalar", "reklam", "nstat", "firebase"]


def test_summary_html_includes_all_six_sections():
    grouped = {
        "firebase": [_Thread(id=1, subject="Crash", route_tag="firebase", snippet="NPE")],
        "doviz": [],
        "sinemalar": [],
        "reklam": [],
        "nstat": [],
        "all": [],
    }
    html_out = build_inbox_summary_html(grouped, _FakeDb())
    for _key, title, *_rest in INBOX_SUMMARY_SECTIONS:
        assert title in html_out
    assert "Crash" in html_out
    assert "Bu sekmede okunmamış mesaj yok." in html_out
