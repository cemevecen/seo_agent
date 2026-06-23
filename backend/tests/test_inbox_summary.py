"""Inbox özet e-postası birim testleri."""

from datetime import datetime, timedelta, timezone

from backend.services.inbox_summary import (
    INBOX_SUMMARY_SECTIONS,
    INBOX_SUMMARY_TAB_ORDER,
    _group_unread_threads,
    _normalize_summary_route,
    _summary_cutoff_ms,
    build_inbox_summary_html,
)


class _Thread:
    def __init__(self, *, id: int, subject: str, route_tag: str, snippet: str = "", last_internal_ms: int = 0):
        self.id = id
        self.subject = subject
        self.route_tag = route_tag
        self.snippet = snippet
        self.last_internal_ms = last_internal_ms


class _Message:
    def __init__(self, *, thread_id: int, from_addr: str, subject: str, body_text: str, internal_ms: int):
        self.thread_id = thread_id
        self.from_addr = from_addr
        self.subject = subject
        self.body_text = body_text
        self.body_html = ""
        self.internal_ms = internal_ms
        self.is_outbound = False


class _FakeQuery:
    def __init__(self, messages: list[_Message] | None = None):
        self._messages = messages or []
        self._filters: list = []

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return None

    def all(self):
        return list(self._messages)


class _FakeDb:
    def __init__(self, messages: list[_Message] | None = None):
        self._messages = messages or []

    def query(self, model):
        return _FakeQuery(self._messages)


def test_normalize_feedback_to_doviz():
    assert _normalize_summary_route("feedback") == "doviz"
    assert _normalize_summary_route("ziyaret") == "nstat"


def test_summary_html_section_order():
    keys = [s[0] for s in INBOX_SUMMARY_SECTIONS]
    assert keys == list(INBOX_SUMMARY_TAB_ORDER)
    assert keys == ["medya", "doviz", "sinemalar", "nstat", "firebase"]


def test_summary_html_five_sections_no_reklam_all():
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    grouped = {
        "firebase": [_Thread(id=1, subject="Crash", route_tag="firebase", snippet="NPE", last_internal_ms=now_ms)],
        "doviz": [],
        "sinemalar": [],
        "nstat": [],
    }
    msg = _Message(
        thread_id=1,
        from_addr="crash@firebase",
        subject="Crash",
        body_text="NPE stack",
        internal_ms=now_ms,
    )
    html_out = build_inbox_summary_html(grouped, _FakeDb([msg]))
    for _key, title, *_rest in INBOX_SUMMARY_SECTIONS:
        assert title in html_out
    assert ">reklam<" not in html_out
    assert ">all<" not in html_out
    assert "Crash" in html_out
    assert "NPE stack" in html_out
    assert "Bu sekmede konuşma yok." in html_out


def test_group_excludes_reklam_all_and_old_threads():
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    old_ms = int((datetime.now(timezone.utc) - timedelta(days=10)).timestamp() * 1000)
    threads = [
        _Thread(id=1, subject="A", route_tag="doviz", last_internal_ms=now_ms),
        _Thread(id=2, subject="B", route_tag="reklam", last_internal_ms=now_ms),
        _Thread(id=3, subject="C", route_tag="all", last_internal_ms=now_ms),
        _Thread(id=4, subject="D", route_tag="firebase", last_internal_ms=old_ms),
    ]
    grouped = _group_unread_threads(threads, cutoff_ms=_summary_cutoff_ms())
    assert list(grouped.keys()) == ["doviz"]
    assert len(grouped["doviz"]) == 1
