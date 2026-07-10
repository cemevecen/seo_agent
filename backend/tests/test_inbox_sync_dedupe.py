"""Inbox senkron tekilleştirme birim testleri."""

from backend.services.inbox_sync import _pick_unique_thread_refs


def test_pick_unique_thread_refs_prefers_specific_route():
    tid = "18f4c69d22cf8081"
    refs = [
        ("all", {"id": tid}),
        ("doviz", {"id": tid}),
        ("medya", {"id": tid}),
    ]
    out = _pick_unique_thread_refs(refs)
    assert len(out) == 1
    assert out[0][0] == "medya"
    assert out[0][1]["id"] == tid


def test_pick_unique_thread_refs_dedupes_same_route_twice():
    tid = "abc123"
    refs = [
        ("doviz", {"id": tid}),
        ("doviz", {"id": tid}),
    ]
    out = _pick_unique_thread_refs(refs)
    assert len(out) == 1
    assert out[0][0] == "doviz"


def test_pick_unique_thread_refs_keeps_distinct_threads():
    refs = [
        ("doviz", {"id": "t1"}),
        ("sinemalar", {"id": "t2"}),
    ]
    out = _pick_unique_thread_refs(refs)
    assert len(out) == 2
    assert {r[1]["id"] for r in out} == {"t1", "t2"}
