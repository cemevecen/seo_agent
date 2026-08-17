"""GA4 kota ölçümü — `return_property_quota` + PropertyQuota örnekleme.

Kota token cinsinden ölçülür ve kalanı yalnızca API söyleyebilir. Buradaki
sözleşme: istek bayrağı her çağrıda açılır, yanıttaki kota okunur, ölçüm hatası
hiçbir koşulda GA4 çağrısını bozmaz.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services import ga4_quota as q


class _Status:
    def __init__(self, consumed, remaining):
        self.consumed = consumed
        self.remaining = remaining


def _quota(day=(100, 199900), hour=(20, 39980), proj=(20, 13980), conc=(1, 9)):
    return SimpleNamespace(
        tokens_per_day=_Status(*day),
        tokens_per_hour=_Status(*hour),
        tokens_per_project_per_hour=_Status(*proj),
        concurrent_requests=_Status(*conc),
    )


class _Req:
    def __init__(self, property_id="123456"):
        self.property = f"properties/{property_id}"
        self.return_property_quota = False


class _Resp:
    def __init__(self, quota=None):
        self.property_quota = quota
        self.rows = []


class _Inner:
    """Sahte GA4 client."""

    def __init__(self, quota=None, boom=False):
        self.calls = []
        self._quota = quota
        self._boom = boom
        self.some_other_attr = "değişmeden geçmeli"

    def run_report(self, request, **kw):
        if self._boom:
            raise RuntimeError("ga4 patladı")
        self.calls.append(("run_report", request, kw))
        return _Resp(self._quota)

    def run_realtime_report(self, request, **kw):
        self.calls.append(("run_realtime_report", request, kw))
        return _Resp(self._quota)


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    q._LATEST.clear()
    q._LAST_SAMPLE.clear()
    # Test kayıt yazmasın; yazma yolu ayrıca test ediliyor
    monkeypatch.setattr(q, "record_quota", q.record_quota)
    yield
    q._LATEST.clear()
    q._LAST_SAMPLE.clear()


def _no_db(monkeypatch):
    """DB'ye yazmayı engelle — sadece bellek yolunu sına."""
    monkeypatch.setattr(q, "_should_persist", lambda *a, **k: False)


def test_flag_is_enabled_on_every_request(monkeypatch):
    _no_db(monkeypatch)
    inner = _Inner(_quota())
    client = q.track(inner)
    req = _Req()
    assert req.return_property_quota is False
    client.run_report(req)
    assert req.return_property_quota is True


def test_quota_is_read_from_the_response(monkeypatch):
    _no_db(monkeypatch)
    client = q.track(_Inner(_quota(day=(500, 199500))))
    client.run_report(_Req("777"))
    latest = q.latest_snapshot()
    assert len(latest) == 1
    row = latest[0]
    assert row["property_id"] == "777"
    assert row["kind"] == "core"
    assert row["tokens_per_day_consumed"] == 500
    assert row["tokens_per_day_remaining"] == 199500
    assert row["concurrent_requests_remaining"] == 9


def test_realtime_goes_to_its_own_bucket(monkeypatch):
    _no_db(monkeypatch)
    client = q.track(_Inner(_quota()), kind="realtime")
    client.run_realtime_report(_Req("555"))
    kinds = {r["kind"] for r in q.latest_snapshot()}
    assert kinds == {"realtime"}


def test_core_client_still_marks_realtime_calls_as_realtime(monkeypatch):
    """Aynı proxy her iki çağrıyı da doğru kovaya yazmalı."""
    _no_db(monkeypatch)
    client = q.track(_Inner(_quota()), kind="core")
    client.run_report(_Req("1"))
    client.run_realtime_report(_Req("1"))
    kinds = sorted(r["kind"] for r in q.latest_snapshot())
    assert kinds == ["core", "realtime"]


def test_missing_quota_field_is_not_an_error(monkeypatch):
    _no_db(monkeypatch)
    client = q.track(_Inner(quota=None))
    client.run_report(_Req())
    assert q.latest_snapshot() == []


def test_measurement_failure_never_breaks_the_call(monkeypatch):
    def _explode(*a, **k):
        raise RuntimeError("ölçüm çöktü")

    monkeypatch.setattr(q, "_extract_quota", _explode)
    client = q.track(_Inner(_quota()))
    resp = client.run_report(_Req())  # patlamamalı
    assert resp is not None


def test_ga4_errors_are_propagated_unchanged(monkeypatch):
    _no_db(monkeypatch)
    client = q.track(_Inner(_quota(), boom=True))
    with pytest.raises(RuntimeError, match="ga4 patladı"):
        client.run_report(_Req())


def test_other_client_members_pass_through(monkeypatch):
    inner = _Inner(_quota())
    client = q.track(inner)
    assert client.some_other_attr == "değişmeden geçmeli"


def test_kwargs_are_forwarded(monkeypatch):
    """timeout gibi ek argümanlar kaybolmamalı."""
    _no_db(monkeypatch)
    inner = _Inner(_quota())
    q.track(inner).run_report(_Req(), timeout=42)
    assert inner.calls[0][2] == {"timeout": 42}


def test_request_passed_as_keyword_is_handled(monkeypatch):
    _no_db(monkeypatch)
    inner = _Inner(_quota())
    req = _Req("909")
    q.track(inner).run_report(request=req)
    assert req.return_property_quota is True
    assert q.latest_snapshot()[0]["property_id"] == "909"


def test_track_does_not_double_wrap():
    once = q.track(_Inner())
    assert q.track(once) is once


def test_sampling_is_throttled_but_a_sharp_drop_still_records():
    key = "core:1"
    # İlk örnek her zaman yazılır
    assert q._should_persist(key, {"tokens_per_day_remaining": 1000}) is True
    q._LAST_SAMPLE[key] = (q.time.time(), 1000)
    # Aralık dolmadan küçük değişim yazılmaz
    assert q._should_persist(key, {"tokens_per_day_remaining": 990}) is False
    # Belirgin düşüş aralığı beklemez
    assert q._should_persist(key, {"tokens_per_day_remaining": 800}) is True


# ── Client fabrikaları gerçekten sarılmış mı ────────────────────────────────

@pytest.mark.parametrize(
    "path,factory",
    [
        ("backend/collectors/ga4.py", "def _client()"),
        ("backend/services/ga4_realtime.py", "def _build_client()"),
        ("backend/services/error_monitor.py", "def _build_ga4_client()"),
    ],
)
def test_every_client_factory_is_wrapped(path, factory):
    from pathlib import Path

    src = (Path(__file__).resolve().parents[2] / path).read_text(encoding="utf-8")
    body = src.split(factory, 1)[1].split("\ndef ", 1)[0]
    assert "track(" in body, f"{path}: client kota proxy'si ile sarılmamış"


def test_quota_endpoint_is_registered():
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "api/ga4.py").read_text(encoding="utf-8")
    assert '@router.get("/ga4/quota")' in src
    assert "quota_summary" in src
