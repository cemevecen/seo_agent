"""Tarayıcı işleri tek kalıcı thread'de koşmalı.

Playwright'ın sync nesneleri thread'e bağlı. Köprü her işi ayrı thread'de
çalıştırınca sıcak Firefox penceresi yeniden kullanılamıyor, düşürülüyor ve
tarayıcı öldürülüp yeniden açılıyor. ASC'de bu, oturumun kaybı demek: 30 günlük
Apple güven çerezi diskte kalsa bile dqsid oturum çerezi tarayıcıyla ölüyor ve
sessiz doğrulama authResult=FAILED dönüyor.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _bridge():
    os.environ.setdefault("NOTIFICATION_INGEST_TOKEN", "x")
    spec = importlib.util.spec_from_file_location(
        "bridge_bw", ROOT / "scripts/doviz_admin_notification_bridge.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
    except SystemExit:
        pass
    return mod


def test_browser_jobs_share_one_thread():
    """Farklı çağıran thread'lerden gelen işler AYNI thread'de koşmalı."""
    m = _bridge()
    seen: list[int] = []

    def _job():
        seen.append(threading.get_ident())
        return {"ok": True}

    def _from_thread():
        m._call_on_browser_worker(lambda **k: _job())

    threads = [threading.Thread(target=_from_thread) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert len(seen) == 4
    assert len(set(seen)) == 1, f"işler {len(set(seen))} farklı thread'de koştu"


def test_worker_thread_is_not_the_caller():
    m = _bridge()
    caller = threading.get_ident()
    got = m._call_on_browser_worker(lambda **k: threading.get_ident())
    assert got != caller


def test_nested_call_runs_inline_without_deadlock():
    """Tek işçi kendi kuyruğunu beklerse kilitlenir — iç içe çağrı satır içi koşmalı."""
    m = _bridge()

    def _inner(**_):
        return "iç"

    def _outer(**_):
        assert m._on_browser_worker() is True
        return m._call_on_browser_worker(_inner)

    assert m._call_on_browser_worker(_outer) == "iç"


def test_exceptions_propagate_to_the_caller():
    m = _bridge()

    def _boom(**_):
        raise RuntimeError("tarama patladı")

    try:
        m._call_on_browser_worker(_boom)
    except RuntimeError as exc:
        assert "tarama patladı" in str(exc)
    else:
        raise AssertionError("hata yukarı taşınmalıydı")


def test_hop_is_centralised_so_every_path_is_covered():
    """Zamanlı, elle (/sync-*), claim ve ertelenmiş yollar aynı thread'e düşmeli.

    Elle tetikleme _run_locked_job'u doğrudan HTTP thread'inden çağırıyordu ve
    _run_browser_scrape_job'a yapılan sarma onu ıskalıyordu — ölçüldü: ikinci
    ASC turunda pencere yine öldü.
    """
    src = (ROOT / "scripts/doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    body = src.split("def _run_locked_job(", 1)[1].split("\ndef ", 1)[0]
    assert "_call_on_browser_worker" in body
    assert "_on_browser_worker()" in body
    # kilit alınmadan ÖNCE geçilmeli
    assert body.index("_call_on_browser_worker") < body.index("lock.acquire")


def test_non_browser_jobs_do_not_hop():
    m = _bridge()
    assert m._is_browser_scrape_kind("notification") is False
    assert m._is_browser_scrape_kind("asc") is True


def test_worker_is_reused_across_calls():
    m = _bridge()
    first = m._browser_worker()
    assert m._browser_worker() is first
