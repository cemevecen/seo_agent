"""Çok makineli worker kuyruğu — ofis + ev Mac aynı kuyruğa bağlı.

Kapsam: kabiliyet-farkında claim, oturum hatasında devir, kimse yapamıyorsa
hızlı ve net hata, zamanlı işler için tek-çalıştırma kirası.
"""

import time

from backend.services import page_tarama as store

OFFICE = "cem-office-mac"
HOME = "cem-home-mac"

ALL_READY = {jid: store.READY_OK for jid in store.JOBS}


def _ready(**overrides: str) -> dict[str, str]:
    out = dict(ALL_READY)
    out.update(overrides)
    return out


def test_claim_skips_jobs_the_worker_cannot_run():
    """Virgül credential'ı olmayan ev Mac'i o işi kapmasın — ofis alsın."""
    store.reset_for_tests()
    run = store.start_run("virgul")
    home_ready = _ready(virgul="no_creds", revenue_targets="no_creds")
    assert store.claim_next(worker=HOME, ready=home_ready) is None
    got = store.claim_next(worker=OFFICE, ready=_ready())
    assert got is not None
    assert got["job_id"] == "virgul"
    assert got["worker"] == OFFICE
    job = store.get_run(run["id"])["jobs"][0]
    assert job["worker"] == OFFICE


def test_progress_shows_which_mac_runs_the_job():
    store.reset_for_tests()
    run = store.start_run("moderation")
    store.claim_next(worker=HOME, ready=_ready())
    store.mark_running(run["id"], "moderation", "scraping", worker=HOME)
    out = store.get_run(run["id"])
    assert out["current_worker"] == HOME
    assert f"on {HOME}" in out["message"]
    assert [w["name"] for w in out["workers"]] == [HOME]


def test_login_failure_hands_job_to_the_other_mac():
    """Oturumu ölü makine hata dönerse iş diğerine devredilir, kullanıcı hata görmez."""
    store.reset_for_tests()
    run = store.start_run("vitals")
    store.heartbeat_worker(OFFICE, ready=_ready())
    claimed = store.claim_next(worker=HOME, ready=_ready())
    assert claimed["job_id"] == "cwv"
    store.record_result(
        run["id"], "cwv", ok=False, message="GSC oturumu yok", worker=HOME, needs_login=True
    )
    job = store.get_run(run["id"])["jobs"][0]
    assert job["status"] == "queued"
    assert HOME in (job["detail"] or "")
    assert OFFICE in (job["detail"] or "")
    # Aynı makine ikinci kez alamaz; ofis alır → sonsuz ping-pong yok
    assert store.claim_next(worker=HOME, ready=_ready()) is None
    again = store.claim_next(worker=OFFICE, ready=_ready())
    assert again is not None and again["job_id"] == "cwv"
    store.record_result(
        run["id"], "cwv", ok=False, message="GSC oturumu yok", worker=OFFICE, needs_login=True
    )
    job = store.get_run(run["id"])["jobs"][0]
    assert job["status"] == "fail"
    assert "login required" in (job["detail"] or "")


def test_queue_fails_fast_when_no_mac_can_run_it():
    """Askıda kalma yok: kimse yapamıyorsa kısa sürede hangi makinede ne eksik yazılır."""
    store.reset_for_tests()
    run = store.start_run("virgul")
    store.heartbeat_worker(HOME, ready=_ready(virgul="no_creds", revenue_targets="no_creds"))
    with store._lock:
        for job in store._runs[run["id"]]["jobs"]:
            job["queued_at"] = time.time() - store.NO_CAPABLE_WORKER_SEC - 1
    out = store.get_run(run["id"])
    statuses = {j["id"]: j["status"] for j in out["jobs"]}
    assert statuses["virgul"] == "fail"
    detail = next(j["detail"] for j in out["jobs"] if j["id"] == "virgul")
    assert HOME in detail
    assert "no creds" in detail


def test_offline_worker_is_not_counted_as_capable():
    store.reset_for_tests()
    run = store.start_run("news")
    store.heartbeat_worker(OFFICE, ready=_ready())
    with store._lock:
        store._workers[OFFICE]["last_seen"] = time.time() - store.WORKER_STALE_SEC - 5
        store._runs[run["id"]]["jobs"][0]["queued_at"] = (
            time.time() - store.NO_CAPABLE_WORKER_SEC - 1
        )
    out = store.get_run(run["id"])
    assert out["jobs"][0]["status"] == "fail"
    assert "offline" in (out["jobs"][0]["detail"] or "")


def test_legacy_worker_without_capabilities_still_claims():
    """Eski bridge (kabiliyet bildirmeyen) çalışmaya devam etsin."""
    store.reset_for_tests()
    store.start_run("news")
    got = store.claim_next()
    assert got is not None and got["job_id"] == "news"
    assert [w["name"] for w in store.workers_public()] == [store.LEGACY_WORKER_NAME]


def test_legacy_mac_keeps_queue_alive_while_only_one_mac_is_updated():
    """Ofis güncel, ev eski koddayken kuyruk 'kimse yapamıyor' deyip işi düşürmesin."""
    store.reset_for_tests()
    run = store.start_run("virgul")
    store.heartbeat_worker(OFFICE, ready=_ready(virgul="no_creds", revenue_targets="no_creds"))
    store.claim_next()  # eski sürüm Mac'in yoklaması — kimlik bildirmiyor
    with store._lock:
        for job in store._runs[run["id"]]["jobs"]:
            if job["status"] == "queued":
                job["queued_at"] = time.time() - store.NO_CAPABLE_WORKER_SEC - 1
    out = store.get_run(run["id"])
    assert all(j["status"] != "fail" for j in out["jobs"]), out["jobs"]


def test_auto_lease_grants_one_mac_per_slot():
    store.reset_for_tests()
    first = store.auto_lease("sinemalar_moderation", "2026-08-17T14:17", HOME)
    assert first["granted"] is True
    second = store.auto_lease("sinemalar_moderation", "2026-08-17T14:17", OFFICE)
    assert second["granted"] is False
    assert second["holder"] == HOME
    # Aynı makine tekrar sorarsa (retry) engellenmez
    again = store.auto_lease("sinemalar_moderation", "2026-08-17T14:17", HOME)
    assert again["granted"] is True
    # Sonraki slot serbest
    nxt = store.auto_lease("sinemalar_moderation", "2026-08-18T03:04", OFFICE)
    assert nxt["granted"] is True


def test_auto_lease_expires_after_ttl():
    store.reset_for_tests()
    assert store.auto_lease("play", "slot-1", HOME, ttl_sec=1)["granted"] is True
    with store._lock:
        store._leases["play:slot-1"]["at"] = time.time() - 5
    assert store.auto_lease("play", "slot-1", OFFICE, ttl_sec=1)["granted"] is True


def test_workers_public_reports_not_ready_jobs():
    store.reset_for_tests()
    store.heartbeat_worker(HOME, ready=_ready(virgul="no_creds"), version="2026.08.17")
    rows = store.workers_public()
    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == HOME
    assert row["online"] is True
    assert row["not_ready"] == {"virgul": "no_creds"}
    assert row["version"] == "2026.08.17"
