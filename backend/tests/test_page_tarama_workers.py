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
    run = store.start_run("backlinks")
    store.heartbeat_worker(OFFICE, ready=_ready())
    claimed = store.claim_next(worker=HOME, ready=_ready())
    assert claimed["job_id"] == "links"
    store.record_result(
        run["id"], "links", ok=False, message="GSC oturumu yok", worker=HOME, needs_login=True
    )
    job = store.get_run(run["id"])["jobs"][0]
    assert job["status"] == "queued"
    assert HOME in (job["detail"] or "")
    assert OFFICE in (job["detail"] or "")
    # Aynı makine ikinci kez alamaz; ofis alır → sonsuz ping-pong yok
    assert store.claim_next(worker=HOME, ready=_ready()) is None
    again = store.claim_next(worker=OFFICE, ready=_ready())
    assert again is not None and again["job_id"] == "links"
    store.record_result(
        run["id"], "links", ok=False, message="GSC oturumu yok", worker=OFFICE, needs_login=True
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


def test_button_press_prefers_the_mac_you_pressed_it_on():
    """Ofiste Update page'e basınca tarayıcı ev Mac'inde açılmasın."""
    store.reset_for_tests()
    store.heartbeat_worker(OFFICE, ready=_ready())
    store.heartbeat_worker(HOME, ready=_ready())
    store.begin_manual("moderation", prefer=OFFICE)
    assert store.claim_next(worker=HOME, ready=_ready()) is None
    got = store.claim_next(worker=OFFICE, ready=_ready())
    assert got is not None and got["worker"] == OFFICE


def test_preference_expires_so_the_other_mac_can_take_over():
    """Tercih edilen Mac meşgulse iş sonsuza kadar beklemesin."""
    store.reset_for_tests()
    store.heartbeat_worker(OFFICE, ready=_ready())
    store.heartbeat_worker(HOME, ready=_ready())
    out = store.begin_manual("moderation", prefer=OFFICE)
    with store._lock:
        for job in store._runs[out["run"]["id"]]["jobs"]:
            job["prefer_until"] = time.time() - 1
    got = store.claim_next(worker=HOME, ready=_ready())
    assert got is not None and got["worker"] == HOME


def test_preference_ignored_when_that_mac_is_offline():
    store.reset_for_tests()
    store.heartbeat_worker(HOME, ready=_ready())
    store.begin_manual("moderation", prefer=OFFICE)  # ofis hiç kayıtlı değil → offline
    got = store.claim_next(worker=HOME, ready=_ready())
    assert got is not None and got["worker"] == HOME


def test_no_preference_keeps_first_free_mac_behaviour():
    store.reset_for_tests()
    store.heartbeat_worker(HOME, ready=_ready())
    store.begin_manual("moderation")
    got = store.claim_next(worker=HOME, ready=_ready())
    assert got is not None and got["job_id"] == "moderation"


def test_login_window_opens_on_the_mac_you_pressed_from():
    """İki Mac'te de oturum yoksa iş, düğmeye basılan makineye verilir (giriş penceresi orada)."""
    store.reset_for_tests()
    both_logged_out = _ready(moderation="login_required")
    store.heartbeat_worker(HOME, ready=both_logged_out)
    store.heartbeat_worker(OFFICE, ready=both_logged_out)
    store.begin_manual("moderation", prefer=OFFICE)
    # Kullanıcının başında olmadığı Mac işi almaz
    assert store.claim_next(worker=HOME, ready=both_logged_out) is None
    got = store.claim_next(worker=OFFICE, ready=both_logged_out)
    assert got is not None
    assert got["worker"] == OFFICE
    assert got["login_ok"] is True
    job = store.get_run(got["run_id"])["jobs"][0]
    assert "waiting for login" in (job["detail"] or "")


def test_capable_mac_still_wins_over_login_prompt():
    """Diğer Mac'in oturumu varsa kullanıcıyı giriş yapmaya zorlama — iş oraya gitsin."""
    store.reset_for_tests()
    store.heartbeat_worker(HOME, ready=_ready())
    store.heartbeat_worker(OFFICE, ready=_ready(moderation="login_required"))
    store.begin_manual("moderation", prefer=OFFICE)
    assert store.claim_next(worker=OFFICE, ready=_ready(moderation="login_required")) is None
    got = store.claim_next(worker=HOME, ready=_ready())
    assert got is not None and got["worker"] == HOME and got["login_ok"] is False


def test_missing_credentials_never_trigger_a_login_prompt():
    """Eksik olan credential ise giriş penceresi çözüm değil — iş düşsün, mesaj net olsun."""
    store.reset_for_tests()
    no_creds = _ready(virgul="no_creds", revenue_targets="no_creds")
    store.heartbeat_worker(OFFICE, ready=no_creds)
    out = store.begin_manual("virgul", prefer=OFFICE)
    assert store.claim_next(worker=OFFICE, ready=no_creds) is None
    with store._lock:
        for job in store._runs[out["run"]["id"]]["jobs"]:
            job["queued_at"] = time.time() - store.NO_CAPABLE_WORKER_SEC - 1
    statuses = {j["id"]: j["status"] for j in store.get_run(out["run"]["id"])["jobs"]}
    assert statuses["virgul"] == "fail"


def test_job_is_not_failed_while_the_user_can_still_log_in():
    """Giriş bekleyen makine varken kuyruk işi 75 sn'de düşürmesin."""
    store.reset_for_tests()
    logged_out = _ready(moderation="login_required")
    store.heartbeat_worker(OFFICE, ready=logged_out)
    out = store.begin_manual("moderation", prefer=OFFICE)
    with store._lock:
        for job in store._runs[out["run"]["id"]]["jobs"]:
            job["queued_at"] = time.time() - store.NO_CAPABLE_WORKER_SEC - 1
    assert store.get_run(out["run"]["id"])["jobs"][0]["status"] == "queued"


def test_lease_request_does_not_create_a_phantom_worker():
    """Kira isteği worker listesine satır eklemesin — panelde hayalet makine olmasın."""
    store.reset_for_tests()
    store.auto_lease("play", "slot-x", "tek-seferlik-istek")
    assert [w["name"] for w in store.workers_public()] == []


def test_stale_unknown_worker_records_are_forgotten():
    """Eski protokol yoklaması kısa sürede düşsün; gerçek worker 24 saat kalsın."""
    store.reset_for_tests()
    store.heartbeat_worker(store.LEGACY_WORKER_NAME)
    store.heartbeat_worker(OFFICE, ready=_ready())
    with store._lock:
        old = time.time() - store.WORKER_FORGET_UNKNOWN_SEC - 5
        store._workers[store.LEGACY_WORKER_NAME]["last_seen"] = old
        store._workers[OFFICE]["last_seen"] = old
    store.claim_next(worker=HOME, ready=_ready())  # prune tetikler
    names = {w["name"] for w in store.workers_public()}
    assert store.LEGACY_WORKER_NAME not in names
    assert OFFICE in names


def test_capability_note_ignores_long_gone_machines():
    store.reset_for_tests()
    run = store.start_run("virgul")
    store.heartbeat_worker(HOME, ready=_ready(virgul="no_creds", revenue_targets="no_creds"))
    store.heartbeat_worker("cok-eski-mac", ready=_ready())
    with store._lock:
        store._workers["cok-eski-mac"]["last_seen"] = time.time() - store.WORKER_NOTE_WINDOW_SEC - 5
        for job in store._runs[run["id"]]["jobs"]:
            job["queued_at"] = time.time() - store.NO_CAPABLE_WORKER_SEC - 1
    detail = next(j["detail"] for j in store.get_run(run["id"])["jobs"] if j["id"] == "virgul")
    assert HOME in detail
    assert "cok-eski-mac" not in detail


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
