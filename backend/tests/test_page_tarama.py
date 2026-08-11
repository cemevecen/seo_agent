"""Sayfa tarama — üst «Sayfayı güncelle» yuvası, katalog ve kuyruk."""

from pathlib import Path

from backend.services import page_tarama as store

ROOT = Path(__file__).resolve().parents[2]

PAGES = {
    "home": "templates/partials/dashboard_content.html",
    "android": "templates/android.html",
    "ios": "templates/ios.html",
    "news": "templates/doviz_news.html",
    "virgul": "templates/ad.html",
    "notification": "templates/notification.html",
    "firebase": "templates/partials/firebase_content.html",
    "app": "templates/partials/app_content.html",
    "vitals": "templates/web_vitals.html",
    "alerts": "templates/partials/alerts_content.html",
    "seo": "templates/seo_audit.html",
    "s-firebase": "templates/s_firebase.html",
    "backlinks": "templates/backlinks.html",
    "policy": "templates/partials/policy_content.html",
    "errors": "templates/errors.html",
}


def test_base_has_overlay_and_script():
    text = (ROOT / "templates/base.html").read_text(encoding="utf-8")
    assert "pc-page-tarama-overlay" in text
    assert "/static/js/page_tarama.js" in text
    assert "pc-page-tarama-bar" in text
    assert "pc-page-tarama-close" in text


OLD_MANUAL_IDS = (
    "pc-refresh",
    "ia-refresh",
    "nt-refresh-sheet",
    "dn-refresh",
    "mz-refresh-sheets",
    "mz-refresh-sheets-side",
    "crash-refresh-btn",
    "app-intel-manual-refresh-btn",
    "wv-run",
    "refresh-alerts-button",
    "run-btn",
    "sf-refresh",
    "err-refresh-btn",
    "bl-refresh-btn",
    "policy-noads-refresh-btn",
)


def test_old_manual_refresh_buttons_removed():
    for rel in PAGES.values():
        text = (ROOT / rel).read_text(encoding="utf-8")
        for bid in OLD_MANUAL_IDS:
            assert f'id="{bid}"' not in text, f"{rel} still has #{bid}"


def test_all_listed_pages_have_slot_and_key():
    for key, rel in PAGES.items():
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert f'data-page-tarama="{key}"' in text, rel
        assert "data-page-tarama-slot" in text, rel


def test_js_uses_railway_queue_on_remote():
    js = (ROOT / "static/js/page_tarama.js").read_text(encoding="utf-8")
    assert "Update page" in js
    assert "/api/page-tarama/manual" in js
    assert "/api/page-tarama/quota" in js
    assert "/api/page-tarama/progress" in js
    assert "Load failed" in js
    assert "skipBridge" in js
    assert "useQueue" in js
    assert "runSequential(jobs, steps, 0, { skipBridge: true })" in js
    assert "At most 3" in js
    for key in PAGES:
        assert f"{key}:" in js or f'"{key}":' in js, key


def test_js_remote_does_not_fall_back_to_localhost_bridge():
    """Canlı panel kullanıcıları kuyruk sonrası 127.0.0.1 köprüye düşmemeli."""
    js = (ROOT / "static/js/page_tarama.js").read_text(encoding="utf-8")
    start_idx = js.find("function start(")
    assert start_idx > 0
    chunk = js[start_idx : start_idx + 1800]
    assert "skipBridge: true" in chunk
    assert "chain.then" not in chunk


def test_android_queue_claim_result():
    store.reset_for_tests()
    run = store.start_run("android")
    assert [j["id"] for j in run["jobs"]] == ["play", "firebase", "market"]
    assert all(j["status"] == "queued" for j in run["jobs"])
    first = store.claim_next()
    assert first is not None
    assert first["job_id"] == "play"
    assert store.claim_next() is None
    store.mark_running(run["id"], "play", "Mac tarama çalışıyor")
    store.record_result(run["id"], "play", ok=True, message="ok")
    second = store.claim_next()
    assert second is not None
    assert second["job_id"] == "firebase"


def test_ios_and_news_and_notification_catalog():
    store.reset_for_tests()
    assert [j["id"] for j in store.jobs_for("ios")] == ["asc", "firebase"]
    assert [j["id"] for j in store.jobs_for("news")] == ["news"]
    assert [j["id"] for j in store.jobs_for("notification")] == ["notification"]
    assert [j["id"] for j in store.jobs_for("vitals")] == ["cwv"]
    assert [j["id"] for j in store.jobs_for("policy")] == ["policy", "noads"]
    assert [j["id"] for j in store.jobs_for("errors")] == ["errors"]


def test_alerts_has_no_bridge_queue():
    store.reset_for_tests()
    try:
        store.start_run("alerts")
        assert False, "alerts should not queue"
    except ValueError as exc:
        assert "no_bridge_jobs" in str(exc)


def test_stale_queue_fails_without_bridge():
    store.reset_for_tests()
    run = store.start_run("news")
    run_id = run["id"]
    with store._lock:
        store._runs[run_id]["started_at"] -= store.BRIDGE_STALE_SEC + 1
    out = store.get_run(run_id)
    assert out is not None
    assert out["jobs"][0]["status"] == "fail"
    assert "daemon" in (out["jobs"][0]["detail"] or "").lower()


def test_manual_limit_three_per_hour():
    store.reset_for_tests()
    for _ in range(store.MANUAL_LIMIT):
        out = store.begin_manual("news")
        assert out["run"] is not None
        assert out["quota"]["remaining"] >= 0
    try:
        store.begin_manual("android")
        assert False, "4th manual should be blocked"
    except store.ManualLimitExceeded as exc:
        assert exc.quota["remaining"] == 0
        assert exc.quota["retry_after_sec"] > 0
        assert "3" in (exc.quota["message"] or "")


def test_manual_limit_expires_after_window():
    store.reset_for_tests()
    store.begin_manual("news")
    store.begin_manual("news")
    store.begin_manual("news")
    with store._lock:
        store._manual_starts[:] = [t - store.MANUAL_WINDOW_SEC - 1 for t in store._manual_starts]
    out = store.begin_manual("ios")
    assert out["run"] is not None
    assert out["quota"]["used"] == 1


def test_manual_alerts_consumes_quota_without_queue():
    store.reset_for_tests()
    out = store.begin_manual("alerts")
    assert out["run"] is None
    assert out["quota"]["used"] == 1


def test_queue_persists_for_other_workers():
    """start bir worker’da, claim başka worker belleğinde boş olsa da DB’den görünür."""
    from backend.database import SessionLocal, init_db
    from backend.models import PageTaramaState

    init_db()
    with SessionLocal() as db:
        db.query(PageTaramaState).delete()
        db.commit()
    store._memory_only = False
    store._runs.clear()
    store._bridge_seen_at = None
    run = store.start_run("news")
    run_id = run["id"]
    store._runs.clear()
    out = store.get_run(run_id)
    assert out is not None
    assert out["jobs"][0]["id"] == "news"
    claimed = store.claim_next()
    assert claimed is not None
    assert claimed["run_id"] == run_id
    store.reset_for_tests()
