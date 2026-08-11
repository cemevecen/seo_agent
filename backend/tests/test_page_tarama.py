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
    assert "Sayfayı güncelle" in js
    assert "/api/page-tarama/start" in js
    assert "/api/page-tarama/progress" in js
    assert "Load failed" in js
    for key in PAGES:
        assert f"{key}:" in js or f'"{key}":' in js, key


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
