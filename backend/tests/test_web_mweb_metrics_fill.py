"""GA4 sonrası /web · /mweb otomatik doldurma."""

from pathlib import Path

from backend.services import page_tarama as store

ROOT = Path(__file__).resolve().parents[2]


def test_begin_scheduled_queues_web_without_quota():
    store.reset_for_tests()
    # Burn manual quota
    for _ in range(3):
        store.begin_manual("news")
    out = store.begin_scheduled("web")
    assert out["source"] == "scheduled"
    run = out["run"]
    assert run is not None
    assert run["page"] == "web"
    assert [j["id"] for j in run["jobs"]] == ["empower_intel", "market"]
    assert all(j["status"] == "queued" for j in run["jobs"])
    # Manual quota untouched by scheduled
    q = store.quota_status()
    assert q["remaining"] == 0


def test_begin_scheduled_mweb():
    store.reset_for_tests()
    out = store.begin_scheduled("mweb")
    assert [j["id"] for j in out["run"]["jobs"]] == ["empower_intel", "market"]


def test_ga4_job_schedules_web_mweb_fill():
    src = (ROOT / "backend" / "main.py").read_text(encoding="utf-8")
    assert "_schedule_web_mweb_metrics_fill_after_ga4" in src
    assert "_run_web_mweb_metrics_fill_job" in src
    assert "begin_scheduled" in src
    assert "web-mweb-metrics-fill-after-ga4" in src


def test_config_delay_default():
    from backend.config import settings

    assert settings.web_mweb_metrics_fill_after_ga4_enabled is True
    assert int(settings.web_mweb_metrics_fill_after_ga4_delay_minutes) == 10


def test_page_tarama_js_lists_web_mweb():
    js = (ROOT / "static" / "js" / "page_tarama.js").read_text(encoding="utf-8")
    assert "web: [\"empower_intel\", \"market\"]" in js or 'web: ["empower_intel", "market"]' in js
    assert "mweb: [\"empower_intel\", \"market\"]" in js or 'mweb: ["empower_intel", "market"]' in js
