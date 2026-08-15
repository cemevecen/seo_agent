"""Bridge ŞU AN — biten / zombi progress listeden düşmeli."""

from __future__ import annotations

import importlib.util
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_bridge():
    path = ROOT / "scripts" / "doviz_admin_notification_bridge.py"
    mod_name = "doviz_admin_notification_bridge_stale_progress_test"
    import sys

    sys.modules.pop(mod_name, None)
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_finish_job_clears_firebase_dedicated_bag():
    mod = _load_bridge()
    mod._set_firebase_progress(
        running=True,
        phase="browser",
        step=0,
        total_steps=24,
        message="Firefox açılıyor",
        ts=time.time() - 100,
    )
    mod._set_job_progress("firebase", running=True, phase="browser", trigger="schedule")
    mod._finish_job_progress(
        "firebase",
        {"ok": False, "message": "scrape boom"},
        trigger="schedule",
        name="Firebase",
    )
    assert mod._firebase_progress.get("running") is False
    assert (mod._JOB_PROGRESS.get("firebase") or {}).get("running") is False


def test_reconcile_clears_zombie_when_browser_lock_free():
    mod = _load_bridge()
    # kilit tutulmuyor varsayımı — test ortamında locked() False olmalı
    assert mod._browser_scrape_lock.locked() is False
    mod._set_firebase_progress(
        running=True,
        phase="browser",
        step=0,
        total_steps=24,
        message="Firefox açılıyor",
    )
    mod._firebase_progress["ts"] = time.time() - 120
    mod._set_job_progress("firebase", running=True, phase="browser", trigger="schedule")
    mod._JOB_PROGRESS["firebase"]["ts"] = time.time() - 120

    running = mod._progress_running_jobs()
    kinds = {j.get("kind") for j in running}
    assert "firebase" not in kinds
    assert mod._firebase_progress.get("running") is False
