"""Play scrape — süre sınırı yok + ANR/Crash öncelik sırası."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_bridge():
    path = ROOT / "scripts" / "doviz_admin_notification_bridge.py"
    saved = {k: os.environ.get(k) for k in ("PLAY_BRIDGE_TIMEOUT_SEC", "PLAY_VITALS_BRIDGE_TIMEOUT_SEC")}
    for k in saved:
        os.environ.pop(k, None)
    mod_name = "doviz_admin_notification_bridge_play_timeout_test"
    import sys

    sys.modules.pop(mod_name, None)
    try:
        spec = importlib.util.spec_from_file_location(mod_name, path)
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)
        return mod
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _load_play_scrape():
    path = ROOT / "scripts" / "play_console_scrape.py"
    mod_name = "play_console_scrape_order_test"
    import sys

    sys.modules.pop(mod_name, None)
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_play_bridge_default_timeout_unlimited():
    mod = _load_bridge()
    # run_play_bridge_once docstring / defaults — timeout_default=0
    src = (ROOT / "scripts" / "doviz_admin_notification_bridge.py").read_text(encoding="utf-8")
    assert 'timeout_default=0' in src
    assert "süre sınırı yok" in src or "timeout_default=0" in src
    assert mod is not None


def test_statistics_views_anr_crash_first():
    mod = _load_play_scrape()
    ordered = mod._ordered_statistics_views()
    ids = [str(v.get("id")) for v in ordered[:4]]
    assert ids[0] == "anrs_date"
    assert ids[1] == "crashes_date"
    assert "anrs_os" in ids or ids[2].startswith("anrs")
