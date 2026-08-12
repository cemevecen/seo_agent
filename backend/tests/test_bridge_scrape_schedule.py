"""Mac bridge — tarayıcı scrape slotları birbirine çok yakın olmasın."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BRIDGE = ROOT / "scripts" / "doviz_admin_notification_bridge.py"

_SCHEDULE_ENV_KEYS = (
    "VIRGUL_BRIDGE_MINUTE",
    "PLAY_CONSOLE_BRIDGE_MINUTE",
    "ASC_CONSOLE_BRIDGE_MINUTE",
    "FIREBASE_CONSOLE_BRIDGE_MINUTE",
    "GSC_LINKS_BRIDGE_MINUTE",
    "REVENUE_TARGETS_BRIDGE_MINUTE",
    "ADMANAGER_POLICY_BRIDGE_MINUTE",
    "PAGESPEED_BRIDGE_MINUTE",
    "SINEMALAR_NOADS_BRIDGE_MINUTE",
    "SEO_AUDIT_BRIDGE_MINUTE",
    "GSC_CWV_BRIDGE_MINUTE",
    "MARKET_TARAMA_BRIDGE_MINUTE",
    "PM_LAB_SERP_BATCH_MINUTES",
    "BRIDGE_SCRAPE_MIN_GAP_SEC",
)


def _load_bridge():
    saved = {k: os.environ.get(k) for k in _SCHEDULE_ENV_KEYS}
    for k in _SCHEDULE_ENV_KEYS:
        os.environ[k] = ""
    import sys

    mod_name = "doviz_admin_notification_bridge_schedule_test"
    sys.modules.pop(mod_name, None)
    try:
        spec = importlib.util.spec_from_file_location(mod_name, BRIDGE)
        mod = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(mod)
        return mod
    finally:
        for k in _SCHEDULE_ENV_KEYS:
            if saved.get(k) is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = saved[k]  # type: ignore[assignment]


def _all_browser_events(mod) -> list[tuple[int, str]]:
    """(hour, minute, name) — aynı saat içindeki çakışmaları test etmek için."""
    events: list[tuple[int, int, str]] = []
    for name, hours, minute in mod.browser_scrape_slot_defs():
        for h in hours:
            events.append((int(h), int(minute), name))
    for h in mod.SERP_CYCLE_HOURS:
        base_h = int(h)
        mins = mod.SERP_BATCH_MINUTES
        events.append((base_h, int(mins[0]), "serp_batch_0"))
        nh = (base_h + 1) % 24
        for i, m in enumerate(mins[1:], start=1):
            events.append((nh, int(m), f"serp_batch_{i}"))
    return events


def test_browser_scrape_slots_at_least_3_min_apart():
    """Planlı slotlar (rakip fiyat hariç — o global kuyrukla aralıklanır)."""
    mod = _load_bridge()
    events = _all_browser_events(mod)
    min_gap = max(3, int(mod.BRIDGE_SCRAPE_MIN_GAP_SEC) // 60)
    for hour in range(24):
        slots = sorted((m, name) for h, m, name in events if h == hour)
        for i in range(len(slots) - 1):
            if slots[i][1] == slots[i + 1][1]:
                continue
            gap = slots[i + 1][0] - slots[i][0]
            if gap < min_gap:
                assert False, (
                    f"Saat {hour:02d}:xx — scrape slotları {min_gap} dk'dan yakın: "
                    f"{slots[i][1]} :{slots[i][0]:02d} → {slots[i + 1][1]} :{slots[i + 1][0]:02d} ({gap} dk)"
                )


def test_serp_batch_minutes_spaced_15_min():
    mod = _load_bridge()
    assert mod.SERP_BATCH_MINUTES == (50, 5, 20, 35)
    gaps = []
    prev = mod.SERP_BATCH_MINUTES[0]
    for m in mod.SERP_BATCH_MINUTES[1:]:
        if m > prev:
            gaps.append(m - prev)
        else:
            gaps.append((60 - prev) + m)
        prev = m
    assert all(g >= 15 for g in gaps)
