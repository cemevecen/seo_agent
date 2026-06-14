"""Realtime sayfa/haber listesi zaman penceresi (15m / 1h / 24h)."""

from backend.services.ga4_realtime import parse_realtime_list_range


def test_parse_realtime_list_range_15m_uses_ga4():
    mode, minutes, rk = parse_realtime_list_range("15m")
    assert mode == "ga4"
    assert minutes == 15
    assert rk == "15m"


def test_parse_realtime_list_range_1h_uses_snapshots():
    mode, minutes, rk = parse_realtime_list_range("1h")
    assert mode == "snapshots"
    assert minutes == 60
    assert rk == "1h"


def test_parse_realtime_list_range_24h_uses_snapshots():
    mode, minutes, rk = parse_realtime_list_range("24h")
    assert mode == "snapshots"
    assert minutes == 24 * 60
    assert rk == "24h"


def test_parse_realtime_list_range_legacy_window():
    mode, minutes, rk = parse_realtime_list_range(None, window=30)
    assert mode == "ga4"
    assert minutes == 30
    assert rk == "15m"
