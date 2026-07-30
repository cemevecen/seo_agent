"""Ana sayfa Search Console özeti — site-geneli pozisyon (GSC uyumlu)."""

from unittest.mock import MagicMock

from backend.main import _home_sc_device_aggregate


def test_home_sc_uses_sitewide_7d_summary_position():
    """Top-query snapshot yerine CollectorRun date×device özeti kullanılır."""
    db = MagicMock()
    summary = {
        "current_7d_summary_by_device": {
            "DESKTOP": {"clicks": 197000, "impressions": 1960000, "position": 6.9, "ctr": 10.1},
            "MOBILE": {"clicks": 1640000, "impressions": 14700000, "position": 5.3, "ctr": 11.2},
        },
        "previous_7d_summary_by_device": {
            "DESKTOP": {"clicks": 180000, "impressions": 1790000, "position": 7.1, "ctr": 10.0},
            "MOBILE": {"clicks": 1550000, "impressions": 14700000, "position": 5.3, "ctr": 10.6},
        },
    }
    desktop = _home_sc_device_aggregate(db, 1, "DESKTOP", summary_payload=summary)
    assert desktop["pos_last_fmt"] == "6.9"
    assert desktop["pos_prev_fmt"] == "7.1"
    assert round(desktop["pos_delta"], 2) == 0.2
    assert desktop["clicks_last_fmt"] == "197K"

    mobile = _home_sc_device_aggregate(db, 1, "MOBILE", summary_payload=summary)
    assert mobile["pos_last_fmt"] == "5.3"
    assert mobile["pos_prev_fmt"] == "5.3"
    assert mobile["pos_delta"] == 0.0
