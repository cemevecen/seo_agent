"""Realtime alarm e-posta gürültü filtresi."""

from unittest.mock import patch

from backend.services.ga4_realtime import alarm_worthy_for_email, filter_alarms_for_email


def test_drop_low_volume_not_emailed():
    low = {
        "rule_id": "page_traffic_drop",
        "previous_users": 24,
        "current_users": 3,
        "change_pct": -87.5,
    }
    assert not alarm_worthy_for_email(low)


def test_drop_high_volume_emailed():
    high = {
        "rule_id": "page_traffic_drop",
        "previous_users": 55,
        "current_users": 20,
        "change_pct": -63.6,
    }
    assert alarm_worthy_for_email(high)


def test_spike_moderate_volume_emailed():
    spike = {
        "rule_id": "news_traffic_spike",
        "previous_users": 12,
        "current_users": 35,
        "change_pct": 191.0,
    }
    assert alarm_worthy_for_email(spike)


def test_filter_alarms_for_email():
    alarms = [
        {"rule_id": "page_traffic_drop", "previous_users": 20, "current_users": 5, "change_pct": -75},
        {"rule_id": "page_traffic_drop", "previous_users": 60, "current_users": 25, "change_pct": -58},
    ]
    out = filter_alarms_for_email(alarms)
    assert len(out) == 1
    assert out[0]["previous_users"] == 60
