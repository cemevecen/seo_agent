from backend.services.realtime_traffic_swing import (
    evaluate_traffic_swing,
    swing_profiles_for_domain,
    swing_subject,
)


def test_swing_profiles():
    assert swing_profiles_for_domain("www.doviz.com") == ("web", "mweb", "android", "ios")
    assert swing_profiles_for_domain("sinemalar.com") == ("web", "mweb")
    assert swing_profiles_for_domain("example.com") is None


def test_swing_needs_70_percent_and_a_previous_window():
    assert evaluate_traffic_swing(100, 169) is None
    assert evaluate_traffic_swing(100, 170)["direction"] == "up"
    assert evaluate_traffic_swing(100, 30)["direction"] == "down"
    assert evaluate_traffic_swing(0, 400) is None
    assert evaluate_traffic_swing(8, 16) is None


def test_subject_is_area_only():
    subj = swing_subject("www.doviz.com", "android", 72.4)
    assert subj == "doviz android +72%"
    assert "@" not in subj
