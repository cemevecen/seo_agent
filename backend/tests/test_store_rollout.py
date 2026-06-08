from backend.services.store_rollout import _ios_phased_pct


def test_ios_phased_day_mapping():
    pct, mode = _ios_phased_pct("ACTIVE", 4)
    assert pct == 10.0
    assert mode == "phased_active"


def test_ios_phased_complete():
    pct, mode = _ios_phased_pct("COMPLETE", 7)
    assert pct == 100.0
    assert mode == "full_release"
