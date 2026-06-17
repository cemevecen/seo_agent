from backend.services.ga4_realtime_quota import (
    domain_is_light_realtime,
    is_daily_quota_exhausted_message,
    is_property_paused,
    normalize_property_id,
    pause_property,
    scheduler_profiles_for_site,
)


def test_quota_message_detection():
    assert is_daily_quota_exhausted_message(
        "429 Exhausted property tokens per day. These quota tokens will return in under 24 hours."
    )


def test_light_domain_sinemalar():
    assert domain_is_light_realtime("www.sinemalar.com")
    assert not domain_is_light_realtime("www.doviz.com")


def test_scheduler_profiles_light():
    props = {"web": "375681147", "mweb": "375681147", "android": "375681147"}
    assert scheduler_profiles_for_site("www.sinemalar.com", props) == ("web",)
    props2 = {"web": "111", "mweb": "222"}
    assert scheduler_profiles_for_site("www.sinemalar.com", props2) == ("web", "mweb")


def test_pause_property():
    pid = normalize_property_id("properties/375681147")
    assert pid == "375681147"
    pause_property(pid)
    assert is_property_paused(pid)
