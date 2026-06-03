from backend.services.error_monitor import (
    merge_channel_items,
    merge_referrer_items,
    normalize_channels,
    normalize_referrers,
)


def test_normalize_legacy_string_referrers():
    out = normalize_referrers(["https://www.google.com/", ""])
    assert len(out) == 1
    assert out[0]["ref"].startswith("https://")
    assert out[0]["users"] == 0


def test_merge_referrer_items_sums_users():
    items = [
        {"ref": "https://a.com", "users": 10, "internal": False},
        {"ref": "https://a.com", "users": 5, "internal": False},
        {"ref": "https://b.com", "users": 3, "internal": True},
    ]
    merged = merge_referrer_items(items)
    assert merged[0]["ref"] == "https://a.com"
    assert merged[0]["users"] == 15
    assert merged[1]["users"] == 3


def test_normalize_and_merge_channels():
    raw = [{"channel": "Direct", "users": 40}, "Organic Search"]
    merged = merge_channel_items(normalize_channels(raw))
    assert merged[0]["channel"] == "Direct"
    assert merged[0]["users"] == 40
