from backend.services.error_monitor import (
    extract_tracking_labels,
    merge_channel_items,
    merge_labeled_items,
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


def test_extract_tracking_labels():
    labels = extract_tracking_labels("/yorum?utm_source=newsletter&utm_medium=email&fbclid=abc")
    assert "utm_source=newsletter" in labels
    assert "utm_medium=email" in labels
    assert "fbclid=abc" in labels


def test_merge_labeled_items():
    merged = merge_labeled_items([
        {"label": "mobile", "users": 10},
        {"label": "mobile", "users": 5},
    ])
    assert merged[0]["label"] == "mobile"
    assert merged[0]["users"] == 15
