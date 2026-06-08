from datetime import date

from backend.services.doviz_ios_app_banner_manual import (
    _expand_buckets_to_daily,
    _MANUAL_MONTHS,
    _IOS_BUCKET_TOTALS,
    fetch_doviz_ios_app_banner_manual,
)


def test_expand_buckets_sums_to_month_totals():
    daily = _expand_buckets_to_daily(_MANUAL_MONTHS, _IOS_BUCKET_TOTALS)
    assert sum(daily.values()) == sum(sum(row) for row in _IOS_BUCKET_TOTALS)


def test_manual_payload_shape():
    out = fetch_doviz_ios_app_banner_manual(
        start="2025-11-01",
        end="2026-06-07",
        top_campaigns=5,
    )
    assert out["data_source"] == "manual_ios_table"
    assert out["total_daily"]["dates"][0] >= "2025-11-01"
    assert out["campaigns"][0]["campaign"] == "ios.d"
    assert len(out["total_daily"]["dates"]) == len(out["total_daily"]["values"])
