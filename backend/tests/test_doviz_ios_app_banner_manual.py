from backend.services.doviz_ios_app_banner_manual import (
    _load_daily_map,
    fetch_doviz_ios_app_banner_manual,
)


def test_daily_file_sums_to_period_total():
    daily = _load_daily_map()
    assert sum(daily.values()) == 16046
    assert min(daily.keys()) == "2025-11-20"
    assert max(daily.keys()) == "2026-06-06"


def test_manual_payload_shape():
    out = fetch_doviz_ios_app_banner_manual(
        start="2025-11-01",
        end="2026-06-07",
        top_campaigns=10,
    )
    assert out["data_source"] == "manual_ios_table"
    assert out["campaigns"] == []
    assert out["total_daily"]["dates"][0] >= "2025-11-20"
    assert len(out["total_daily"]["dates"]) == len(out["total_daily"]["values"])
    assert out["manual_period_total"] == 16046
