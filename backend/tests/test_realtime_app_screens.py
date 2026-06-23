"""Mobil (iOS/Android) realtime ekran listesi boş dönme senaryoları."""

from unittest.mock import patch

from backend.services import ga4_realtime as rt


def _page(name: str, au: float = 0, pv: float = 0) -> dict:
    return {"page": name, "activeUsers": au, "screenPageViews": pv}


def test_pick_best_skips_empty_dimension_results():
    calls: list[str] = []

    def fake_fetch(_pid, *, dimension, **kwargs):
        calls.append(dimension)
        if dimension == "unifiedScreenName":
            return {"pages": []}
        if dimension == "screenName":
            return {"pages": [_page("HomeActivity", au=3, pv=10)]}
        return {"pages": []}

    with patch.object(rt, "fetch_realtime_top_pages", side_effect=fake_fetch):
        res = rt.fetch_realtime_top_pages_pick_best_screen_dimension(
            "123",
            window_minutes=15,
            limit=6,
            sort_by="activeUsers",
            client=object(),
        )

    assert res["breakdown"] == "screenName"
    assert res["pages"][0]["page"] == "HomeActivity"
    assert "unifiedScreenName" in calls


def test_with_app_fallback_rescues_empty_pick_best():
    empty = {"pages": [], "comparison_enabled": False}
    rescued = {
        "pages": [_page("MainActivity", au=2, pv=5)],
        "comparison_enabled": False,
        "breakdown": "screenClass",
        "screen_rescue": True,
    }

    with patch.object(
        rt,
        "fetch_realtime_top_pages_pick_best_screen_dimension",
        return_value=empty,
    ), patch.object(rt, "_rescue_app_empty_screen_pages", return_value=rescued) as rescue:
        res = rt.fetch_realtime_top_pages_with_app_fallback(
            "123",
            profile="android",
            window_minutes=15,
            limit=6,
            sort_by="activeUsers",
            client=object(),
        )

    rescue.assert_called_once()
    assert res["pages"][0]["page"] == "MainActivity"
    assert res.get("screen_rescue") is True


def test_compare_previous_does_not_wipe_pages_with_empty_compare():
    base_pages = [_page("Home", au=4, pv=9)]
    base = {"pages": base_pages, "comparison_enabled": False, "breakdown": "screenName"}
    empty_compare = {"pages": [], "comparison_enabled": True}

    with patch.object(
        rt,
        "fetch_realtime_top_pages_pick_best_screen_dimension",
        return_value=base,
    ), patch.object(
        rt,
        "fetch_realtime_top_pages",
        return_value=empty_compare,
    ) as fetch_pages:
        res = rt.fetch_realtime_top_pages_with_app_fallback(
            "123",
            profile="android",
            window_minutes=15,
            limit=6,
            compare_previous=True,
            client=object(),
        )

    fetch_pages.assert_called_once()
    assert res["pages"] == base_pages
