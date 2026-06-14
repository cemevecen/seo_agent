"""Search Console ek rapor yardimcilari testleri."""

from backend.services.search_console_reports import (
    SC_VIEW_SPECS,
    _merge_rows_by_key,
    _normalize_dimension_rows,
    sc_extra_views_for_nav,
    sc_view_groups,
    sc_views_for_nav,
)


def test_sc_view_specs_has_performance_and_extras():
    assert "performance" in SC_VIEW_SPECS
    assert "countries" not in SC_VIEW_SPECS
    assert SC_VIEW_SPECS["discover"].get("position_supported") is False
    assert SC_VIEW_SPECS["news"].get("position_supported") is False
    slugs = {v["slug"] for v in SC_VIEW_SPECS.values()}
    for expected in ("discover", "news", "appearance", "page-query", "url-inspection", "sitemaps"):
        assert expected in slugs


def test_sc_view_groups_order():
    groups = sc_view_groups()
    assert groups == ["Performans", "Analiz", "İndeks"]


def test_sc_views_for_nav_sorted():
    items = sc_views_for_nav()
    orders = [int(i["order"]) for i in items]
    assert orders == sorted(orders)
    assert items[0]["slug"] == "performance"
    assert items[-1]["slug"] == "sitemaps"
    assert len(sc_extra_views_for_nav()) == len(items) - 1


def test_normalize_and_merge_rows():
    raw = [
        {"keys": ["a", "q1"], "clicks": 2, "impressions": 10, "ctr": 0.2, "position": 3.0},
        {"keys": ["a", "q1"], "clicks": 1, "impressions": 5, "ctr": 0.2, "position": 5.0},
    ]
    rows = _normalize_dimension_rows(raw, ["page", "query"])
    merged = _merge_rows_by_key(rows, ["page", "query"])
    assert len(merged) == 1
    assert merged[0]["clicks"] == 3
    assert merged[0]["impressions"] == 15
    assert merged[0]["page"] == "a"
    assert merged[0]["query"] == "q1"
