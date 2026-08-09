"""ASC metrics warehouse — total_downloads derive from units+redownloads."""

from datetime import date

from backend.services.asc_metrics_warehouse import _series_from_scrape_facts


def test_total_downloads_derived_from_units_and_redownloads():
    facts = [
        {"metric": "units", "date": "2026-08-01", "dim": "overview", "value": 10},
        {"metric": "redownloads", "date": "2026-08-01", "dim": "overview", "value": 5},
        {"metric": "units", "date": "2026-08-02", "dim": "overview", "value": 3},
    ]
    series = _series_from_scrape_facts(
        facts,
        "total_downloads",
        start=date(2026, 8, 1),
        end=date(2026, 8, 9),
    )
    assert series == [
        {"key": "2026-08-01", "value": 15.0},
        {"key": "2026-08-02", "value": 3.0},
    ]


def test_total_downloads_prefers_direct_facts():
    facts = [
        {"metric": "units", "date": "2026-08-01", "dim": "overview", "value": 10},
        {"metric": "redownloads", "date": "2026-08-01", "dim": "overview", "value": 5},
        {"metric": "total_downloads", "date": "2026-08-01", "dim": "overview", "value": 99},
    ]
    series = _series_from_scrape_facts(
        facts,
        "total_downloads",
        start=date(2026, 8, 1),
        end=date(2026, 8, 1),
    )
    assert series == [{"key": "2026-08-01", "value": 99.0}]
