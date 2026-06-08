"""ASC Analytics Reports yardımcıları."""

from datetime import date

from backend.services import asc_analytics as aa


def test_pick_column_flexible():
    headers = ["Date", "Impressions Unique Device", "Product Page Views"]
    assert aa._pick_column(headers, "Impressions") == "Impressions Unique Device"
    assert aa._pick_column(headers, "Product Page Views") == "Product Page Views"


def test_aggregate_report_rows_sums_by_date():
    rows = [
        {"Date": "2026-06-01", "Impressions": "100", "Product Page Views": "10"},
        {"Date": "2026-06-01", "Impressions": "50", "Product Page Views": "5"},
        {"Date": "2026-06-02", "Impressions": "200", "Product Page Views": "20"},
    ]
    metrics = {
        "impressions": ("Impressions",),
        "product_page_views": ("Product Page Views",),
    }
    daily, totals = aa._aggregate_report_rows(
        rows,
        start=date(2026, 6, 1),
        end=date(2026, 6, 3),
        country="all",
        metrics=metrics,
    )
    assert daily["2026-06-01"]["impressions"] == 150
    assert totals["impressions"] == 350
