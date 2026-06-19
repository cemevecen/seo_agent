"""GA4 sayfa tablosu — Search Console page scope eşlemesi."""

from backend.main import _ga4_days_to_sc_page_scopes


def test_ga4_days_to_sc_page_scopes_includes_90d():
    assert _ga4_days_to_sc_page_scopes(90) == ("current_90d_pages", "previous_90d_pages")


def test_ga4_days_to_sc_page_scopes_unknown_period():
    assert _ga4_days_to_sc_page_scopes(365) is None
