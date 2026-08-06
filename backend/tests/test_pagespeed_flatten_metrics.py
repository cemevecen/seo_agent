"""PSI metric flatten: field/lab yardımcı anahtarları kaydı bozmamalı."""

from backend.collectors.pagespeed import (
    _flatten_strategy_metrics,
    resolve_pagespeed_target_url,
)
from backend.models import Site


def test_flatten_ignores_field_and_lab_helper_keys():
    payload = {
        "performance_score": 64.0,
        "accessibility_score": 80.0,
        "best_practices_score": 90.0,
        "seo_score": 91.0,
        "lcp": 2472.0,
        "fcp": 1200.0,
        "ttfb": 600.0,
        "cls": 0.03,
        "inp": 204.0,
        "lcp_field": 2472.0,
        "fcp_field": 1200.0,
        "ttfb_field": 600.0,
        "cls_field": 0.03,
        "inp_field": 204.0,
        "lcp_lab": 4171.0,
        "fcp_lab": 2628.0,
        "ttfb_lab": 100.0,
        "cls_lab": 0.01,
        "inp_lab": 0.0,
    }
    flat = _flatten_strategy_metrics("mobile", payload)
    assert flat["pagespeed_mobile_score"] == 64.0
    assert flat["pagespeed_mobile_lcp"] == 2472.0
    assert flat["pagespeed_mobile_inp"] == 204.0
    assert "lcp_field" not in flat
    assert all(not key.endswith(("_field", "_lab")) for key in flat)


def test_sinemalar_mobile_target_matches_psi_web():
    site = Site(domain="www.sinemalar.com", display_name="Sinemalar")
    assert resolve_pagespeed_target_url(site, "mobile") == "https://www.sinemalar.com"
    assert resolve_pagespeed_target_url(site, "desktop") == "https://www.sinemalar.com"
