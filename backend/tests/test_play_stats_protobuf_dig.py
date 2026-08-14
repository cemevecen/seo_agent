"""Play stats protobuf — DAU değeri field '1' altında kaldığında fact üretilmeli."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "play_console_scrape", ROOT / "scripts" / "play_console_scrape.py"
)
assert _SPEC and _SPEC.loader
mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mod)


def test_parse_dau_value_in_field_one():
    body = {
        "1": [
            {
                "1": [{"2": [{"1": 51100, "4": 100}]}],
                "2": {"1": 2026, "2": 8, "3": 7},
            },
            {
                "1": [{"2": [{"2": {"1": "52340"}}]}],
                "2": {"1": 2026, "2": 8, "3": 8},
            },
            {
                "1": [{"2": [{"2": {"2": "53000"}}]}],
                "2": {"1": 2026, "2": 8, "3": 9},
            },
        ]
    }
    facts = mod._parse_stats_protobuf(body, metric_key="dau", view_id="dau", dim_hint="country")
    assert len(facts) >= 3
    by_date = {f["date"]: f["value"] for f in facts}
    assert by_date["2026-08-07"] == 51100.0
    assert by_date["2026-08-08"] == 52340.0
    assert by_date["2026-08-09"] == 53000.0


def test_parse_rating_still_prefers_field_two_over_type_flag():
    body = {
        "1": [
            {
                "1": [{"2": [{"1": 1, "2": "4.65"}]}],
                "2": {"1": 2026, "2": 8, "3": 7},
            },
            {
                "1": [{"2": [{"1": 1, "2": "4.70"}]}],
                "2": {"1": 2026, "2": 8, "3": 8},
            },
            {
                "1": [{"2": [{"1": 1, "2": "4.72"}]}],
                "2": {"1": 2026, "2": 8, "3": 9},
            },
        ]
    }
    facts = mod._parse_stats_protobuf(
        body, metric_key="rating", view_id="rating", dim_hint="overview"
    )
    assert len(facts) == 3
    assert facts[0]["value"] == 4.65


def test_fact_value_ok_allows_small_dau_mau():
    assert mod._fact_value_ok("dau_mau", 0.27, source="card", raw="0,27")
    assert mod._fact_value_ok("dau_mau", 15.0, source="card", raw="15")
    assert not mod._fact_value_ok("active_devices", 5.0, source="card", raw="5")
