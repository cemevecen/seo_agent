"""GSC Top target pages CSV parse."""

from backend.services.backlink_csv import (
    GSC_TARGET_AGG_ANCHOR_PREFIX,
    _parse_gsc_agg_anchor,
    parse_csv_text,
)


def test_parse_top_target_pages_csv():
    csv = """Target page,Incoming links,Linking sites
https://www.doviz.com/,447667,6866
https://kur.doviz.com/serbest-piyasa/amerikan-dolari,881,73
"""
    rows = parse_csv_text(csv, report_type="top_target_pages")
    assert len(rows) == 2
    assert rows[0]["target_url"] == "https://www.doviz.com/"
    assert rows[0]["incoming_links"] == 447667
    assert rows[0]["linking_sites"] == 6866
    assert rows[0]["anchor_text"].startswith(GSC_TARGET_AGG_ANCHOR_PREFIX)
    assert rows[1]["incoming_links"] == 881
    assert rows[1]["linking_sites"] == 73


def test_parse_gsc_agg_anchor():
    assert _parse_gsc_agg_anchor("gsc_agg:447667:6866") == (447667, 6866)
