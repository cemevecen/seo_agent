"""doviz/sinemalar: speed UI yalnızca pagespeed.web.dev scrape — CrUX/PSI API yok."""

from backend.main import (
    _is_pagespeed_scrape_primary_domain,
    _schedule_data_explorer_backfill_if_needed,
    _schedule_crux_refresh_if_stale,
)


def test_scrape_primary_domains():
    assert _is_pagespeed_scrape_primary_domain("www.doviz.com")
    assert _is_pagespeed_scrape_primary_domain("https://sinemalar.com/foo")
    assert not _is_pagespeed_scrape_primary_domain("example.com")


def test_backfill_skipped_for_scrape_primary():
    assert (
        _schedule_data_explorer_backfill_if_needed(
            1, "www.doviz.com", need_pagespeed=True, need_crux=True
        )
        is False
    )
    assert _schedule_crux_refresh_if_stale(1, "www.sinemalar.com") is False
