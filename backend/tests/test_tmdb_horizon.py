"""TMDB upcoming — ay ufku dilimleme (3/5/8/12)."""

from datetime import date
from unittest.mock import patch

import backend.services.tmdb as tmdb


def _mock_today(d: date):
    return patch.object(tmdb, "date", wraps=date, **{"today.return_value": d})


def test_horizon_end_date_twelve_months():
    with _mock_today(date(2026, 6, 17)):
        assert tmdb._horizon_end_date(12) == "2027-06-30"
        assert tmdb._horizon_end_date(3) == "2026-09-30"


def test_filter_combined_horizon_drops_far_releases():
    with _mock_today(date(2026, 6, 17)):
        raw = {
            "theatrical": [
                {"id": 1, "release_date": "2026-07-01", "release_month": "2026-07", "popularity": 50},
                {"id": 2, "release_date": "2027-08-01", "release_month": "2027-08", "popularity": 50},
            ],
            "streaming": [],
            "turkish_only": [],
            "tv_series": [],
        }
        out = tmdb._filter_combined_horizon(raw, 3)
        assert [m["id"] for m in out["theatrical"]] == [1]
        assert out["months_ahead"] == 3
        assert out["horizon_end"] == "2026-09-30"

        out12 = tmdb._filter_combined_horizon(raw, 12)
        assert sorted(m["id"] for m in out12["theatrical"]) == [1, 2]
